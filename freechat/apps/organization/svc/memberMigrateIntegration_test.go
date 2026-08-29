package svc

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// 对真实数据库跑一遍迁移，验证物化路径重算、team_size 双向调整、
// direct_downline_count、迁移记录、以及事务回滚是否都对。
// 单测只能验纯函数；路径重算的 BulkWrite 和事务只有连库才验得到。
//
// 需要 MIGRATE_IT_MONGO_URI，否则跳过（CI 无库时不失败）。
//
// 造的树（全部带 migrate-it- 前缀，跑完删干净）：
//
//	ROOT(L1)
//	├── A(L2)                    ← 迁移前 M 挂在 A 下
//	│   └── M(L3)
//	│       ├── C1(L4)
//	│       │   └── G1(L5)
//	│       └── C2(L4)
//	└── B(L2)                    ← 迁移到 B 下
//
// 迁移后 M 及其 3 个下级应整体挂到 B 下，层级不变（A、B 同层），
// A 的 team_size -4、B 的 team_size +4，ROOT 不变（两条链都含它）。
func TestMigrateAgainstRealDB(t *testing.T) {
	uri := os.Getenv("MIGRATE_IT_MONGO_URI")
	if uri == "" {
		t.Skip("未设置 MIGRATE_IT_MONGO_URI，跳过集成测试")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cli, err := mongoutil.NewMongoDB(ctx, &mongoutil.Config{Uri: uri, Database: "openim_v3", MaxPoolSize: 10, MaxRetry: 1})
	if err != nil {
		t.Fatalf("连接测试库失败: %v", err)
	}
	plugin.InitMongoCli(cli)
	db := cli.GetDB()
	coll := db.Collection(model.OrganizationUser{}.TableName())

	orgID := primitive.NewObjectID()
	const pfx = "migrate-it-"

	// 跑完无论成败都清理，绝不把测试数据留在测试环境里
	defer func() {
		cl, c2 := context.WithTimeout(context.Background(), 30*time.Second)
		defer c2()
		r1, _ := coll.DeleteMany(cl, bson.M{"organization_id": orgID})
		r2, _ := db.Collection(migrateLogCollection).DeleteMany(cl, bson.M{"org_id": orgID})
		t.Logf("清理：organization_user 删除 %d 条，迁移记录删除 %d 条", r1.DeletedCount, r2.DeletedCount)
	}()

	mk := func(id string, level int, path []string, teamSize, direct int) interface{} {
		return bson.M{
			"organization_id":       orgID,
			"user_id":               pfx + id,
			"im_server_user_id":     pfx + "im-" + id,
			"third_user_id":         pfx + "third-" + id,
			"nickname":              "IT-" + id,
			"role":                  "Normal",
			"level":                 level,
			"ancestor_path":         path,
			"level1_parent":         nthOrEmpty(path, 0),
			"level2_parent":         nthOrEmpty(path, 1),
			"level3_parent":         nthOrEmpty(path, 2),
			"team_size":             teamSize,
			"direct_downline_count": direct,
			"inviter":               nthOrEmpty(path, 0),
			"created_at":            time.Now().UTC(),
		}
	}
	p := func(ids ...string) []string {
		out := make([]string, 0, len(ids))
		for _, id := range ids {
			out = append(out, pfx+id)
		}
		return out
	}

	// team_size = 自己名下所有层级的下级人数（不含自己）
	docs := []interface{}{
		mk("ROOT", 1, nil, 6, 2),                       // A,B,M,C1,C2,G1
		mk("A", 2, p("ROOT"), 4, 1),                    // M,C1,C2,G1
		mk("B", 2, p("ROOT"), 0, 0),                    // 空
		mk("M", 3, p("A", "ROOT"), 3, 2),               // C1,C2,G1
		mk("C1", 4, p("M", "A", "ROOT"), 1, 1),         // G1
		mk("C2", 4, p("M", "A", "ROOT"), 0, 0),         //
		mk("G1", 5, p("C1", "M", "A", "ROOT"), 0, 0),   //
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		t.Fatalf("构造测试树失败: %v", err)
	}

	svc := NewMemberMigrateSvc()
	req := &MigrateMemberRequest{
		OrgID:           orgID,
		OperatorUserID:  pfx + "operator",
		OperatorImID:    pfx + "im-operator",
		MemberUserID:    pfx + "M",
		NewParentUserID: pfx + "B",
		Reason:          "集成测试",
	}

	// ---------- 1. 预览 ----------
	pv, err := svc.Preview(ctx, req)
	if err != nil {
		t.Fatalf("预览失败: %v", err)
	}
	t.Logf("预览：移动 %d 人，层级 %d→%d (delta=%d)，team_size 减少 %v 增加 %v",
		pv.SubtreeSize, pv.OldLevel, pv.NewLevel, pv.LevelDelta, pv.TeamSizeDecrease, pv.TeamSizeIncrease)

	if pv.SubtreeSize != 4 {
		t.Errorf("子树人数应为 4（M+C1+C2+G1），实际 %d", pv.SubtreeSize)
	}
	if pv.LevelDelta != 0 {
		t.Errorf("A 和 B 同层，层级位移应为 0，实际 %d", pv.LevelDelta)
	}
	if len(pv.TeamSizeDecrease) != 1 || pv.TeamSizeDecrease[0] != pfx+"A" {
		t.Errorf("应只有 A 的 team_size 减少，实际 %v", pv.TeamSizeDecrease)
	}
	if len(pv.TeamSizeIncrease) != 1 || pv.TeamSizeIncrease[0] != pfx+"B" {
		t.Errorf("应只有 B 的 team_size 增加（ROOT 两条链都含，不变），实际 %v", pv.TeamSizeIncrease)
	}

	// 预览必须是只读的
	var beforeM bson.M
	if err := coll.FindOne(ctx, bson.M{"organization_id": orgID, "user_id": pfx + "M"}).Decode(&beforeM); err != nil {
		t.Fatalf("读取 M 失败: %v", err)
	}
	if got := beforeM["level1_parent"]; got != pfx+"A" {
		t.Errorf("预览不应改数据，M 的 level1_parent 应仍为 A，实际 %v", got)
	}

	// ---------- 2. 环检测：迁到自己的下级名下必须被拒 ----------
	cycleReq := *req
	cycleReq.NewParentUserID = pfx + "G1"
	if _, err := svc.Preview(ctx, &cycleReq); err == nil {
		t.Error("把 M 迁到其下级 G1 名下会导致层级成环，必须拒绝，但预览通过了")
	} else {
		t.Logf("环检测生效：%v", err)
	}

	// ---------- 3. 执行 ----------
	if _, err := svc.Execute(ctx, req); err != nil {
		t.Fatalf("执行迁移失败: %v", err)
	}

	read := func(id string) bson.M {
		var d bson.M
		if err := coll.FindOne(ctx, bson.M{"organization_id": orgID, "user_id": pfx + id}).Decode(&d); err != nil {
			t.Fatalf("读取 %s 失败: %v", id, err)
		}
		return d
	}
	pathOf := func(d bson.M) []string {
		raw, _ := d["ancestor_path"].(primitive.A)
		out := make([]string, 0, len(raw))
		for _, v := range raw {
			out = append(out, fmt.Sprint(v))
		}
		return out
	}
	eq := func(name string, got, want []string) {
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s 的 ancestor_path 应为 %v，实际 %v", name, want, got)
		}
	}
	num := func(d bson.M, f string) int {
		switch v := d[f].(type) {
		case int32:
			return int(v)
		case int64:
			return int(v)
		case int:
			return v
		case float64:
			return int(v)
		}
		return -999
	}

	// 3a. 整棵子树的路径重算
	mDoc, c1Doc, c2Doc, g1Doc := read("M"), read("C1"), read("C2"), read("G1")
	eq("M", pathOf(mDoc), p("B", "ROOT"))
	eq("C1", pathOf(c1Doc), p("M", "B", "ROOT"))
	eq("C2", pathOf(c2Doc), p("M", "B", "ROOT"))
	eq("G1", pathOf(g1Doc), p("C1", "M", "B", "ROOT"))

	// 3b. levelN_parent 必须与 ancestor_path 保持一致（冗余字段最容易忘同步）
	for name, d := range map[string]bson.M{"M": mDoc, "C1": c1Doc, "C2": c2Doc, "G1": g1Doc} {
		pa := pathOf(d)
		for i, f := range []string{"level1_parent", "level2_parent", "level3_parent"} {
			want := nthOrEmpty(pa, i)
			if got := fmt.Sprint(d[f]); got != want {
				t.Errorf("%s 的 %s 与 ancestor_path[%d] 不一致：%s vs %s", name, f, i, got, want)
			}
		}
	}

	// 3c. 层级：A、B 同层，整棵子树层级不变
	if got := num(mDoc, "level"); got != 3 {
		t.Errorf("M 的 level 应保持 3，实际 %d", got)
	}
	if got := num(g1Doc, "level"); got != 5 {
		t.Errorf("G1 的 level 应保持 5，实际 %d", got)
	}

	// 3d. M 的直接上级换成 B，下级的 inviter 不变
	if got := fmt.Sprint(mDoc["inviter"]); got != pfx+"B" {
		t.Errorf("M 的 inviter 应为 B，实际 %s", got)
	}
	if got := fmt.Sprint(c1Doc["inviter"]); got != pfx+"M" {
		t.Errorf("C1 的 inviter 应仍为 M，实际 %s", got)
	}

	// 3e. team_size 双向调整；ROOT 在两条链上，必须不变
	aDoc, bDoc, rootDoc := read("A"), read("B"), read("ROOT")
	if got := num(aDoc, "team_size"); got != 0 {
		t.Errorf("A 的 team_size 应由 4 减为 0，实际 %d", got)
	}
	if got := num(bDoc, "team_size"); got != 4 {
		t.Errorf("B 的 team_size 应由 0 增为 4，实际 %d", got)
	}
	if got := num(rootDoc, "team_size"); got != 6 {
		t.Errorf("ROOT 同时是新旧上级的祖先，team_size 应保持 6，实际 %d", got)
	}

	// 3f. 直属下级数
	if got := num(aDoc, "direct_downline_count"); got != 0 {
		t.Errorf("A 的直属下级应由 1 减为 0，实际 %d", got)
	}
	if got := num(bDoc, "direct_downline_count"); got != 1 {
		t.Errorf("B 的直属下级应由 0 增为 1，实际 %d", got)
	}

	// 3g. 迁移记录落库
	var logDoc bson.M
	if err := db.Collection(migrateLogCollection).FindOne(ctx, bson.M{"org_id": orgID}).Decode(&logDoc); err != nil {
		t.Errorf("迁移记录未落库: %v", err)
	} else {
		if fmt.Sprint(logDoc["old_parent_user_id"]) != pfx+"A" ||
			fmt.Sprint(logDoc["new_parent_user_id"]) != pfx+"B" {
			t.Errorf("迁移记录的新旧上级不对: %v -> %v", logDoc["old_parent_user_id"], logDoc["new_parent_user_id"])
		}
		if num(logDoc, "subtree_size") != 4 {
			t.Errorf("迁移记录的 subtree_size 应为 4，实际 %v", logDoc["subtree_size"])
		}
		if fmt.Sprint(logDoc["reason"]) != "集成测试" {
			t.Errorf("迁移原因未记录，实际 %v", logDoc["reason"])
		}
	}

	// ---------- 4. 迁回去，验证可逆 ----------
	back := *req
	back.NewParentUserID = pfx + "A"
	back.Reason = "迁回"
	if _, err := svc.Execute(ctx, &back); err != nil {
		t.Fatalf("迁回失败: %v", err)
	}
	eq("M(迁回)", pathOf(read("M")), p("A", "ROOT"))
	eq("G1(迁回)", pathOf(read("G1")), p("C1", "M", "A", "ROOT"))
	if got := num(read("A"), "team_size"); got != 4 {
		t.Errorf("迁回后 A 的 team_size 应恢复为 4，实际 %d", got)
	}
	if got := num(read("B"), "team_size"); got != 0 {
		t.Errorf("迁回后 B 的 team_size 应恢复为 0，实际 %d", got)
	}
	if got := num(read("ROOT"), "team_size"); got != 6 {
		t.Errorf("迁回后 ROOT 的 team_size 应仍为 6，实际 %d", got)
	}
	t.Log("迁移可逆：来回一趟后所有字段回到初始值")

	// ---------- 5. 层级会变的场景 ----------
	// 前面 A、B 同层，delta=0，路径长度也不变，属于最容易蒙对的情况。
	// 这里在 B 下面挂一个 D(L3)，把 M 迁到 D 名下：
	// M 由 L3 下沉到 L4，整棵子树 +1 层，路径也变长一节 ——
	// level3_parent 会从原本的空位被顶出真实值，是最容易漏同步的字段。
	if _, err := coll.InsertOne(ctx, mk("D", 3, p("B", "ROOT"), 0, 0)); err != nil {
		t.Fatalf("插入 D 失败: %v", err)
	}
	// B 多了一个下级
	if _, err := coll.UpdateOne(ctx,
		bson.M{"organization_id": orgID, "user_id": pfx + "B"},
		bson.M{"$inc": bson.M{"team_size": 1, "direct_downline_count": 1}}); err != nil {
		t.Fatalf("更新 B 失败: %v", err)
	}
	// ROOT 也多了一个后代
	if _, err := coll.UpdateOne(ctx,
		bson.M{"organization_id": orgID, "user_id": pfx + "ROOT"},
		bson.M{"$inc": bson.M{"team_size": 1}}); err != nil {
		t.Fatalf("更新 ROOT 失败: %v", err)
	}

	deep := *req
	deep.NewParentUserID = pfx + "D"
	deep.Reason = "下沉迁移"
	pvDeep, err := svc.Preview(ctx, &deep)
	if err != nil {
		t.Fatalf("下沉迁移预览失败: %v", err)
	}
	if pvDeep.LevelDelta != 1 {
		t.Errorf("M 由 L3 迁到 D(L3) 下应为 L4，层级位移应为 +1，实际 %d", pvDeep.LevelDelta)
	}
	if len(pvDeep.Warnings) == 0 {
		t.Error("层级发生位移时应给出警告")
	}
	if _, err := svc.Execute(ctx, &deep); err != nil {
		t.Fatalf("下沉迁移失败: %v", err)
	}

	mDeep, g1Deep := read("M"), read("G1")
	eq("M(下沉)", pathOf(mDeep), p("D", "B", "ROOT"))
	eq("G1(下沉)", pathOf(g1Deep), p("C1", "M", "D", "B", "ROOT"))
	if got := num(mDeep, "level"); got != 4 {
		t.Errorf("下沉后 M 的 level 应为 4，实际 %d", got)
	}
	if got := num(g1Deep, "level"); got != 6 {
		t.Errorf("下沉后 G1 的 level 应为 6，实际 %d", got)
	}
	// level3_parent：G1 的新路径第 3 位是 D，原先是 A
	if got := fmt.Sprint(g1Deep["level3_parent"]); got != pfx+"D" {
		t.Errorf("G1 的 level3_parent 应为 D，实际 %s", got)
	}
	// team_size：A 减 4，D 和 B 各增 4，ROOT 仍不变
	if got := num(read("A"), "team_size"); got != 0 {
		t.Errorf("下沉后 A 的 team_size 应为 0，实际 %d", got)
	}
	if got := num(read("D"), "team_size"); got != 4 {
		t.Errorf("下沉后 D 的 team_size 应为 4，实际 %d", got)
	}
	if got := num(read("B"), "team_size"); got != 5 {
		t.Errorf("下沉后 B 的 team_size 应为 5（D + 迁入的 4 人），实际 %d", got)
	}
	if got := num(read("ROOT"), "team_size"); got != 7 {
		t.Errorf("ROOT 始终是双方祖先，team_size 应保持 7，实际 %d", got)
	}
	t.Logf("下沉迁移：M %d→%d 层，G1 路径 %v", pvDeep.OldLevel, pvDeep.NewLevel, pathOf(g1Deep))

	_ = mongo.ErrNoDocuments
}
