package svc

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// 对真实数据库跑一遍后台调节余额，验证金额精度、幂等、余额不足回滚、
// 账单流水与操作日志是否都对。
//
// 【安全约束】全程只操作本测试自己创建的合成钱包（owner_id 带 adjust-it- 前缀），
// 不碰任何真实用户的钱；跑完无论成败都删除。
//
// 需要 ADJUST_IT_MONGO_URI，否则跳过。
func TestAdjustBalanceAgainstRealDB(t *testing.T) {
	uri := os.Getenv("ADJUST_IT_MONGO_URI")
	if uri == "" {
		t.Skip("未设置 ADJUST_IT_MONGO_URI，跳过集成测试")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cli, err := mongoutil.NewMongoDB(ctx, &mongoutil.Config{
		Uri: uri, Database: "openim_v3", MaxPoolSize: 10, MaxRetry: 1,
	})
	if err != nil {
		t.Fatalf("连接测试库失败: %v", err)
	}
	plugin.InitMongoCli(cli)
	db := cli.GetDB()

	const pfx = "adjust-it-"
	orgID := primitive.NewObjectID()
	currencyID := primitive.NewObjectID()
	walletID := primitive.NewObjectID()
	targetUserID := pfx + "target"

	orgUserColl := db.Collection(orgModel.OrganizationUser{}.TableName())
	infoColl := db.Collection(walletModel.WalletInfo{}.TableName())
	balColl := db.Collection(walletModel.WalletBalance{}.TableName())

	defer func() {
		cl, c2 := context.WithTimeout(context.Background(), 30*time.Second)
		defer c2()
		n1, _ := orgUserColl.DeleteMany(cl, bson.M{"organization_id": orgID})
		n2, _ := infoColl.DeleteMany(cl, bson.M{"_id": walletID})
		n3, _ := balColl.DeleteMany(cl, bson.M{"wallet_id": walletID})
		n4, _ := db.Collection(adjustLogCollection).DeleteMany(cl, bson.M{"org_id": orgID})
		n5, _ := db.Collection("wallet_transaction_record").DeleteMany(cl, bson.M{"wallet_id": walletID})
		t.Logf("清理：org_user %d，wallet_info %d，wallet_balance %d，调节记录 %d，账单流水 %d",
			n1.DeletedCount, n2.DeletedCount, n3.DeletedCount, n4.DeletedCount, n5.DeletedCount)
	}()

	dec := func(s string) primitive.Decimal128 { d, _ := primitive.ParseDecimal128(s); return d }

	// 合成组织成员 + 钱包 + 余额（初始 1000）
	if _, err := orgUserColl.InsertOne(ctx, bson.M{
		"organization_id": orgID, "user_id": targetUserID,
		"im_server_user_id": pfx + "im-target", "third_user_id": pfx + "third-target",
		"nickname": "IT-target", "role": "Normal", "level": 1,
		"created_at": time.Now().UTC(),
	}); err != nil {
		t.Fatalf("造组织成员失败: %v", err)
	}
	if _, err := infoColl.InsertOne(ctx, bson.M{
		"_id": walletID, "owner_id": targetUserID,
		"owner_type": string(walletModel.WalletInfoOwnerTypeOrdinary),
		"pay_pwd": "", "compensation_balance": dec("0"),
		"created_at": time.Now().UTC(), "updated_at": time.Now().UTC(),
	}); err != nil {
		t.Fatalf("造钱包失败: %v", err)
	}
	if _, err := balColl.InsertOne(ctx, bson.M{
		"wallet_id": walletID, "currency_id": currencyID,
		"available_balance": dec("1000"), "red_packet_frozen_balance": dec("0"),
		"transfer_frozen_balance": dec("0"), "compensation_balance": dec("0"),
		"created_at": time.Now().UTC(), "updated_at": time.Now().UTC(),
	}); err != nil {
		t.Fatalf("造余额失败: %v", err)
	}

	svc := NewBalanceAdjustSvc()
	base := func(amount, reqID, reason string) *AdjustBalanceRequest {
		a, _ := decimal.NewFromString(amount)
		return &AdjustBalanceRequest{
			OrgID: orgID, OperatorUserID: pfx + "op", OperatorImID: pfx + "im-op",
			TargetUserID: targetUserID, CurrencyID: currencyID,
			Amount: a, Reason: reason, RequestID: reqID,
		}
	}
	readBalance := func() string {
		var d bson.M
		if err := balColl.FindOne(ctx, bson.M{"wallet_id": walletID, "currency_id": currencyID}).Decode(&d); err != nil {
			t.Fatalf("读余额失败: %v", err)
		}
		return fmt.Sprint(d["available_balance"])
	}
	// Decimal128 会保留标度：1100 存进去读出来是 "1100.00"。
	// 数值相等才是要断言的东西，字符串原样比较会误报。
	eqAmount := func(t *testing.T, label, got, want string) {
		t.Helper()
		g, err1 := decimal.NewFromString(got)
		w, err2 := decimal.NewFromString(want)
		if err1 != nil || err2 != nil {
			t.Errorf("%s：金额无法解析 got=%s want=%s", label, got, want)
			return
		}
		if !g.Equal(w) {
			t.Errorf("%s：应为 %s，实际 %s", label, want, got)
		}
	}

	// ---------- 1. 增加，且金额精度不能漂 ----------
	got, err := svc.AdjustUserBalance(ctx, base("123.45", pfx+"req-1", "补发活动奖励"))
	if err != nil {
		t.Fatalf("增加余额失败: %v", err)
	}
	eqAmount(t, "1000 + 123.45", got, "1123.45")
	eqAmount(t, "库里余额", readBalance(), "1123.45")

	// ---------- 2. 幂等：同一个 requestID 重复提交只生效一次 ----------
	got2, err := svc.AdjustUserBalance(ctx, base("123.45", pfx+"req-1", "补发活动奖励"))
	if err != nil {
		t.Fatalf("幂等重放不应报错: %v", err)
	}
	eqAmount(t, "幂等重放应返回同一余额", got2, "1123.45")
	eqAmount(t, "幂等重放后余额不应再变", readBalance(), "1123.45")
	n, _ := db.Collection(adjustLogCollection).CountDocuments(ctx, bson.M{"request_id": pfx + "req-1"})
	if n != 1 {
		t.Errorf("同一幂等键应只有 1 条调节记录，实际 %d", n)
	}

	// ---------- 3. 扣减 ----------
	got3, err := svc.AdjustUserBalance(ctx, base("-23.45", pfx+"req-2", "误发回收"))
	if err != nil {
		t.Fatalf("扣减失败: %v", err)
	}
	eqAmount(t, "1123.45 - 23.45", got3, "1100")

	// ---------- 4. 余额不足必须整体回滚 ----------
	// 关键：失败时不能只回滚余额而把幂等记录留下，否则这个 requestID
	// 会被永久占用，用户重试将拿到「已处理」的假成功。
	// 注意金额要小于单笔上限（100000），否则会先被上限拦掉，
	// 根本进不了事务，也就验不到回滚。余额此时是 1100，扣 5000 必然不足。
	before := readBalance()
	if _, err := svc.AdjustUserBalance(ctx, base("-5000", pfx+"req-3", "余额不足测试")); err == nil {
		t.Error("扣减金额超过可用余额应当失败")
	} else {
		t.Logf("余额不足被拒：%v", err)
	}
	if after := readBalance(); after != before {
		t.Errorf("失败后余额应保持 %s，实际 %s", before, after)
	}
	if n, _ := db.Collection(adjustLogCollection).CountDocuments(ctx, bson.M{"request_id": pfx + "req-3"}); n != 0 {
		t.Errorf("失败的调整不应留下幂等记录，否则重试会被误判为已处理，实际 %d 条", n)
	}

	// ---------- 5. 入参校验 ----------
	for _, c := range []struct {
		name   string
		req    *AdjustBalanceRequest
		reason string
	}{
		{"金额为 0", base("0", pfx+"req-4", "x"), "调整金额不能为 0"},
		{"原因为空", base("10", pfx+"req-5", "   "), "调整原因必填"},
		{"超单笔上限", base("100000.01", pfx+"req-6", "x"), "超过单笔上限"},
	} {
		if _, err := svc.AdjustUserBalance(ctx, c.req); err == nil {
			t.Errorf("%s 应被拒绝（%s）", c.name, c.reason)
		}
	}
	// 缺幂等键
	noID := base("10", "", "x")
	if _, err := svc.AdjustUserBalance(ctx, noID); err == nil {
		t.Error("缺少 requestId 应被拒绝")
	}

	// ---------- 6. 跨组织越权 ----------
	cross := base("10", pfx+"req-7", "越权测试")
	cross.OrgID = primitive.NewObjectID() // 换一个组织
	if _, err := svc.AdjustUserBalance(ctx, cross); err == nil {
		t.Error("调整非本组织用户的余额应被拒绝")
	} else {
		t.Logf("跨组织越权被拒：%v", err)
	}

	// ---------- 7. 账单流水：用户要能在账单里看到原因 ----------
	tsColl := db.Collection("wallet_transaction_record")
	if n, _ := tsColl.CountDocuments(ctx, bson.M{"wallet_id": walletID}); n != 2 {
		t.Errorf("两次成功调整应各写一条账单流水，实际 %d 条", n)
	}
	var ts bson.M
	if err := tsColl.FindOne(ctx, bson.M{"wallet_id": walletID, "remark": "补发活动奖励"}).Decode(&ts); err != nil {
		t.Errorf("账单流水里查不到本次调整，用户将看到余额莫名变化: %v", err)
	} else {
		// 用户在账单里看到的就是这几个字段
		if fmt.Sprint(ts["source"]) != pfx+"op" {
			t.Errorf("账单流水应记录操作人，实际 source=%v", ts["source"])
		}
		t.Logf("账单流水：type=%v amount=%v remark=%v source=%v",
			ts["type"], ts["amount"], ts["remark"], ts["source"])
	}
	// 失败的那笔不能留下流水
	if n, _ := tsColl.CountDocuments(ctx, bson.M{"wallet_id": walletID, "remark": "余额不足测试"}); n != 0 {
		t.Errorf("失败的调整不应留下账单流水，实际 %d 条", n)
	}

	t.Logf("最终余额 %s（初始 1000，+123.45，-23.45）", readBalance())
}
