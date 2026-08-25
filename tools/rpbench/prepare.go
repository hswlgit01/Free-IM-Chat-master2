package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Plan 是 prepare 阶段产出、bench 阶段消费的压测计划。
// 把「造数据」和「打压力」分开，是为了让每一轮压测都跑在完全相同的数据集上，
// 改代码前后的对比才有意义。
type Plan struct {
	OrgID      string `json:"org_id"`
	CurrencyID string `json:"currency_id"`

	SenderUserID string `json:"sender_user_id"` // organization_user.user_id，红包发送者
	SenderImID   string `json:"sender_im_id"`
	SenderThird  string `json:"sender_third_user_id"` // 嵌入式登录用，用来换 chat token
	// SenderToken 在 prepare 阶段就换好存下来，bench 直接复用。
	// 这样每轮压测不必再走嵌入式登录，也就绕开了服务端组织缓存丢密钥的缺陷。
	SenderToken string `json:"sender_chat_token"`

	GroupID string `json:"group_id"` // 压测专用群

	// Receivers 是抢红包的用户池：既是组织用户、又开了钱包，才能领。
	Receivers []Receiver `json:"receivers"`

	CreatedAt time.Time `json:"created_at"`
}

type Receiver struct {
	UserID string `json:"user_id"` // organization_user.user_id，即 receive 接口的 receiver_id
	ImID   string `json:"im_id"`   // im_server_user_id，用来进群 / 发消息
}

func runPrepare(cfg *Config, args []string) error {
	fs := flag.NewFlagSet("prepare", flag.ExitOnError)
	cfg.bindCommon(fs)
	var (
		receivers = fs.Int("receivers", 5000, "抢红包用户池大小")
		groupID   = fs.String("group-id", "stressgrp001", "压测群 ID（已存在则复用）")
		topup     = fs.String("topup", "500000", "给发送者钱包补足的可用余额")
		skipGroup = fs.Bool("skip-group", false, "只造用户池，不建群（群校验压测才需要建群）")
		senderID  = fs.String("sender-id", "rpbench-sender-001", "压测发送者的 third_user_id；换一个即可造出独立发送者（用于验证发送者钱包是否为热点）")
	)
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		return fmt.Errorf("连接 Mongo 失败: %w", err)
	}
	defer cli.Disconnect(ctx)
	db := cli.Database("openim_v3")

	// ---- 1. 组织 + 币种 -------------------------------------------------
	var org struct {
		ID           primitive.ObjectID `bson:"_id"`
		AesKeyBase64 string             `bson:"aesKeyBase64"`
	}
	if err := db.Collection("organization").FindOne(ctx, bson.M{"status": "pass"}).Decode(&org); err != nil {
		return fmt.Errorf("找不到 status=pass 的组织: %w", err)
	}
	orgID := org.ID.Hex()

	var currency struct {
		ID primitive.ObjectID `bson:"_id"`
	}
	if err := db.Collection("wallet_currency").FindOne(ctx, bson.M{}).Decode(&currency); err != nil {
		return fmt.Errorf("找不到币种: %w", err)
	}
	fmt.Printf("组织 %s，币种 %s\n", orgID, currency.ID.Hex())

	// ---- 2. 找出「组织用户 ∩ 已开钱包」的候选池 --------------------------
	// wallet_info.owner_id 存的就是 organization_user.user_id（见 ValidateWalletStatus）。
	fmt.Println("正在求组织用户与钱包的交集...")
	cur, err := db.Collection("organization_user").Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"organization_id": org.ID}}},
		{{Key: "$lookup", Value: bson.M{
			"from":         "wallet_info",
			"localField":   "user_id",
			"foreignField": "owner_id",
			"as":           "w",
		}}},
		{{Key: "$match", Value: bson.M{"w.owner_type": "ordinary"}}},
		{{Key: "$project", Value: bson.M{
			"user_id": 1, "im_server_user_id": 1, "third_user_id": 1, "_id": 0,
		}}},
		{{Key: "$limit", Value: *receivers + 50}},
	}, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return fmt.Errorf("聚合候选用户失败: %w", err)
	}
	var candidates []struct {
		UserID string `bson:"user_id"`
		ImID   string `bson:"im_server_user_id"`
		Third  string `bson:"third_user_id"`
	}
	if err := cur.All(ctx, &candidates); err != nil {
		return err
	}
	if len(candidates) < 10 {
		return fmt.Errorf("可用（组织用户+钱包）账号只有 %d 个，不足以压测", len(candidates))
	}
	fmt.Printf("候选账号 %d 个\n", len(candidates))

	// ---- 3. 造一个专用的压测发送者并补足余额 -----------------------------
	// 现有组织用户都是走常规注册的，没有 third_user_id，换不了 chat token。
	// 所以这里用嵌入式登录新建一个固定的压测账号（third_user_id 固定，可重复复用），
	// 再直接在 Mongo 里给它开钱包充值。只影响这一个账号，不动任何真实用户。
	httpCli := newHTTPClient(64, 120*time.Second)
	chatCli := newChatClient(cfg, httpCli)
	stressThirdID := *senderID
	login, err := chatCli.embedLogin(orgID, org.AesKeyBase64, stressThirdID, "bench_"+stressThirdID[len(stressThirdID)-3:])
	if err != nil && strings.Contains(err.Error(), "invalid key size") {
		// 服务端的组织缓存（GetCache）用 json.Marshal 序列化，而 Organization.AesKeyBase64
		// 带着 json:"-"，所以缓存一命中，服务端拿到的密钥就是空串 → aes 报 invalid key size 0。
		// 这是被测代码本身的缺陷（见压测报告）。压测流程里先把组织缓存清掉再重试一次。
		fmt.Println("组织缓存命中导致服务端密钥为空，清缓存后重试...")
		flushOrgCache()
		login, err = chatCli.embedLogin(orgID, org.AesKeyBase64, stressThirdID, "bench_"+stressThirdID[len(stressThirdID)-3:])
	}
	if err != nil {
		return fmt.Errorf("嵌入式登录创建压测发送者失败: %w", err)
	}
	// 注意：EmbedLoginResponse.UserID 装的是 im_server_user_id（见 svc.EmbedLogin），
	// 而 chat token 是按 organization_user.user_id 签的。CreateTransaction 用的是后者，
	// 所以这里要反查一次拿到真正的 user_id。
	sender := struct {
		UserID string
		ImID   string
		Third  string
	}{ImID: login.UserID, Third: stressThirdID}

	var senderOrgUser struct {
		UserID string `bson:"user_id"`
	}
	if err := db.Collection("organization_user").FindOne(ctx,
		bson.M{"im_server_user_id": sender.ImID}).Decode(&senderOrgUser); err != nil {
		return fmt.Errorf("按 im_server_user_id=%s 查压测发送者失败: %w", sender.ImID, err)
	}
	sender.UserID = senderOrgUser.UserID

	// 嵌入式登录建出来的账号是默认角色，多半没有 send_red_packet 权限，
	// CreateTransaction 会直接 10003 拒绝。这里把它改成本组织里已经拥有该权限的角色，
	// 不新增权限配置，只挪动这一个压测账号的角色。
	var perm struct {
		Role string `bson:"role"`
	}
	if err := db.Collection("organization_role_permission").FindOne(ctx, bson.M{
		"org_id": org.ID, "permission_code": "send_red_packet",
	}).Decode(&perm); err != nil {
		return fmt.Errorf("本组织没有任何角色配置了 send_red_packet 权限，无法发红包: %w", err)
	}
	if _, err := db.Collection("organization_user").UpdateOne(ctx,
		bson.M{"im_server_user_id": sender.ImID},
		bson.M{"$set": bson.M{"role": perm.Role}}); err != nil {
		return fmt.Errorf("给压测发送者设置角色失败: %w", err)
	}
	fmt.Printf("压测发送者角色已设为 %s（含 send_red_packet 权限）\n", perm.Role)
	fmt.Printf("压测发送者 user_id=%s im=%s\n", sender.UserID, sender.ImID)

	// 给发送者开钱包（已存在则复用）
	var wallet struct {
		ID primitive.ObjectID `bson:"_id"`
	}
	err = db.Collection("wallet_info").FindOne(ctx,
		bson.M{"owner_id": sender.UserID, "owner_type": "ordinary"}).Decode(&wallet)
	if err != nil {
		zero, _ := primitive.ParseDecimal128("0")
		ins, insErr := db.Collection("wallet_info").InsertOne(ctx, bson.M{
			"owner_id": sender.UserID, "owner_type": "ordinary",
			"pay_pwd": "", "compensation_balance": zero,
			"created_at": time.Now().UTC(), "updated_at": time.Now().UTC(),
		})
		if insErr != nil {
			return fmt.Errorf("给压测发送者开钱包失败: %w", insErr)
		}
		wallet.ID = ins.InsertedID.(primitive.ObjectID)
		fmt.Println("已为压测发送者新建钱包")
	}
	topupDec, err := primitive.ParseDecimal128(*topup)
	if err != nil {
		return fmt.Errorf("topup 金额非法: %w", err)
	}
	res, err := db.Collection("wallet_balance").UpdateOne(ctx,
		bson.M{"wallet_id": wallet.ID, "currency_id": currency.ID},
		bson.M{"$set": bson.M{"available_balance": topupDec, "updated_at": time.Now().UTC()}},
	)
	if err != nil {
		return fmt.Errorf("给发送者充值失败: %w", err)
	}
	if res.MatchedCount == 0 {
		zero, _ := primitive.ParseDecimal128("0")
		if _, err := db.Collection("wallet_balance").InsertOne(ctx, bson.M{
			"wallet_id": wallet.ID, "currency_id": currency.ID,
			"available_balance": topupDec, "red_packet_frozen_balance": zero,
			"transfer_frozen_balance": zero, "compensation_balance": zero,
			"created_at": time.Now().UTC(), "updated_at": time.Now().UTC(),
		}); err != nil {
			return fmt.Errorf("创建发送者余额记录失败: %w", err)
		}
	}
	// 支付密码置空，配合 create 接口传空 pay_password 走「生物识别」分支跳过校验
	if _, err := db.Collection("wallet_info").UpdateOne(ctx,
		bson.M{"_id": wallet.ID}, bson.M{"$set": bson.M{"pay_pwd": ""}}); err != nil {
		return fmt.Errorf("清空支付密码失败: %w", err)
	}
	fmt.Printf("发送者 %s (im=%s) 余额已置为 %s\n", sender.UserID, sender.ImID, *topup)

	// ---- 4. 组装收包用户池 -----------------------------------------------
	plan := &Plan{
		OrgID:        orgID,
		CurrencyID:   currency.ID.Hex(),
		SenderUserID: sender.UserID,
		SenderImID:   sender.ImID,
		SenderThird:  sender.Third,
		SenderToken:  login.ChatToken,
		GroupID:      *groupID,
		CreatedAt:    time.Now(),
	}
	for _, c := range candidates {
		if c.UserID == sender.UserID || c.ImID == "" {
			continue
		}
		plan.Receivers = append(plan.Receivers, Receiver{UserID: c.UserID, ImID: c.ImID})
		if len(plan.Receivers) >= *receivers {
			break
		}
	}
	fmt.Printf("收包用户池 %d 个\n", len(plan.Receivers))

	// ---- 5. 建压测群 -------------------------------------------------------
	if !*skipGroup {
		im := newIMClient(cfg, httpCli)
		if err := ensureStressGroup(ctx, db, im, plan); err != nil {
			// 建群失败不致命：receive_stress 接口本来就跳过群校验，
			// 只是没法压 CheckGroupMembership 那条路径，明确告知即可。
			fmt.Printf("[警告] 建压测群失败，将只能跑「跳过群校验」的场景: %v\n", err)
		}
	}

	// ---- 6. 落盘 -----------------------------------------------------------
	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return err
	}
	path := cfg.PlanFile
	if !filepath.IsAbs(path) {
		path = filepath.Join(".", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	buf, _ := json.MarshalIndent(plan, "", "  ")
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		return err
	}
	fmt.Printf("压测计划已写入 %s\n", path)
	return nil
}

// flushOrgCache 清掉 Redis 里的组织缓存，绕开「缓存命中后 aesKeyBase64 丢失」的缺陷。
// 只删缓存键，不动任何业务数据。
func flushOrgCache() {
	script := `for _,k in ipairs(redis.call('KEYS','*C_ORG_ID*')) do redis.call('DEL',k) end return 1`
	out, err := exec.Command("docker", "exec", "redis", "redis-cli", "-a", "openIM123", "--no-auth-warning", "EVAL", script, "0").CombinedOutput()
	if err != nil {
		fmt.Printf("清组织缓存失败（可手动执行 redis-cli KEYS '*C_ORG_ID*' 删除）: %v %s\n", err, out)
	}
}

// ensureStressGroup 建群并分批拉人。OpenIM 单次 create_group 塞几千人会超时，
// 所以先小批建群，再按 500 一批 invite。
//
// 建完还要补一个 org_id 字段：CheckGroupOrganizationRelation 用
// group.{org_id, group_id} 判断群属不属于发红包人的组织，而 OpenIM 原生建群接口
// 不认识这个字段，不补上的话 CreateTransaction 会直接以「群不属于该组织」失败。
func ensureStressGroup(ctx context.Context, db *mongo.Database, im *imClient, plan *Plan) error {
	ids := make([]string, 0, len(plan.Receivers))
	for _, r := range plan.Receivers {
		ids = append(ids, r.ImID)
	}
	seed := ids
	if len(seed) > 100 {
		seed = seed[:100]
	}
	err := im.createGroup(plan.GroupID, "红包压测群", plan.SenderImID, seed)
	if err != nil {
		// 群已存在时 create_group 会报错，继续走 invite 补人即可
		fmt.Printf("create_group 返回（群可能已存在，继续补人）: %v\n", err)
	}
	const batch = 500
	for i := 100; i < len(ids); i += batch {
		end := i + batch
		if end > len(ids) {
			end = len(ids)
		}
		if err := im.inviteToGroup(plan.GroupID, ids[i:end]); err != nil {
			// 重复执行 prepare 时成员已在群里，会撞 group_member 唯一索引，属正常
			if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "E11000") {
				continue
			}
			fmt.Printf("invite [%d,%d) 失败: %s\n", i, end, truncate(err.Error(), 160))
		}
	}

	// 补 org_id，否则 CheckGroupOrganizationRelation 过不去
	res, err := db.Collection("group").UpdateOne(ctx,
		bson.M{"group_id": plan.GroupID},
		bson.M{"$set": bson.M{"org_id": plan.OrgID}})
	if err != nil {
		return fmt.Errorf("给压测群补 org_id 失败: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("群 %s 在 group 集合里不存在，建群应该失败了", plan.GroupID)
	}

	n, _ := db.Collection("group_member").CountDocuments(ctx, bson.M{"group_id": plan.GroupID})
	fmt.Printf("压测群 %s 就绪，实际成员 %d（目标 %d），org_id 已补齐\n", plan.GroupID, n, len(ids)+1)
	return nil
}
