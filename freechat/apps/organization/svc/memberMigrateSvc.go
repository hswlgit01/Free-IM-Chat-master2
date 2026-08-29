package svc

import (
	"context"
	"fmt"
	"time"

	"github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 会员整体迁移：把某个会员连同其整棵下级团队，挂到另一个会员名下。
// 业务场景：业务员离职、团队重组、分配调整。
//
// 【层级是怎么存的】organization_user 用物化路径冗余存了一整套字段：
//
//	ancestor_path        祖先链路，**从近到远**：[直接上级, 祖父, 曾祖父, ...]
//	level1/2/3_parent    等价于 ancestor_path[0] / [1] / [2]
//	level                层级深度 = 上级 level + 1
//	team_size            团队总人数（含所有层级下级）
//	direct_downline_count 直属下级数
//
// 所以「改上级」远不止改一个字段：要重算整棵子树的路径与层级，
// 还要双向调整新旧两条上级链路上每个人的 team_size。
//
// 【为什么先做预览】这个操作会一次性改动几十上百条记录且难以人工还原，
// 所以拆成 Preview（只读，算出将要发生什么）和 Execute（在事务里落库）两步，
// 让操作者先确认影响面。

// MigrateMemberRequest 迁移入参。
type MigrateMemberRequest struct {
	OrgID          primitive.ObjectID
	OperatorUserID string
	OperatorImID   string
	// MemberUserID 被迁移的会员（连同其下级团队一起搬走）
	MemberUserID string
	// NewParentUserID 迁移到谁名下
	NewParentUserID string
	// Reason 迁移原因，写入迁移记录备查
	Reason string
}

// MigratePreview 迁移预览：不改数据，只算出将要发生什么。
type MigratePreview struct {
	MemberUserID    string `json:"memberUserId"`
	MemberNickname  string `json:"memberNickname"`
	NewParentUserID string `json:"newParentUserId"`

	// OldParentUserID 迁移前的直接上级
	OldParentUserID string `json:"oldParentUserId"`
	// SubtreeSize 会被移动的总人数（含被迁移者本人）
	SubtreeSize int `json:"subtreeSize"`
	// OldLevel / NewLevel 被迁移者迁移前后的层级深度
	OldLevel int `json:"oldLevel"`
	NewLevel int `json:"newLevel"`
	// LevelDelta 整棵子树的层级位移，正数为下沉、负数为上浮
	LevelDelta int `json:"levelDelta"`

	// TeamSizeDecrease 这些上级的 team_size 将减少 SubtreeSize
	TeamSizeDecrease []string `json:"teamSizeDecrease"`
	// TeamSizeIncrease 这些上级的 team_size 将增加 SubtreeSize
	TeamSizeIncrease []string `json:"teamSizeIncrease"`

	// Warnings 需要操作者注意但不阻断的情况
	Warnings []string `json:"warnings"`
}

type MemberMigrateSvc struct{}

func NewMemberMigrateSvc() *MemberMigrateSvc { return &MemberMigrateSvc{} }

// maxSubtreeSize 单次迁移的人数上限。
// 超过这个规模，单个 Mongo 事务的体积和耗时都不可控，应当拆分或走离线任务。
const maxSubtreeSize = 2000

// Preview 计算迁移影响面，不改任何数据。
func (s *MemberMigrateSvc) Preview(ctx context.Context, req *MigrateMemberRequest) (*MigratePreview, error) {
	db := plugin.MongoCli().GetDB()
	member, newParent, err := s.loadAndValidate(ctx, db, req)
	if err != nil {
		return nil, err
	}

	subtree, err := s.loadSubtree(ctx, db, req.OrgID, member)
	if err != nil {
		return nil, err
	}
	if len(subtree) > maxSubtreeSize {
		return nil, errs.NewCodeError(freeErrors.ErrInvalidParams,
			fmt.Sprintf("该会员团队共 %d 人，超过单次迁移上限 %d 人，请联系技术支持", len(subtree), maxSubtreeSize))
	}

	newPath := buildChildAncestorPath(newParent)
	newLevel := newParent.Level + 1

	dec, inc := diffAncestorChains(member.AncestorPath, newPath)

	preview := &MigratePreview{
		MemberUserID:     member.UserId,
		NewParentUserID:  newParent.UserId,
		OldParentUserID:  firstOrEmpty(member.AncestorPath),
		SubtreeSize:      len(subtree),
		OldLevel:         member.Level,
		NewLevel:         newLevel,
		LevelDelta:       newLevel - member.Level,
		TeamSizeDecrease: dec,
		TeamSizeIncrease: inc,
	}

	if preview.LevelDelta != 0 {
		preview.Warnings = append(preview.Warnings,
			fmt.Sprintf("整棵团队的层级将%s %d 层，若有按层级计算的业务规则请留意",
				map[bool]string{true: "下沉", false: "上浮"}[preview.LevelDelta > 0],
				abs(preview.LevelDelta)))
	}
	// 业绩统计是实时按 inviter 聚合的（见 GetInviteDailyRegisterStats），
	// 没有每日快照，所以迁移会改写历史报表。这一点必须让操作者知道。
	preview.Warnings = append(preview.Warnings,
		"业务员业绩统计按上级实时聚合，迁移后该团队的历史业绩将从原上级报表转移到新上级报表")

	return preview, nil
}

// Execute 执行迁移。所有改动在一个事务内完成，失败整体回滚。
func (s *MemberMigrateSvc) Execute(ctx context.Context, req *MigrateMemberRequest) (*MigratePreview, error) {
	db := plugin.MongoCli().GetDB()

	// 先跑一次预览：既做完整校验，也把影响面记进迁移记录
	preview, err := s.Preview(ctx, req)
	if err != nil {
		return nil, err
	}

	member, newParent, err := s.loadAndValidate(ctx, db, req)
	if err != nil {
		return nil, err
	}
	subtree, err := s.loadSubtree(ctx, db, req.OrgID, member)
	if err != nil {
		return nil, err
	}

	newMemberPath := buildChildAncestorPath(newParent)
	newMemberLevel := newParent.Level + 1
	levelDelta := newMemberLevel - member.Level
	oldMemberPathLen := len(member.AncestorPath)
	oldDirectParent := firstOrEmpty(member.AncestorPath)

	coll := db.Collection(model.OrganizationUser{}.TableName())

	session, err := db.Client().StartSession()
	if err != nil {
		return nil, errs.Wrap(err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		models := make([]mongo.WriteModel, 0, len(subtree)+len(preview.TeamSizeDecrease)+len(preview.TeamSizeIncrease))

		// 1) 重算子树内每个人（含被迁移者本人）的路径与层级
		for _, u := range subtree {
			// 旧路径形如 [...到 member 为止的自己人.., member, member的旧上级链...]
			// 把「member 及其之后」的部分整体换成 member 的新链路。
			// selfPart 是该节点到 member 之间的那段（不含 member），对被迁移者本人为空。
			var selfPart []string
			if u.UserId == member.UserId {
				selfPart = nil
			} else {
				idx := indexOf(u.AncestorPath, member.UserId)
				if idx < 0 {
					// 理论上不会发生：subtree 是按 ancestor_path 含 member 查出来的
					return nil, errs.New("子树节点的祖先路径不含被迁移者，数据异常: " + u.UserId)
				}
				selfPart = append([]string{}, u.AncestorPath[:idx]...)
			}

			newPath := append(append([]string{}, selfPart...), member.UserId)
			if u.UserId == member.UserId {
				newPath = append([]string{}, newMemberPath...)
			} else {
				newPath = append(newPath, newMemberPath...)
			}

			set := bson.M{
				"ancestor_path":  newPath,
				"level":          u.Level + levelDelta,
				"level1_parent":  nthOrEmpty(newPath, 0),
				"level2_parent":  nthOrEmpty(newPath, 1),
				"level3_parent":  nthOrEmpty(newPath, 2),
			}
			// 只有被迁移者本人换了直接上级；其下级的直接上级不变
			if u.UserId == member.UserId {
				set["inviter"] = newParent.UserId
				set["inviter_im_server_user_id"] = newParent.ImServerUserId
			}

			models = append(models, mongo.NewUpdateOneModel().
				SetFilter(bson.M{"_id": u.ID}).
				SetUpdate(bson.M{"$set": set}))
		}

		// 2) 双向调整两条上级链路的 team_size。
		//    取新旧链路的对称差：同时在两条链上的祖先，其团队仍然包含这棵子树，人数不变。
		size := len(subtree)
		if len(preview.TeamSizeDecrease) > 0 {
			models = append(models, mongo.NewUpdateManyModel().
				SetFilter(bson.M{"organization_id": req.OrgID, "user_id": bson.M{"$in": preview.TeamSizeDecrease}}).
				SetUpdate(bson.M{"$inc": bson.M{"team_size": -size}}))
		}
		if len(preview.TeamSizeIncrease) > 0 {
			models = append(models, mongo.NewUpdateManyModel().
				SetFilter(bson.M{"organization_id": req.OrgID, "user_id": bson.M{"$in": preview.TeamSizeIncrease}}).
				SetUpdate(bson.M{"$inc": bson.M{"team_size": size}}))
		}

		// 3) 直属下级数：旧上级 -1，新上级 +1
		if oldDirectParent != "" {
			models = append(models, mongo.NewUpdateOneModel().
				SetFilter(bson.M{"organization_id": req.OrgID, "user_id": oldDirectParent}).
				SetUpdate(bson.M{"$inc": bson.M{"direct_downline_count": -1}}))
		}
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"organization_id": req.OrgID, "user_id": newParent.UserId}).
			SetUpdate(bson.M{"$inc": bson.M{"direct_downline_count": 1}}))

		if _, err := coll.BulkWrite(sessCtx, models, options.BulkWrite().SetOrdered(false)); err != nil {
			return nil, err
		}

		// 4) 迁移记录。业绩统计没有每日快照、迁移后历史报表会变，
		//    这条记录是事后解释「上个月报表为什么变了」的唯一依据，必须落库。
		if _, err := db.Collection(migrateLogCollection).InsertOne(sessCtx, bson.M{
			"org_id":              req.OrgID,
			"member_user_id":      member.UserId,
			"old_parent_user_id":  oldDirectParent,
			"new_parent_user_id":  newParent.UserId,
			"subtree_size":        size,
			"old_level":           member.Level,
			"new_level":           newMemberLevel,
			"old_ancestor_path":   member.AncestorPath,
			"new_ancestor_path":   newMemberPath,
			"team_size_decrease":  preview.TeamSizeDecrease,
			"team_size_increase":  preview.TeamSizeIncrease,
			"operator_user_id":    req.OperatorUserID,
			"operator_im_user_id": req.OperatorImID,
			"reason":              req.Reason,
			"created_at":          time.Now().UTC(),
		}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		log.ZError(ctx, "会员迁移失败", err,
			"member", req.MemberUserID, "new_parent", req.NewParentUserID)
		return nil, err
	}

	_ = oldMemberPathLen
	log.ZInfo(ctx, "会员迁移完成",
		"member", member.UserId, "from", oldDirectParent, "to", newParent.UserId,
		"subtree_size", len(subtree), "level_delta", levelDelta, "operator", req.OperatorUserID)
	return preview, nil
}

const migrateLogCollection = "organization_member_migrate_log"

// loadAndValidate 载入双方并做全部前置校验。
func (s *MemberMigrateSvc) loadAndValidate(ctx context.Context, db *mongo.Database, req *MigrateMemberRequest) (*model.OrganizationUser, *model.OrganizationUser, error) {
	if req.MemberUserID == "" || req.NewParentUserID == "" {
		return nil, nil, errs.NewCodeError(freeErrors.ErrInvalidParams, "缺少会员或目标上级")
	}
	if req.MemberUserID == req.NewParentUserID {
		return nil, nil, errs.NewCodeError(freeErrors.ErrInvalidParams, "不能把会员迁移到自己名下")
	}

	dao := model.NewOrganizationUserDao(db)
	member, err := dao.GetByUserIdAndOrgID(ctx, req.MemberUserID, req.OrgID.Hex())
	if err != nil || member == nil {
		return nil, nil, errs.NewCodeError(freeErrors.ErrInvalidParams, "被迁移的会员不存在或不属于当前组织")
	}
	newParent, err := dao.GetByUserIdAndOrgID(ctx, req.NewParentUserID, req.OrgID.Hex())
	if err != nil || newParent == nil {
		return nil, nil, errs.NewCodeError(freeErrors.ErrInvalidParams, "目标上级不存在或不属于当前组织")
	}

	// 环检测：目标上级不能是被迁移者自己的下级。
	// 否则这棵子树会指向自身，形成环 —— 之后所有按 ancestor_path 的递归查询都会死循环。
	// 判据：新上级的祖先链里出现了被迁移者。
	if indexOf(newParent.AncestorPath, member.UserId) >= 0 {
		return nil, nil, errs.NewCodeError(freeErrors.ErrInvalidParams,
			"目标上级是该会员的下级，迁移后层级会成环，已阻止")
	}

	// 已经在目标名下，无需迁移
	if firstOrEmpty(member.AncestorPath) == newParent.UserId {
		return nil, nil, errs.NewCodeError(freeErrors.ErrInvalidParams, "该会员已在目标上级名下，无需迁移")
	}
	return member, newParent, nil
}

// loadSubtree 取出被迁移者本人 + 其全部下级。
func (s *MemberMigrateSvc) loadSubtree(ctx context.Context, db *mongo.Database, orgID primitive.ObjectID, member *model.OrganizationUser) ([]*model.OrganizationUser, error) {
	coll := db.Collection(model.OrganizationUser{}.TableName())
	cur, err := coll.Find(ctx, bson.M{
		"organization_id": orgID,
		"$or": []bson.M{
			{"user_id": member.UserId},
			{"ancestor_path": member.UserId},
		},
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	var list []*model.OrganizationUser
	if err := cur.All(ctx, &list); err != nil {
		return nil, errs.Wrap(err)
	}
	return list, nil
}

// buildChildAncestorPath 返回「挂在 parent 下面的子节点」应有的祖先链路。
// 与注册时的构建方式一致：[parent, parent的祖先...]，从近到远。
func buildChildAncestorPath(parent *model.OrganizationUser) []string {
	return append([]string{parent.UserId}, parent.AncestorPath...)
}

// diffAncestorChains 求新旧祖先链的对称差。
// 同时存在于两条链上的祖先，其团队仍包含这棵子树，team_size 不变。
func diffAncestorChains(oldPath, newPath []string) (decrease, increase []string) {
	oldSet := make(map[string]struct{}, len(oldPath))
	for _, id := range oldPath {
		oldSet[id] = struct{}{}
	}
	newSet := make(map[string]struct{}, len(newPath))
	for _, id := range newPath {
		newSet[id] = struct{}{}
	}
	for _, id := range oldPath {
		if _, ok := newSet[id]; !ok {
			decrease = append(decrease, id)
		}
	}
	for _, id := range newPath {
		if _, ok := oldSet[id]; !ok {
			increase = append(increase, id)
		}
	}
	return decrease, increase
}

func indexOf(list []string, target string) int {
	for i, v := range list {
		if v == target {
			return i
		}
	}
	return -1
}

func firstOrEmpty(list []string) string { return nthOrEmpty(list, 0) }

func nthOrEmpty(list []string, n int) string {
	if n < len(list) {
		return list[n]
	}
	return ""
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// EnsureMigrateLogIndex 迁移记录的查询索引。
func EnsureMigrateLogIndex(ctx context.Context, db *mongo.Database) error {
	_, err := db.Collection(migrateLogCollection).Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "org_id", Value: 1}, {Key: "created_at", Value: -1}}},
		{Keys: bson.D{{Key: "member_user_id", Value: 1}}},
	})
	return err
}
