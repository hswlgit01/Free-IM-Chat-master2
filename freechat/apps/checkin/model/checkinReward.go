package model

import (
	"context"
	"strconv"
	"time"

	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/constant"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CheckinRewardStatus string

const (
	CheckinRewardStatusApply   CheckinRewardStatus = "apply"
	CheckinRewardStatusPending CheckinRewardStatus = "pending"
)

type CheckinRewardSource string

const (
	CheckinRewardSourceDaily      CheckinRewardSource = "daily"      // 日常签到奖励
	CheckinRewardSourceContinuous CheckinRewardSource = "continuous" // 连续签到奖励
)

// CheckinReward 签到奖励表
type CheckinReward struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	ImServerUserId string `bson:"im_server_user_id" json:"im_server_user_id"`

	CheckinRewardConfigId primitive.ObjectID `bson:"checkin_reward_config_id" json:"checkin_reward_config_id"` // 签到奖励的id
	CheckinId             primitive.ObjectID `bson:"checkin_id" json:"checkin_id"`                             // 签到触发记录id
	RewardType            CheckinRewardType  `bson:"type" json:"type"`                                         // 奖励类型

	RewardId     string               `bson:"reward_id" json:"reward_id"`
	RewardAmount primitive.Decimal128 `bson:"reward_amount" json:"reward_amount"`

	Status      CheckinRewardStatus `bson:"status" json:"status"`
	OrgID       primitive.ObjectID  `bson:"org_id" json:"org_id"`
	CheckinDate time.Time           `bson:"checkin_date" json:"checkin_date"`                   // 签到日期
	Source      CheckinRewardSource `bson:"source" json:"source"`                               // 奖励来源
	Description string              `bson:"description,omitempty" json:"description,omitempty"` // 额外描述信息

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (CheckinReward) TableName() string {
	return constant.CollectionCheckinReward
}

func CreateCheckinRewardIndex(db *mongo.Database) error {
	m := &CheckinReward{}

	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "im_server_user_id", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "created_at", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
			},
		},
		// 组合索引：匹配 org_id + status + checkin_date 并按 created_at 排序
		// 对于“按组织 + 状态 + 签到日期范围 + created_at 倒序 + 分页”的典型查询性能提升明显
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
				{Key: "status", Value: 1},
				{Key: "checkin_date", Value: 1},
				{Key: "created_at", Value: -1},
			},
			Options: options.Index().SetBackground(true),
		},
		{
			Keys: bson.D{
				{Key: "checkin_date", Value: 1},
			},
		},
		// 先分页再 join：按 checkin_date 倒序分页主表时使用
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
				{Key: "status", Value: 1},
				{Key: "checkin_date", Value: -1},
			},
			Options: options.Index().SetBackground(true),
		},
	})
	return err
}

type CheckinRewardDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewCheckinRewardDao(db *mongo.Database) *CheckinRewardDao {
	return &CheckinRewardDao{
		DB:         db,
		Collection: db.Collection(CheckinReward{}.TableName()),
	}
}

func (o *CheckinRewardDao) Create(ctx context.Context, obj *CheckinReward) error {
	obj.CreatedAt = time.Now()
	obj.UpdatedAt = time.Now()
	return mongoutil.InsertMany(ctx, o.Collection, []*CheckinReward{obj})
}

func (o *CheckinRewardDao) GetByIdAndOrgId(ctx context.Context, id primitive.ObjectID, organizationId primitive.ObjectID) (*CheckinReward, error) {
	return mongoutil.FindOne[*CheckinReward](ctx, o.Collection, bson.M{"_id": id, "org_id": organizationId})
}

func (o *CheckinRewardDao) UpdateStatusById(ctx context.Context, id primitive.ObjectID, status CheckinRewardStatus) error {
	data := map[string]any{
		"status": status,
	}
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"_id": id}, bson.M{"$set": data}, false)
}

// UpdateStatus 更新签到奖励状态
func (o *CheckinRewardDao) UpdateStatus(ctx context.Context, id primitive.ObjectID, status CheckinRewardStatus) error {
	return o.UpdateStatusById(ctx, id, status)
}

// FixUserContinuousRewards 对指定组织+用户的连续签到奖励去重：
// 同一 (org_id, im_server_user_id, checkin_reward_config_id) 仅保留一条记录，其余删除。
// 返回删除的奖励条数。
func (o *CheckinRewardDao) FixUserContinuousRewards(ctx context.Context, orgId primitive.ObjectID, imServerUserId string) (int64, error) {
	if orgId.IsZero() || imServerUserId == "" {
		return 0, nil
	}

	// 查出该用户在本组织下的所有连续签到奖励，按配置ID+创建时间排序
	filter := bson.M{
		"org_id":            orgId,
		"im_server_user_id": imServerUserId,
		"source":            CheckinRewardSourceContinuous,
	}
	opts := options.Find().SetSort(bson.D{
		{Key: "checkin_reward_config_id", Value: 1},
		{Key: "created_at", Value: 1},
		{Key: "_id", Value: 1},
	})

	rewards, err := mongoutil.Find[*CheckinReward](ctx, o.Collection, filter, opts)
	if err != nil {
		return 0, err
	}
	if len(rewards) == 0 {
		return 0, nil
	}

	// 对同一配置的奖励，仅保留第一条（最早创建），其余标记为删除
	keptConfig := make(map[primitive.ObjectID]primitive.ObjectID)
	toDelete := make([]primitive.ObjectID, 0)

	for _, r := range rewards {
		if r == nil {
			continue
		}
		cfgID := r.CheckinRewardConfigId
		if cfgID.IsZero() {
			// 没有关联配置ID的记录不做去重，避免误删
			continue
		}
		if _, exists := keptConfig[cfgID]; !exists {
			keptConfig[cfgID] = r.ID
		} else {
			toDelete = append(toDelete, r.ID)
		}
	}

	if len(toDelete) == 0 {
		return 0, nil
	}

	res, err := o.Collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": toDelete}})
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}

// FixContinuousRewardsByOrg 对指定组织下所有「阶段性奖励」（continuous）去重：
// 同一 (org_id, im_server_user_id, checkin_reward_config_id) 仅保留一条（最早创建），其余删除。
// checkin_reward_config_id 为零的记录不参与去重。
// 返回删除的奖励条数。
func (o *CheckinRewardDao) FixContinuousRewardsByOrg(ctx context.Context, orgId primitive.ObjectID) (int64, error) {
	if orgId.IsZero() {
		return 0, nil
	}

	filter := bson.M{
		"org_id": orgId,
		"source": CheckinRewardSourceContinuous,
	}
	opts := options.Find().SetSort(bson.D{
		{Key: "im_server_user_id", Value: 1},
		{Key: "checkin_reward_config_id", Value: 1},
		{Key: "created_at", Value: 1},
		{Key: "_id", Value: 1},
	})

	rewards, err := mongoutil.Find[*CheckinReward](ctx, o.Collection, filter, opts)
	if err != nil {
		return 0, err
	}
	if len(rewards) == 0 {
		return 0, nil
	}

	// 按 (im_server_user_id, checkin_reward_config_id) 分组，同组只保留第一条
	type key struct {
		User   string
		Config primitive.ObjectID
	}
	kept := make(map[key]primitive.ObjectID)
	toDelete := make([]primitive.ObjectID, 0)

	for _, r := range rewards {
		if r == nil || r.CheckinRewardConfigId.IsZero() {
			continue
		}
		k := key{r.ImServerUserId, r.CheckinRewardConfigId}
		if _, exists := kept[k]; exists {
			toDelete = append(toDelete, r.ID)
		} else {
			kept[k] = r.ID
		}
	}

	if len(toDelete) == 0 {
		return 0, nil
	}

	res, err := o.Collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": toDelete}})
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}

// 允许的阶段连续天数：仅 7、30、90、180、365 可保留，每阶段每用户只保留一条
var allowedContinuousStreaks = map[int]struct{}{7: {}, 30: {}, 90: {}, 180: {}, 365: {}}

// CleanupInvalidContinuousRewardsByOrg 清理非法阶段奖励：只保留 7/30/90/180/365 且每阶段每用户一条，其余返回待删除列表（调用方先冲减钱包再删除）
func (o *CheckinRewardDao) CleanupInvalidContinuousRewardsByOrg(ctx context.Context, orgId primitive.ObjectID) (toDelete []*CheckinReward, err error) {
	if orgId.IsZero() {
		return nil, nil
	}
	filter := bson.M{"org_id": orgId, "source": CheckinRewardSourceContinuous}
	opts := options.Find().SetSort(bson.D{
		{Key: "im_server_user_id", Value: 1},
		{Key: "created_at", Value: 1},
		{Key: "_id", Value: 1},
	})
	rewards, err := mongoutil.Find[*CheckinReward](ctx, o.Collection, filter, opts)
	if err != nil || len(rewards) == 0 {
		return nil, err
	}

	type userStreak struct {
		User   string
		Streak int
	}
	kept := make(map[userStreak]struct{})
	toDelete = make([]*CheckinReward, 0)

	for _, r := range rewards {
		if r == nil {
			continue
		}
		streak := 0
		if r.Description != "" {
			streak, _ = strconv.Atoi(r.Description)
		}
		if _, allowed := allowedContinuousStreaks[streak]; !allowed {
			toDelete = append(toDelete, r)
			continue
		}
		k := userStreak{r.ImServerUserId, streak}
		if _, exists := kept[k]; exists {
			toDelete = append(toDelete, r)
		} else {
			kept[k] = struct{}{}
		}
	}
	return toDelete, nil
}

// FixDailyRewardsByOrg 修复指定组织下的“非阶段性奖励”（daily）数据：
// - 若奖励缺少 checkin_id，则按 (im_server_user_id, org_id, checkin_date 当天) 反查签到记录并补全；
// - 若奖励缺少 checkin_date，但有 checkin_id，则从签到记录补全；
// - 若 description 为空或与签到记录的 streak 不一致，则同步为签到记录的 streak。
// 返回被更新的奖励条数。
func (o *CheckinRewardDao) FixDailyRewardsByOrg(ctx context.Context, orgId primitive.ObjectID) (int64, error) {
	if orgId.IsZero() {
		return 0, nil
	}

	// 查出该组织下所有日常签到奖励
	filter := bson.M{
		"org_id": orgId,
		"source": CheckinRewardSourceDaily,
	}
	rewards, err := mongoutil.Find[*CheckinReward](ctx, o.Collection, filter)
	if err != nil {
		return 0, err
	}
	if len(rewards) == 0 {
		return 0, nil
	}

	checkinColl := o.DB.Collection(Checkin{}.TableName())

	// 缓存，避免重复查询
	checkinByID := make(map[primitive.ObjectID]*Checkin)
	checkinByKey := make(map[string]*Checkin) // key = imServerUserId|orgId|YYYY-MM-DD

	var updated int64

	for _, r := range rewards {
		if r == nil {
			continue
		}

		var ch *Checkin

		// 1) 优先通过 checkin_id 查找签到记录
		if !r.CheckinId.IsZero() {
			if cached, ok := checkinByID[r.CheckinId]; ok {
				ch = cached
			} else {
				c, err := mongoutil.FindOne[*Checkin](ctx, checkinColl, bson.M{"_id": r.CheckinId})
				if err == nil && c != nil {
					checkinByID[r.CheckinId] = c
					ch = c
				}
			}
		}

		// 2) 若没有 checkin_id 或未查到记录，则根据 (user, org, 日期) 反查签到
		if ch == nil && !r.CheckinDate.IsZero() && r.ImServerUserId != "" {
			day := utils.TimeToCST(r.CheckinDate)
			dayStr := day.Format("2006-01-02")
			key := r.ImServerUserId + "|" + orgId.Hex() + "|" + dayStr

			if cached, ok := checkinByKey[key]; ok {
				ch = cached
			} else {
				dayStart := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, utils.CST)
				dayEnd := dayStart.AddDate(0, 0, 1).Add(-time.Nanosecond)

				cList, err := mongoutil.Find[*Checkin](ctx, checkinColl, bson.M{
					"im_server_user_id": r.ImServerUserId,
					"org_id":            orgId,
					"date": bson.M{
						"$gte": dayStart,
						"$lte": dayEnd,
					},
				})
				if err == nil && len(cList) > 0 {
					// 如有多条，同一天只取第一条即可
					c := cList[0]
					checkinByKey[key] = c
					ch = c
				}
			}
		}

		// 如果仍然找不到签到记录，就无法修复该条奖励，跳过
		if ch == nil {
			continue
		}

		update := bson.M{}

		if r.CheckinId.IsZero() {
			update["checkin_id"] = ch.ID
		}
		if r.CheckinDate.IsZero() {
			update["checkin_date"] = ch.Date
		}

		streakStr := strconv.Itoa(ch.Streak)
		if r.Description == "" || r.Description != streakStr {
			update["description"] = streakStr
		}
		if len(update) == 0 {
			continue
		}

		update["updated_at"] = time.Now()

		if err := o.Collection.FindOneAndUpdate(ctx, bson.M{"_id": r.ID}, bson.M{"$set": update}).Err(); err != nil {
			continue
		}
		updated++
	}

	return updated, nil
}

type CheckinRewardJoinAll struct {
	*CheckinReward   `bson:",inline"`
	Checkin          map[string]interface{} `bson:"checkin"`
	User             map[string]interface{} `bson:"user"`
	OrganizationUser map[string]interface{} `bson:"organization_user"`
	Attribute        map[string]interface{} `bson:"attribute"`
}

// resolveKeywordToImServerUserIds 根据关键字在 user/attribute 中查找，再通过 organization_user 解析出本组织的 im_server_user_id 列表。
// user 表查出的为 im_server_user_id，用 org 的 im_server_user_id 匹配；attribute 表查出的为业务 user_id，用 org 的 user_id 匹配，避免混用导致搜不到。
func resolveKeywordToImServerUserIds(ctx context.Context, db *mongo.Database, orgId primitive.ObjectID, keyword string, limit int) ([]string, error) {
	if keyword == "" || orgId.IsZero() {
		return nil, nil
	}
	if limit <= 0 {
		limit = 1000
	}
	imIDSet := make(map[string]struct{})

	// 1）user 表：昵称或 user_id 模糊匹配，得到的是 im_server_user_id
	userColl := db.Collection(openImModel.User{}.TableName())
	userCur, err := userColl.Find(ctx, bson.M{
		"$or": []bson.M{
			{"nickname": bson.M{"$regex": keyword, "$options": "i"}},
			{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
		},
	}, options.Find().SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	var users []*openImModel.User
	if err = userCur.All(ctx, &users); err != nil {
		return nil, err
	}
	imIDsFromUser := make([]string, 0)
	for _, u := range users {
		if u != nil && u.UserID != "" {
			imIDsFromUser = append(imIDsFromUser, u.UserID)
		}
	}
	// 本组织下这些 im_server_user_id 的保留
	if len(imIDsFromUser) > 0 {
		orgColl := db.Collection(orgModel.OrganizationUser{}.TableName())
		orgCur, err := orgColl.Find(ctx, bson.M{
			"organization_id":   orgId,
			"im_server_user_id": bson.M{"$in": imIDsFromUser},
		}, options.Find().SetLimit(int64(limit)))
		if err != nil {
			return nil, err
		}
		var orgUsers []*orgModel.OrganizationUser
		if err = orgCur.All(ctx, &orgUsers); err != nil {
			return nil, err
		}
		for _, ou := range orgUsers {
			if ou != nil && ou.ImServerUserId != "" {
				imIDSet[ou.ImServerUserId] = struct{}{}
			}
		}
	}

	// 2）attribute 表：account 模糊匹配，得到的是业务 user_id
	attrColl := db.Collection(chatModel.Attribute{}.TableName())
	attrCur, err := attrColl.Find(ctx, bson.M{"account": bson.M{"$regex": keyword, "$options": "i"}}, options.Find().SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	var attrs []*chatModel.Attribute
	if err = attrCur.All(ctx, &attrs); err != nil {
		return nil, err
	}
	userIDsFromAttr := make([]string, 0)
	for _, a := range attrs {
		if a != nil && a.UserID != "" {
			userIDsFromAttr = append(userIDsFromAttr, a.UserID)
		}
	}
	if len(userIDsFromAttr) > 0 {
		orgColl := db.Collection(orgModel.OrganizationUser{}.TableName())
		orgCur, err := orgColl.Find(ctx, bson.M{
			"organization_id": orgId,
			"user_id":         bson.M{"$in": userIDsFromAttr},
		}, options.Find().SetLimit(int64(limit)))
		if err != nil {
			return nil, err
		}
		var orgUsers []*orgModel.OrganizationUser
		if err = orgCur.All(ctx, &orgUsers); err != nil {
			return nil, err
		}
		for _, ou := range orgUsers {
			if ou != nil && ou.ImServerUserId != "" {
				imIDSet[ou.ImServerUserId] = struct{}{}
			}
		}
	}

	imIds := make([]string, 0, len(imIDSet))
	for id := range imIDSet {
		imIds = append(imIds, id)
	}
	return imIds, nil
}

func (o *CheckinRewardDao) SelectJoinAll(ctx context.Context, imServerUserId, keyword string, orgId primitive.ObjectID, status CheckinRewardStatus,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (int64, []*CheckinRewardJoinAll, error) {
	// 1. 构建主表过滤条件（与“签到日期”筛选一致：使用 checkin_date 字段）
	baseMatch := bson.M{}

	if imServerUserId != "" {
		baseMatch["im_server_user_id"] = imServerUserId
	}

	if !orgId.IsZero() {
		baseMatch["org_id"] = orgId
	}

	if status != "" {
		baseMatch["status"] = status
	}

	tsTime := bson.M{}
	if !startTime.IsZero() {
		tsTime["$gte"] = startTime
	}
	if !endTime.IsZero() {
		tsTime["$lte"] = endTime
	}
	if len(tsTime) > 0 {
		baseMatch["checkin_date"] = tsTime
	}

	// 关键字：先解析为 im_server_user_id 列表，再在主表上过滤，避免先全量 join 再 regex
	if keyword != "" {
		imIds, err := resolveKeywordToImServerUserIds(ctx, o.DB, orgId, keyword, 1000)
		if err != nil {
			return 0, nil, err
		}
		if len(imIds) == 0 {
			return 0, []*CheckinRewardJoinAll{}, nil
		}
		baseMatch["im_server_user_id"] = bson.M{"$in": imIds}
	}

	// 2. 总数：主表 count，不再跑重聚合
	total, err := mongoutil.Count(ctx, o.Collection, baseMatch)
	if err != nil {
		return 0, nil, err
	}
	if total == 0 {
		return 0, []*CheckinRewardJoinAll{}, nil
	}

	// 3. 先分页：主表按 checkin_date 倒序 + skip/limit，只取当前页奖励
	skip := int64(0)
	limit := int64(10)
	if page != nil {
		if page.Page > 0 && page.PageSize > 0 {
			skip = int64((page.Page - 1) * page.PageSize)
			limit = int64(page.PageSize)
		}
	}
	opts := options.Find().SetSort(bson.D{{Key: "checkin_date", Value: -1}}).SetSkip(skip).SetLimit(limit)
	rewards, err := mongoutil.Find[*CheckinReward](ctx, o.Collection, baseMatch, opts)
	if err != nil {
		return 0, nil, err
	}
	if len(rewards) == 0 {
		return total, []*CheckinRewardJoinAll{}, nil
	}

	// 4. 收集当前页涉及的 im_server_user_id、checkin_id
	imIDSet := make(map[string]struct{})
	checkinIDSet := make(map[primitive.ObjectID]struct{})
	for _, r := range rewards {
		if r == nil {
			continue
		}
		if r.ImServerUserId != "" {
			imIDSet[r.ImServerUserId] = struct{}{}
		}
		if !r.CheckinId.IsZero() {
			checkinIDSet[r.CheckinId] = struct{}{}
		}
	}
	imIDs := make([]string, 0, len(imIDSet))
	for id := range imIDSet {
		imIDs = append(imIDs, id)
	}
	checkinIDs := make([]primitive.ObjectID, 0, len(checkinIDSet))
	for id := range checkinIDSet {
		checkinIDs = append(checkinIDs, id)
	}

	// 5. 批量查 organization_user（本组织）
	orgUserMap := make(map[string]*orgModel.OrganizationUser)
	if len(imIDs) > 0 {
		orgFilter := bson.M{"im_server_user_id": bson.M{"$in": imIDs}}
		if !orgId.IsZero() {
			orgFilter["organization_id"] = orgId
		}
		orgColl := o.DB.Collection(orgModel.OrganizationUser{}.TableName())
		orgUsers, err := mongoutil.Find[*orgModel.OrganizationUser](ctx, orgColl, orgFilter)
		if err != nil {
			return 0, nil, err
		}
		for _, ou := range orgUsers {
			if ou != nil {
				orgUserMap[ou.ImServerUserId] = ou
			}
		}
	}

	// 6. 批量查 user（OpenIM user_id 即 im_server_user_id）、attribute（用 org 的 user_id）、checkin
	userMap := make(map[string]*openImModel.User)
	if len(imIDs) > 0 {
		userColl := o.DB.Collection(openImModel.User{}.TableName())
		users, err := mongoutil.Find[*openImModel.User](ctx, userColl, bson.M{"user_id": bson.M{"$in": imIDs}})
		if err != nil {
			return 0, nil, err
		}
		for _, u := range users {
			if u != nil {
				userMap[u.UserID] = u
			}
		}
	}
	userIDSet := make(map[string]struct{})
	for _, ou := range orgUserMap {
		if ou.UserId != "" {
			userIDSet[ou.UserId] = struct{}{}
		}
	}
	userIDs := make([]string, 0, len(userIDSet))
	for id := range userIDSet {
		userIDs = append(userIDs, id)
	}
	attrMap := make(map[string]*chatModel.Attribute)
	if len(userIDs) > 0 {
		attrColl := o.DB.Collection(chatModel.Attribute{}.TableName())
		attrs, err := mongoutil.Find[*chatModel.Attribute](ctx, attrColl, bson.M{"user_id": bson.M{"$in": userIDs}})
		if err != nil {
			return 0, nil, err
		}
		for _, a := range attrs {
			if a != nil {
				attrMap[a.UserID] = a
			}
		}
	}

	checkinMap := make(map[primitive.ObjectID]*Checkin)
	if len(checkinIDs) > 0 {
		checkinColl := o.DB.Collection(Checkin{}.TableName())
		checkins, err := mongoutil.Find[*Checkin](ctx, checkinColl, bson.M{"_id": bson.M{"$in": checkinIDs}})
		if err != nil {
			return 0, nil, err
		}
		for _, ch := range checkins {
			if ch != nil {
				checkinMap[ch.ID] = ch
			}
		}
	}

	// 6.5 对无 checkin_id 的奖励按 (用户, 组织, 签到日期) 补查 checkin，避免列表里「签到连续天数」缺失
	checkinByDateMap := make(map[string]*Checkin)
	var minDate, maxDate time.Time
	for _, r := range rewards {
		if r == nil || !r.CheckinId.IsZero() || r.CheckinDate.IsZero() {
			continue
		}
		d := utils.TimeToCST(r.CheckinDate)
		dayStart := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, utils.CST)
		if minDate.IsZero() || dayStart.Before(minDate) {
			minDate = dayStart
		}
		dayEnd := dayStart.AddDate(0, 0, 1).Add(-time.Nanosecond)
		if maxDate.IsZero() || dayEnd.After(maxDate) {
			maxDate = dayEnd
		}
	}
	if !minDate.IsZero() && !maxDate.IsZero() && len(imIDs) > 0 {
		checkinColl := o.DB.Collection(Checkin{}.TableName())
		filter := bson.M{
			"im_server_user_id": bson.M{"$in": imIDs},
			"date":              bson.M{"$gte": minDate, "$lte": maxDate},
		}
		if !orgId.IsZero() {
			filter["org_id"] = orgId
		}
		opts := options.Find().SetSort(bson.M{"date": 1})
		checkins, err := mongoutil.Find[*Checkin](ctx, checkinColl, filter, opts)
		if err == nil {
			for _, ch := range checkins {
				if ch == nil {
					continue
				}
				key := ch.ImServerUserId + "|" + ch.OrgId.Hex() + "|" + utils.TimeToCST(ch.Date).Format("2006-01-02")
				checkinByDateMap[key] = ch
			}
		}
	}

	// 7. 组装 CheckinRewardJoinAll（与原先聚合结构一致，保证前端不缺失数据）
	result := make([]*CheckinRewardJoinAll, 0, len(rewards))
	for _, r := range rewards {
		if r == nil {
			continue
		}
		item := &CheckinRewardJoinAll{CheckinReward: r}

		if ou, ok := orgUserMap[r.ImServerUserId]; ok && ou != nil {
			item.OrganizationUser = map[string]interface{}{
				"user_id":           ou.UserId,
				"organization_id":   ou.OrganizationId,
				"im_server_user_id": ou.ImServerUserId,
			}
			if attr, ok := attrMap[ou.UserId]; ok && attr != nil {
				item.Attribute = map[string]interface{}{
					"account":  attr.Account,
					"nickname": attr.Nickname,
					"user_id":  attr.UserID,
				}
			}
		}
		// User 表用 im_server_user_id 查，不依赖 org.user_id 是否与 IM 一致
		if u, ok := userMap[r.ImServerUserId]; ok && u != nil {
			item.User = map[string]interface{}{
				"user_id":     u.UserID,
				"nickname":    u.Nickname,
				"face_url":    u.FaceURL,
				"create_time": u.CreateTime,
			}
		}
		if ch, ok := checkinMap[r.CheckinId]; ok && ch != nil {
			item.Checkin = map[string]interface{}{
				"date":   ch.Date,
				"streak": ch.Streak,
				"_id":    ch.ID,
			}
		} else if r.CheckinId.IsZero() && !r.CheckinDate.IsZero() {
			key := r.ImServerUserId + "|" + r.OrgID.Hex() + "|" + utils.TimeToCST(r.CheckinDate).Format("2006-01-02")
			if ch, ok := checkinByDateMap[key]; ok && ch != nil {
				item.Checkin = map[string]interface{}{
					"date":   ch.Date,
					"streak": ch.Streak,
					"_id":    ch.ID,
				}
			}
		}
		result = append(result, item)
	}

	return total, result, nil
}

func (o *CheckinRewardDao) SelectJoinOgrUser(ctx context.Context, imServerUserId string, orgId primitive.ObjectID, status CheckinRewardStatus,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (int64, []*CheckinReward, error) {
	// 聚合查询
	pipeline := []bson.M{
		{
			"$lookup": bson.M{
				"from":         orgModel.OrganizationUser{}.TableName(),
				"localField":   "im_server_user_id",
				"foreignField": "im_server_user_id",
				"as":           "org_user",
			},
		},
	}

	// 构建过滤条件
	filter := bson.M{}

	if imServerUserId != "" {
		filter["im_server_user_id"] = imServerUserId
	}

	if !orgId.IsZero() {
		filter["org_user.organization_id"] = orgId
	}

	if status != "" {
		filter["status"] = status
	}

	tsTime := bson.M{}
	if !startTime.IsZero() {
		tsTime["$gte"] = startTime
	}
	if !endTime.IsZero() {
		tsTime["$lte"] = endTime
	}
	if len(tsTime) > 0 {
		// 使用奖励表自身的 checkin_date 字段进行时间过滤
		filter["checkin_date"] = tsTime
	}

	findPipeline := make([]bson.M, 0)
	countPipeline := make([]bson.M, 0)

	if len(filter) > 0 {
		findPipeline = append(pipeline, bson.M{"$match": filter})
		countPipeline = append(pipeline, bson.M{"$match": filter})
	}

	// 按时间倒序排列：从新到旧
	findPipeline = append(findPipeline, bson.M{"$sort": bson.M{"created_at": -1}})

	// 添加排序和分页
	if page != nil {
		findPipeline = append(findPipeline, page.ToBsonMList()...)
	}

	// 执行聚合查询获取数据
	data, err := mongoutil.Aggregate[*CheckinReward](ctx, o.Collection, findPipeline)
	if err != nil {
		return 0, nil, err
	}

	countPipeline = append(countPipeline, bson.M{"$count": "total"})

	var countResult []bson.M
	cursor, err := o.Collection.Aggregate(ctx, countPipeline)
	if err != nil {
		return 0, nil, err
	}
	if err = cursor.All(ctx, &countResult); err != nil {
		return 0, nil, err
	}

	total := int32(0)
	if len(countResult) > 0 {
		total = countResult[0]["total"].(int32)
	}

	return int64(total), data, nil
}

func (o *CheckinRewardDao) SelectByCheckinId(ctx context.Context, checkinId primitive.ObjectID) ([]*CheckinReward, error) {
	filter := bson.M{}

	if !checkinId.IsZero() {
		filter["checkin_id"] = checkinId
	}

	opts := make([]*options.FindOptions, 0)
	if len(filter) == 0 {
		filter = nil
	}

	data, err := mongoutil.Find[*CheckinReward](ctx, o.Collection, filter, opts...)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// ExistContinuousByOrgIdAndImServerUserIdAndConfigId 是否已有该用户本组织下该阶段配置的连续签到奖励（用于阶段奖励只发一次的判重）
func (o *CheckinRewardDao) ExistContinuousByOrgIdAndImServerUserIdAndConfigId(ctx context.Context, orgId primitive.ObjectID, imServerUserId string, configId primitive.ObjectID) (bool, error) {
	if orgId.IsZero() || imServerUserId == "" || configId.IsZero() {
		return false, nil
	}
	filter := bson.M{
		"org_id":                   orgId,
		"im_server_user_id":        imServerUserId,
		"checkin_reward_config_id": configId,
		"source":                   CheckinRewardSourceContinuous,
	}
	n, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
