package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Checkin struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	ImServerUserId string             `bson:"im_server_user_id" json:"im_server_user_id"`
	OrgId          primitive.ObjectID `bson:"org_id" json:"org_id"`
	Date           time.Time          `bson:"date" json:"date"`
	Streak         int                `bson:"streak" json:"streak"` // 当前连续签到天数

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}

func (Checkin) TableName() string {
	return constant.CollectionCheckin
}

func CreateCheckinIndex(db *mongo.Database) error {
	m := &Checkin{}

	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "im_server_user_id", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "date", Value: 1},
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
	})
	return err
}

type CheckInDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewCheckinDao(db *mongo.Database) *CheckInDao {
	return &CheckInDao{
		DB:         db,
		Collection: db.Collection(Checkin{}.TableName()),
	}
}

func (o *CheckInDao) GetByImServerUserId(ctx context.Context, imServerUserId string) (*Checkin, error) {
	return mongoutil.FindOne[*Checkin](ctx, o.Collection, bson.M{"im_server_user_id": imServerUserId})
}

func (o *CheckInDao) GetByImServerUserIdAndDate(ctx context.Context, imServerUserId string, date time.Time) (*Checkin, error) {
	return o.GetByImServerUserIdAndDateAndOrgId(ctx, imServerUserId, primitive.NilObjectID, date)
}

// GetByImServerUserIdAndDateAndOrgId 获取用户在指定组织下某日的签到记录（用于按组织判断“今天是否已签到”）
func (o *CheckInDao) GetByImServerUserIdAndDateAndOrgId(ctx context.Context, imServerUserId string, orgId primitive.ObjectID, date time.Time) (*Checkin, error) {
	filter := bson.M{"im_server_user_id": imServerUserId, "date": date}
	if !orgId.IsZero() {
		filter["org_id"] = orgId
	}
	return mongoutil.FindOne[*Checkin](ctx, o.Collection, filter)
}

func (o *CheckInDao) Create(ctx context.Context, obj *Checkin) error {
	// 如果没有设置创建时间，则设置
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = utils.NowCST()
	}
	// 如果没有设置ID，则生成
	if obj.ID.IsZero() {
		obj.ID = primitive.NewObjectID()
	}
	return mongoutil.InsertMany(ctx, o.Collection, []*Checkin{obj})
}

// GetLatestCheckInByImServerUserId 获取用户最近一次签到记录（按创建时间，用于补签等需要“最后写入”语义的场景）
func (o *CheckInDao) GetLatestCheckInByImServerUserId(ctx context.Context, imServerUserId string) (*Checkin, error) {
	filter := bson.M{"im_server_user_id": imServerUserId}
	opts := options.FindOne().SetSort(bson.M{"created_at": -1})

	// 查找记录
	var record Checkin
	err := o.Collection.FindOne(ctx, filter, opts).Decode(&record)
	if err != nil {
		return nil, err
	}

	// 将记录的日期转换为CST时区
	record.Date = utils.TimeToCST(record.Date)
	record.CreatedAt = utils.TimeToCST(record.CreatedAt)

	// 添加详细日志记录
	if ctx.Value("log_context") != nil {
		log.ZInfo(ctx, "查询到最近签到记录",
			"userID", imServerUserId,
			"签到日期", record.Date.Format("2006-01-02"),
			"创建时间", record.CreatedAt.Format("2006-01-02 15:04:05"),
			"连续签到", record.Streak,
			"记录ID", record.ID.Hex())
	}

	return &record, nil
}

// GetLatestCheckInByDateByImServerUserId 获取用户「签到日期」最新的一条记录（同一天多条时取创建时间最新）
// 用于查询接口返回连续签到天数：应展示“最近一次签到日”的 streak，而非“最后创建”的那条（补签会导致后者不是最近签到日）
func (o *CheckInDao) GetLatestCheckInByDateByImServerUserId(ctx context.Context, imServerUserId string) (*Checkin, error) {
	return o.GetLatestCheckInByDateByImServerUserIdAndOrgId(ctx, imServerUserId, primitive.NilObjectID)
}

// GetLatestCheckInByDateByImServerUserIdAndOrgId 获取用户在指定组织下「签到日期」最新的一条记录（用于按组织展示连续签到天数）
func (o *CheckInDao) GetLatestCheckInByDateByImServerUserIdAndOrgId(ctx context.Context, imServerUserId string, orgId primitive.ObjectID) (*Checkin, error) {
	filter := bson.M{"im_server_user_id": imServerUserId}
	if !orgId.IsZero() {
		filter["org_id"] = orgId
	}
	opts := options.FindOne().SetSort(bson.D{{Key: "date", Value: -1}, {Key: "created_at", Value: -1}})
	var record Checkin
	err := o.Collection.FindOne(ctx, filter, opts).Decode(&record)
	if err != nil {
		return nil, err
	}
	record.Date = utils.TimeToCST(record.Date)
	record.CreatedAt = utils.TimeToCST(record.CreatedAt)
	return &record, nil
}

// GetAllByImServerUserId 获取用户所有签到记录（不区分组织，用于全局修复等场景）
func (o *CheckInDao) GetAllByImServerUserId(ctx context.Context, imServerUserId string) ([]*Checkin, error) {
	return o.GetAllByImServerUserIdAndOrgId(ctx, imServerUserId, primitive.NilObjectID)
}

// GetAllByImServerUserIdAndOrgId 获取用户在指定组织下的所有签到记录（按日期升序）
// 计算连续签到天数时必须按组织过滤，否则多组织或历史数据会混在一起导致 streak 被算小
func (o *CheckInDao) GetAllByImServerUserIdAndOrgId(ctx context.Context, imServerUserId string, orgId primitive.ObjectID) ([]*Checkin, error) {
	filter := bson.M{"im_server_user_id": imServerUserId}
	if !orgId.IsZero() {
		filter["org_id"] = orgId
	}

	// 按日期排序
	opts := options.Find().SetSort(bson.M{"date": 1})

	checkins, err := mongoutil.Find[*Checkin](ctx, o.Collection, filter, opts)
	if err != nil {
		return nil, err
	}

	return checkins, nil
}

// UpdateStreak 更新签到记录的连续签到天数
func (o *CheckInDao) UpdateStreak(ctx context.Context, id primitive.ObjectID, streak int) error {
	filter := bson.M{"_id": id}
	update := bson.M{"$set": bson.M{"streak": streak}}
	return mongoutil.UpdateOne(ctx, o.Collection, filter, update, false)
}

// GetAllUserIds 获取所有有签到记录的用户ID
func (o *CheckInDao) GetAllUserIds(ctx context.Context) ([]string, error) {
	pipeline := []bson.M{
		{
			"$group": bson.M{
				"_id": "$im_server_user_id",
			},
		},
		{
			"$project": bson.M{
				"_id":     0,
				"user_id": "$_id",
			},
		},
	}

	// 执行聚合查询
	cursor, err := o.Collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// 解析结果
	var results []struct {
		UserId string `bson:"user_id"`
	}
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	// 提取用户ID
	userIds := make([]string, 0, len(results))
	for _, result := range results {
		userIds = append(userIds, result.UserId)
	}

	log.ZInfo(ctx, "获取所有签到用户ID成功", "用户数量", len(userIds))
	return userIds, nil
}

// UserOrgPair 用户+组织对，用于按组织分组修复 streak
type UserOrgPair struct {
	ImServerUserId string
	OrgId          primitive.ObjectID
}

// GetAllUserOrgPairs 获取所有有签到记录的 (im_server_user_id, org_id) 对
func (o *CheckInDao) GetAllUserOrgPairs(ctx context.Context) ([]UserOrgPair, error) {
	pipeline := []bson.M{
		{"$match": bson.M{"org_id": bson.M{"$exists": true, "$ne": primitive.NilObjectID}}},
		{"$group": bson.M{
			"_id": bson.M{
				"im_server_user_id": "$im_server_user_id",
				"org_id":            "$org_id",
			},
		}},
		{"$project": bson.M{
			"_id":               0,
			"im_server_user_id": "$_id.im_server_user_id",
			"org_id":            "$_id.org_id",
		}},
	}
	cursor, err := o.Collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []struct {
		ImServerUserId string             `bson:"im_server_user_id"`
		OrgId          primitive.ObjectID `bson:"org_id"`
	}
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	pairs := make([]UserOrgPair, 0, len(results))
	for _, r := range results {
		if r.ImServerUserId != "" && !r.OrgId.IsZero() {
			pairs = append(pairs, UserOrgPair{ImServerUserId: r.ImServerUserId, OrgId: r.OrgId})
		}
	}
	return pairs, nil
}

func (o *CheckInDao) Select(ctx context.Context, imServerUserId string, orgId primitive.ObjectID,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (int64, []*Checkin, error) {
	filter := bson.M{}

	if imServerUserId != "" {
		filter["im_server_user_id"] = imServerUserId
	}

	if !orgId.IsZero() {
		filter["org_id"] = orgId
	}

	tsTime := bson.M{}
	if !startTime.IsZero() {
		tsTime["$gte"] = startTime
	}
	if !endTime.IsZero() {
		tsTime["$lte"] = endTime
	}
	if len(tsTime) > 0 {
		filter["date"] = tsTime // 修改为按date字段过滤，而不是created_at
	}

	opts := make([]*options.FindOptions, 0)
	// 默认按date降序排序（签到日期）
	opts = append(opts, options.Find().SetSort(bson.M{"date": -1}))

	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	if len(filter) == 0 {
		filter = nil
	}

	data, err := mongoutil.Find[*Checkin](ctx, o.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}

func (o *CheckInDao) SelectJoinOgrUserAndUser(ctx context.Context, imServerUserId string, keyword string, orgId primitive.ObjectID,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (int64, []*Checkin, error) {
	// 1. 基于主表字段构建过滤条件（充分利用索引）
	baseMatch := bson.M{}

	if imServerUserId != "" {
		baseMatch["im_server_user_id"] = imServerUserId
	}

	if !orgId.IsZero() {
		baseMatch["org_id"] = orgId
	}

	tsTime := bson.M{}
	if !startTime.IsZero() {
		tsTime["$gte"] = startTime
	}
	if !endTime.IsZero() {
		tsTime["$lte"] = endTime
	}
	if len(tsTime) > 0 {
		baseMatch["date"] = tsTime
	}

	// 2. 关键字搜索：复用 CheckinReward 的解析逻辑，先解析出本组织下的 im_server_user_id，再在主表过滤
	if keyword != "" && imServerUserId == "" && !orgId.IsZero() {
		imIDs, err := resolveKeywordToImServerUserIds(ctx, o.DB, orgId, keyword, 1000)
		if err != nil {
			return 0, nil, err
		}
		if len(imIDs) == 0 {
			return 0, []*Checkin{}, nil
		}
		baseMatch["im_server_user_id"] = bson.M{"$in": imIDs}
	}

	// 3. 统计总数：直接在主表 count，避免聚合开销
	total, err := mongoutil.Count(ctx, o.Collection, baseMatch)
	if err != nil {
		return 0, nil, err
	}
	if total == 0 {
		return 0, []*Checkin{}, nil
	}

	// 4. 构造分页 + 排序（默认按 date 倒序，可额外按指定字段降序）
	sort := bson.D{}
	if page != nil && page.Order != "" {
		sort = append(sort, bson.E{Key: page.Order, Value: -1})
	}
	// 始终保证按签到日期倒序
	sort = append(sort, bson.E{Key: "date", Value: -1})

	opts := options.Find().SetSort(sort)
	// 仅在明确传入分页参数时才做 skip/limit；像补签日历这种不传分页的场景，需要拿到完整日期集合用于渲染
	if page != nil && page.Page > 0 && page.PageSize > 0 {
		skip := int64((page.Page - 1) * page.PageSize)
		limit := int64(page.PageSize)
		opts = opts.SetSkip(skip).SetLimit(limit)
	}
	data, err := mongoutil.Find[*Checkin](ctx, o.Collection, baseMatch, opts)
	if err != nil {
		return 0, nil, err
	}

	return total, data, nil
}
