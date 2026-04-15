package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// 用户的抽奖记录
type LotteryUserRecord struct {
	ID                  primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	ImServerUserId      string             `bson:"im_server_user_id" json:"im_server_user_id"`                 // 用户ID
	LotteryId           primitive.ObjectID `bson:"lottery_id" json:"lottery_id"`                               // 抽奖活动ID
	LotteryUserTicketId primitive.ObjectID `bson:"lottery_user_ticket_id" json:"lottery_user_ticket_id"`       // 用户抽奖券ID
	WinTime             time.Time          `bson:"win_time" json:"win_time"`                                   // 中奖时间
	IsWin               bool               `bson:"is_win" json:"is_win"`                                       // 是否中奖
	RewardId            primitive.ObjectID `bson:"reward_id,omitempty" json:"reward_id,omitempty"`             // 奖品ID（空ObjectID表示未中奖）
	Status              int                `bson:"status" json:"status"`                                       // 发放状态：0-未发放，1-已发放
	DistributeTime      *time.Time         `bson:"distribute_time,omitempty" json:"distribute_time,omitempty"` // 发放时间（可空）
	CreatedAt           time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt           time.Time          `bson:"updated_at" json:"updated_at"`
}

// HasReward 检查是否有奖品
func (r *LotteryUserRecord) HasReward() bool {
	return !r.RewardId.IsZero()
}

// SetNoReward 设置为未中奖
func (r *LotteryUserRecord) SetNoReward() {
	r.RewardId = primitive.NilObjectID
	r.IsWin = false
}

// SetReward 设置中奖奖品
func (r *LotteryUserRecord) SetReward(rewardId primitive.ObjectID) {
	r.RewardId = rewardId
	r.IsWin = !rewardId.IsZero()
}

func (LotteryUserRecord) TableName() string {
	return constant.CollectionLotteryUserRecord
}

// CreateLotteryUserRecordIndex 创建抽奖用户记录表索引
func CreateLotteryUserRecordIndex(db *mongo.Database) error {
	m := &LotteryUserRecord{}
	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		// 核心查询字段索引
		{
			Keys: bson.D{
				{Key: "im_server_user_id", Value: 1},
			},
		},
		// 抽奖活动ID索引
		{
			Keys: bson.D{
				{Key: "lottery_id", Value: 1},
			},
		},
		// 中奖时间索引（用于排序和时间范围查询）
		{
			Keys: bson.D{
				{Key: "win_time", Value: -1},
			},
		},
		// 复合索引：用户+抽奖活动（用户端查询优化）
		{
			Keys: bson.D{
				{Key: "im_server_user_id", Value: 1},
				{Key: "lottery_id", Value: 1},
				{Key: "win_time", Value: -1},
			},
		},
		// 是否中奖索引
		{
			Keys: bson.D{
				{Key: "is_win", Value: 1},
			},
		},
		// 发放状态索引
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
			},
		},
		// 奖品ID索引
		{
			Keys: bson.D{
				{Key: "reward_id", Value: 1},
			},
		},
		// 发放时间索引
		{
			Keys: bson.D{
				{Key: "distribute_time", Value: -1},
			},
		},
		// 复合索引：状态+中奖时间（管理端查询优化）
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
				{Key: "win_time", Value: -1},
			},
		},
		// 复合索引：是否中奖+中奖时间
		{
			Keys: bson.D{
				{Key: "is_win", Value: 1},
				{Key: "win_time", Value: -1},
			},
		},
	})
	return err
}

type LotteryUserRecordDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewLotteryUserRecordDao(db *mongo.Database) *LotteryUserRecordDao {
	return &LotteryUserRecordDao{
		DB:         db,
		Collection: db.Collection(LotteryUserRecord{}.TableName()),
	}
}

// Create 插入抽奖记录（用于对接抽奖逻辑）
func (o *LotteryUserRecordDao) Create(ctx context.Context, obj *LotteryUserRecord) error {
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	//obj.WinTime = time.Now().UTC()

	// 根据奖品ID判断是否中奖
	obj.IsWin = !obj.RewardId.IsZero()

	if obj.IsWin {
		obj.WinTime = time.Now().UTC()
	}

	// 默认未发放状态
	obj.Status = 0

	return mongoutil.InsertMany(ctx, o.Collection, []*LotteryUserRecord{obj})
}

// SelectAdminRecords 管理端查询抽奖记录
func (o *LotteryUserRecordDao) SelectAdminRecords(ctx context.Context,
	organizationId primitive.ObjectID,
	lotteryId *primitive.ObjectID, isWin *bool, status *int,
	winStartTime *time.Time, winEndTime *time.Time,
	keyword string, rewardId *primitive.ObjectID,
	distributeStartTime *time.Time, distributeEndTime *time.Time,
	page *paginationUtils.DepPagination) (int64, []*LotteryUserRecordWithUserInfo, error) {

	// 构建聚合管道
	pipeline := []bson.M{
		// 第一步：连接 user 表获取IM用户信息（包含组织ID）
		{
			"$lookup": bson.M{
				"from":         "user",
				"localField":   "im_server_user_id",
				"foreignField": "user_id",
				"as":           "user",
			},
		},
		// 第二步：按组织ID过滤（使用user表的org_id字段）
		{
			"$match": bson.M{
				"user.org_id": organizationId.Hex(),
			},
		},
		// 第三步：连接 organization_user 表获取用户详细信息
		{
			"$lookup": bson.M{
				"from":         "organization_user",
				"localField":   "im_server_user_id",
				"foreignField": "im_server_user_id",
				"as":           "org_user",
			},
		},
		// 第四步：连接 attribute 表获取用户属性信息
		{
			"$lookup": bson.M{
				"from":         "attribute",
				"localField":   "org_user.user_id",
				"foreignField": "user_id",
				"as":           "attribute",
			},
		},
		// 第五步：连接 lottery_reward 表获取奖品信息（左连接）
		{
			"$lookup": bson.M{
				"from":         "lottery_reward",
				"localField":   "reward_id",
				"foreignField": "_id",
				"as":           "reward",
			},
		},
	}

	// 构建匹配条件
	matchConditions := bson.M{}

	if lotteryId != nil && !lotteryId.IsZero() {
		matchConditions["lottery_id"] = *lotteryId
	}

	if isWin != nil {
		matchConditions["is_win"] = *isWin
	}

	if status != nil {
		matchConditions["status"] = *status
	}

	if rewardId != nil && !rewardId.IsZero() {
		matchConditions["reward_id"] = *rewardId
	}

	// 中奖时间范围
	if winStartTime != nil || winEndTime != nil {
		timeFilter := bson.M{}
		if winStartTime != nil {
			timeFilter["$gte"] = *winStartTime
		}
		if winEndTime != nil {
			timeFilter["$lte"] = *winEndTime
		}
		matchConditions["win_time"] = timeFilter
	}

	// 发放时间范围
	if distributeStartTime != nil || distributeEndTime != nil {
		distTimeFilter := bson.M{}
		if distributeStartTime != nil {
			distTimeFilter["$gte"] = *distributeStartTime
		}
		if distributeEndTime != nil {
			distTimeFilter["$lte"] = *distributeEndTime
		}
		matchConditions["distribute_time"] = distTimeFilter
	}

	// keyword模糊查询（用户昵称，用户ID，账户account）
	if keyword != "" {
		matchConditions["$or"] = []bson.M{
			{"user.nickname": bson.M{"$regex": keyword, "$options": "i"}},
			{"attribute.account": bson.M{"$regex": keyword, "$options": "i"}},
			{"im_server_user_id": bson.M{"$regex": keyword, "$options": "i"}},
		}
	}

	// 添加匹配阶段
	if len(matchConditions) > 0 {
		pipeline = append(pipeline, bson.M{"$match": matchConditions})
	}

	// 添加排序
	pipeline = append(pipeline, bson.M{"$sort": bson.M{"win_time": -1}})

	// 计算总数的管道
	countPipeline := append(pipeline, bson.M{"$count": "total"})

	// 添加分页
	if page != nil {
		pipeline = append(pipeline, page.ToBsonMList()...)
	}

	// 执行聚合查询获取数据
	data, err := mongoutil.Aggregate[*LotteryUserRecordWithUserInfo](ctx, o.Collection, pipeline)
	if err != nil {
		return 0, nil, err
	}

	// 执行计数查询
	var countResult []bson.M
	cursor, err := o.Collection.Aggregate(ctx, countPipeline)
	if err != nil {
		return 0, nil, err
	}
	if err = cursor.All(ctx, &countResult); err != nil {
		return 0, nil, err
	}

	total := int64(0)
	if len(countResult) > 0 {
		if count, ok := countResult[0]["total"].(int32); ok {
			total = int64(count)
		} else if count, ok := countResult[0]["total"].(int64); ok {
			total = count
		}
	}

	return total, data, nil
}

// UpdateDistributeStatus 管理员审核接口 - 更新发放状态
func (o *LotteryUserRecordDao) UpdateDistributeStatus(ctx context.Context, id primitive.ObjectID, status int) error {
	updateField := bson.M{"$set": bson.M{
		"status":          status,
		"distribute_time": time.Now().UTC(),
		"updated_at":      time.Now().UTC(),
	}}
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"_id": id}, updateField, false)
}

// GetRecordById 根据ID查询抽奖记录详情
func (o *LotteryUserRecordDao) GetRecordById(ctx context.Context, id primitive.ObjectID) (*LotteryUserRecord, error) {
	return mongoutil.FindOne[*LotteryUserRecord](ctx, o.Collection, bson.M{"_id": id})
}

// 连表查询结果结构体
type LotteryUserRecordWithUserInfo struct {
	*LotteryUserRecord `bson:",inline"`
	Attribute          []map[string]interface{} `bson:"attribute" json:"attribute"`
	User               []map[string]interface{} `bson:"user" json:"user"`
	Reward             []map[string]interface{} `bson:"reward" json:"reward"`
}

// 用户端连表查询结果结构体
type LotteryUserRecordWithReward struct {
	*LotteryUserRecord `bson:",inline"`
	Reward             []map[string]interface{} `bson:"reward" json:"reward"`
}

// SelectUserRecordsWithReward 用户端查询抽奖记录（连表奖品信息）
func (o *LotteryUserRecordDao) SelectUserRecordsWithReward(ctx context.Context, imServerUserId string,
	lotteryId *primitive.ObjectID, isWin *bool, status *int,
	winStartTime *time.Time, winEndTime *time.Time,
	page *paginationUtils.DepPagination) (int64, []*LotteryUserRecordWithReward, error) {

	// 构建聚合管道
	pipeline := []bson.M{
		// 连接 lottery_reward 表获取奖品信息（左连接）
		{
			"$lookup": bson.M{
				"from":         "lottery_reward",
				"localField":   "reward_id",
				"foreignField": "_id",
				"as":           "reward",
			},
		},
	}

	// 构建匹配条件
	matchConditions := bson.M{}

	if imServerUserId != "" {
		matchConditions["im_server_user_id"] = imServerUserId
	}

	if lotteryId != nil && !lotteryId.IsZero() {
		matchConditions["lottery_id"] = *lotteryId
	}

	if isWin != nil {
		matchConditions["is_win"] = *isWin
	}

	if status != nil {
		matchConditions["status"] = *status
	}

	// 中奖时间范围查询
	if winStartTime != nil || winEndTime != nil {
		timeFilter := bson.M{}
		if winStartTime != nil {
			timeFilter["$gte"] = *winStartTime
		}
		if winEndTime != nil {
			timeFilter["$lte"] = *winEndTime
		}
		matchConditions["win_time"] = timeFilter
	}

	// 添加匹配阶段
	if len(matchConditions) > 0 {
		pipeline = append(pipeline, bson.M{"$match": matchConditions})
	}

	// 添加排序
	pipeline = append(pipeline, bson.M{"$sort": bson.M{"win_time": -1}})

	// 计算总数的管道
	countPipeline := append(pipeline, bson.M{"$count": "total"})

	// 添加分页
	if page != nil {
		pipeline = append(pipeline, page.ToBsonMList()...)
	}

	// 执行聚合查询获取数据
	data, err := mongoutil.Aggregate[*LotteryUserRecordWithReward](ctx, o.Collection, pipeline)
	if err != nil {
		return 0, nil, err
	}

	// 执行计数查询
	var countResult []bson.M
	cursor, err := o.Collection.Aggregate(ctx, countPipeline)
	if err != nil {
		return 0, nil, err
	}
	if err = cursor.All(ctx, &countResult); err != nil {
		return 0, nil, err
	}

	total := int64(0)
	if len(countResult) > 0 {
		if count, ok := countResult[0]["total"].(int32); ok {
			total = int64(count)
		} else if count, ok := countResult[0]["total"].(int64); ok {
			total = count
		}
	}

	return total, data, nil
}
