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
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 抽奖活动配置

type LotteryConfig struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	LotteryId       primitive.ObjectID `bson:"lottery_id" json:"lottery_id"`
	LotteryRewardId primitive.ObjectID `bson:"lottery_reward_id" json:"lottery_reward_id"`

	Left  primitive.Decimal128 `bson:"left" json:"left"`
	Right primitive.Decimal128 `bson:"right" json:"right"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (LotteryConfig) TableName() string {
	return constant.CollectionLotteryConfig
}

func CreateLotteryConfigIndex(db *mongo.Database) error {
	m := &LotteryConfig{}

	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "created_at", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "lottery_id", Value: 1},
			},
		},
	})
	return err
}

type LotteryConfigDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewLotteryConfigDao(db *mongo.Database) *LotteryConfigDao {
	return &LotteryConfigDao{
		DB:         db,
		Collection: db.Collection(LotteryConfig{}.TableName()),
	}
}

func (o *LotteryConfigDao) Create(ctx context.Context, obj *LotteryConfig) error {
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*LotteryConfig{obj})
}

func (o *LotteryConfigDao) DeleteByLotteryId(ctx context.Context, lotteryId primitive.ObjectID) error {
	filter := bson.M{}
	filter["lottery_id"] = lotteryId
	return mongoutil.DeleteMany(ctx, o.Collection, filter)
}

func (o *LotteryConfigDao) Select(ctx context.Context, lotteryId primitive.ObjectID, page *paginationUtils.DepPagination) (int64, []*LotteryConfig, error) {
	filter := bson.M{}

	if !lotteryId.IsZero() {
		filter["lottery_id"] = lotteryId
	}

	opts := make([]*options.FindOptions, 0)
	// 默认排序
	opts = append(opts, options.Find().SetSort(bson.M{"created_at": -1}))

	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	if len(filter) == 0 {
		filter = nil
	}

	data, err := mongoutil.Find[*LotteryConfig](ctx, o.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}

type LotteryConfigJoinLotteryReward struct {
	*LotteryConfig
}

func (o *LotteryConfigDao) SelectJoinLotteryReward(ctx context.Context, lotteryId primitive.ObjectID,
	page *paginationUtils.DepPagination) (int64, []*LotteryConfigJoinLotteryReward, error) {
	// 聚合查询
	pipeline := []bson.M{
		//{
		//	"$lookup": bson.M{
		//		"from":         orgModel.OrganizationUser{}.TableName(),
		//		"localField":   "im_server_user_id",
		//		"foreignField": "im_server_user_id",
		//		"as":           "org_user",
		//	},
		//},
	}

	// 构建过滤条件
	filter := bson.M{}

	if !lotteryId.IsZero() {
		filter["lottery_id"] = lotteryId
	}

	findPipeline := make([]bson.M, 0)
	countPipeline := make([]bson.M, 0)

	if len(filter) > 0 {
		findPipeline = append(pipeline, bson.M{"$match": filter})
		countPipeline = append(pipeline, bson.M{"$match": filter})
	}

	findPipeline = append(findPipeline, bson.M{"$sort": bson.M{"created_at": -1}})

	// 添加排序和分页
	if page != nil {
		findPipeline = append(findPipeline, page.ToBsonMList()...)
	}

	// 执行聚合查询获取数据
	data, err := mongoutil.Aggregate[*LotteryConfigJoinLotteryReward](ctx, o.Collection, findPipeline)
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

// CheckRewardInUse 检查某个奖品是否被抽奖配置使用
func (o *LotteryConfigDao) CheckRewardInUse(ctx context.Context, rewardId primitive.ObjectID) (bool, error) {
	filter := bson.M{"lottery_reward_id": rewardId}
	count, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
