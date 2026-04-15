package model

import (
	"context"
	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type CheckinRewardType string

const (
	CheckinRewardTypeLottery  CheckinRewardType = "lottery"
	CheckinRewardTypeCash     CheckinRewardType = "cash"
	CheckinRewardTypeIntegral CheckinRewardType = "integral"
)

type CheckinRewardConfig struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrgId  primitive.ObjectID `bson:"org_id" json:"org_id"`
	Streak int                `bson:"streak" json:"streak"`

	RewardType   CheckinRewardType    `bson:"type" json:"type"`
	RewardId     string               `bson:"reward_id" json:"reward_id"`
	RewardAmount primitive.Decimal128 `bson:"reward_amount" json:"reward_amount"`

	Auto bool `bson:"auto"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (CheckinRewardConfig) TableName() string {
	return constant.CollectionCheckinRewardConfig
}

type CheckinRewardConfigDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewCheckinRewardConfigDao(db *mongo.Database) *CheckinRewardConfigDao {
	return &CheckinRewardConfigDao{
		DB:         db,
		Collection: db.Collection(CheckinRewardConfig{}.TableName()),
	}
}

func (o *CheckinRewardConfigDao) Create(ctx context.Context, obj *CheckinRewardConfig) error {
	obj.CreatedAt = time.Now()
	obj.UpdatedAt = time.Now()
	return mongoutil.InsertMany(ctx, o.Collection, []*CheckinRewardConfig{obj})
}

func (o *CheckinRewardConfigDao) DeleteById(ctx context.Context, id primitive.ObjectID) error {
	filter := bson.M{}
	filter["_id"] = id
	return mongoutil.DeleteOne(ctx, o.Collection, filter)
}

// SelectByOrgIdAndStreak 根据组织ID和精确的连续签到天数查询奖励配置
func (o *CheckinRewardConfigDao) SelectByOrgIdAndStreak(ctx context.Context, organizationId primitive.ObjectID, streak int) ([]*CheckinRewardConfig, error) {
	filter := bson.M{}

	if !organizationId.IsZero() {
		filter["org_id"] = organizationId
	}

	if streak >= 0 {
		filter["streak"] = streak
	}

	opts := make([]*options.FindOptions, 0)
	if len(filter) == 0 {
		filter = nil
	}

	data, err := mongoutil.Find[*CheckinRewardConfig](ctx, o.Collection, filter, opts...)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// SelectByOrgIdAndStreakThreshold 根据组织ID和连续签到天数查询奖励配置
// 本方法使用阈值查询，返回所有小于等于给定连续签到天数的配置
// 例如: 当连续签到达到7天时，会触发streak=1,3,5,7等所有满足条件的奖励配置
func (o *CheckinRewardConfigDao) SelectByOrgIdAndStreakThreshold(ctx context.Context, organizationId primitive.ObjectID, streak int) ([]*CheckinRewardConfig, error) {
	filter := bson.M{}

	if !organizationId.IsZero() {
		filter["org_id"] = organizationId
	}

	if streak >= 0 {
		// 阈值匹配，查找所有小于等于当前连续签到天数的配置
		// 这确保了当用户连续签到达到某一天数时，能触发所有对应的奖励
		filter["streak"] = bson.M{"$lte": streak}
	}

	opts := make([]*options.FindOptions, 0)
	if len(filter) == 0 {
		filter = nil
	}

	return mongoutil.Find[*CheckinRewardConfig](ctx, o.Collection, filter, opts...)
}

func (o *CheckinRewardConfigDao) Select(ctx context.Context, organizationId primitive.ObjectID,
	page *paginationUtils.DepPagination) (int64, []*CheckinRewardConfig, error) {
	filter := bson.M{}

	if !organizationId.IsZero() {
		filter["org_id"] = organizationId
	}

	opts := make([]*options.FindOptions, 0)
	// 默认排序
	opts = append(opts, options.Find().SetSort(bson.M{"streak": 1}))

	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	if len(filter) == 0 {
		filter = nil
	}

	data, err := mongoutil.Find[*CheckinRewardConfig](ctx, o.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}
