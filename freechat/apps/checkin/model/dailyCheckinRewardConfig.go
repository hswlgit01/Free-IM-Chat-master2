package model

import (
	"context"
	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

// DailyCheckinRewardConfig 日常签到奖励配置
// 每个组织只有一条记录，每天签到必发
type DailyCheckinRewardConfig struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrgId primitive.ObjectID `bson:"org_id" json:"org_id"`

	// 固定为 "cash"
	RewardType CheckinRewardType `bson:"type" json:"type"`

	// 货币ID
	RewardId string `bson:"reward_id" json:"reward_id"`

	// 奖励金额
	RewardAmount primitive.Decimal128 `bson:"reward_amount" json:"reward_amount"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (DailyCheckinRewardConfig) TableName() string {
	return constant.CollectionDailyCheckinRewardConfig
}

type DailyCheckinRewardConfigDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewDailyCheckinRewardConfigDao(db *mongo.Database) *DailyCheckinRewardConfigDao {
	return &DailyCheckinRewardConfigDao{
		DB:         db,
		Collection: db.Collection(DailyCheckinRewardConfig{}.TableName()),
	}
}

func (o *DailyCheckinRewardConfigDao) Create(ctx context.Context, obj *DailyCheckinRewardConfig) error {
	obj.CreatedAt = time.Now().UTC()
	obj.UpdatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*DailyCheckinRewardConfig{obj})
}

func (o *DailyCheckinRewardConfigDao) Update(ctx context.Context, obj *DailyCheckinRewardConfig) error {
	obj.UpdatedAt = time.Now().UTC()
	filter := bson.M{"_id": obj.ID}
	update := bson.M{"$set": obj}
	return mongoutil.UpdateOne(ctx, o.Collection, filter, update, false)
}

func (o *DailyCheckinRewardConfigDao) GetByOrgId(ctx context.Context, orgId primitive.ObjectID) (*DailyCheckinRewardConfig, error) {
	return mongoutil.FindOne[*DailyCheckinRewardConfig](ctx, o.Collection, bson.M{"org_id": orgId})
}

func (o *DailyCheckinRewardConfigDao) DeleteByOrgId(ctx context.Context, orgId primitive.ObjectID) error {
	filter := bson.M{"org_id": orgId}
	return mongoutil.DeleteOne(ctx, o.Collection, filter)
}

// CreateOrUpdate 创建或更新（每个组织只有一条）
func (o *DailyCheckinRewardConfigDao) CreateOrUpdate(ctx context.Context, obj *DailyCheckinRewardConfig) error {
	existing, err := o.GetByOrgId(ctx, obj.OrgId)
	if err != nil {
		// 不存在则创建
		return o.Create(ctx, obj)
	}

	// 存在则更新
	obj.ID = existing.ID
	obj.CreatedAt = existing.CreatedAt
	return o.Update(ctx, obj)
}
