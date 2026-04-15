package model

import (
	"context"
	"github.com/jinzhu/copier"
	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// LotteryReward LotteryItem 抽奖奖品
type LotteryReward struct {
	ID       primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"` // 奖品id
	Name     string             `bson:"name" json:"name"`                  // 奖品名称
	Entity   *bool              `bson:"entity" bson:"entity"`              // 实体奖品
	Img      string             `bson:"img" json:"img"`                    // 奖品图片
	Type     string             `bson:"type" json:"type"`                  // 奖品类型
	Remark   string             `bson:"remark" json:"remark"`              // 奖品描述
	OrgId    primitive.ObjectID `bson:"org_id" json:"org_id"`              // 组织id
	CreateAt time.Time          `bson:"create_at" json:"create_at"`        // 创建时间
	Status   int                `bson:"status" json:"status"`              // 奖品状态 0 禁用 1 启用
}

func (LotteryReward) TableName() string {
	return constant.CollectionLotteryReward
}

type LotteryRewardDao struct {
	Collection *mongo.Collection
	DB         *mongo.Database
}

func NewLotteryRewardDao(db *mongo.Database) *LotteryRewardDao {
	return &LotteryRewardDao{
		Collection: db.Collection(LotteryReward{}.TableName()),
		DB:         db,
	}
}

func (o *LotteryRewardDao) CreateLotteryReward(ctx context.Context, obj *LotteryReward) error {
	_, err := o.Collection.InsertOne(ctx, obj)
	return err
}

func (o *LotteryRewardDao) GetById(ctx context.Context, id primitive.ObjectID) (*LotteryReward, error) {
	return mongoutil.FindOne[*LotteryReward](ctx, o.Collection, bson.M{"_id": id})
}

func (o *LotteryRewardDao) UpdateLotteryReward(ctx context.Context, orgId primitive.ObjectID, obj *LotteryReward) error {
	updates := &LotteryReward{}
	err := copier.CopyWithOption(updates, obj, copier.Option{IgnoreEmpty: true})
	if err != nil {
		return err
	}
	updates.OrgId = orgId
	_, err = o.Collection.UpdateOne(ctx, bson.M{"_id": obj.ID, "org_id": orgId}, bson.M{"$set": updates})
	return err
}

func (o *LotteryRewardDao) DeleteLotteryReward(ctx context.Context, orgId primitive.ObjectID, id primitive.ObjectID) error {
	//todo 检测是否有相关的未发放奖品
	return o.Collection.FindOneAndDelete(ctx, bson.M{"_id": id, "org_id": orgId}).Err()
}

func (o *LotteryRewardDao) FindLotteryReward(ctx context.Context, orgId primitive.ObjectID, item LotteryReward,
	pagination *paginationUtils.DepPagination) (int64, []*LotteryReward, error) {
	filter := bson.M{}
	if item.Name != "" {
		filter["name"] = bson.M{"$regex": item.Name}
	}
	if item.Entity != nil {
		filter["entity"] = item.Entity
	}
	if item.Type != "" {
		filter["type"] = item.Type
	}
	filter["org_id"] = orgId
	opts := make([]*options.FindOptions, 0)
	if pagination != nil {
		opts = append(opts, pagination.ToOptions())
	}
	records, err := mongoutil.Find[*LotteryReward](ctx, o.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}
	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, records, nil
}
