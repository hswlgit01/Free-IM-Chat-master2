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

// 用户的抽奖券

type LotteryUserTicket struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	ImServerUserId string             `bson:"im_server_user_id" json:"im_server_user_id"`
	LotteryId      primitive.ObjectID `bson:"lottery_id" json:"lottery_id"`

	Use    bool      `bson:"use" json:"use"`         // 是否已使用
	UsedAt time.Time `bson:"used_at" json:"used_at"` // 使用时间

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (LotteryUserTicket) TableName() string {
	return constant.CollectionLotteryUserTicket
}

func CreateLotteryUserTicketIndex(db *mongo.Database) error {
	m := &LotteryUserTicket{}

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
		{
			Keys: bson.D{
				{Key: "im_server_user_id", Value: 1},
			},
		},
	})
	return err
}

type LotteryUserTicketDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewLotteryUserTicketDao(db *mongo.Database) *LotteryUserTicketDao {
	return &LotteryUserTicketDao{
		DB:         db,
		Collection: db.Collection(LotteryUserTicket{}.TableName()),
	}
}

func (o *LotteryUserTicketDao) GetById(ctx context.Context, id primitive.ObjectID) (*LotteryUserTicket, error) {
	return mongoutil.FindOne[*LotteryUserTicket](ctx, o.Collection, bson.M{"_id": id})
}

func (o *LotteryUserTicketDao) Create(ctx context.Context, obj *LotteryUserTicket) error {
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	_, err := o.Collection.InsertOne(ctx, obj)
	return err
}

// CreateBatch 批量创建抽奖券
func (o *LotteryUserTicketDao) CreateBatch(ctx context.Context, imServerUserId string, lotteryId primitive.ObjectID, count int) error {
	if count <= 0 {
		return nil
	}

	now := time.Now().UTC()
	tickets := make([]interface{}, count)

	for i := 0; i < count; i++ {
		tickets[i] = &LotteryUserTicket{
			ImServerUserId: imServerUserId,
			LotteryId:      lotteryId,
			Use:            false,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
	}

	_, err := o.Collection.InsertMany(ctx, tickets)
	return err
}

func (o *LotteryUserTicketDao) Select(ctx context.Context, imServerUserId string, page *paginationUtils.DepPagination) (int64, []*LotteryUserTicket, error) {
	filter := bson.M{}

	if imServerUserId != "" {
		filter["im_server_user_id"] = imServerUserId
	}

	opts := make([]*options.FindOptions, 0)
	// 默认排序
	opts = append(opts, options.Find().SetSort(bson.D{
		{"use", 1},
		{"created_at", -1}},
	))

	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	if len(filter) == 0 {
		filter = nil
	}

	data, err := mongoutil.Find[*LotteryUserTicket](ctx, o.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}

func (o *LotteryUserTicketDao) UpdateUseById(ctx context.Context, id primitive.ObjectID) error {
	updateField := bson.M{"$set": bson.M{
		"use":        true,
		"updated_at": time.Now().UTC(),
		"used_at":    time.Now().UTC(),
	}}
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"_id": id}, updateField, false)
}
