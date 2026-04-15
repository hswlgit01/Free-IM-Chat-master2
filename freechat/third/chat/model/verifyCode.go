package model

import (
	"context"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type VerifyCode struct {
	ID         primitive.ObjectID `bson:"_id,omitempty"`
	Account    string             `bson:"account"`
	Platform   string             `bson:"platform"`
	Code       string             `bson:"code"`
	Duration   uint               `bson:"duration"`
	Count      int                `bson:"count"`
	Used       bool               `bson:"used"`
	CreateTime time.Time          `bson:"create_time"`
	UsedFor    int32              `bson:"used_for"`
}

func (VerifyCode) TableName() string {
	return "verify_codes"
}

type VerifyCodeDao struct {
	coll *mongo.Collection
}

func NewVerifyCodeDao(db *mongo.Database) *VerifyCodeDao {
	coll := db.Collection(VerifyCode{}.TableName())
	return &VerifyCodeDao{
		coll: coll,
	}
}

func (o *VerifyCodeDao) parseID(s string) (primitive.ObjectID, error) {
	objID, err := primitive.ObjectIDFromHex(s)
	if err != nil {
		var zero primitive.ObjectID
		return zero, errs.Wrap(err)
	}
	return objID, nil
}

func (o *VerifyCodeDao) Add(ctx context.Context, ms []*VerifyCode) error {
	return mongoutil.InsertMany(ctx, o.coll, ms)
}

func (o *VerifyCodeDao) RangeNum(ctx context.Context, account string, start time.Time, end time.Time) (int64, error) {
	filter := bson.M{
		"account": account,
		"create_time": bson.M{
			"$gte": start,
			"$lte": end,
		},
	}
	return mongoutil.Count(ctx, o.coll, filter)
}

func (o *VerifyCodeDao) TakeLast(ctx context.Context, account string, usedFor int32) (*VerifyCode, error) {
	filter := bson.M{
		"account":  account,
		"used_for": usedFor, // 添加用途过滤
	}
	opt := options.FindOne().SetSort(bson.M{"_id": -1})
	last, err := mongoutil.FindOne[*VerifyCode](ctx, o.coll, filter, opt)
	if err != nil {
		return nil, err
	}
	return last, nil
}

func (o *VerifyCodeDao) Incr(ctx context.Context, id primitive.ObjectID) error {
	return mongoutil.UpdateOne(ctx, o.coll, bson.M{"_id": id}, bson.M{"$inc": bson.M{"count": 1}}, false)
}

func (o *VerifyCodeDao) Delete(ctx context.Context, id primitive.ObjectID) error {
	return mongoutil.DeleteOne(ctx, o.coll, bson.M{"_id": id})
}
