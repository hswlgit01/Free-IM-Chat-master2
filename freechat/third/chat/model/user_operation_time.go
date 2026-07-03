package model

import (
	"context"
	"sync"
	"time"

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// UserOperationTime dawn 2026-07-04 最近一次操作时间：客户端每次打开 APP 上报，按 chat user_id 记录一行。
type UserOperationTime struct {
	UserID        string    `bson:"user_id"`
	OperationTime time.Time `bson:"operation_time"`
}

func (UserOperationTime) TableName() string {
	return "user_operation_time"
}

type UserOperationTimeDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

var userOperationTimeIndexOnce sync.Once

func NewUserOperationTimeDao(db *mongo.Database) *UserOperationTimeDao {
	coll := db.Collection(UserOperationTime{}.TableName())
	userOperationTimeIndexOnce.Do(func() {
		_, _ = coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys:    bson.D{{Key: "user_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		})
	})
	return &UserOperationTimeDao{DB: db, Collection: coll}
}

// Upsert 上报/更新某用户的最近操作时间为当前时间。
func (d *UserOperationTimeDao) Upsert(ctx context.Context, userID string) error {
	if userID == "" {
		return nil
	}
	_, err := d.Collection.UpdateOne(ctx,
		bson.M{"user_id": userID},
		bson.M{"$set": bson.M{"operation_time": time.Now()}},
		options.Update().SetUpsert(true),
	)
	return err
}

// FindByUserIDs 批量查询用户的最近操作时间。
func (d *UserOperationTimeDao) FindByUserIDs(ctx context.Context, userIDs []string) ([]*UserOperationTime, error) {
	valid := make([]string, 0, len(userIDs))
	for _, id := range userIDs {
		if id != "" {
			valid = append(valid, id)
		}
	}
	if len(valid) == 0 {
		return []*UserOperationTime{}, nil
	}
	return mongoutil.Find[*UserOperationTime](ctx, d.Collection, bson.M{"user_id": bson.M{"$in": valid}})
}
