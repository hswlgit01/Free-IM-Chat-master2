package model

import (
	"context"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type Register struct {
	UserID      string    `bson:"user_id"`
	DeviceID    string    `bson:"device_id"`
	IP          string    `bson:"ip"`
	Platform    string    `bson:"platform"`
	AccountType string    `bson:"account_type"`
	Mode        string    `bson:"mode"`
	CreateTime  time.Time `bson:"create_time"`
}

func (Register) TableName() string {
	return "registers"
}

type RegisterDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewRegisterDao(db *mongo.Database) *RegisterDao {
	return &RegisterDao{
		DB:         db,
		Collection: db.Collection(Register{}.TableName()),
	}
}

func (o *RegisterDao) GetByUserId(ctx context.Context, userId string) (*Register, error) {
	return mongoutil.FindOne[*Register](ctx, o.Collection, bson.M{"user_id": userId})
}

// FindByUserIDs 按主账户 user_id 批量查询注册记录（registers 表中的 ip 等）
func (o *RegisterDao) FindByUserIDs(ctx context.Context, userIDs []string) ([]*Register, error) {
	if len(userIDs) == 0 {
		return []*Register{}, nil
	}
	valid := make([]string, 0, len(userIDs))
	for _, id := range userIDs {
		if id != "" {
			valid = append(valid, id)
		}
	}
	if len(valid) == 0 {
		return []*Register{}, nil
	}
	return mongoutil.Find[*Register](ctx, o.Collection, bson.M{"user_id": bson.M{"$in": valid}})
}

func (o *RegisterDao) Create(ctx context.Context, objs ...*Register) error {
	return mongoutil.InsertMany(ctx, o.Collection, objs)
}
