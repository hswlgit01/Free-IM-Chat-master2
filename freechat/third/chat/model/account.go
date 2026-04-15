package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type Account struct {
	UserID         string    `bson:"user_id"`
	Password       string    `bson:"password"`
	CreateTime     time.Time `bson:"create_time"`
	ChangeTime     time.Time `bson:"change_time"`
	OperatorUserID string    `bson:"operator_user_id"`
}

func (Account) TableName() string {
	return "account"
}

type AccountDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewAccountDao(db *mongo.Database) *AccountDao {
	return &AccountDao{
		DB:         db,
		Collection: db.Collection(Account{}.TableName()),
	}
}

func (o *AccountDao) GetByUserId(ctx context.Context, userId string) (*Account, error) {
	return mongoutil.FindOne[*Account](ctx, o.Collection, bson.M{"user_id": userId})
}

func (o *AccountDao) Create(ctx context.Context, objs ...*Account) error {
	return mongoutil.InsertMany(ctx, o.Collection, objs)
}
func (o *AccountDao) UpdatePassword(ctx context.Context, userId string, password string) error {
	filter := bson.M{"user_id": userId}
	update := bson.M{"$set": bson.M{"password": password, "change_time": time.Now()}}
	return mongoutil.UpdateOne(ctx, o.Collection, filter, update, false)
}
