package model

import (
	"context"

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type Credential struct {
	UserID      string `bson:"user_id"`
	Account     string `bson:"account"`
	Type        int    `bson:"type"` // 1:phone;2:email
	AllowChange bool   `bson:"allow_change"`
}

func (Credential) TableName() string {
	return "credential"
}

type CredentialDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewCredentialDao(db *mongo.Database) *CredentialDao {
	return &CredentialDao{
		DB:         db,
		Collection: db.Collection(Credential{}.TableName()),
	}
}

func (o *CredentialDao) GetByUserId(ctx context.Context, userId string) (*Credential, error) {
	return mongoutil.FindOne[*Credential](ctx, o.Collection, bson.M{"user_id": userId})
}

func (o *CredentialDao) GetByUserIdAndType(ctx context.Context, userId string, credType int) (*Credential, error) {
	return mongoutil.FindOne[*Credential](ctx, o.Collection, bson.M{"user_id": userId, "type": credType})
}

func (o *CredentialDao) UpdateAccount(ctx context.Context, userId string, credType int, newAccount string) error {
	filter := bson.M{"user_id": userId, "type": credType}
	update := bson.M{"$set": bson.M{"account": newAccount}}
	return mongoutil.UpdateOne(ctx, o.Collection, filter, update, false)
}

func (a *CredentialDao) Create(ctx context.Context, objs ...*Credential) error {
	return mongoutil.InsertMany(ctx, a.Collection, objs)
}
