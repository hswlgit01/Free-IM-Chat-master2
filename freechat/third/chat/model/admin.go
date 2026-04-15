package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Admin 超管用户表
type Admin struct {
	Account    string    `bson:"account"`
	Password   string    `bson:"password"`
	FaceURL    string    `bson:"face_url"`
	Nickname   string    `bson:"nickname"`
	UserID     string    `bson:"user_id"`
	Level      int32     `bson:"level"`
	Email      string    `bson:"email"` // 新增邮箱字段，默认为空
	CreateTime time.Time `bson:"create_time"`
}

func (Admin) TableName() string {
	return "admin"
}

// AdminDao 数据访问层
type AdminDao struct {
	coll *mongo.Collection
}

func NewAdminDao(db *mongo.Database) *AdminDao {
	coll := db.Collection("admin")
	return &AdminDao{
		coll: coll,
	}
}

// TakeByAccount 根据账号查找管理员
func (d *AdminDao) TakeByAccount(ctx context.Context, account string) (*Admin, error) {
	return mongoutil.FindOne[*Admin](ctx, d.coll, bson.M{"account": account})
}

// TakeByUserID 根据用户ID查找管理员
func (d *AdminDao) TakeByUserID(ctx context.Context, userID string) (*Admin, error) {
	return mongoutil.FindOne[*Admin](ctx, d.coll, bson.M{"user_id": userID})
}

// UpdateEmail 更新管理员邮箱
func (d *AdminDao) UpdateEmail(ctx context.Context, userID string, email string) error {
	return mongoutil.UpdateOne(ctx, d.coll, bson.M{"user_id": userID}, bson.M{"$set": bson.M{"email": email}}, false)
}
