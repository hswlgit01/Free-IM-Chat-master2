package model

import (
	"context"

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// GroupMember 对应 IM 服务写入的 group_member 集合。
//
// chat 和 im-server 共用同一个 MongoDB（openim_v3），所以判断「某人是否在某群」
// 不必绕一圈去调 OpenIM 的 HTTP 接口拉全量成员——直接查一条即可。
// 该集合上已有唯一索引 group_id_1_user_id_1，是一次索引命中的单文档读。
type GroupMember struct {
	GroupID   string `bson:"group_id" json:"group_id"`
	UserID    string `bson:"user_id" json:"user_id"`
	RoleLevel int32  `bson:"role_level" json:"role_level"`
}

func (GroupMember) TableName() string {
	return "group_member"
}

type GroupMemberDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewGroupMemberDao(db *mongo.Database) *GroupMemberDao {
	return &GroupMemberDao{
		DB:         db,
		Collection: db.Collection(GroupMember{}.TableName()),
	}
}

// Exist 判断用户是否是群成员。走 (group_id, user_id) 索引，只取 _id 减少传输。
func (d *GroupMemberDao) Exist(ctx context.Context, groupID, userID string) (bool, error) {
	opt := options.FindOne().SetProjection(bson.M{"_id": 1})
	_, err := mongoutil.FindOne[*GroupMember](ctx, d.Collection,
		bson.M{"group_id": groupID, "user_id": userID}, opt)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// CountByGroupID 统计群成员数。发红包时用来校验「红包个数不能超过群人数」，
// 比拉回全量 ID 再取 len() 便宜得多。
func (d *GroupMemberDao) CountByGroupID(ctx context.Context, groupID string) (int64, error) {
	return mongoutil.Count(ctx, d.Collection, bson.M{"group_id": groupID})
}
