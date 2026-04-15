package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// GroupStatus.
	GroupOk              = 0
	GroupBanChat         = 1
	GroupStatusDismissed = 2
	GroupStatusMuted     = 3
)

type Group struct {
	GroupID                string    `bson:"group_id"`
	GroupName              string    `bson:"group_name"`
	Notification           string    `bson:"notification"`
	Introduction           string    `bson:"introduction"`
	FaceURL                string    `bson:"face_url"`
	CreateTime             time.Time `bson:"create_time"`
	Ex                     string    `bson:"ex"`
	Status                 int32     `bson:"status"`
	CreatorUserID          string    `bson:"creator_user_id"`
	GroupType              int32     `bson:"group_type"`
	NeedVerification       int32     `bson:"need_verification"`
	LookMemberInfo         int32     `bson:"look_member_info"`
	ApplyMemberFriend      int32     `bson:"apply_member_friend"`
	NotificationUpdateTime time.Time `bson:"notification_update_time"`
	NotificationUserID     string    `bson:"notification_user_id"`
	OrgID                  string    `bson:"org_id"`
}

func (Group) TableName() string {
	return constant.CollectionGroup
}

type GroupDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewGroupDao(db *mongo.Database) *GroupDao {
	return &GroupDao{
		DB:         db,
		Collection: db.Collection(Group{}.TableName()),
	}
}

func (g *GroupDao) GetByGroupIDAndOrgID(ctx context.Context, groupID, orgID string) (*Group, error) {
	filter := bson.M{
		"org_id":   orgID,
		"group_id": groupID,
	}
	return mongoutil.FindOne[*Group](ctx, g.Collection, filter)
}

// GetGroupsByOrgID 根据组织ID分页查询群组
func (g *GroupDao) GetGroupsByOrgID(ctx context.Context, orgID string, page, pageSize int, groupName string) ([]*Group, int64, error) {
	filter := bson.M{
		"org_id": orgID,
		"status": bson.M{"$ne": GroupStatusDismissed},
	}
	if groupName != "" {
		filter["group_name"] = bson.M{"$regex": groupName}
	}

	total, err := mongoutil.Count(ctx, g.Collection, filter)
	if err != nil {
		return nil, 0, errs.NewCodeError(freeErrors.ErrSystem, "Failed to count groups")
	}

	opts := make([]*options.FindOptions, 0)
	opts = append(opts, options.Find().SetSort(bson.M{"create_time": -1}))

	if page > 0 && pageSize > 0 {
		skip := (page - 1) * pageSize
		limit := pageSize
		opt := options.Find().SetSkip(int64(skip)).SetLimit(int64(limit))
		opts = append(opts, opt)
	}

	groups, err := mongoutil.Find[*Group](ctx, g.Collection, filter, opts...)
	if err != nil {
		return nil, 0, errs.NewCodeError(freeErrors.ErrSystem, "Failed to query group list")
	}

	return groups, total, nil
}

// CountGroupsByOrgID 根据组织ID获取群组数量
func (g *GroupDao) CountGroupsByOrgID(ctx context.Context, orgID string) (int64, error) {
	filter := bson.M{
		"org_id": orgID,
		"status": bson.M{"$ne": GroupStatusDismissed},
	}

	total, err := mongoutil.Count(ctx, g.Collection, filter)
	if err != nil {
		return 0, errs.NewCodeError(freeErrors.ErrSystem, "Failed to count groups")
	}
	return total, err
}
