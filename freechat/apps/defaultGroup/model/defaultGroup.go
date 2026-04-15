package model

import (
	"context"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const (
	DefaultGroupCName = "default_groups"
)

type DefaultGroup struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	GroupID   string             `bson:"group_id" json:"group_id"`
	OrgId     primitive.ObjectID `bson:"org_id" json:"org_id"`
	CreatedAt time.Time          `bson:"created_at" json:"created_at"`
}

type DefaultGroupDao struct {
	coll *mongo.Collection
}

func NewDefaultGroupDao(db *mongo.Database) *DefaultGroupDao {
	return &DefaultGroupDao{
		coll: db.Collection(DefaultGroupCName),
	}
}

func (dao *DefaultGroupDao) Add(ctx context.Context, defaultGroups []*DefaultGroup) error {
	if len(defaultGroups) == 0 {
		return nil
	}

	now := time.Now()
	docs := make([]interface{}, 0, len(defaultGroups))
	for _, group := range defaultGroups {
		group.CreatedAt = now
		docs = append(docs, group)
	}

	_, err := dao.coll.InsertMany(ctx, docs)
	return err
}

func (dao *DefaultGroupDao) Del(ctx context.Context, orgId primitive.ObjectID, groupIDs []string) error {
	if len(groupIDs) == 0 {
		return nil
	}

	filter := bson.M{
		"org_id":   orgId,
		"group_id": bson.M{"$in": groupIDs},
	}

	_, err := dao.coll.DeleteMany(ctx, filter)
	return err
}

func (dao *DefaultGroupDao) ExistByGroupId(ctx context.Context, groupID string) (bool, error) {
	filter := bson.M{"group_id": groupID}
	count, err := dao.coll.CountDocuments(ctx, filter)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (dao *DefaultGroupDao) SelectByOrgIdAndGroupIds(ctx context.Context, orgId primitive.ObjectID, groupIDs []string) ([]string, error) {
	filter := bson.M{"org_id": orgId}
	if len(groupIDs) > 0 {
		filter["group_id"] = bson.M{"$in": groupIDs}
	}

	cursor, err := dao.coll.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []string
	for cursor.Next(ctx) {
		var group DefaultGroup
		if err := cursor.Decode(&group); err != nil {
			return nil, err
		}
		results = append(results, group.GroupID)
	}

	return results, cursor.Err()
}

func (dao *DefaultGroupDao) SelectJoinAll(ctx context.Context, keyword string, orgId primitive.ObjectID, page *paginationUtils.DepPagination) (int64, []*DefaultGroup, error) {
	filter := bson.M{"org_id": orgId}

	if keyword != "" {
		filter["group_id"] = bson.M{"$regex": keyword, "$options": "i"}
	}

	total, err := dao.coll.CountDocuments(ctx, filter)
	if err != nil {
		return 0, nil, err
	}

	opts := options.Find().
		SetSkip(int64((page.Page - 1) * page.PageSize)).
		SetLimit(int64(page.PageSize)).
		SetSort(bson.D{{Key: "created_at", Value: -1}})

	cursor, err := dao.coll.Find(ctx, filter, opts)
	if err != nil {
		return 0, nil, err
	}
	defer cursor.Close(ctx)

	var results []*DefaultGroup
	if err := cursor.All(ctx, &results); err != nil {
		return 0, nil, err
	}

	return total, results, nil
}
