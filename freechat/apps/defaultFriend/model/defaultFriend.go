package model

import (
	"context"
	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/third/chat/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type DefaultFriend struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	ImServerUserId string             `bson:"im_server_user_id" json:"im_server_user_id"`
	OrgId          primitive.ObjectID `bson:"org_id" json:"org_id"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}

func (DefaultFriend) TableName() string {
	return "default_friend"
}

type DefaultFriendDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewDefaultFriendDao(db *mongo.Database) *DefaultFriendDao {
	return &DefaultFriendDao{
		DB:         db,
		Collection: db.Collection(DefaultFriend{}.TableName()),
	}
}

func (o *DefaultFriendDao) ExistByImUserId(ctx context.Context, imServerUserId string) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"im_server_user_id": imServerUserId})
}

func (o *DefaultFriendDao) Add(ctx context.Context, registerAddFriends []*DefaultFriend) error {
	for _, friend := range registerAddFriends {
		friend.CreatedAt = time.Now().UTC()
	}
	return mongoutil.InsertMany(ctx, o.Collection, registerAddFriends)
}

func (o *DefaultFriendDao) Del(ctx context.Context, orgId primitive.ObjectID, imUserIDs []string) error {
	if len(imUserIDs) == 0 {
		return nil
	}

	filter := bson.M{}

	if orgId != primitive.NilObjectID {
		filter["org_id"] = orgId
	}

	if len(imUserIDs) > 0 {
		filter["im_server_user_id"] = bson.M{"$in": imUserIDs}
	}

	return mongoutil.DeleteMany(ctx, o.Collection, filter)
}

func (o *DefaultFriendDao) SelectByOrgIdAndImUserIds(ctx context.Context, orgId primitive.ObjectID, imUserIDs []string) ([]string, error) {
	filter := bson.M{}

	if orgId != primitive.NilObjectID {
		filter["org_id"] = orgId
	}

	if len(imUserIDs) > 0 {
		filter["im_server_user_id"] = bson.M{"$in": imUserIDs}
	}
	return mongoutil.Find[string](ctx, o.Collection, filter, options.Find().SetProjection(bson.M{"_id": 0, "im_server_user_id": 1}))
}

func (o *DefaultFriendDao) Select(ctx context.Context, keyword string, page *paginationUtils.DepPagination) (int64, []*DefaultFriend, error) {
	filter := bson.M{}

	if keyword != "" {
		filter = bson.M{
			"$or": []bson.M{
				{"im_server_user_id": bson.M{"$regex": keyword, "$options": "i"}},
			},
		}
	}

	opts := make([]*options.FindOptions, 0)
	// 默认排序
	opts = append(opts, options.Find().SetSort(bson.M{"created_at": -1}))

	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	if len(filter) == 0 {
		filter = nil
	}

	data, err := mongoutil.Find[*DefaultFriend](ctx, o.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}

type RegisterAddFriendJoinAll struct {
	*DefaultFriend   `bson:",inline"`
	User             map[string]interface{} `bson:"user"`
	OrganizationUser map[string]interface{} `bson:"organization_user"`
	Attribute        map[string]interface{} `bson:"attribute"`
}

func (o *DefaultFriendDao) SelectJoinAll(ctx context.Context, keyword string, orgId primitive.ObjectID,
	page *paginationUtils.DepPagination) (int64, []*RegisterAddFriendJoinAll, error) {
	// 聚合查询
	pipeline := []bson.M{
		{
			"$lookup": bson.M{
				"from":         constant.CollectionOrganizationUser,
				"localField":   "im_server_user_id",
				"foreignField": "im_server_user_id",
				"as":           "organization_user",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$organization_user",
				"preserveNullAndEmptyArrays": false,
			},
		},

		{
			"$lookup": bson.M{
				"from":         openImModel.User{}.TableName(),
				"localField":   "im_server_user_id",
				"foreignField": "user_id",
				"as":           "user",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$user",
				"preserveNullAndEmptyArrays": false,
			},
		},

		{
			"$lookup": bson.M{
				"from":         model.Attribute{}.TableName(),
				"localField":   "organization_user.user_id",
				"foreignField": "user_id",
				"as":           "attribute",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$attribute",
				"preserveNullAndEmptyArrays": false,
			},
		},
	}

	// 构建过滤条件
	filter := bson.M{}

	if !orgId.IsZero() {
		filter["organization_user.organization_id"] = orgId
	}

	if keyword != "" {
		filter = bson.M{
			"$or": []bson.M{
				{"attribute.user_id": bson.M{"$regex": keyword, "$options": "i"}},
				{"im_server_user_id": bson.M{"$regex": keyword, "$options": "i"}},
				{"user.nickname": bson.M{"$regex": keyword, "$options": "i"}},
			},
		}
	}

	findPipeline := make([]bson.M, 0)
	countPipeline := make([]bson.M, 0)

	if len(filter) > 0 {
		findPipeline = append(pipeline, bson.M{"$match": filter})
		countPipeline = append(pipeline, bson.M{"$match": filter})
	}

	// 按时间倒序排列：从新到旧
	findPipeline = append(findPipeline, bson.M{"$sort": bson.M{"created_at": -1}})

	// 添加排序和分页
	if page != nil {
		findPipeline = append(findPipeline, page.ToBsonMList()...)
	}

	// 执行聚合查询获取数据
	data, err := mongoutil.Aggregate[*RegisterAddFriendJoinAll](ctx, o.Collection, findPipeline)
	if err != nil {
		return 0, nil, err
	}

	countPipeline = append(countPipeline, bson.M{"$count": "total"})

	var countResult []bson.M
	cursor, err := o.Collection.Aggregate(ctx, countPipeline)
	if err != nil {
		return 0, nil, err
	}
	if err = cursor.All(ctx, &countResult); err != nil {
		return 0, nil, err
	}

	total := int32(0)
	if len(countResult) > 0 {
		total = countResult[0]["total"].(int32)
	}

	return int64(total), data, nil
}
