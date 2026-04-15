package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// UserTag 用户标签表
type UserTag struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	OrganizationId primitive.ObjectID `bson:"organization_id" json:"organization_id"`
	TagName        string             `bson:"tag_name" json:"tag_name"`
	Description    string             `bson:"description" json:"description"`
	CreatedAt      time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt      time.Time          `bson:"updated_at" json:"updated_at"`
}

func (UserTag) TableName() string {
	return "user_tag"
}

// CreateUserTagIndex 创建用户标签索引
func CreateUserTagIndex(db *mongo.Database) error {
	m := &UserTag{}
	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "tag_name", Value: 1},
			},
		},
	})
	return err
}

type UserTagDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewUserTagDao(db *mongo.Database) *UserTagDao {
	return &UserTagDao{
		DB:         db,
		Collection: db.Collection(UserTag{}.TableName()),
	}
}

// Create 创建标签
func (u *UserTagDao) Create(ctx context.Context, obj *UserTag) error {
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, u.Collection, []*UserTag{obj})
}

// GetById 根据ID获取标签
func (u *UserTagDao) GetById(ctx context.Context, id primitive.ObjectID) (*UserTag, error) {
	return mongoutil.FindOne[*UserTag](ctx, u.Collection, bson.M{"_id": id})
}

// GetByTagNameAndOrgId 根据标签名和组织ID获取标签
func (u *UserTagDao) GetByTagNameAndOrgId(ctx context.Context, tagName string, orgId primitive.ObjectID) (*UserTag, error) {
	return mongoutil.FindOne[*UserTag](ctx, u.Collection, bson.M{"tag_name": tagName, "organization_id": orgId})
}

// ExistByTagNameAndOrgId 检查标签名是否在组织中已存在
func (u *UserTagDao) ExistByTagNameAndOrgId(ctx context.Context, tagName string, orgId primitive.ObjectID) (bool, error) {
	return mongoutil.Exist(ctx, u.Collection, bson.M{"tag_name": tagName, "organization_id": orgId})
}

// UpdateById 根据ID更新标签
func (u *UserTagDao) UpdateById(ctx context.Context, id primitive.ObjectID, tagName, description string) error {
	updateField := bson.M{
		"$set": bson.M{
			"tag_name":    tagName,
			"description": description,
			"updated_at":  time.Now().UTC(),
		},
	}
	return mongoutil.UpdateOne(ctx, u.Collection, bson.M{"_id": id}, updateField, false)
}

// GetByIdsAndOrgId 根据标签ID列表和组织ID批量获取标签
func (u *UserTagDao) GetByIdsAndOrgId(ctx context.Context, tagIds []primitive.ObjectID, orgId primitive.ObjectID) ([]*UserTag, error) {
	filter := bson.M{
		"_id":             bson.M{"$in": tagIds},
		"organization_id": orgId,
	}
	return mongoutil.Find[*UserTag](ctx, u.Collection, filter)
}

// GetByOrgId 根据组织ID获取所有标签
func (u *UserTagDao) GetByOrgId(ctx context.Context, orgId primitive.ObjectID) ([]*UserTag, error) {
	filter := bson.M{
		"organization_id": orgId,
	}
	return mongoutil.Find[*UserTag](ctx, u.Collection, filter)
}

// Select 查询标签列表
func (u *UserTagDao) Select(ctx context.Context, orgId primitive.ObjectID, page *paginationUtils.DepPagination) (int64, []*UserTag, error) {
	filter := bson.M{"organization_id": orgId}

	opts := make([]*options.FindOptions, 0)
	// 按创建时间倒序
	opts = append(opts, options.Find().SetSort(bson.M{"created_at": -1}))

	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	data, err := mongoutil.Find[*UserTag](ctx, u.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, u.Collection, filter)
	if err != nil {
		return 0, nil, err
	}

	return total, data, nil
}
