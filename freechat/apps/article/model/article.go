package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ArticleStatus 文章状态枚举
type ArticleStatus string

const (
	ArticleStatusDraft     ArticleStatus = "draft"     // 草稿
	ArticleStatusPublished ArticleStatus = "published" // 已发布
	ArticleStatusOffline   ArticleStatus = "offline"   // 已下架
)

// Article 文章模型
type Article struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	Title          string             `bson:"title" json:"title"`                     // 标题
	Content        string             `bson:"content" json:"content"`                 // 正文内容（富文本）
	CreatorID      string             `bson:"creator_id" json:"creator_id"`           // 创建者ID
	Status         ArticleStatus      `bson:"status" json:"status"`                   // 状态
	OrganizationID primitive.ObjectID `bson:"organization_id" json:"organization_id"` // 组织ID
	CreatedAt      time.Time          `bson:"created_at" json:"created_at"`           // 创建时间
	UpdatedAt      time.Time          `bson:"updated_at" json:"updated_at"`           // 更新时间
}

// TableName 获取表名
func (Article) TableName() string {
	return constant.CollectionArticle
}

// CreateArticleIndex 创建文章索引
func CreateArticleIndex(db *mongo.Database) error {
	m := &Article{}
	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "status", Value: 1},
				{Key: "updated_at", Value: -1},
			},
		},
		{
			Keys: bson.D{
				{Key: "creator_id", Value: 1},
				{Key: "status", Value: 1},
			},
		},
	})
	return err
}

// ArticleDao 文章数据访问对象
type ArticleDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

// NewArticleDao 创建文章数据访问对象
func NewArticleDao(db *mongo.Database) *ArticleDao {
	return &ArticleDao{
		DB:         db,
		Collection: db.Collection(Article{}.TableName()),
	}
}

// Create 创建文章
func (a *ArticleDao) Create(ctx context.Context, article *Article) error {
	article.CreatedAt = time.Now().UTC()
	article.UpdatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, a.Collection, []*Article{article})
}

// GetByID 根据ID获取文章
func (a *ArticleDao) GetByID(ctx context.Context, id primitive.ObjectID) (*Article, error) {
	return mongoutil.FindOne[*Article](ctx, a.Collection, bson.M{"_id": id})
}

// GetByIDAndOrgID 根据ID和组织ID获取文章
func (a *ArticleDao) GetByIDAndOrgID(ctx context.Context, id primitive.ObjectID, orgID primitive.ObjectID) (*Article, error) {
	return mongoutil.FindOne[*Article](ctx, a.Collection, bson.M{"_id": id, "organization_id": orgID})
}

// UpdateArticleParam 更新文章参数
type UpdateArticleParam struct {
	Title   *string        `bson:"title,omitempty"`
	Content *string        `bson:"content,omitempty"`
	Status  *ArticleStatus `bson:"status,omitempty"`
}

// Update 更新文章
func (a *ArticleDao) Update(ctx context.Context, id primitive.ObjectID, param UpdateArticleParam) error {
	updateFields := bson.M{"updated_at": time.Now().UTC()}

	if param.Title != nil {
		updateFields["title"] = *param.Title
	}
	if param.Content != nil {
		updateFields["content"] = *param.Content
	}
	if param.Status != nil {
		updateFields["status"] = *param.Status
	}

	updateDoc := bson.M{"$set": updateFields}
	return mongoutil.UpdateOne(ctx, a.Collection, bson.M{"_id": id}, updateDoc, false)
}

// ArticleListFilter 文章列表过滤条件
type ArticleListFilter struct {
	OrganizationID primitive.ObjectID `bson:"organization_id"`
	Status         *ArticleStatus     `bson:"status,omitempty"`
	StartTime      *time.Time         `bson:"start_time,omitempty"`
	EndTime        *time.Time         `bson:"end_time,omitempty"`
}

// GetList 获取文章列表（不包含正文内容）
func (a *ArticleDao) GetList(ctx context.Context, filter ArticleListFilter, pagination *paginationUtils.DepPagination) (int64, []*Article, error) {
	// 构建查询条件
	query := bson.M{"organization_id": filter.OrganizationID}

	if filter.Status != nil {
		query["status"] = *filter.Status
	}

	// 时间过滤（使用更新时间）
	if filter.StartTime != nil || filter.EndTime != nil {
		timeQuery := bson.M{}
		if filter.StartTime != nil {
			timeQuery["$gte"] = *filter.StartTime
		}
		if filter.EndTime != nil {
			timeQuery["$lte"] = *filter.EndTime
		}
		query["updated_at"] = timeQuery
	}

	// 配置查询选项，按创建时间降序排序
	opts := []*options.FindOptions{
		options.Find().SetSort(bson.D{
			{Key: "updated_at", Value: -1},
		}),
		options.Find().SetProjection(bson.M{
			"content": 0, // 排除正文内容
		}),
	}

	return mongoutil.FindPage[*Article](ctx, a.Collection, query, pagination, opts...)
}

// GetContent 获取文章正文内容
func (a *ArticleDao) GetContent(ctx context.Context, id primitive.ObjectID, orgID primitive.ObjectID) (string, error) {
	// 只返回正文内容
	opts := options.FindOne().SetProjection(bson.M{
		"content": 1,
	})

	result, err := mongoutil.FindOne[*Article](ctx, a.Collection, bson.M{"_id": id, "organization_id": orgID}, opts)
	if err != nil {
		return "", err
	}

	return result.Content, nil
}

// ExistByID 检查文章是否存在
func (a *ArticleDao) ExistByID(ctx context.Context, id primitive.ObjectID) (bool, error) {
	return mongoutil.Exist(ctx, a.Collection, bson.M{"_id": id})
}

// ExistByIDAndOrgID 检查文章是否存在于指定组织
func (a *ArticleDao) ExistByIDAndOrgID(ctx context.Context, id primitive.ObjectID, orgID primitive.ObjectID) (bool, error) {
	return mongoutil.Exist(ctx, a.Collection, bson.M{"_id": id, "organization_id": orgID})
}
