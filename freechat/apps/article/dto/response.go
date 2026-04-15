package dto

import (
	"time"

	"github.com/openimsdk/chat/freechat/apps/article/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ArticleListResp 文章列表响应（不包含正文内容）
type ArticleListResp struct {
	ID             primitive.ObjectID `json:"id"`              // 文章ID
	Title          string             `json:"title"`           // 标题
	CreatorID      string             `json:"creator_id"`      // 创建者ID
	Status         string             `json:"status"`          // 状态
	OrganizationID primitive.ObjectID `json:"organization_id"` // 组织ID
	CreatedAt      time.Time          `json:"created_at"`      // 创建时间
	UpdatedAt      time.Time          `json:"updated_at"`      // 更新时间
}

// NewArticleListResp 创建文章列表响应
func NewArticleListResp(article *model.Article) *ArticleListResp {
	return &ArticleListResp{
		ID:             article.ID,
		Title:          article.Title,
		CreatorID:      article.CreatorID,
		Status:         string(article.Status),
		OrganizationID: article.OrganizationID,
		CreatedAt:      article.CreatedAt,
		UpdatedAt:      article.UpdatedAt,
	}
}

// ArticleDetailResp 文章详情响应（包含正文内容）
type ArticleDetailResp struct {
	ID             primitive.ObjectID `json:"id"`              // 文章ID
	Title          string             `json:"title"`           // 标题
	Content        string             `json:"content"`         // 正文内容（富文本）
	CreatorID      string             `json:"creator_id"`      // 创建者ID
	Status         string             `json:"status"`          // 状态
	OrganizationID primitive.ObjectID `json:"organization_id"` // 组织ID
	CreatedAt      time.Time          `json:"created_at"`      // 创建时间
	UpdatedAt      time.Time          `json:"updated_at"`      // 更新时间
}

// NewArticleDetailResp 创建文章详情响应
func NewArticleDetailResp(article *model.Article) *ArticleDetailResp {
	return &ArticleDetailResp{
		ID:             article.ID,
		Title:          article.Title,
		Content:        article.Content,
		CreatorID:      article.CreatorID,
		Status:         string(article.Status),
		OrganizationID: article.OrganizationID,
		CreatedAt:      article.CreatedAt,
		UpdatedAt:      article.UpdatedAt,
	}
}

// ArticleContentResp 文章正文内容响应
type ArticleContentResp struct {
	ID      primitive.ObjectID `json:"id"`      // 文章ID
	Content string             `json:"content"` // 正文内容（富文本）
}

// NewArticleContentResp 创建文章正文内容响应
func NewArticleContentResp(id primitive.ObjectID, content string) *ArticleContentResp {
	return &ArticleContentResp{
		ID:      id,
		Content: content,
	}
}

// ArticleListWithPaginationResp 带分页的文章列表响应
type ArticleListWithPaginationResp struct {
	Total int64              `json:"total"` // 总数
	List  []*ArticleListResp `json:"list"`  // 文章列表
}

// NewArticleListWithPaginationResp 创建带分页的文章列表响应
func NewArticleListWithPaginationResp(total int64, articles []*model.Article) *ArticleListWithPaginationResp {
	list := make([]*ArticleListResp, 0, len(articles))
	for _, article := range articles {
		list = append(list, NewArticleListResp(article))
	}

	return &ArticleListWithPaginationResp{
		Total: total,
		List:  list,
	}
}

// CreateArticleResp 创建文章响应
type CreateArticleResp struct {
	ID primitive.ObjectID `json:"id"` // 文章ID
}

// NewCreateArticleResp 创建文章响应
func NewCreateArticleResp(id primitive.ObjectID) *CreateArticleResp {
	return &CreateArticleResp{
		ID: id,
	}
}

// UpdateArticleResp 更新文章响应
type UpdateArticleResp struct {
	ID        primitive.ObjectID `json:"id"`         // 文章ID
	UpdatedAt time.Time          `json:"updated_at"` // 更新时间
}

// NewUpdateArticleResp 创建更新文章响应
func NewUpdateArticleResp(id primitive.ObjectID, updatedAt time.Time) *UpdateArticleResp {
	return &UpdateArticleResp{
		ID:        id,
		UpdatedAt: updatedAt,
	}
}
