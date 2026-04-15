package dto

import (
	"github.com/openimsdk/chat/freechat/apps/article/model"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
)

// CreateArticleReq 创建文章请求
type CreateArticleReq struct {
	Title   string `json:"title" binding:"required"`   // 标题
	Content string `json:"content" binding:"required"` // 正文内容（富文本）
	Status  string `json:"status" binding:"required"`  // 状态
}

// UpdateArticleReq 更新文章请求
type UpdateArticleReq struct {
	ID      string  `json:"id" binding:"required"` // 文章ID
	Title   *string `json:"title,omitempty"`       // 标题
	Content *string `json:"content,omitempty"`     // 正文内容（富文本）
	Status  *string `json:"status,omitempty"`      // 状态
}

// GetArticleListReq 查询文章列表请求
type GetArticleListReq struct {
	Status     *string                        `json:"status,omitempty"`              // 状态过滤
	StartTime  *int64                         `json:"start_time,omitempty"`          // 更新时间开始过滤（时间戳秒）
	EndTime    *int64                         `json:"end_time,omitempty"`            // 更新时间结束过滤（时间戳秒）
	Pagination *paginationUtils.DepPagination `json:"pagination" binding:"required"` // 分页
}

// GetArticleDetailReq 查询文章详情请求
type GetArticleDetailReq struct {
	ID string `json:"id" binding:"required"` // 文章ID
}

// ValidateStatus 验证状态值
func ValidateStatus(status string) bool {
	return status == string(model.ArticleStatusDraft) ||
		status == string(model.ArticleStatusPublished) ||
		status == string(model.ArticleStatusOffline)
}

// ToArticleStatus 转换为文章状态
func ToArticleStatus(status string) model.ArticleStatus {
	return model.ArticleStatus(status)
}
