package svc

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/apps/article/dto"
	"github.com/openimsdk/chat/freechat/apps/article/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ArticleService 文章业务服务
type ArticleService struct {
	articleDao *model.ArticleDao
}

// NewArticleService 创建文章业务服务实例
func NewArticleService() *ArticleService {
	db := plugin.MongoCli().GetDB()
	return &ArticleService{
		articleDao: model.NewArticleDao(db),
	}
}

// CreateArticle 创建文章
func (s *ArticleService) CreateArticle(ctx context.Context, req *dto.CreateArticleReq, creatorID string, organizationID primitive.ObjectID) (*dto.CreateArticleResp, error) {
	// 验证状态
	if !dto.ValidateStatus(req.Status) {
		return nil, freeErrors.ParameterInvalidErr.WrapMsg("invalid article status")
	}

	// 创建文章对象
	article := &model.Article{
		ID:             primitive.NewObjectID(),
		Title:          req.Title,
		Content:        req.Content,
		CreatorID:      creatorID,
		Status:         dto.ToArticleStatus(req.Status),
		OrganizationID: organizationID,
	}

	// 保存到数据库
	if err := s.articleDao.Create(ctx, article); err != nil {
		return nil, errs.WrapMsg(err, "failed to create article")
	}

	return dto.NewCreateArticleResp(article.ID), nil
}

// UpdateArticle 更新文章
func (s *ArticleService) UpdateArticle(ctx context.Context, req *dto.UpdateArticleReq, organizationID primitive.ObjectID) (*dto.UpdateArticleResp, error) {
	// 转换文章ID
	articleID, err := primitive.ObjectIDFromHex(req.ID)
	if err != nil {
		return nil, freeErrors.ParameterInvalidErr.WrapMsg("invalid article ID")
	}

	// 检查文章是否存在且属于当前组织
	exists, err := s.articleDao.ExistByIDAndOrgID(ctx, articleID, organizationID)
	if err != nil {
		return nil, errs.WrapMsg(err, "failed to check article existence")
	}
	if !exists {
		return nil, freeErrors.NotFoundErr.WrapMsg("article not found")
	}

	// 构建更新参数
	updateParam := model.UpdateArticleParam{}

	if req.Title != nil {
		updateParam.Title = req.Title
	}
	if req.Content != nil {
		updateParam.Content = req.Content
	}
	if req.Status != nil {
		// 验证状态
		if !dto.ValidateStatus(*req.Status) {
			return nil, freeErrors.ParameterInvalidErr.WrapMsg("invalid article status")
		}
		status := dto.ToArticleStatus(*req.Status)
		updateParam.Status = &status
	}

	// 更新文章
	if err := s.articleDao.Update(ctx, articleID, updateParam); err != nil {
		return nil, errs.WrapMsg(err, "failed to update article")
	}

	return dto.NewUpdateArticleResp(articleID, time.Now().UTC()), nil
}

// GetArticleList 查询文章列表
func (s *ArticleService) GetArticleList(ctx context.Context, req *dto.GetArticleListReq, organizationID primitive.ObjectID) (*dto.ArticleListWithPaginationResp, error) {
	// 验证状态
	if req.Status != nil && !dto.ValidateStatus(*req.Status) {
		return nil, freeErrors.ParameterInvalidErr.WrapMsg("invalid article status")
	}

	// 构建过滤条件
	filter := model.ArticleListFilter{
		OrganizationID: organizationID,
	}

	// 将时间戳转换为 time.Time
	if req.StartTime != nil {
		startTime := time.Unix(*req.StartTime, 0).UTC()
		filter.StartTime = &startTime
	}
	if req.EndTime != nil {
		endTime := time.Unix(*req.EndTime, 0).UTC()
		filter.EndTime = &endTime
	}

	if req.Status != nil {
		status := dto.ToArticleStatus(*req.Status)
		filter.Status = &status
	}

	// 查询文章列表
	total, articles, err := s.articleDao.GetList(ctx, filter, req.Pagination)
	if err != nil {
		return nil, errs.WrapMsg(err, "failed to get article list")
	}

	return dto.NewArticleListWithPaginationResp(total, articles), nil
}

// GetArticleDetail 查询文章详情
func (s *ArticleService) GetArticleDetail(ctx context.Context, req *dto.GetArticleDetailReq, organizationID primitive.ObjectID) (*dto.ArticleDetailResp, error) {
	// 转换文章ID
	articleID, err := primitive.ObjectIDFromHex(req.ID)
	if err != nil {
		return nil, freeErrors.ParameterInvalidErr.WrapMsg("invalid article ID")
	}

	return s.GetArticleDetailByID(ctx, articleID, organizationID, false)
}

// GetArticleDetailByID 根据文章ID和组织ID查询文章详情
func (s *ArticleService) GetArticleDetailByID(ctx context.Context, articleID primitive.ObjectID, organizationID primitive.ObjectID, publicOnly bool) (*dto.ArticleDetailResp, error) {
	// 查询文章详情
	article, err := s.articleDao.GetByIDAndOrgID(ctx, articleID, organizationID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErr.WrapMsg("article not found")
		}
		return nil, errs.WrapMsg(err, "failed to get article detail")
	}

	// 如果是公开访问，检查文章状态，只有已发布的文章才能公开访问
	if publicOnly && article.Status != model.ArticleStatusPublished {
		return nil, freeErrors.NotFoundErr.WrapMsg("article not found")
	}

	return dto.NewArticleDetailResp(article), nil
}

// GetPublicArticleDetail 获取公开文章详情（用于普通用户访问）
func (s *ArticleService) GetPublicArticleDetail(ctx context.Context, articleID primitive.ObjectID, organizationID primitive.ObjectID) (*dto.ArticleDetailResp, error) {
	return s.GetArticleDetailByID(ctx, articleID, organizationID, true)
}

// GetArticleContent 获取文章正文内容
func (s *ArticleService) GetArticleContent(ctx context.Context, articleID primitive.ObjectID, organizationID primitive.ObjectID) (*dto.ArticleContentResp, error) {
	// 获取文章正文内容
	content, err := s.articleDao.GetContent(ctx, articleID, organizationID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErr.WrapMsg("article not found")
		}
		return nil, errs.WrapMsg(err, "failed to get article content")
	}

	return dto.NewArticleContentResp(articleID, content), nil
}
