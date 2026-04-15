package article

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/article/dto"
	"github.com/openimsdk/chat/freechat/apps/article/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/tools/apiresp"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ArticleCtl struct{}

func NewArticleCtl() *ArticleCtl {
	return &ArticleCtl{}
}

// CmsPostCreateArticle 创建文章接口
func (a *ArticleCtl) CmsPostCreateArticle(c *gin.Context) {
	var req dto.CreateArticleReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 获取当前用户ID作为创建者
	userID := org.OrgUser.UserId

	// 创建文章
	articleSvc := svc.NewArticleService()
	resp, err := articleSvc.CreateArticle(c.Request.Context(), &req, userID, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CmsPostUpdateArticle 更新文章接口
func (a *ArticleCtl) CmsPostUpdateArticle(c *gin.Context) {
	var req dto.UpdateArticleReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 更新文章
	articleSvc := svc.NewArticleService()
	resp, err := articleSvc.UpdateArticle(c.Request.Context(), &req, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CmsPostArticleList 查询文章列表接口
func (a *ArticleCtl) CmsPostArticleList(c *gin.Context) {
	var req dto.GetArticleListReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 查询文章列表
	articleSvc := svc.NewArticleService()
	resp, err := articleSvc.GetArticleList(c.Request.Context(), &req, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CmsGetArticleDetail 查询文章详情接口
func (a *ArticleCtl) CmsGetArticleDetail(c *gin.Context) {
	// 获取文章ID参数
	articleID := c.Param("id")
	if articleID == "" {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 构建请求参数
	req := dto.GetArticleDetailReq{
		ID: articleID,
	}

	// 获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 查询文章详情
	articleSvc := svc.NewArticleService()
	resp, err := articleSvc.GetArticleDetail(c.Request.Context(), &req, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// GetPublicArticleDetail 获取公开文章详情接口（普通用户访问）
func (a *ArticleCtl) GetPublicArticleDetail(c *gin.Context) {
	// 获取文章ID参数
	articleIDStr := c.Param("id")
	if articleIDStr == "" {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 从请求头获取组织ID
	orgIDStr := c.GetHeader("orgid")
	if orgIDStr == "" {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 转换组织ID
	orgID, err := primitive.ObjectIDFromHex(orgIDStr)
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 转换文章ID
	articleID, err := primitive.ObjectIDFromHex(articleIDStr)
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 查询公开文章详情（只返回已发布的文章）
	articleSvc := svc.NewArticleService()
	resp, err := articleSvc.GetPublicArticleDetail(c.Request.Context(), articleID, orgID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}
