package webhook

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/webhook/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/ginUtils"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/tools/apiresp"
	"strconv"
)

type WebhookCtl struct {
}

func NewWebhookCtl() *WebhookCtl {
	return &WebhookCtl{}
}

func (w *WebhookCtl) PostCreateWebhook(c *gin.Context) {
	var req svc.CreateWebhookReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 从上下文中获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	webhookSvc := svc.NewOrgWebhookSvc()

	err = webhookSvc.CreateWebhook(c, req, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *WebhookCtl) PostUpdateWebhook(c *gin.Context) {
	var req svc.UpdateWebhookReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 从上下文中获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	webhookSvc := svc.NewOrgWebhookSvc()

	err = webhookSvc.UpdateWebhook(c, req, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *WebhookCtl) PostDeleteWebhook(c *gin.Context) {
	var req svc.DeleteWebhookReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 从上下文中获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	webhookSvc := svc.NewOrgWebhookSvc()

	err = webhookSvc.DeleteWebhook(c, req, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *WebhookCtl) GetWebhook(c *gin.Context) {
	pagination, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgId, err := ginUtils.QueryToObjectId(c, "organization_id")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	keyword := c.Query("keyword")

	statusQuery := c.Query("status")
	var status *bool
	if statusQuery != "" {
		result, _ := strconv.ParseBool(statusQuery)
		status = &result
	}

	webhookSvc := svc.NewOrgWebhookSvc()
	resp, err := webhookSvc.ListWebhook(c, orgId, status, keyword, pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

type WebhookTriggerCtl struct {
}

func NewWebhookTriggerCtl() *WebhookTriggerCtl {
	return &WebhookTriggerCtl{}
}

func (w *WebhookTriggerCtl) GetWebhookTriggerEvent(c *gin.Context) {
	webhookEventSvc := svc.NewWebhookEventSvc()
	apiresp.GinSuccess(c, webhookEventSvc.ListWebhookEvent())
}
