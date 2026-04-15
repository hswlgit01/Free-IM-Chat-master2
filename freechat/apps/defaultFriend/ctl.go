package defaultFriend

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/defaultFriend/svc"
	opModel "github.com/openimsdk/chat/freechat/apps/operationLog/model"
	opSvc "github.com/openimsdk/chat/freechat/apps/operationLog/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/log"
)

type DefaultFriendCtl struct {
}

func NewDefaultFriendCtl() *DefaultFriendCtl {
	return &DefaultFriendCtl{}
}

func (w *DefaultFriendCtl) CmsPostCreateDefaultFriend(c *gin.Context) {
	var req svc.SuperCmsAddDefaultFriendReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	defaultFriendSvc := svc.NewDefaultFriendSvc()

	req.OrgId = org.ID
	err = defaultFriendSvc.SuperCmsAddDefaultFriend(c, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeCreateDefaultFriend,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *DefaultFriendCtl) CmsGetListDefaultFriend(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	pagination, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	defaultFriendSvc := svc.NewDefaultFriendSvc()
	resp, err := defaultFriendSvc.SuperCmsListDefaultFriend(c, org.ID, c.Query("keyword"), pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (w *DefaultFriendCtl) CmsPostDeleteDefaultFriend(c *gin.Context) {
	var req svc.SuperCmsDelDefaultFriendReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	defaultFriendSvc := svc.NewDefaultFriendSvc()
	req.OrgId = org.ID
	err = defaultFriendSvc.SuperCmsDelDefaultFriend(c, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeDeleteDefaultFriend,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *DefaultFriendCtl) CmsGetSearchDefaultFriend(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lotterySvc := svc.NewDefaultFriendSvc()
	resp, err := lotterySvc.SuperCmsSearchDefaultFriend(c, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}
