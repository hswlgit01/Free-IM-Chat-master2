package defaultGroup

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/defaultGroup/svc"
	opModel "github.com/openimsdk/chat/freechat/apps/operationLog/model"
	opSvc "github.com/openimsdk/chat/freechat/apps/operationLog/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/log"
)

type DefaultGroupCtl struct {
}

func NewDefaultGroupCtl() *DefaultGroupCtl {
	return &DefaultGroupCtl{}
}

func (w *DefaultGroupCtl) CmsPostCreateDefaultGroup(c *gin.Context) {
	var req svc.SuperCmsAddDefaultGroupReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	defaultGroupSvc := svc.NewDefaultGroupSvc()

	req.OrgId = org.ID
	err = defaultGroupSvc.SuperCmsAddDefaultGroup(c, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeCreateDefaultGroup,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *DefaultGroupCtl) CmsGetListDefaultGroup(c *gin.Context) {
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

	defaultGroupSvc := svc.NewDefaultGroupSvc()
	resp, err := defaultGroupSvc.SuperCmsListDefaultGroup(c, org.ID, c.Query("keyword"), pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (w *DefaultGroupCtl) CmsPostDeleteDefaultGroup(c *gin.Context) {
	var req svc.SuperCmsDelDefaultGroupReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	defaultGroupSvc := svc.NewDefaultGroupSvc()
	req.OrgId = org.ID
	err = defaultGroupSvc.SuperCmsDelDefaultGroup(c, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeDeleteDefaultGroup,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *DefaultGroupCtl) CmsGetSearchDefaultGroup(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	defaultGroupSvc := svc.NewDefaultGroupSvc()
	resp, err := defaultGroupSvc.SuperCmsSearchDefaultGroup(c, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}
