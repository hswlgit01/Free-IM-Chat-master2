package operationLog

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/operationLog/model"
	"github.com/openimsdk/chat/freechat/apps/operationLog/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/ginUtils"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/tools/apiresp"
)

type GroupOperationLogCtl struct{}

func NewGroupOperationLogCtl() *GroupOperationLogCtl {
	return &GroupOperationLogCtl{}
}

// CmsGetGroupOperationLog 后台获取群组操作日志
func (w *GroupOperationLogCtl) CmsGetGroupOperationLog(c *gin.Context) {

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	group := c.Query("groupId")

	page, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, freeErrors.PageParameterInvalidErr)
		return
	}

	startTimeUtc, err := ginUtils.QueryToUtcTime(c, "startTime")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	endTimeUtc, err := ginUtils.QueryToUtcTime(c, "endTime")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	groupOperationLogSvc := svc.NewGroupOperationLogSvc()
	resp, err := groupOperationLogSvc.ListGroupOperationLogSvc(org.ID, group, startTimeUtc, endTimeUtc, page)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

type OperationLogCtl struct{}

func NewOperationLogCtl() *OperationLogCtl {
	return &OperationLogCtl{}
}
func (w *OperationLogCtl) CmsGetListOperationLog(c *gin.Context) {
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

	operationLogTypeStr := c.Query("operation_log_type")
	operationLogType := model.OperationLogType(operationLogTypeStr)

	operationLogSvc := svc.NewOperationLogSvc()
	resp, err := operationLogSvc.CmsListOperationLog(c, org.ID, operationLogType, c.Query("keyword"), pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}
