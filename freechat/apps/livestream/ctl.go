package livestream

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/livestream/model"
	"github.com/openimsdk/chat/freechat/apps/livestream/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/ginUtils"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/tools/apiresp"
	"strings"
)

type LivestreamCtl struct{}

func NewLivestreamCtl() *LivestreamCtl {
	return &LivestreamCtl{}
}

func (o *LivestreamCtl) WebPostStartRecording(c *gin.Context) {
	reqData := svc.WebStartRecordingReq{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	//orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	//if err != nil {
	//	apiresp.GinError(c, err)
	//	return
	//}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	res, err := lsSvc.WebStartRecording(c, &reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, res)
}

func (o *LivestreamCtl) WebPostStopRecording(c *gin.Context) {
	reqData := svc.WebStopRecordingReq{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	//orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	//if err != nil {
	//	apiresp.GinError(c, err)
	//	return
	//}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	res, err := lsSvc.WebStopRecording(c, &reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, res)
}

func (o *LivestreamCtl) WebPostListRecording(c *gin.Context) {
	reqData := svc.WebListRecordingReq{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	//orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	//if err != nil {
	//	apiresp.GinError(c, err)
	//	return
	//}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	res, err := lsSvc.WebListRecording(c, &reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, res)
}

func (o *LivestreamCtl) WebPostCreateStream(c *gin.Context) {
	reqData := svc.CreateStreamParams{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	resp, err := lsSvc.WebCreateStream(c, orgInfo.OrgUser, reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

func (o *LivestreamCtl) WebPostJoinStream(c *gin.Context) {
	reqData := svc.JoinStreamParams{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	resp, err := lsSvc.WebJoinStream(c, orgInfo.OrgUser, reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

func (o *LivestreamCtl) WebPostInviteToStage(c *gin.Context) {
	reqData := svc.InviteToStageParams{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	err = lsSvc.WebInviteToStage(c, orgInfo.OrgUser.ImServerUserId, reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (o *LivestreamCtl) WebPostRemoveFromStage(c *gin.Context) {
	reqData := svc.RemoveFromStageParams{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	err = lsSvc.WebRemoveFromStage(c, orgInfo.OrgUser.ImServerUserId, reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (o *LivestreamCtl) WebPostBlockViewer(c *gin.Context) {
	reqData := svc.BlockViewerParams{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	err = lsSvc.WebBlockViewer(c, orgInfo.OrgUser.ImServerUserId, reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (o *LivestreamCtl) WebPostApproveHandRaise(c *gin.Context) {
	reqData := svc.ApproveHandRaiseParams{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	err = lsSvc.WebApproveHandRaise(c, orgInfo.OrgUser.ImServerUserId, reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (o *LivestreamCtl) WebPostRaiseHandRaise(c *gin.Context) {
	reqData := svc.RaiseHandParams{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	err = lsSvc.WebRaiseHand(c, orgInfo.OrgUser.ImServerUserId, reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (o *LivestreamCtl) WebPostStopStream(c *gin.Context) {
	reqData := svc.StopStreamParams{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	err = lsSvc.WebStopStream(c, orgInfo.OrgUser, reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (o *LivestreamCtl) WebPostSetAdminRole(c *gin.Context) {
	reqData := svc.SetAdminParams{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	err = lsSvc.WebSetAdminRole(c, orgInfo.OrgUser.ImServerUserId, reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (o *LivestreamCtl) WebPostRevokeAdminRole(c *gin.Context) {
	reqData := svc.RevokeAdminRoleParams{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	err = lsSvc.WebRevokeAdminRole(c, orgInfo.OrgUser.ImServerUserId, reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

type LivestreamStatisticsCtl struct{}

func NewLivestreamStatisticsCtl() *LivestreamStatisticsCtl {
	return &LivestreamStatisticsCtl{}
}

// WebGetLivestreamStatistics 获取单个房间统计记录
func (w *LivestreamStatisticsCtl) WebGetLivestreamStatistics(c *gin.Context) {
	roomName := strings.TrimSpace(c.Query("room_name"))

	lsSvc := svc.NewLivestreamStatisticsService()
	resp, err := lsSvc.WebDetailStatisticsSvc(roomName)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

// WebGetListLivestreamStatistics 批量获取房间统计记录
func (w *LivestreamStatisticsCtl) WebGetListLivestreamStatistics(c *gin.Context) {
	page, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, freeErrors.PageParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	startTimeUtc, err := ginUtils.QueryToUtcTime(c, "start_time")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	endTimeUtc, err := ginUtils.QueryToUtcTime(c, "end_time")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	status := model.LivestreamStatisticsStatus(strings.TrimSpace(c.Query("status")))
	if status == "" {
		status = model.LivestreamStatisticsStatusStart
	}

	lsSvc := svc.NewLivestreamStatisticsService()
	resp, err := lsSvc.WebListStatisticsSvc(orgInfo.OrgUser.OrganizationId, c.Query("keyword"), status, startTimeUtc, endTimeUtc, page)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CmsPostDetailRecordFileUrl 获取单个房间录制文件下载地址
func (w *LivestreamStatisticsCtl) CmsPostDetailRecordFileUrl(c *gin.Context) {
	reqData := svc.CmsDetailRecordFileUrlReq{}
	if err := c.ShouldBind(&reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	//orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	//if err != nil {
	//	apiresp.GinError(c, err)
	//	return
	//}

	lsSvc, err := svc.NewLivestreamService()
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	res, err := lsSvc.CmsDetailRecordFileUrl(c, &reqData)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, res)
}

func (w *LivestreamStatisticsCtl) CmsGetListLivestreamStatistics(c *gin.Context) {
	page, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, freeErrors.PageParameterInvalidErr)
		return
	}

	// todo 遗漏权限校验
	//orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	//if err != nil {
	//	apiresp.GinError(c, err)
	//	return
	//}
	orgId, err := ginUtils.QueryToObjectId(c, "org_id")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	startTimeUtc, err := ginUtils.QueryToUtcTime(c, "start_time")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	endTimeUtc, err := ginUtils.QueryToUtcTime(c, "end_time")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	status := model.LivestreamStatisticsStatus(strings.TrimSpace(c.Query("status")))

	lsSvc := svc.NewLivestreamStatisticsService()
	resp, err := lsSvc.WebListStatisticsSvc(orgId, c.Query("keyword"), status, startTimeUtc, endTimeUtc, page)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}
