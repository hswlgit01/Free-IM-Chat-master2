package points

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/openimsdk/chat/freechat/apps/points/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/tools/apiresp"
)

type PointsCtl struct{}

func NewPointsCtl() *PointsCtl {
	return &PointsCtl{}
}

// QueryPointsRecords 查询积分记录列表（管理端）
func (w *PointsCtl) QueryPointsRecords(c *gin.Context) {
	var req svc.QueryPointsRecordsReq
	if err := c.ShouldBindWith(&req, binding.JSON); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取组织信息
	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	result, err := svc.NewPointsSvc().QueryPointsRecords(c.Request.Context(), &req, orgInfo.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, result)
}

// QueryUserPoints 查询用户积分列表（用户端）
func (w *PointsCtl) QueryUserPoints(c *gin.Context) {
	var req svc.UserPointsReq
	if err := c.ShouldBindWith(&req, binding.JSON); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取组织信息
	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	result, err := svc.NewPointsSvc().QueryUserPoints(c.Request.Context(), &req, orgInfo.OrgUser.ImServerUserId, orgInfo.OrgUser.OrganizationId)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, result)
}
