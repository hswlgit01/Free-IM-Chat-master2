package group

import (
	"context"
	"github.com/openimsdk/tools/log"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/group/dto"
	"github.com/openimsdk/chat/freechat/apps/group/svc"
	opModel "github.com/openimsdk/chat/freechat/apps/operationLog/model"
	opSvc "github.com/openimsdk/chat/freechat/apps/operationLog/svc"

	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/protocol/group"
	"github.com/openimsdk/tools/apiresp"
)

type GroupCtl struct{}

func NewGroupCtl() *GroupCtl {
	return &GroupCtl{}
}

// GetGroupsByOrgID 获取群组列表
func (a *GroupCtl) GetGroupsByOrgID(c *gin.Context) {
	var req dto.GetGroupsByOrgIDReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	userID := mctx.GetOpUserID(c)
	if userID == "" {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 从上下文中获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgGroupSvc := svc.NewOrgGroupSvc()
	resp, err := orgGroupSvc.GetGroupsByOrgID(c.Request.Context(), operationID, userID,
		req.Pagination.PageNumber, req.Pagination.ShowNumber, req.GroupName, org.Organization)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CreateGroupWithOrg 创建群组
func (a *GroupCtl) CreateGroupWithOrg(c *gin.Context) {
	var req group.CreateGroupReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	userID := mctx.GetOpUserID(c)
	if userID == "" {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 从上下文中获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgGroupSvc := svc.NewOrgGroupSvc()
	resp, err := orgGroupSvc.CmsCreateGroupWithOrg(c.Request.Context(), req, userID, operationID, org.Organization, org.OrgUser)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeCreateGroup,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, resp)
}

// GetMembers 获取群组成员列表
func (a *GroupCtl) GetMembers(c *gin.Context) {
	var req svc.ListGroupMembersReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	//groupId := c.Query("group_id")
	//if groupId == "" {
	//	apiresp.GinError(c, freeErrors.ParameterInvalidErr)
	//	return
	//}
	//
	//keyword := c.Query("keyword")
	//
	//pagination, err := paginationUtils.QueryToDepPagination(c)
	//if err != nil {
	//	apiresp.GinError(c, err)
	//	return
	//}

	groupSvc := svc.NewOrgGroupSvc()
	resp, err := groupSvc.ListGroupMembers(context.TODO(), org.ID, operationID, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// PostInviteUserToGroup 管理员添加群成员
func (a *GroupCtl) PostInviteUserToGroup(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req group.InviteUserToGroupReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	groupSvc := svc.NewOrgGroupSvc()
	err = groupSvc.InviteUserToGroup(context.TODO(), req, org.ID, operationID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// PostDismissGroup 解散群组
func (a *GroupCtl) PostDismissGroup(c *gin.Context) {
	var req svc.DismissGroupReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	groupSvc := svc.NewOrgGroupSvc()
	resp, err := groupSvc.DismissGroup(context.TODO(), req, org.ID, operationID, org.OrgUser)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (a *GroupCtl) PostMuteGroup(c *gin.Context) {
	var req svc.MuteGroupReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	groupSvc := svc.NewOrgGroupSvc()
	resp, err := groupSvc.MuteGroup(context.TODO(), req, org.ID, operationID, org.OrgUser)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (a *GroupCtl) PostCancelMuteGroup(c *gin.Context) {
	var req svc.CancelMuteGroupReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	groupSvc := svc.NewOrgGroupSvc()
	resp, err := groupSvc.CancelMuteGroup(context.TODO(), req, org.ID, operationID, org.OrgUser)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (a *GroupCtl) PostUpdateGroupInfo(c *gin.Context) {
	var req group.SetGroupInfoReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	groupSvc := svc.NewOrgGroupSvc()
	resp, err := groupSvc.UpdateGroupInfo(context.TODO(), req, org.ID, operationID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (a *GroupCtl) PostUpdateGroupMemberInfo(c *gin.Context) {
	var req group.SetGroupMemberInfoReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	groupSvc := svc.NewOrgGroupSvc()
	resp, err := groupSvc.UpdateGroupMemberInfo(context.TODO(), req, org.ID, operationID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (a *GroupCtl) PostTransferGroup(c *gin.Context) {
	var req group.TransferGroupOwnerReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	groupSvc := svc.NewOrgGroupSvc()

	resp, err := groupSvc.TransferGroupOwner(context.TODO(), req, org.ID, operationID, org.OrgUser)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (a *GroupCtl) PostMuteGroupMember(c *gin.Context) {
	var req group.MuteGroupMemberReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	groupSvc := svc.NewOrgGroupSvc()
	resp, err := groupSvc.MuteGroupMember(context.TODO(), req, org.ID, operationID, org.OrgUser)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (a *GroupCtl) PostCancelMuteGroupMember(c *gin.Context) {
	var req group.CancelMuteGroupMemberReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	groupSvc := svc.NewOrgGroupSvc()
	resp, err := groupSvc.CancelMuteGroupMember(context.TODO(), req, org.ID, operationID, org.OrgUser)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (a *GroupCtl) PostKickGroupMember(c *gin.Context) {
	var req group.KickGroupMemberReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	groupSvc := svc.NewOrgGroupSvc()
	resp, err := groupSvc.KickGroupMember(context.TODO(), req, org.ID, operationID, org.OrgUser)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}
