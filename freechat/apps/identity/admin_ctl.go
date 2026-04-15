package identity

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/identity/dto"
	"github.com/openimsdk/chat/freechat/apps/identity/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
)

type AdminIdentityCtl struct{}

func NewAdminIdentityCtl() *AdminIdentityCtl {
	return &AdminIdentityCtl{}
}

// AdminGetIdentityList 管理员获取认证列表
func (a *AdminIdentityCtl) AdminGetIdentityList(ctx *gin.Context) {
	var req dto.AdminGetIdentityListReq
	if err := ctx.ShouldBind(&req); err != nil {
		apiresp.GinError(ctx, freeErrors.ParameterInvalidErr)
		return
	}

	// CheckOrganization middleware已验证权限
	orgInfo, err := middleware.GetOrgInfoFromCtx(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	// 调用服务层获取列表，传入组织ID
	identitySvc := svc.NewIdentitySvc()
	resp, err := identitySvc.AdminGetIdentityList(ctx, &req, orgInfo.ID.Hex())
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	apiresp.GinSuccess(ctx, resp)
}

// AdminGetIdentityDetail 按 keyword 查询单条实名认证详情（响应结构与 /third_admin/identity/list 相同：data.total + data.list）
func (a *AdminIdentityCtl) AdminGetIdentityDetail(ctx *gin.Context) {
	var req dto.AdminGetIdentityDetailReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		apiresp.GinError(ctx, freeErrors.ParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	identitySvc := svc.NewIdentitySvc()
	resp, err := identitySvc.AdminGetIdentityDetailByKeyword(ctx, req.Keyword, orgInfo.ID.Hex())
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	apiresp.GinSuccess(ctx, resp)
}

// AdminGetPendingIdentityList 待审核实名列表（status=审核中）
func (a *AdminIdentityCtl) AdminGetPendingIdentityList(ctx *gin.Context) {
	var req dto.AdminGetPendingIdentityListReq
	if err := ctx.ShouldBindQuery(&req); err != nil {
		apiresp.GinError(ctx, freeErrors.ParameterInvalidErr)
		return
	}

	orgInfo, err := middleware.GetOrgInfoFromCtx(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	identitySvc := svc.NewIdentitySvc()
	resp, err := identitySvc.AdminGetPendingIdentityList(ctx, &req, orgInfo.ID.Hex())
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}
	apiresp.GinSuccess(ctx, resp)
}

// AdminApprove 管理员审核通过
func (a *AdminIdentityCtl) AdminApprove(ctx *gin.Context) {
	var req dto.AdminApproveReq
	if err := ctx.ShouldBind(&req); err != nil {
		apiresp.GinError(ctx, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取当前用户ID（CheckOrganization middleware已验证权限）
	_, err := middleware.GetOrgInfoFromCtx(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	adminID, _, err := mctx.Check(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	// 调用服务层审核通过
	identitySvc := svc.NewIdentitySvc()
	err = identitySvc.AdminApprove(ctx, req.UserID, adminID)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	apiresp.GinSuccess(ctx, map[string]interface{}{})
}

// AdminApproveBatch 批量审核通过（与 AdminApprove 单条逻辑相同）
func (a *AdminIdentityCtl) AdminApproveBatch(ctx *gin.Context) {
	var req dto.AdminApproveBatchReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(ctx, freeErrors.ParameterInvalidErr)
		return
	}

	if _, err := middleware.GetOrgInfoFromCtx(ctx); err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	adminID, _, err := mctx.Check(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	identitySvc := svc.NewIdentitySvc()
	resp := identitySvc.AdminApproveBatch(ctx, req.UserIDs, adminID)
	apiresp.GinSuccess(ctx, resp)
}

// AdminReject 管理员审核拒绝
func (a *AdminIdentityCtl) AdminReject(ctx *gin.Context) {
	var req dto.AdminRejectReq
	if err := ctx.ShouldBind(&req); err != nil {
		apiresp.GinError(ctx, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取当前用户ID（CheckOrganization middleware已验证权限）
	_, err := middleware.GetOrgInfoFromCtx(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	adminID, _, err := mctx.Check(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	// 调用服务层审核拒绝
	identitySvc := svc.NewIdentitySvc()
	err = identitySvc.AdminReject(ctx, req.UserID, adminID, req.RejectReason)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	apiresp.GinSuccess(ctx, map[string]interface{}{})
}

// AdminCancelVerification 管理员取消用户实名认证
func (a *AdminIdentityCtl) AdminCancelVerification(ctx *gin.Context) {
	var req dto.AdminCancelVerificationReq
	if err := ctx.ShouldBind(&req); err != nil {
		apiresp.GinError(ctx, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取当前用户ID（CheckOrganization middleware已验证权限）
	_, err := middleware.GetOrgInfoFromCtx(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	adminID, _, err := mctx.Check(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	// 调用服务层取消认证
	identitySvc := svc.NewIdentitySvc()
	err = identitySvc.AdminCancelVerification(ctx, req.UserID, adminID)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	apiresp.GinSuccess(ctx, map[string]interface{}{})
}
