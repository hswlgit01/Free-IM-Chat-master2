package withdrawal

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/withdrawal/dto"
	"github.com/openimsdk/chat/freechat/apps/withdrawal/svc"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
)

type AdminWithdrawalCtl struct{}

func NewAdminWithdrawalCtl() *AdminWithdrawalCtl {
	return &AdminWithdrawalCtl{}
}

// GetWithdrawalList 获取提现列表
// @router GET /withdrawal/list
func (a *AdminWithdrawalCtl) GetWithdrawalList(c *gin.Context) {
	_, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	organizationID, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.AdminGetWithdrawalListReq
	if err := c.ShouldBindQuery(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	adminSvc := svc.NewAdminWithdrawalSvc()
	resp, err := adminSvc.GetWithdrawalList(c.Request.Context(), organizationID, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// GetWithdrawalDetail 获取提现详情
// @router GET /withdrawal/detail/:id
func (a *AdminWithdrawalCtl) GetWithdrawalDetail(c *gin.Context) {
	_, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	organizationID, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	recordID := c.Param("id")
	if recordID == "" {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg("record ID is required"))
		return
	}

	adminSvc := svc.NewAdminWithdrawalSvc()
	resp, err := adminSvc.GetWithdrawalDetail(c.Request.Context(), organizationID, recordID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// ApproveWithdrawal 审批通过提现
// @router POST /withdrawal/approve
func (a *AdminWithdrawalCtl) ApproveWithdrawal(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	organizationID, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.AdminApproveWithdrawalReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	adminSvc := svc.NewAdminWithdrawalSvc()
	if err := adminSvc.ApproveWithdrawal(c.Request.Context(), organizationID, opUserID, &req); err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, nil)
}

// RejectWithdrawal 审批拒绝提现
// @router POST /withdrawal/reject
func (a *AdminWithdrawalCtl) RejectWithdrawal(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	organizationID, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.AdminRejectWithdrawalReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	adminSvc := svc.NewAdminWithdrawalSvc()
	if err := adminSvc.RejectWithdrawal(c.Request.Context(), organizationID, opUserID, &req); err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, nil)
}

// TransferWithdrawal 确认打款
// @router POST /withdrawal/transfer
func (a *AdminWithdrawalCtl) TransferWithdrawal(c *gin.Context) {
	_, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	organizationID, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.AdminTransferWithdrawalReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	adminSvc := svc.NewAdminWithdrawalSvc()
	if err := adminSvc.TransferWithdrawal(c.Request.Context(), organizationID, &req); err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, nil)
}

// CompleteWithdrawal 确认完成
// @router POST /withdrawal/complete
func (a *AdminWithdrawalCtl) CompleteWithdrawal(c *gin.Context) {
	_, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	organizationID, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.AdminCompleteWithdrawalReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	adminSvc := svc.NewAdminWithdrawalSvc()
	if err := adminSvc.CompleteWithdrawal(c.Request.Context(), organizationID, &req); err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, nil)
}

// BatchApprove 批量审批
// @router POST /withdrawal/batch-approve
func (a *AdminWithdrawalCtl) BatchApprove(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	organizationID, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.AdminBatchApproveReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	adminSvc := svc.NewAdminWithdrawalSvc()
	if err := adminSvc.BatchApprove(c.Request.Context(), organizationID, opUserID, &req); err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, nil)
}

// GetWithdrawalRule 获取提现规则
// @router GET /withdrawal/rule
func (a *AdminWithdrawalCtl) GetWithdrawalRule(c *gin.Context) {
	_, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	organizationID, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	adminSvc := svc.NewAdminWithdrawalSvc()
	rule, err := adminSvc.GetWithdrawalRule(c.Request.Context(), organizationID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, rule)
}

// SaveWithdrawalRule 保存提现规则
// @router POST /withdrawal/rule
func (a *AdminWithdrawalCtl) SaveWithdrawalRule(c *gin.Context) {
	_, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	organizationID, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.SaveWithdrawalRuleReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	adminSvc := svc.NewAdminWithdrawalSvc()
	if err := adminSvc.SaveWithdrawalRule(c.Request.Context(), organizationID, &req); err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, nil)
}
