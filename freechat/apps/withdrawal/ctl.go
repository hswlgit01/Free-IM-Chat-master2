package withdrawal

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/withdrawal/dto"
	"github.com/openimsdk/chat/freechat/apps/withdrawal/svc"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
)

type WithdrawalCtl struct{}

func NewWithdrawalCtl() *WithdrawalCtl {
	return &WithdrawalCtl{}
}

// GetWithdrawalRule 获取提现规则
// @router GET /wallet/withdrawal/rule
func (w *WithdrawalCtl) GetWithdrawalRule(c *gin.Context) {
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

	withdrawalSvc := svc.NewWithdrawalSvc()
	rule, err := withdrawalSvc.GetWithdrawalRule(c.Request.Context(), organizationID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, rule)
}

// SubmitWithdrawal 提交提现申请
// @router POST /wallet/withdrawal/submit
func (w *WithdrawalCtl) SubmitWithdrawal(c *gin.Context) {
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

	var req dto.SubmitWithdrawalReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	withdrawalSvc := svc.NewWithdrawalSvc()
	resp, err := withdrawalSvc.SubmitWithdrawal(c.Request.Context(), opUserID, organizationID, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// GetWithdrawalRecordList 获取提现记录列表
// @router GET /wallet/withdrawal/records
func (w *WithdrawalCtl) GetWithdrawalRecordList(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.GetWithdrawalRecordListReq
	if err := c.ShouldBindQuery(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	withdrawalSvc := svc.NewWithdrawalSvc()
	resp, err := withdrawalSvc.GetWithdrawalRecordList(c.Request.Context(), opUserID, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// GetWithdrawalDetail 获取提现详情
// @router GET /wallet/withdrawal/detail/:orderNo
func (w *WithdrawalCtl) GetWithdrawalDetail(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orderNo := c.Param("orderNo")
	if orderNo == "" {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg("order number is required"))
		return
	}

	withdrawalSvc := svc.NewWithdrawalSvc()
	resp, err := withdrawalSvc.GetWithdrawalDetail(c.Request.Context(), opUserID, orderNo)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CancelWithdrawal 取消提现
// @router POST /wallet/withdrawal/cancel
func (w *WithdrawalCtl) CancelWithdrawal(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.CancelWithdrawalReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	withdrawalSvc := svc.NewWithdrawalSvc()
	if err := withdrawalSvc.CancelWithdrawal(c.Request.Context(), opUserID, &req); err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, nil)
}

// CheckPendingWithdrawal 检查是否有未处理的提现申请
// @router GET /wallet/withdrawal/check-pending
func (w *WithdrawalCtl) CheckPendingWithdrawal(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	withdrawalSvc := svc.NewWithdrawalSvc()
	resp, err := withdrawalSvc.CheckPendingWithdrawal(c.Request.Context(), opUserID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}
