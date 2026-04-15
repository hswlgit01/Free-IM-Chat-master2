package admin

import (
	"github.com/gin-gonic/gin"
	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	walletSvc "github.com/openimsdk/chat/freechat/apps/wallet/svc"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/shopspring/decimal"
)

// 补偿金系统设置响应
type CompensationSettingsResp struct {
	Enabled       bool   `json:"enabled"`        // 是否启用补偿金系统
	InitialAmount string `json:"initial_amount"` // 初始补偿金金额
	NoticeText    string `json:"notice_text"`    // 钱包开通时显示的说明文本
}

// 获取补偿金系统设置
func (o *Api) GetCompensationSettings(c *gin.Context) {
	// 验证管理员权限
	userID, err := mctx.CheckAdmin(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 获取管理员信息
	adminInfo, err := o.adminClient.GetAdminInfo(c, nil)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 创建组织用户对象
	var role orgModel.OrganizationUserRole

	// 根据管理员级别设置相应的角色
	switch adminInfo.Level {
	case 1:
		role = orgModel.OrganizationUserSuperAdminRole
	case 2:
		role = orgModel.OrganizationUserBackendAdminRole
	default:
		role = orgModel.OrganizationUserNormalRole
	}

	orgUser := &orgModel.OrganizationUser{
		UserId: userID,
		Role:   role,
	}

	// 获取补偿金系统设置
	compensationAdminSvc := walletSvc.NewCompensationAdminService()
	settings, err := compensationAdminSvc.GetCompensationSystemSettings(c, orgUser)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 添加调试日志
	log.ZInfo(c, "补偿金设置信息", "settings", settings, "notice_text", settings.NoticeText)

	// 返回响应
	resp := &CompensationSettingsResp{
		Enabled:       settings.Enabled,
		InitialAmount: settings.InitialAmount.String(),
		NoticeText:    settings.NoticeText,
	}

	// 调试响应内容
	log.ZInfo(c, "API响应", "response", resp)
	apiresp.GinSuccess(c, resp)
}

// 更新补偿金系统设置请求
type UpdateCompensationSettingsReq struct {
	Enabled       bool   `json:"enabled"`        // 是否启用补偿金系统
	InitialAmount string `json:"initial_amount"` // 初始补偿金金额
	NoticeText    string `json:"notice_text"`    // 钱包开通时显示的说明文本
}

// 更新补偿金系统设置
func (o *Api) UpdateCompensationSettings(c *gin.Context) {
	// 解析请求
	var req UpdateCompensationSettingsReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, errs.ErrArgs)
		return
	}

	// 验证管理员权限
	userID, err := mctx.CheckAdmin(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 获取管理员信息
	adminInfo, err := o.adminClient.GetAdminInfo(c, nil)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 创建组织用户对象
	var role orgModel.OrganizationUserRole

	// 根据管理员级别设置相应的角色
	switch adminInfo.Level {
	case 1:
		role = orgModel.OrganizationUserSuperAdminRole
	case 2:
		role = orgModel.OrganizationUserBackendAdminRole
	default:
		role = orgModel.OrganizationUserNormalRole
	}

	orgUser := &orgModel.OrganizationUser{
		UserId: userID,
		Role:   role,
	}

	// 解析初始补偿金金额
	initialAmount, err := decimal.NewFromString(req.InitialAmount)
	if err != nil {
		apiresp.GinError(c, errs.ErrArgs.WrapMsg("invalid initial amount"))
		return
	}

	// 添加调试日志
	log.ZInfo(c, "更新补偿金设置请求", "request", req, "notice_text", req.NoticeText)

	// 更新补偿金系统设置
	compensationAdminSvc := walletSvc.NewCompensationAdminService()
	err = compensationAdminSvc.UpdateCompensationSystemSettings(c, orgUser, &walletSvc.UpdateCompensationSystemSettingsReq{
		Enabled:       req.Enabled,
		InitialAmount: initialAmount,
		NoticeText:    req.NoticeText,
	})
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, nil)
}

// 获取用户补偿金余额请求
type GetUserCompensationBalanceReq struct {
	UserID     string `json:"user_id"`     // 用户ID
	CurrencyID string `json:"currency_id"` // 货币ID
}

// 获取用户补偿金余额响应
type GetUserCompensationBalanceResp struct {
	UserID              string `json:"user_id"`              // 用户ID
	Username            string `json:"username"`             // 用户名
	WalletID            string `json:"wallet_id"`            // 钱包ID
	CurrencyID          string `json:"currency_id"`          // 币种ID
	CurrencyName        string `json:"currency_name"`        // 币种名称
	CompensationBalance string `json:"compensation_balance"` // 补偿金余额
}

// 获取用户补偿金余额
func (o *Api) GetUserCompensationBalance(c *gin.Context) {
	// 解析请求
	var req GetUserCompensationBalanceReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, errs.ErrArgs)
		return
	}

	// 验证管理员权限
	userID, err := mctx.CheckAdmin(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 获取管理员信息
	adminInfo, err := o.adminClient.GetAdminInfo(c, nil)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 创建组织用户对象
	var role orgModel.OrganizationUserRole

	// 根据管理员级别设置相应的角色
	switch adminInfo.Level {
	case 1:
		role = orgModel.OrganizationUserSuperAdminRole
	case 2:
		role = orgModel.OrganizationUserBackendAdminRole
	default:
		role = orgModel.OrganizationUserNormalRole
	}

	orgUser := &orgModel.OrganizationUser{
		UserId: userID,
		Role:   role,
	}

	// 获取用户补偿金余额
	compensationAdminSvc := walletSvc.NewCompensationAdminService()
	userBalance, err := compensationAdminSvc.GetUserCompensationBalance(c, orgUser, req.UserID, req.CurrencyID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 返回响应
	resp := &GetUserCompensationBalanceResp{
		UserID:              userBalance.UserID,
		Username:            userBalance.Username,
		WalletID:            userBalance.WalletID,
		CurrencyID:          userBalance.CurrencyID,
		CurrencyName:        userBalance.CurrencyName,
		CompensationBalance: userBalance.CompensationBalance.String(),
	}
	apiresp.GinSuccess(c, resp)
}

// 调整用户补偿金余额请求
type AdjustUserCompensationBalanceReq struct {
	UserID     string `json:"user_id"`     // 用户ID
	CurrencyID string `json:"currency_id"` // 币种ID
	Amount     string `json:"amount"`      // 调整金额（正数增加，负数减少）
	Reason     string `json:"reason"`      // 调整原因
}

// 调整用户补偿金余额
func (o *Api) AdjustUserCompensationBalance(c *gin.Context) {
	// 解析请求
	var req AdjustUserCompensationBalanceReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, errs.ErrArgs)
		return
	}

	// 验证管理员权限
	userID, err := mctx.CheckAdmin(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 获取管理员信息
	adminInfo, err := o.adminClient.GetAdminInfo(c, nil)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 创建组织用户对象
	var role orgModel.OrganizationUserRole

	// 根据管理员级别设置相应的角色
	switch adminInfo.Level {
	case 1:
		role = orgModel.OrganizationUserSuperAdminRole
	case 2:
		role = orgModel.OrganizationUserBackendAdminRole
	default:
		role = orgModel.OrganizationUserNormalRole
	}

	orgUser := &orgModel.OrganizationUser{
		UserId: userID,
		Role:   role,
	}

	// 解析调整金额
	amount, err := decimal.NewFromString(req.Amount)
	if err != nil {
		apiresp.GinError(c, errs.ErrArgs.WrapMsg("invalid amount"))
		return
	}

	// 调整用户补偿金余额
	compensationAdminSvc := walletSvc.NewCompensationAdminService()
	err = compensationAdminSvc.AdjustUserCompensationBalance(c, orgUser, &walletSvc.AdjustUserCompensationBalanceReq{
		UserID:     req.UserID,
		CurrencyID: req.CurrencyID,
		Amount:     amount,
		Reason:     req.Reason,
	})
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, nil)
}
