package wallet

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/wallet/dto"
	"github.com/openimsdk/chat/freechat/apps/wallet/model"
	"github.com/openimsdk/chat/freechat/apps/wallet/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/ginUtils"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/errs"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type WalletCtl struct{}

func NewWalletCtl() *WalletCtl {
	return &WalletCtl{}
}

// PostTriggerCompensationInit 为当前用户钱包触发补偿金初始化
func (w *WalletCtl) PostTriggerCompensationInit(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 获取钱包信息
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	walletInfoDao := model.NewWalletInfoDao(db)

	// 检查钱包是否存在
	exist, err := walletInfoDao.ExistByOwnerIdAndOwnerType(context.TODO(), opUserID, model.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	if !exist {
		apiresp.GinError(c, freeErrors.WalletNotOpenErr)
		return
	}

	// 获取钱包信息
	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(context.TODO(), opUserID, model.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}

	// 获取补偿金服务
	compensationSvc := svc.NewCompensationService()

	// 触发补偿金初始化 - 直接在钱包级别操作，不再需要币种参数
	err = compensationSvc.InitializeCompensationBalance(c, walletInfo.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, nil)
}

// GetWalletExist 查询当前用户是否开通钱包
func (w *WalletCtl) GetWalletExist(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	mongoCli := plugin.MongoCli()
	walletInfoDao := model.NewWalletInfoDao(mongoCli.GetDB())
	exist, err := walletInfoDao.ExistByOwnerIdAndOwnerType(context.TODO(), opUserID, model.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, exist)
}

// GetWalletInfo 展示当前用户的钱包信息(旧接口)
func (w *WalletCtl) GetWalletInfo(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	walletSvc := &svc.WalletSvc{}
	resp, err := walletSvc.DetailWalletSvc(opUserID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

// PostWallet 创建钱包
func (w *WalletCtl) PostWallet(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	reqData := &dto.PostBalanceReq{}
	if err := c.ShouldBind(reqData); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
	}

	walletSvc := &svc.WalletSvc{}
	resp, err := walletSvc.CreateOrdinaryWalletSvc(opUserID, reqData.PayPwd)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

// PostRechargeTest 测试充值接口
func (w *WalletCtl) PostRechargeTest(c *gin.Context) {
	var data struct {
		Amount string `json:"amount" form:"amount" xml:"amount"`
	}

	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
	}

	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	walletSvc := svc.NewWalletService()
	err = walletSvc.TestRechargeWalletBalanceSvc(opUserID, data.Amount)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, "success")
}

// PostUpdatePayPwd 修改支付密码
func (w *WalletCtl) PostUpdatePayPwd(c *gin.Context) {
	var data struct {
		LoginPwd  string `json:"login_pwd" form:"login_pwd" xml:"login_pwd"`
		NewPayPwd string `json:"new_pay_pwd" form:"new_pay_pwd" xml:"new_pay_pwd"`
	}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	walletSvc := &svc.WalletSvc{}
	if err := walletSvc.UpdateWalletPayPwd(opUserID, data.LoginPwd, data.NewPayPwd); err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, "success")
}

// PostUpdatePayPwdByVerifyCode 通过验证码修改支付密码
func (w *WalletCtl) PostUpdatePayPwdByVerifyCode(c *gin.Context) {
	var data struct {
		NewPayPwd  string `json:"new_pay_pwd" form:"new_pay_pwd" xml:"new_pay_pwd"`
		VerifyCode string `json:"verify_code" form:"verify_code" xml:"verify_code"`
	}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	walletSvc := svc.NewWalletService()
	// 使用验证码修改支付密码
	if err := walletSvc.UpdatePayPwdByVerifyCode(c, opUserID, data.NewPayPwd, data.VerifyCode); err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, "success")
}

type WalletCurrencyCtl struct{}

func NewWalletCurrencyCtl() *WalletCurrencyCtl {
	return &WalletCurrencyCtl{}

}

// GetWalletCurrencies 查询所有代币
func (w *WalletCurrencyCtl) GetWalletCurrencies(c *gin.Context) {
	currencyIds, err := ginUtils.QueryToObjectIds(c, "creator_ids")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	page, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, freeErrors.PageParameterInvalidErr)
		return
	}

	walletCurrencySvc := svc.NewWalletCurrencySvc()
	resp, err := walletCurrencySvc.ListCurrencies(currencyIds, page)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

type DepAdminWalletCtl struct{}

func NewDepAdminWalletCtl() *DepAdminWalletCtl {
	return &DepAdminWalletCtl{}
}

// GetOrgBalance 展示组织用户的钱包信息
func (w *DepAdminWalletCtl) GetOrgBalance(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	walletSvc := &svc.OrgWalletSvc{}
	resp, err := walletSvc.DetailOrgWalletSvc(org.Organization)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

// AdjustUserBalanceReq 后台手动调节用户余额的请求体。
type AdjustUserBalanceReq struct {
	// TargetUserID 被调整用户的 organization_user.user_id
	TargetUserID string `json:"targetUserId" binding:"required"`
	// CurrencyID 币种 ID
	CurrencyID string `json:"currencyId" binding:"required"`
	// Amount 调整金额，字符串传递避免浮点精度丢失。正数增加，负数扣减。
	Amount string `json:"amount" binding:"required"`
	// Reason 调整原因，必填，会写入用户账单备注
	Reason string `json:"reason" binding:"required"`
	// RequestID 幂等键，同一个只生效一次。前端每次打开弹窗生成一个 UUID 即可，
	// 提交失败重试时沿用同一个，避免网络重试导致重复加钱。
	RequestID string `json:"requestId" binding:"required"`
}

// PostAdjustUserBalance 后台手动调节用户可用余额。
//
// 权限在路由层用 CheckOrganization 限定为 SuperAdmin / BackendAdmin。
func (w *DepAdminWalletCtl) PostAdjustUserBalance(c *gin.Context) {
	var req AdjustUserBalanceReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	currencyID, err := primitive.ObjectIDFromHex(req.CurrencyID)
	if err != nil {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, "币种 ID 格式错误"))
		return
	}
	// 金额用字符串解析成 decimal，全程不经过 float64，避免精度丢失
	amount, err := decimal.NewFromString(req.Amount)
	if err != nil {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, "金额格式错误"))
		return
	}

	balance, err := svc.NewBalanceAdjustSvc().AdjustUserBalance(c, &svc.AdjustBalanceRequest{
		OrgID:          org.OrgUser.OrganizationId,
		OperatorUserID: org.OrgUser.UserId,
		OperatorImID:   org.OrgUser.ImServerUserId,
		TargetUserID:   req.TargetUserID,
		CurrencyID:     currencyID,
		Amount:         amount,
		Reason:         req.Reason,
		RequestID:      req.RequestID,
	})
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, gin.H{"balance": balance})
}

type WalletBalanceCtl struct{}

func NewWalletBalanceCtl() *WalletBalanceCtl {
	return &WalletBalanceCtl{}

}

// GetBalance 展示用户某个组织钱包的余额信息
func (w *WalletBalanceCtl) GetBalance(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgId, err := ginUtils.QueryToObjectId(c, "org_id")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	walletSvc := svc.NewWalletBalanceSvc()
	resp, err := walletSvc.DetailWalletBalance(orgId, opUserID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

// GetAllBalance 展示用户所有组织钱包的余额信息
func (w *WalletBalanceCtl) GetAllBalance(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	walletSvc := svc.NewWalletBalanceSvc()
	resp, err := walletSvc.ListWalletBalance(opUserID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}
