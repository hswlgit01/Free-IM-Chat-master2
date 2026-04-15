package svc

import (
	"context"

	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/apps/wallet/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// CompensationAdminSvc 管理员补偿金管理服务
type CompensationAdminSvc struct{}

func NewCompensationAdminService() *CompensationAdminSvc {
	return &CompensationAdminSvc{}
}

// CompensationSystemSettings 补偿金系统设置
type CompensationSystemSettings struct {
	Enabled          bool            `json:"enabled"`           // 是否启用补偿金系统
	InitialAmount    decimal.Decimal `json:"initial_amount"`    // 初始补偿金金额
	NoticeText       string          `json:"notice_text"`       // 钱包开通时显示的说明文本
	CanBeConfigured  bool            `json:"can_be_configured"` // 是否可以配置
	CurrentlyEnabled bool            `json:"currently_enabled"` // 当前是否启用
}

// GetCompensationSystemSettings 获取补偿金系统设置
func (c *CompensationAdminSvc) GetCompensationSystemSettings(ctx context.Context, admin *orgModel.OrganizationUser) (*CompensationSystemSettings, error) {
	// 检查权限
	if admin.Role != orgModel.OrganizationUserSuperAdminRole && admin.Role != orgModel.OrganizationUserBackendAdminRole {
		return nil, freeErrors.ApiErr("no permission")
	}

	db := plugin.MongoCli().GetDB()
	settingsDao := model.NewWalletSettingsDao(db)
	settings, err := settingsDao.GetDefaultSettings(ctx)
	if err != nil {
		return nil, errs.Unwrap(err)
	}

	// 解析初始补偿金金额
	initialAmount, err := decimal.NewFromString(settings.InitialCompensation.String())
	if err != nil {
		return nil, errs.Unwrap(err)
	}

	// 添加调试日志
	log.ZInfo(ctx, "获取到钱包设置", "settings", settings, "notice_text", settings.NoticeText)

	return &CompensationSystemSettings{
		Enabled:          settings.CompensationEnabled,
		InitialAmount:    initialAmount,
		NoticeText:       settings.NoticeText,
		CanBeConfigured:  true,
		CurrentlyEnabled: settings.CompensationEnabled,
	}, nil
}

// UpdateCompensationSystemSettingsReq 更新补偿金系统设置请求
type UpdateCompensationSystemSettingsReq struct {
	Enabled       bool            `json:"enabled"`        // 是否启用补偿金系统
	InitialAmount decimal.Decimal `json:"initial_amount"` // 初始补偿金金额
	NoticeText    string          `json:"notice_text"`    // 钱包开通时显示的说明文本
}

// UpdateCompensationSystemSettings 更新补偿金系统设置
func (c *CompensationAdminSvc) UpdateCompensationSystemSettings(ctx context.Context, admin *orgModel.OrganizationUser, req *UpdateCompensationSystemSettingsReq) error {
	// 检查权限
	if admin.Role != orgModel.OrganizationUserSuperAdminRole && admin.Role != orgModel.OrganizationUserBackendAdminRole {
		return freeErrors.ApiErr("no permission")
	}

	// 验证初始补偿金金额
	if req.InitialAmount.IsNegative() {
		return freeErrors.ApiErr("initial compensation amount cannot be negative")
	}

	db := plugin.MongoCli().GetDB()
	settingsDao := model.NewWalletSettingsDao(db)

	// 事务更新设置
	err := plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		settings, err := settingsDao.GetDefaultSettings(sessionCtx)
		if err != nil {
			return err
		}

		// 转换初始补偿金金额为Decimal128
		initialAmount128, err := primitive.ParseDecimal128(req.InitialAmount.String())
		if err != nil {
			return err
		}

		// 添加调试日志
		log.ZInfo(sessionCtx, "更新钱包设置前", "settings", settings, "current_notice_text", settings.NoticeText, "new_notice_text", req.NoticeText)

		// 更新设置
		settings.CompensationEnabled = req.Enabled
		settings.InitialCompensation = initialAmount128
		settings.NoticeText = req.NoticeText

		// 添加调试日志
		log.ZInfo(sessionCtx, "更新钱包设置后", "settings", settings, "notice_text", settings.NoticeText)

		return settingsDao.UpdateSettings(sessionCtx, settings)
	})

	return errs.Unwrap(err)
}

// UserCompensationBalance 用户补偿金余额
type UserCompensationBalance struct {
	UserID              string          `json:"user_id"`              // 用户ID
	Username            string          `json:"username"`             // 用户名
	WalletID            string          `json:"wallet_id"`            // 钱包ID
	CurrencyID          string          `json:"currency_id"`          // 币种ID
	CurrencyName        string          `json:"currency_name"`        // 币种名称
	CompensationBalance decimal.Decimal `json:"compensation_balance"` // 补偿金余额
	CanBeAdjusted       bool            `json:"can_be_adjusted"`      // 是否可以调整
}

// GetUserCompensationBalance 获取用户补偿金余额
func (c *CompensationAdminSvc) GetUserCompensationBalance(ctx context.Context, admin *orgModel.OrganizationUser, userID string, currencyID string) (*UserCompensationBalance, error) {
	// 检查权限
	if admin.Role != orgModel.OrganizationUserSuperAdminRole && admin.Role != orgModel.OrganizationUserBackendAdminRole {
		return nil, freeErrors.ApiErr("no permission")
	}

	db := plugin.MongoCli().GetDB()
	walletInfoDao := model.NewWalletInfoDao(db)
	walletBalanceDao := model.NewWalletBalanceDao(db)
	walletCurrencyDao := model.NewWalletCurrencyDao(db)
	compensationSvc := NewCompensationService()

	// 检查补偿金功能是否启用
	enabled, err := compensationSvc.IsCompensationEnabled(ctx)
	if err != nil {
		return nil, errs.Unwrap(err)
	}
	if !enabled {
		return nil, freeErrors.ApiErr("compensation system is disabled")
	}

	// 获取用户钱包信息
	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, userID, model.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		if mongo.ErrNoDocuments == err {
			return nil, freeErrors.WalletNotOpenErr
		}
		return nil, errs.Unwrap(err)
	}

	// 获取币种信息
	currencyObjID, err := primitive.ObjectIDFromHex(currencyID)
	if err != nil {
		return nil, freeErrors.ApiErr("invalid currency id")
	}

	currency, err := walletCurrencyDao.GetById(ctx, currencyObjID)
	if err != nil {
		if mongo.ErrNoDocuments == err {
			return nil, freeErrors.ApiErr("currency not found")
		}
		return nil, errs.Unwrap(err)
	}

	// 获取钱包余额
	walletBalance, err := walletBalanceDao.GetByWalletIdAndCurrencyId(ctx, walletInfo.ID, currency.ID)
	if err != nil {
		if mongo.ErrNoDocuments == err {
			// 如果没有余额记录，创建一个零余额
			walletBalance = model.WalletBalance{}.ZeroBalance(walletInfo.ID, currency.ID)
		} else {
			return nil, errs.Unwrap(err)
		}
	}

	// 解析补偿金余额
	compensationBalance, err := decimal.NewFromString(walletBalance.CompensationBalance.String())
	if err != nil {
		return nil, errs.Unwrap(err)
	}

	// 获取用户信息 (简化处理，实际可能需要查询用户服务)
	username := userID // 简化处理，实际应该查询用户名

	return &UserCompensationBalance{
		UserID:              userID,
		Username:            username,
		WalletID:            walletInfo.ID.Hex(),
		CurrencyID:          currency.ID.Hex(),
		CurrencyName:        currency.Name,
		CompensationBalance: compensationBalance,
		CanBeAdjusted:       true,
	}, nil
}

// AdjustUserCompensationBalanceReq 调整用户补偿金余额请求
type AdjustUserCompensationBalanceReq struct {
	UserID     string          `json:"user_id"`     // 用户ID
	CurrencyID string          `json:"currency_id"` // 币种ID
	Amount     decimal.Decimal `json:"amount"`      // 调整金额（正数增加，负数减少）
	Reason     string          `json:"reason"`      // 调整原因
}

// AdjustUserCompensationBalance 调整用户补偿金余额
func (c *CompensationAdminSvc) AdjustUserCompensationBalance(ctx context.Context, admin *orgModel.OrganizationUser, req *AdjustUserCompensationBalanceReq) error {
	// 检查权限
	if admin.Role != orgModel.OrganizationUserSuperAdminRole && admin.Role != orgModel.OrganizationUserBackendAdminRole {
		return freeErrors.ApiErr("no permission")
	}

	db := plugin.MongoCli().GetDB()
	walletInfoDao := model.NewWalletInfoDao(db)
	compensationSvc := NewCompensationService()

	// 检查补偿金功能是否启用
	enabled, err := compensationSvc.IsCompensationEnabled(ctx)
	if err != nil {
		return errs.Unwrap(err)
	}
	if !enabled {
		return freeErrors.ApiErr("compensation system is disabled")
	}

	// 获取用户钱包信息
	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, req.UserID, model.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		if mongo.ErrNoDocuments == err {
			return freeErrors.WalletNotOpenErr
		}
		return errs.Unwrap(err)
	}

	// 检查币种ID格式 (为了保持API兼容性)
	_, err = primitive.ObjectIDFromHex(req.CurrencyID)
	if err != nil {
		return freeErrors.ApiErr("invalid currency id")
	}

	// 注意：补偿金与币种无关，只是保留参数进行格式验证

	// 执行调整 - 补偿金与币种无关，不需要传递币种ID
	err = compensationSvc.AdjustCompensationBalance(ctx, walletInfo.ID, req.Amount)
	if err != nil {
		return errs.Unwrap(err)
	}

	return nil
}
