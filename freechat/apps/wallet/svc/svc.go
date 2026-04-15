package svc

import (
	"context"
	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/apps/wallet/dto"
	"github.com/openimsdk/chat/freechat/apps/wallet/model"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/constant"
	"github.com/openimsdk/chat/pkg/protocol/chat"

	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type WalletSvc struct{}

func NewWalletService() *WalletSvc {
	return &WalletSvc{}
}

func (w *WalletSvc) DetailWalletSvc(userId string) (*dto.DetailWalletInfoResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	walletInfoDao := model.NewWalletInfoDao(db)
	exist, err := walletInfoDao.ExistByOwnerIdAndOwnerType(context.TODO(), userId, model.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, freeErrors.WalletNotOpenErr
	}

	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(context.TODO(), userId, model.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	detailWalletInfo, err := dto.NewDetailWalletInfoResp(context.TODO(), db, walletInfo)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}
	return detailWalletInfo, nil
}

func (w *WalletSvc) CreateOrdinaryWalletSvc(userId string, payPwd string) (*dto.CreateWalletResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	walletInfoDao := model.NewWalletInfoDao(db)
	compensationSvc := NewCompensationService()
	walletSettingsDao := model.NewWalletSettingsDao(db)

	// hash加密
	hashPayPwd, err := utils.HashPassword(payPwd)
	if err != nil {
		return nil, err
	}

	var newWalletInfo *model.WalletInfo

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		exist, err := walletInfoDao.ExistByOwnerIdAndOwnerType(sessionCtx, userId, model.WalletInfoOwnerTypeOrdinary)
		if err != nil && !dbutil.IsDBNotFound(err) {
			return err
		}
		if exist {
			return freeErrors.WalletOpenedErr
		}

		newWalletInfo = &model.WalletInfo{
			OwnerId:   userId,
			OwnerType: model.WalletInfoOwnerTypeOrdinary,
			PayPwd:    hashPayPwd,
		}

		if err = walletInfoDao.Create(sessionCtx, newWalletInfo); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, errs.Unwrap(err)
	}

	// 获取刚创建的钱包
	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(context.TODO(), userId, model.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		return nil, err
	}

	// 同步初始化补偿金（直接在钱包级别操作，不再需要币种参数）
	log.ZInfo(context.TODO(), "Starting compensation balance initialization", "walletId", walletInfo.ID)
	err = compensationSvc.InitializeCompensationBalance(context.TODO(), walletInfo.ID)
	if err != nil {
		// 记录错误但不影响钱包创建成功
		log.ZError(context.TODO(), "Initialize compensation balance failed", err, "walletId", walletInfo.ID)
		// 注意：这里选择记录错误但仍然返回成功
		// 这样即使补偿金初始化失败，用户仍能成功创建钱包
	}
	log.ZInfo(context.TODO(), "Compensation balance initialization completed", "walletId", walletInfo.ID)

	// 获取钱包开通提示文本
	settings, err := walletSettingsDao.GetDefaultSettings(context.TODO())
	if err != nil {
		log.ZError(context.TODO(), "Get wallet settings failed", err)
		// 继续处理，不因为获取设置失败而影响整个流程
	}

	noticeText := ""
	if settings != nil {
		noticeText = settings.NoticeText
	}

	return &dto.CreateWalletResp{
		WalletInfo: walletInfo,
		NoticeText: noticeText,
	}, nil
}

func (w *WalletSvc) TestRechargeWalletBalanceSvc(userId string, amount string) error {
	if userId == "" || amount == "" {
		return freeErrors.ParameterInvalidErr
	}
	log.ZInfo(context.TODO(), "TestRechargeWalletBalanceSvc")

	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	//walletDao := model.NewWalletBalanceDao(db)
	walletInfoDao := model.NewWalletInfoDao(db)
	ctx := context.Background()

	err := mongoCli.GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		amountD, err := decimal.NewFromString(amount)
		if err != nil {
			return err
		}
		if amountD.Cmp(decimal.NewFromInt(0)) < 0 {
			return errs.NewCodeError(freeErrors.ErrInvalidParams, "余额充值不允许为负数")
		}

		_, err = walletInfoDao.GetByOwnerIdAndOwnerType(sessionCtx, userId, model.WalletInfoOwnerTypeOrdinary)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.WalletNotOpenErr
			}
			return err
		}

		//usdCurrency := model.NewWalletCurrencyDao(db).GetUsdCurrency()

		//if err = walletDao.UpdateAvailableBalanceAndAddTsRecord(sessionCtx, wallet.ID, usdCurrency.ID, amountD, walletTransactionRecordModel.TsRecordTypeDeposit, "", ""); err != nil {
		//	return err
		//}

		return nil
	})
	if err != nil {
		log.ZWarn(ctx, "TestRechargeWalletBalanceSvc transaction error: ", errs.Unwrap(err))
	}

	return err
}

func (w *WalletSvc) UpdateWalletPayPwd(userId string, loginPwd, payPwd string) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	walletInfoDao := model.NewWalletInfoDao(db)
	accountDao := chatModel.NewAccountDao(db)

	// hash加密
	hashPayPwd, err := utils.HashPassword(payPwd)
	if err != nil {
		return err
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		wallet, err := walletInfoDao.GetByOwnerIdAndOwnerType(sessionCtx, userId, model.WalletInfoOwnerTypeOrdinary)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.WalletNotOpenErr
			}
			return err
		}
		account, err := accountDao.GetByUserId(sessionCtx, userId)
		if err != nil {
			return err
		}

		if loginPwd != account.Password {
			return freeErrors.UserPwdErrErr
		}
		wallet.PayPwd = hashPayPwd
		if err = walletInfoDao.UpdatePayPwd(sessionCtx, userId, hashPayPwd, model.WalletInfoOwnerTypeOrdinary); err != nil {
			return err
		}
		return nil
	})

	return errs.Unwrap(err)
}

// UpdatePayPwdByVerifyCode 使用验证码更新钱包支付密码（新增方法，不修改原有方法）
func (w *WalletSvc) UpdatePayPwdByVerifyCode(ctx context.Context, userId, newPayPwd, verifyCode string) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	walletInfoDao := model.NewWalletInfoDao(db)

	// 使用AttributeDao获取用户信息
	attributeDao := chatModel.NewAttributeDao(db)
	userInfo, err := attributeDao.Take(ctx, userId)
	if err != nil {
		log.ZError(ctx, "获取用户信息失败", err, "userId", userId)
		if dbutil.IsDBNotFound(err) {
			return errs.NewCodeError(freeErrors.UserNotFoundCode, freeErrors.ErrorMessages[freeErrors.UserNotFoundCode])
		}
		return errs.NewCodeError(freeErrors.ErrSystem, "获取用户信息失败")
	}

	// 获取用户的手机号和区号
	areaCode := userInfo.AreaCode
	phoneNumber := userInfo.PhoneNumber

	// 检查是否有手机号
	if areaCode == "" || phoneNumber == "" {
		log.ZError(ctx, "用户未绑定手机号", nil, "userId", userId)
		return errs.NewCodeError(freeErrors.ErrSystem, "用户未绑定手机号，无法使用验证码修改支付密码")
	}

	// 验证验证码
	// 使用更安全的错误处理方式，避免将错误传递过来
	chatClient := plugin.ChatClient()
	if chatClient == nil {
		log.ZError(ctx, "获取验证码服务客户端失败", nil, "userId", userId)
		return errs.NewCodeError(freeErrors.ErrSystem, "验证码服务不可用")
	}

	// 构建验证码验证请求
	verifyReq := &chat.VerifyCodeReq{
		AreaCode:          areaCode,
		PhoneNumber:       phoneNumber,
		VerifyCode:        verifyCode,
		UsedFor:           constant.VerificationCodeForPaymentPwd,
		DeleteAfterVerify: true,
	}

	// 直接在这里验证，而不是调用w.VerifyPayPwdCode
	_, verifyErr := chatClient.VerifyCode(ctx, verifyReq)
	if verifyErr != nil {
		log.ZError(ctx, "验证码验证失败", verifyErr, "userId", userId)
		return errs.NewCodeError(freeErrors.ErrUnauthorized, freeErrors.ErrorMessages[freeErrors.ErrUnauthorized])
	}

	// hash加密
	hashPayPwd, err := utils.HashPassword(newPayPwd)
	if err != nil {
		return err
	}

	err = mongoCli.GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(sessionCtx, userId, model.WalletInfoOwnerTypeOrdinary)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.WalletNotOpenErr
			}
			return err
		}

		walletInfo.PayPwd = hashPayPwd
		if err = walletInfoDao.UpdatePayPwd(sessionCtx, userId, hashPayPwd, model.WalletInfoOwnerTypeOrdinary); err != nil {
			return err
		}
		return nil
	})

	return errs.Unwrap(err)
}

type WalletBalanceSvc struct{}

func NewWalletBalanceSvc() *WalletBalanceSvc {
	return &WalletBalanceSvc{}
}
func (w *WalletBalanceSvc) DetailWalletBalance(orgId primitive.ObjectID, userId string) (*dto.WalletBalanceByOrgUserResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	orgUserDao := orgModel.NewOrganizationUserDao(db)

	orgUser, err := orgUserDao.GetByUserIdAndOrgId(context.TODO(), userId, orgId)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.ApiErr("user has not joined the organization, user id: " + userId)
		}
		return nil, errs.Unwrap(err)
	}

	resp, err := dto.NewWalletBalanceByOrgUserResp(db, orgUser)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (w *WalletBalanceSvc) ListWalletBalance(userId string) ([]*dto.WalletBalanceByOrgUserResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	orgUserDao := orgModel.NewOrganizationUserDao(db)

	orgUsers, err := orgUserDao.Select(context.TODO(), userId, primitive.NilObjectID, nil)
	if err != nil {
		return nil, err
	}

	resp := make([]*dto.WalletBalanceByOrgUserResp, 0)

	for _, orgUser := range orgUsers {
		walletBalanceByOrgUserResp, err := dto.NewWalletBalanceByOrgUserResp(db, orgUser)
		if err != nil {
			return nil, err
		}
		resp = append(resp, walletBalanceByOrgUserResp)
	}

	return resp, nil
}

type WalletCurrencySvc struct{}

func NewWalletCurrencySvc() *WalletCurrencySvc {
	return &WalletCurrencySvc{}
}

func (w *WalletCurrencySvc) ListCurrencies(creatorIds []primitive.ObjectID, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.WalletCurrencyResp], error) {
	db := plugin.MongoCli().GetDB()

	walletCurrencyDao := model.NewWalletCurrencyDao(db)
	total, currencies, err := walletCurrencyDao.Select(context.TODO(), creatorIds, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.WalletCurrencyResp]{
		Total: total,
		List:  []*dto.WalletCurrencyResp{},
	}

	for _, currency := range currencies {
		data := dto.NewWalletCurrencyResp(currency)
		resp.List = append(resp.List, data)
	}

	return resp, nil
}

type OrgWalletSvc struct{}

func NewOrgWalletService() *OrgWalletSvc {
	return &OrgWalletSvc{}
}

func (w *OrgWalletSvc) DetailOrgWalletSvc(org *orgModel.Organization) (*dto.DetailWalletInfoResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	walletInfoDao := model.NewWalletInfoDao(db)
	exist, err := walletInfoDao.ExistByOwnerIdAndOwnerType(context.TODO(), org.ID.Hex(), model.WalletInfoOwnerTypeOrganization)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, freeErrors.WalletNotOpenErr
	}

	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(context.TODO(), org.ID.Hex(), model.WalletInfoOwnerTypeOrganization)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	detailWalletInfo, err := dto.NewDetailWalletInfoResp(context.TODO(), db, walletInfo)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}
	return detailWalletInfo, nil
}
