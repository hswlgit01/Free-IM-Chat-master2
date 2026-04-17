package svc

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	orgCache "github.com/openimsdk/chat/freechat/apps/organization/cache"
	cacheRedis "github.com/openimsdk/chat/freechat/cache/redis"

	adminModel "github.com/openimsdk/chat/freechat/apps/admin/model"

	"github.com/openimsdk/chat/tools/db/mongoutil"

	defaultFriendSvc "github.com/openimsdk/chat/freechat/apps/defaultFriend/svc"
	defaultGroupSvc "github.com/openimsdk/chat/freechat/apps/defaultGroup/svc"
	notificationSvc "github.com/openimsdk/chat/freechat/apps/notification/svc"
	systemStatistics "github.com/openimsdk/chat/freechat/apps/systemStatistics"

	"github.com/openimsdk/chat/freechat/apps/organization/dto"
	"github.com/openimsdk/chat/freechat/apps/organization/model"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	walletTransactionRecordModel "github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/model"
	depConstant "github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/plugin"
	chatCache "github.com/openimsdk/chat/freechat/third/chat/cache"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"

	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/constant"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/chat/pkg/common/imapi"
	"github.com/openimsdk/chat/pkg/common/mctx"
	pkgConstant "github.com/openimsdk/chat/pkg/constant"
	adminpb "github.com/openimsdk/chat/pkg/protocol/admin"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// 这里使用了在 orgRootNodeSvc.go 中定义的常量:
// OrgRootNodePrefix 和 OrgRootNodeNickname

// validateCurrencyDecimals 验证货币精度是否在安全范围内
//
// 这个验证是为了确保在Redis Lua脚本中进行红包拆分计算时不会出现精度丢失问题。
// Lua脚本使用IEEE 754双精度浮点数，当精度过高时scale_factor会超出安全范围。
//
// 参数:
//   - decimals: 要验证的精度位数
//
// 返回:
//   - error: 如果精度不在安全范围内则返回错误，否则返回nil
func validateCurrencyDecimals(decimals int) error {
	if decimals < depConstant.MinCurrencyDecimals {
		return freeErrors.ApiErr(fmt.Sprintf("Currency decimals cannot be less than %d", depConstant.MinCurrencyDecimals))
	}

	if decimals > depConstant.MaxCurrencyDecimals {
		return freeErrors.ApiErr(fmt.Sprintf("Currency decimals cannot exceed %d (current: %d)",
			depConstant.MaxCurrencyDecimals, decimals))
	}

	return nil
}

// fillOrgUserWalletFields 将钱包多币种余额与补偿金写入 OrgUserResp（列表与 wallet_snapshot 共用）
func fillOrgUserWalletFields(ctx context.Context, db *mongo.Database, userResp *dto.OrgUserResp) {
	if userResp == nil || userResp.UserId == "" {
		return
	}
	walletInfoDao := walletModel.NewWalletInfoDao(db)
	walletBalanceDao := walletModel.NewWalletBalanceDao(db)
	currencyDao := walletModel.NewWalletCurrencyDao(db)

	wallet, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, userResp.UserId, walletModel.WalletInfoOwnerTypeOrdinary)
	if err != nil || wallet == nil {
		return
	}

	compensationBalance, err := decimal.NewFromString(wallet.CompensationBalance.String())
	if err == nil && !compensationBalance.IsZero() {
		userResp.CompensationBalance = compensationBalance.String()
	}

	balances, err := walletBalanceDao.FindByWalletId(ctx, wallet.ID)
	if err != nil || len(balances) == 0 {
		return
	}

	walletBalances := make([]*dto.WalletBalanceInfo, 0, len(balances))
	for _, balance := range balances {
		currency, err := currencyDao.GetById(ctx, balance.CurrencyId)
		if err != nil {
			continue
		}
		availableBalance, _ := decimal.NewFromString(balance.AvailableBalance.String())
		if !availableBalance.IsZero() {
			walletBalances = append(walletBalances, &dto.WalletBalanceInfo{
				CurrencyId:   balance.CurrencyId.Hex(),
				CurrencyName: currency.Name,
				Balance:      availableBalance.String(),
			})
		}
	}
	if len(walletBalances) > 0 {
		userResp.WalletBalances = walletBalances
	}
}

// validateMaxRedPacketAmountSafety 验证最大红包金额
func validateMaxRedPacketAmountSafety(maxAmount string) error {
	if maxAmount == "" {
		return nil // 允许空值
	}

	// 仅验证格式是否正确
	_, err := decimal.NewFromString(maxAmount)
	if err != nil {
		return freeErrors.ApiErr(fmt.Sprintf("Invalid max red packet amount format: %s", maxAmount))
	}

	// 移除金额上限限制，允许任意金额
	return nil
}

type OrganizationSvc struct{}

func NewOrganizationService() *OrganizationSvc {
	return &OrganizationSvc{}
}

func (w *OrganizationSvc) SuperCmsListOrg(ctx context.Context, keyword string, operationID string,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.OrganizationResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	orgDao := model.NewOrganizationDao(db)

	total, result, err := orgDao.SelectJoinAll(ctx, keyword, startTime, endTime, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.OrganizationResp]{
		List:  []*dto.OrganizationResp{},
		Total: total,
	}

	for _, record := range result {
		item, err := dto.NewOrganizationResp(record.Organization, operationID)
		if err != nil {
			return nil, errs.Unwrap(err)
		}
		resp.List = append(resp.List, item)
	}
	return resp, nil
}

type CreateTestOrganizationReq struct {
	Secret      string                 `json:"secret"`
	Name        string                 `bson:"name" json:"name"`
	Type        model.OrganizationType `bson:"type" json:"type"`
	Email       string                 `bson:"email" json:"email"`
	Phone       string                 `bson:"phone" json:"phone"`
	Description string                 `bson:"description" json:"description"`
	Contacts    string                 `bson:"contacts" json:"contacts"`
	Logo        string                 `bson:"logo" json:"logo"`

	User struct {
		//UserID      string `json:"userID,omitempty" binding:"required"`
		Nickname string `json:"nickname"`
		FaceURL  string `json:"faceURL"`
		Birth    int64  `json:"birth"`
		Gender   int32  `json:"gender"`
		//AreaCode    string `json:"areaCode"`
		//PhoneNumber string `json:"phoneNumber"`
		Email    string `json:"email"`
		Account  string `json:"account" binding:"required"`
		Password string `json:"password"`
	} ` json:"user" binding:"required"`
}

func (w *OrganizationSvc) CreateTestOrganization(operatorId string, params CreateTestOrganizationReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	organizationDao := model.NewOrganizationDao(db)
	orgRolePermissionDao := model.NewOrganizationRolePermissionDao(db)

	orgUserDao := model.NewOrganizationUserDao(db)
	attributeDao := chatModel.NewAttributeDao(db)
	accountDao := chatModel.NewAccountDao(db)
	registerDao := chatModel.NewRegisterDao(db)
	credentialDao := chatModel.NewCredentialDao(db)

	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(context.Background(), constantpb.OperationID, operatorId)
	imApiCallerToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return err
	}
	imApiCallerCtx := mctx.WithApiToken(ctxWithOpID, imApiCallerToken)

	if params.Secret != plugin.ChatCfg().Share.OpenIM.Secret {
		return freeErrors.ForbiddenErr("invalid secret")
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		if exist, err := organizationDao.ExistByNameAndStatus(sessionCtx, params.Name, model.OrganizationStatusPass); err != nil {
			return err
		} else if exist {
			return errors.New("organization name already exists")
		}

		newUserID, err := utils.NewId()
		if err != nil {
			return err
		}

		newImServerUserID, err := utils.NewId()
		if err != nil {
			return err
		}

		attributes, err := attributeDao.FindAccountCaseInsensitive(sessionCtx, []string{params.User.Account})
		if err != nil && !dbutil.IsDBNotFound(err) {
			return err
		}
		if len(attributes) > 0 {
			return fmt.Errorf("account %s already exists", params.User.Account)
		}

		aesKeyBase64, err := utils.GenerateAESKey()
		if err != nil {
			return err
		}

		organization := &model.Organization{
			Name:         params.Name,
			Type:         params.Type,
			Email:        params.Email,
			Phone:        params.Phone,
			Description:  params.Description,
			Contacts:     params.Contacts,
			CreatorId:    newUserID,
			Status:       model.OrganizationStatusPass,
			Logo:         params.Logo,
			AesKeyBase64: aesKeyBase64,
		}
		if err = organizationDao.Create(sessionCtx, organization); err != nil {
			return err
		}

		organization, err = organizationDao.GetByNameAndStatus(sessionCtx, organization.Name, model.OrganizationStatusPass)
		if err != nil {
			return err
		}

		err = orgRolePermissionDao.CreateDefaultRolePermission(sessionCtx, organization.ID)
		if err != nil {
			return err
		}

		organization, err = organizationDao.GetByNameAndStatus(sessionCtx, organization.Name, model.OrganizationStatusPass)
		if err != nil {
			return err
		}

		orgUser := &model.OrganizationUser{
			OrganizationId: organization.ID,
			UserId:         newUserID,
			Role:           model.OrganizationUserSuperAdminRole,
			Status:         model.OrganizationUserEnableStatus,
			RegisterType:   model.OrganizationUserRegisterTypeBackend,
			ImServerUserId: newImServerUserID,
		}
		if err := orgUserDao.Create(sessionCtx, orgUser, false); err != nil {
			return err
		}

		// 创建credential记录
		credentials := make([]*chatModel.Credential, 0)
		var registerType int32 = constant.AccountRegister
		if params.User.Account != "" {
			credentials = append(credentials, &chatModel.Credential{
				UserID:      newUserID,
				Account:     params.User.Account,
				Type:        constant.CredentialAccount,
				AllowChange: true,
			})
			registerType = constant.AccountRegister
		}

		register := &chatModel.Register{
			UserID:      newUserID,
			DeviceID:    "",
			IP:          "",
			Platform:    pkgConstant.H5PlatformStr,
			AccountType: "",
			Mode:        constant.UserMode,
			CreateTime:  time.Now(),
		}
		account := &chatModel.Account{
			UserID:         newUserID,
			Password:       params.User.Password,
			OperatorUserID: operatorId,
			ChangeTime:     register.CreateTime,
			CreateTime:     register.CreateTime,
		}

		attribute := &chatModel.Attribute{
			UserID:         newUserID,
			Account:        params.User.Account,
			PhoneNumber:    "",
			AreaCode:       "",
			Email:          params.User.Email,
			Nickname:       "",
			FaceURL:        "",
			Gender:         params.User.Gender,
			BirthTime:      time.UnixMilli(params.User.Birth),
			ChangeTime:     register.CreateTime,
			CreateTime:     register.CreateTime,
			AllowVibration: constant.DefaultAllowVibration,
			AllowBeep:      constant.DefaultAllowBeep,
			AllowAddFriend: constant.DefaultAllowAddFriend,
			RegisterType:   registerType,
		}

		if err := registerDao.Create(sessionCtx, register); err != nil {
			return err
		}
		if err := accountDao.Create(sessionCtx, account); err != nil {
			return err
		}
		if err := attributeDao.Create(sessionCtx, attribute); err != nil {
			return err
		}
		// 创建credential记录
		if err := credentialDao.Create(sessionCtx, credentials...); err != nil {
			return err
		}

		userInfo := &imapi.OrgUserInfo{
			UserID:         newImServerUserID,
			Nickname:       params.User.Nickname,
			FaceURL:        params.User.FaceURL,
			CreateTime:     time.Now().UnixMilli(),
			OrgId:          orgUser.OrganizationId.Hex(),
			OrgRole:        string(orgUser.Role),
			CanSendFreeMsg: 0,
		}

		err = imApiCaller.RegisterOrgUser(imApiCallerCtx, []*imapi.OrgUserInfo{userInfo})
		return err
	})
	return errs.Unwrap(err)
}

type UpdateOrganization struct {
	Email       string `bson:"email" json:"email"`
	Phone       string `bson:"phone" json:"phone"`
	Description string `bson:"description" json:"description"`
	Contacts    string `bson:"contacts" json:"contacts"`
	//InvitationCode string `bson:"invitation_code" json:"invitation_code"`
	Logo string `bson:"logo" json:"logo"`
}

func (w *OrganizationSvc) UpdateOrganization(orgId primitive.ObjectID, params UpdateOrganization) (bool, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	organizationDao := model.NewOrganizationDao(db)

	err := mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		org, err := organizationDao.GetByIdAndStatus(sessionCtx, orgId, model.OrganizationStatusPass)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.NotFoundErrWithResource("organization: " + orgId.Hex())
			}
			return freeErrors.SystemErr(err)
		}

		// 构建更新参数，只包含传入的非空字段
		organization := model.OrgUpdateInfoFieldParam{}

		// 只有传入值时才更新对应字段
		if params.Email != "" {
			organization.Email = params.Email
		} else {
			organization.Email = org.Email // 保持原值
		}

		if params.Phone != "" {
			organization.Phone = params.Phone
		} else {
			organization.Phone = org.Phone // 保持原值
		}

		if params.Description != "" {
			organization.Description = params.Description
		} else {
			organization.Description = org.Description // 保持原值
		}

		if params.Contacts != "" {
			organization.Contacts = params.Contacts
		} else {
			organization.Contacts = org.Contacts // 保持原值
		}

		organization.InvitationCode = org.InvitationCode // 保持原值

		if params.Logo != "" {
			organization.Logo = params.Logo
		} else {
			organization.Logo = org.Logo // 保持原值
		}

		if err := organizationDao.UpdateInfo(sessionCtx, org.ID, organization); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return false, errs.Unwrap(err)
	}
	return true, nil
}

// SuperAdminUpdateOrganizationReq 超管更新组织信息请求结构
type SuperAdminUpdateOrganizationReq struct {
	OrgId       primitive.ObjectID `json:"org_id" binding:"required"` // 组织ID
	Name        *string            `json:"name,omitempty"`            // 组织名称（可选）
	Email       *string            `json:"email,omitempty"`           // 邮箱（可选）
	Phone       *string            `json:"phone,omitempty"`           // 手机号（可选）
	Description *string            `json:"description,omitempty"`     // 描述（可选）
	Contacts    *string            `json:"contacts,omitempty"`        // 联系人（可选）
	Status      *string            `json:"status,omitempty"`          // 状态（可选）
	Logo        *string            `json:"logo,omitempty"`            // Logo（可选）
}

// SuperAdminUpdateOrganization 超管更新组织信息
func (w *OrganizationSvc) SuperAdminUpdateOrganization(ctx context.Context, req SuperAdminUpdateOrganizationReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	organizationDao := model.NewOrganizationDao(db)

	err := mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		// 查询原组织信息，不限制状态
		org, err := organizationDao.GetById(sessionCtx, req.OrgId)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.NotFoundErrWithResource("organization: " + req.OrgId.Hex())
			}
			return freeErrors.SystemErr(err)
		}

		// 参数验证
		if req.Name != nil && strings.TrimSpace(*req.Name) == "" {
			return freeErrors.ApiErr("organization name cannot be empty")
		}
		if req.Status != nil && *req.Status != string(model.OrganizationStatusPass) &&
			*req.Status != string(model.OrganizationStatusReject) && *req.Status != string(model.OrganizationStatusWait) {
			return freeErrors.ApiErr("invalid organization status")
		}

		// 如果要修改名称，需要检查是否重复
		if req.Name != nil && *req.Name != org.Name {
			exist, err := organizationDao.ExistByNameAndStatus(sessionCtx, *req.Name, model.OrganizationStatusPass)
			if err != nil {
				return freeErrors.SystemErr(err)
			}
			if exist {
				return freeErrors.ApiErr(fmt.Sprintf("organization name '%s' already exists", *req.Name))
			}
		}

		// 构建更新字段
		updateFields := bson.M{"updated_at": time.Now().UTC()}

		// 只有传入值时才更新对应字段
		if req.Name != nil {
			updateFields["name"] = *req.Name
		}
		if req.Email != nil {
			updateFields["email"] = *req.Email
		}
		if req.Phone != nil {
			updateFields["phone"] = *req.Phone
		}
		if req.Description != nil {
			updateFields["description"] = *req.Description
		}
		if req.Contacts != nil {
			updateFields["contacts"] = *req.Contacts
		}
		if req.Status != nil {
			updateFields["status"] = model.OrganizationStatus(*req.Status)
		}
		if req.Logo != nil {
			updateFields["logo"] = *req.Logo
		}

		// 执行更新
		if len(updateFields) > 1 { // 除了updated_at还有其他字段才执行更新
			return mongoutil.UpdateOne(sessionCtx, organizationDao.Collection,
				bson.M{"_id": req.OrgId},
				bson.M{"$set": updateFields},
				false)
		}
		return nil
	})
	return errs.Unwrap(err)
}

type CreateOrganizationWalletReq struct {
	PayPwd string `bson:"pay_pwd" json:"pay_pwd"` // 支付密码
}

func (w *OrganizationSvc) CreateOrganizationWallet(userId string, organizationId primitive.ObjectID, params CreateOrganizationWalletReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	organizationDao := model.NewOrganizationDao(db)
	walletInfoDao := walletModel.NewWalletInfoDao(db)
	//walletBalanceDao := walletModel.NewWalletBalanceDao(db)

	// hash加密
	hashPayPwd, err := utils.HashPassword(params.PayPwd)
	if err != nil {
		return err
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		// 检查组织是否存在
		organization, err := organizationDao.GetByIdAndStatus(sessionCtx, organizationId, model.OrganizationStatusPass)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.NotFoundErrWithResource("organization: " + organizationId.Hex())
			}
			return err
		}

		// 检查组织创建人是否为该用户
		if organization.CreatorId != userId {
			return freeErrors.UserAccountErr
		}

		// 检查组织钱包是否开通
		exists, err := walletInfoDao.ExistByOwnerIdAndOwnerType(sessionCtx, organization.ID.Hex(), walletModel.WalletInfoOwnerTypeOrganization)
		if err != nil {
			return err
		} else if exists {
			return freeErrors.WalletOpenedErr
		}

		// 创建钱包
		currency := &walletModel.WalletInfo{
			OwnerId:   organization.ID.Hex(),
			OwnerType: walletModel.WalletInfoOwnerTypeOrganization,
			PayPwd:    hashPayPwd,
		}
		if err := walletInfoDao.Create(sessionCtx, currency); err != nil {
			return err
		}
		return nil
	})
	return errs.Unwrap(err)
}

type UpdateOrganizationWalletPayPwdReq struct {
	LoginPwd  string `json:"login_pwd" form:"login_pwd" xml:"login_pwd"`
	NewPayPwd string `json:"new_pay_pwd" form:"new_pay_pwd" xml:"new_pay_pwd"`
}

func (w *OrganizationSvc) UpdateOrganizationWalletPayPwd(userId string, organizationId primitive.ObjectID, params UpdateOrganizationWalletPayPwdReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	walletInfoDao := walletModel.NewWalletInfoDao(db)
	accountDao := chatModel.NewAccountDao(db)

	// hash加密
	hashPayPwd, err := utils.HashPassword(params.NewPayPwd)
	if err != nil {
		return err
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		wal, err := walletInfoDao.GetByOwnerIdAndOwnerType(sessionCtx, organizationId.Hex(), walletModel.WalletInfoOwnerTypeOrganization)
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

		if params.LoginPwd != account.Password {
			return freeErrors.UserPwdErrErr
		}
		wal.PayPwd = hashPayPwd
		if err = walletInfoDao.UpdatePayPwd(sessionCtx, organizationId.Hex(), hashPayPwd, wal.OwnerType); err != nil {
			return err
		}
		return nil
	})

	return errs.Unwrap(err)
}

type CreateOrganizationCurrencyReq struct {
	Name               string               `bson:"name" json:"name" binding:"required"`
	Icon               string               `bson:"icon" json:"icon"`
	ExchangeRate       primitive.Decimal128 `bson:"exchange_rate" json:"exchange_rate" binding:"required"`
	MinAvailableAmount primitive.Decimal128 `bson:"min_available_amount" json:"min_available_amount"`
	MaxRedPacketAmount primitive.Decimal128 `bson:"max_red_packet_amount" json:"max_red_packet_amount"`
	Decimals           int                  `bson:"decimals" json:"decimals"`
	MaxTotalSupply     int64                `bson:"max_total_supply" json:"max_total_supply"`
}

func (w *OrganizationSvc) CreateOrganizationCurrency(userId string, organizationId primitive.ObjectID, params CreateOrganizationCurrencyReq) error {
	// ========== 设置默认值 ==========
	// 如果没有设置最大红包金额，使用默认值
	if params.MaxRedPacketAmount.String() == "" || params.MaxRedPacketAmount.String() == "0" {
		defaultAmount, err := primitive.ParseDecimal128(depConstant.DefaultMaxRedPacketAmount)
		if err != nil {
			return freeErrors.ApiErr("Internal error: invalid default max red packet amount")
		}
		params.MaxRedPacketAmount = defaultAmount
	}

	// ========== 精度安全验证 ==========
	if err := validateCurrencyDecimals(params.Decimals); err != nil {
		return err
	}

	// ========== 最大红包金额安全验证 ==========
	if err := validateMaxRedPacketAmountSafety(params.MaxRedPacketAmount.String()); err != nil {
		return err
	}

	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	organizationDao := model.NewOrganizationDao(db)
	walletCurrencyDao := walletModel.NewWalletCurrencyDao(db)
	walletInfoDao := walletModel.NewWalletInfoDao(db)
	walletBalanceDao := walletModel.NewWalletBalanceDao(db)

	//判断当前组织下币种名称是否重复
	currency, err := walletCurrencyDao.GetByNameAndOrgId(context.TODO(), params.Name, organizationId)
	if err != nil {
		if !dbutil.IsDBNotFound(err) {
			log.ZError(context.TODO(), "GetByNameAndOrgId 查询出错", err, "name", params.Name, "organization_id", organizationId.Hex())
			return err
		}
	}
	if currency != nil {
		return freeErrors.ApiErr("currency name already exists")
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		// 检查组织是否存在
		organization, err := organizationDao.GetByIdAndStatus(sessionCtx, organizationId, model.OrganizationStatusPass)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.NotFoundErrWithResource("organization: " + organizationId.Hex())
			}
			return err
		}

		// 检查组织创建人是否为该用户
		if organization.CreatorId != userId {
			return freeErrors.UserAccountErr
		}

		// 检查组织钱包是否开通
		walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(sessionCtx, organization.ID.Hex(), walletModel.WalletInfoOwnerTypeOrganization)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.WalletNotOpenErr
			}
			return err
		}

		// 创建代币
		currency := &walletModel.WalletCurrency{
			Name:               params.Name,
			Icon:               params.Icon,
			Order:              100,
			ExchangeRate:       params.ExchangeRate,
			MinAvailableAmount: params.MinAvailableAmount,
			Decimals:           params.Decimals,
			CreatorId:          organizationId,
			MaxRedPacketAmount: params.MaxRedPacketAmount,
			MaxTotalSupply:     params.MaxTotalSupply,
		}
		if err := walletCurrencyDao.Create(sessionCtx, currency); err != nil {
			return err
		}

		currency, err = walletCurrencyDao.GetByNameAndOrgId(sessionCtx, params.Name, organizationId)
		if err != nil {
			return err
		}

		// 将初始代币都发送给组织机构
		err = walletBalanceDao.UpdateAvailableBalanceAndAddTsRecord(sessionCtx, walletInfo.ID, currency.ID, decimal.NewFromInt(params.MaxTotalSupply),
			walletTransactionRecordModel.TsRecordTypeCreateCurrency, "", "")
		if err != nil {
			return err
		}

		return nil
	})
	return errs.Unwrap(err)
}

type UpdateOrganizationCurrencyReq struct {
	Name               string               `bson:"name" json:"name" binding:"required"`
	CurrencyId         primitive.ObjectID   `json:"currency_id" form:"currency_id" xml:"currency_id" binding:"required"`
	Icon               string               `bson:"icon" json:"icon" binding:"required"`
	ExchangeRate       primitive.Decimal128 `bson:"exchange_rate" json:"exchange_rate" binding:"required"`
	MinAvailableAmount primitive.Decimal128 `bson:"min_available_amount" json:"min_available_amount"`
	MaxRedPacketAmount primitive.Decimal128 `bson:"max_red_packet_amount" json:"max_red_packet_amount"`
	Decimals           int                  `bson:"decimals" json:"decimals" binding:"required"`
	MaxTotalSupply     int64                `bson:"max_total_supply" json:"max_total_supply" binding:"required"`
}

func (w *OrganizationSvc) UpdateOrganizationCurrency(userId string, organizationId primitive.ObjectID, params UpdateOrganizationCurrencyReq) error {
	// ========== 精度安全验证 ==========
	if err := validateCurrencyDecimals(params.Decimals); err != nil {
		return err
	}

	// ========== 最大红包金额安全验证 ==========
	if err := validateMaxRedPacketAmountSafety(params.MaxRedPacketAmount.String()); err != nil {
		return err
	}

	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	//organizationDao := model.NewOrganizationDao(db)
	walletCurrencyDao := walletModel.NewWalletCurrencyDao(db)
	walletInfoDao := walletModel.NewWalletInfoDao(db)
	walletBalanceDao := walletModel.NewWalletBalanceDao(db)

	err := mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {

		// 检查组织钱包是否开通
		walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(sessionCtx, organizationId.Hex(), walletModel.WalletInfoOwnerTypeOrganization)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.WalletNotOpenErr
			}
			return err
		}

		currency, err := walletCurrencyDao.GetById(sessionCtx, params.CurrencyId)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.NotFoundErrWithResource("currency: " + params.CurrencyId.Hex())
			}
			return freeErrors.SystemErr(err)
		}

		if currency.CreatorId != organizationId {
			return freeErrors.ApiErr("organizationId error")
		}

		if params.MaxTotalSupply != currency.MaxTotalSupply {
			subMaxTotalSupplyDecimal := decimal.NewFromInt(params.MaxTotalSupply - currency.MaxTotalSupply)
			err = walletBalanceDao.UpdateAvailableBalanceAndAddTsRecord(sessionCtx, walletInfo.ID, currency.ID, subMaxTotalSupplyDecimal,
				walletTransactionRecordModel.TsRecordTypeCreateCurrency, "", "")
			if err != nil {
				return err
			}
		}

		//currencyBalance, err := walletBalanceDao.GetByWalletIdAndCurrencyId(sessionCtx, wallet.ID, currency.ID)
		//if err != nil {
		//	return fmt.Errorf("GetByWalletIdAndCurrencyId error: %s", err)
		//}
		//currencyAvailableBalanceDecimal, err := decimal.NewFromString(currencyBalance.AvailableBalance.String())
		//if err != nil {
		//	return fmt.Errorf("decimal.NewFromString(currencyBalance.AvailableBalance.String()) error: %s", err)
		//}

		// 更新代币信息
		update := walletModel.WalletCurrencyUpdateInfoField{
			Name:               params.Name,
			Icon:               params.Icon,
			ExchangeRate:       params.ExchangeRate,
			MinAvailableAmount: params.MinAvailableAmount,
			Decimals:           params.Decimals,
			MaxTotalSupply:     params.MaxTotalSupply,
			MaxRedPacketAmount: params.MaxRedPacketAmount,
		}
		if err := walletCurrencyDao.UpdateInfoById(sessionCtx, params.CurrencyId, update); err != nil {
			return err
		}

		return nil
	})
	return errs.Unwrap(err)
}

type JoinOrgUsingInvitationCodeReq struct {
	InvitationCode string `json:"invitation_code" binding:"required"`
	Nickname       string `bson:"nickname" json:"nickname"`
	FaceURL        string `bson:"face_url" json:"face_url"`
	//Platform int32              `json:"platform"`
}

type JoinOrgUsingInvitationCodeResp struct {
	OrgId          primitive.ObjectID `json:"org_id"`
	ImServerUserId string             `json:"im_server_user_id"`
}

// JoinOrgUsingInvitationCode 通过邀请码加入组织
func (w *OrganizationSvc) JoinOrgUsingInvitationCode(operationID string, userId string, req JoinOrgUsingInvitationCodeReq) (*JoinOrgUsingInvitationCodeResp, error) {
	mongoCli := plugin.MongoCli()
	attributeDao := chatModel.NewAttributeDao(mongoCli.GetDB())
	attr, err := attributeDao.Take(context.TODO(), userId)
	if err != nil {
		return nil, err
	}

	var resp *JoinOrgUsingInvitationCodeResp
	err = plugin.MongoCli().GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		resp, err = w.JoinOrgUsingInvitationCodeByAttr(sessionCtx, operationID, userId, req, attr)
		return err
	})
	if err != nil {
		return nil, errs.Unwrap(err)
	}

	if resp != nil {
		systemStatistics.NotifySalesDailyStatsChangedAsync(resp.OrgId)
	}

	defFriendSvc := defaultFriendSvc.NewDefaultFriendSvc()
	defFriendSvc.InternalAddDefaultFriend(operationID, resp.OrgId, resp.ImServerUserId)

	defGroupSvc := defaultGroupSvc.NewDefaultGroupSvc()
	defGroupSvc.InternalAddDefaultGroup(operationID, resp.OrgId, resp.ImServerUserId)

	return resp, nil
}

func (w *OrganizationSvc) JoinOrgUsingInvitationCodeByAttr(ctx context.Context, operationID string, userId string, req JoinOrgUsingInvitationCodeReq, attr *chatModel.Attribute) (*JoinOrgUsingInvitationCodeResp, error) {
	mongoCli := plugin.MongoCli()
	orgUserDao := model.NewOrganizationUserDao(mongoCli.GetDB())
	organizationDao := model.NewOrganizationDao(mongoCli.GetDB())

	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(context.Background(), constantpb.OperationID, operationID)
	imApiCallerToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return nil, err
	}
	imApiCallerCtx := mctx.WithApiToken(ctxWithOpID, imApiCallerToken)

	var inviterType model.OrganizationUserInviterType
	var inviter string
	var inviterImServerUserId string
	var inviterOrgUser *model.OrganizationUser // 用户邀请时：邀请码所属组织用户（用于状态与封禁校验）
	orgId := primitive.NilObjectID

	if len([]rune(req.InvitationCode)) == model.OrgInvitationCodeLength {
		// 组织邀请
		org, err := organizationDao.GetByInvitationCode(ctx, req.InvitationCode)
		if err != nil {
			return nil, freeErrors.InvitationCodeErr(req.InvitationCode)
		}
		orgId = org.ID
		inviter = orgId.Hex()
		inviterType = model.OrganizationUserInviterTypeOrg

	} else if len([]rune(req.InvitationCode)) == model.OrgUserInvitationCodeLength {
		// 用户邀请
		orgUser, err := orgUserDao.GetByInvitationCode(ctx, req.InvitationCode)
		if err != nil {
			return nil, freeErrors.InvitationCodeErr(req.InvitationCode)
		}
		inviterOrgUser = orgUser
		orgId = orgUser.OrganizationId
		inviter = orgUser.UserId
		inviterType = model.OrganizationUserInviterTypeOrgUser
		inviterImServerUserId = orgUser.ImServerUserId
	} else {
		return nil, freeErrors.InvitationCodeErr(req.InvitationCode)
	}

	if err := ValidateAppUserNickname(ctx, mongoCli.GetDB(), orgId, req.Nickname, ""); err != nil {
		return nil, err
	}

	// 用户邀请：邀请码所属账号在组织内被禁用或在全局封禁表中，不允许使用该邀请码注册/加入
	if inviterType == model.OrganizationUserInviterTypeOrgUser && inviterOrgUser != nil {
		if inviterOrgUser.Status == model.OrganizationUserDisableStatus {
			return nil, freeErrors.ApiErr("inviter account has been disabled")
		}
		if inviterImServerUserId != "" {
			forbiddenAccountDao := chatModel.NewForbiddenAccountDao(mongoCli.GetDB())
			banned, err := forbiddenAccountDao.ExistByUserId(ctx, inviterImServerUserId)
			if err != nil {
				return nil, errs.Wrap(err)
			}
			if banned {
				return nil, freeErrors.ApiErr("inviter account has been banned")
			}
		}
	}

	user, err := orgUserDao.GetByUserIdAndOrgId(ctx, userId, orgId)
	if user != nil {
		forbiddenAccountDao := chatModel.NewForbiddenAccountDao(mongoCli.GetDB())
		forbiddenAccount, err := forbiddenAccountDao.Take(ctx, user.ImServerUserId)
		if err != nil {
			return nil, errs.Unwrap(err)
		}
		if forbiddenAccount != nil {
			return nil, freeErrors.ApiErr("the user has been banned")
		}
		return nil, freeErrors.ApiErr("the user has joined the organization")
	}
	if err != nil && !dbutil.IsDBNotFound(err) {
		return nil, errs.Unwrap(err)
	}

	newImServerUserID, err := utils.NewId()
	if err != nil {
		return nil, err
	}

	// 初始化组织用户对象
	orgUser := &model.OrganizationUser{
		OrganizationId:        orgId,
		UserId:                attr.UserID,
		Role:                  model.OrganizationUserNormalRole,
		Status:                model.OrganizationUserEnableStatus,
		RegisterType:          model.OrganizationUserRegisterTypeWeb,
		ImServerUserId:        newImServerUserID,
		Inviter:               inviter,
		InviterType:           inviterType,
		InviterImServerUserId: inviterImServerUserId,
	}

	// 在创建用户前先查询邀请者信息，计算层级字段
	var level = 1 // 默认级别为1（顶级用户）
	var ancestorPath []string
	var level1Parent, level2Parent, level3Parent string
	var directParentID string
	var needUpdateAncestors bool = false

	// 根据邀请人类型计算层级
	if inviterType == model.OrganizationUserInviterTypeOrgUser && inviter != "" {
		log.ZInfo(ctx, "处理用户邀请注册", nil,
			"invitation_code", inviter)
		// 这里有一个重要问题：邀请码与userId可能混淆
		// 首先尝试按照邀请码查询邀请者
		var parentByInvCode *model.OrganizationUser
		parentByInvCode, err = orgUserDao.GetByInvitationCode(ctx, inviter)

		if err != nil && !dbutil.IsDBNotFound(err) {
			log.ZWarn(ctx, "查询邀请码失败", err,
				"invitation_code", inviter)
		}

		// 如果未找到，尝试直接通过user_id查询
		var parent *model.OrganizationUser
		if parentByInvCode != nil {
			// 找到了邀请码对应的用户
			parent = parentByInvCode
			log.ZInfo(ctx, "通过邀请码找到邀请者", nil,
				"invitation_code", inviter,
				"parent_user_id", parent.UserId)
		} else {
			// 尝试直接按用户ID查询
			parent, err = orgUserDao.GetByUserId(ctx, inviter)
			if err != nil {
				if !dbutil.IsDBNotFound(err) {
					log.ZWarn(ctx, "查询邀请者信息失败", err,
						"user_id", inviter)
				} else {
					log.ZWarn(ctx, "未找到邀请者，设置用户为顶级节点", nil,
						"user_id_or_invitation_code", inviter)
				}
			} else {
				log.ZInfo(ctx, "通过用户ID找到邀请者", nil,
					"user_id", inviter,
					"parent_user_id", parent.UserId)
			}
		}

		// 如果找到了邀请者，获取其层级信息并设置当前用户的层级关系
		if parent != nil {
			collection := orgUserDao.Collection

			// 直接查询完整的用户信息，包括所有层级字段
			var parentFull struct {
				UserId              string      `bson:"user_id"`
				OrganizationId      interface{} `bson:"organization_id"`
				Level               int         `bson:"level"`
				AncestorPath        []string    `bson:"ancestor_path"`
				TeamSize            int         `bson:"team_size"`
				DirectDownlineCount int         `bson:"direct_downline_count"`
			}

			// 使用精确的查询条件
			err := collection.FindOne(ctx, bson.M{
				"user_id":         parent.UserId,
				"organization_id": orgId, // 确保在同一组织内
			}).Decode(&parentFull)

			if err != nil {
				log.ZWarn(ctx, "查询邀请者完整层级信息失败", err,
					"user_id", parent.UserId,
					"organization_id", orgId)
			} else {
				// 记录父级详细信息，便于调试
				log.ZInfo(ctx, "找到邀请者完整信息", nil,
					"parent_id", parentFull.UserId,
					"parent_level", parentFull.Level,
					"parent_ancestor_path", parentFull.AncestorPath,
					"parent_team_size", parentFull.TeamSize,
					"parent_direct_downline_count", parentFull.DirectDownlineCount)

				// 找到层级信息，计算新用户的层级
				// 确保层级正确，不可能小于1
				if parentFull.Level < 1 {
					log.ZWarn(ctx, "父级层级异常，重置为1", nil,
						"parent_id", parentFull.UserId,
						"invalid_level", parentFull.Level)
					level = 2 // 父级是1，子级是2
				} else {
					level = parentFull.Level + 1 // 正常计算子级层级
				}

				log.ZInfo(ctx, "计算新用户层级", nil,
					"parent_level", parentFull.Level,
					"new_user_level", level)

				// 确保ancestorPath不为nil
				if parentFull.AncestorPath != nil {
					ancestorPath = append([]string{parentFull.UserId}, parentFull.AncestorPath...)
					log.ZInfo(ctx, "构建完整祖先路径", nil,
						"ancestor_path", ancestorPath)
				} else {
					// 如果邀请者的祖先路径为nil，只包含邀请者自己
					ancestorPath = []string{parentFull.UserId}
					log.ZInfo(ctx, "父级无祖先路径，只设置直接父级", nil,
						"ancestor_path", ancestorPath)
				}

				level1Parent = parentFull.UserId
				directParentID = parentFull.UserId
				needUpdateAncestors = true

				// 设置二级父节点（如果存在）
				if parentFull.AncestorPath != nil && len(parentFull.AncestorPath) > 0 {
					level2Parent = parentFull.AncestorPath[0]
				}

				// 设置三级父节点（如果存在）
				if parentFull.AncestorPath != nil && len(parentFull.AncestorPath) > 1 {
					level3Parent = parentFull.AncestorPath[1]
				}

				log.ZInfo(ctx, "成功设置层级关系",
					"inviter", inviter,
					"parent_user_id", parentFull.UserId,
					"parent_level", parentFull.Level,
					"new_user_level", level,
					"ancestor_path", ancestorPath)
			}
		}
	} else if inviterType == model.OrganizationUserInviterTypeOrg {
		// 组织邀请 - 尝试获取组织虚拟根节点
		orgRootNodeID := OrgRootNodePrefix + orgId.Hex()

		// 查询组织根节点
		collection := orgUserDao.Collection

		// 使用模型匹配的字段创建查询
		var orgRootNodeBasic model.OrganizationUser
		// 使用结构体获取层级字段
		var orgRootNodeHierarchy struct {
			Level        int      `bson:"level"`
			AncestorPath []string `bson:"ancestor_path"`
		}

		// 首先查询基本信息
		err := collection.FindOne(ctx, bson.M{
			"organization_id": orgId,
			"user_id":         orgRootNodeID,
			"user_type":       "ORGANIZATION",
		}).Decode(&orgRootNodeBasic)

		if err != nil {
			if err == mongo.ErrNoDocuments {
				// 创建组织根节点
				now := time.Now()

				// 准备数据库特定字段
				extraFields := bson.M{
					"user_type":     "ORGANIZATION",
					"level":         0,
					"ancestor_path": []string{},
					"created_at":    now,
					"updated_at":    now,
				}

				// 合并文档
				rootNodeDoc := bson.M{
					"organization_id": orgId,
					"user_id":         orgRootNodeID,
					"role":            string(model.OrganizationUserSuperAdminRole),
					"status":          string(model.OrganizationUserEnableStatus),
				}

				// 添加额外字段
				for k, v := range extraFields {
					rootNodeDoc[k] = v
				}

				// 创建根节点
				_, err = collection.InsertOne(ctx, rootNodeDoc)
				if err != nil {
					log.ZWarn(ctx, "创建组织根节点失败", err,
						"organization_id", orgId.Hex())
				} else {
					// 查询新创建的根节点的完整信息
					err = collection.FindOne(ctx, bson.M{
						"organization_id": orgId,
						"user_id":         orgRootNodeID,
					}).Decode(&orgRootNodeBasic)

					if err != nil {
						log.ZWarn(ctx, "查询新创建的组织根节点失败", err)
					}

					// 同时查询层级信息
					err = collection.FindOne(ctx, bson.M{
						"organization_id": orgId,
						"user_id":         orgRootNodeID,
					}).Decode(&orgRootNodeHierarchy)

					if err != nil {
						log.ZWarn(ctx, "查询新创建的组织根节点层级信息失败", err)
					}

					log.ZInfo(ctx, "成功创建组织根节点",
						"root_node_id", orgRootNodeID)
				}
			} else {
				log.ZWarn(ctx, "查询组织根节点失败", err,
					"organization_id", orgId.Hex())
			}
		} else {
			// 如果找到了根节点，查询它的层级信息
			err = collection.FindOne(ctx, bson.M{
				"organization_id": orgId,
				"user_id":         orgRootNodeID,
			}).Decode(&orgRootNodeHierarchy)

			if err != nil {
				log.ZWarn(ctx, "查询组织根节点层级信息失败", err)
			}
		}

		// 如果找到或创建了组织根节点，设置层级关系
		if orgRootNodeBasic.UserId != "" {
			level = 1
			ancestorPath = []string{orgRootNodeBasic.UserId}
			level1Parent = orgRootNodeBasic.UserId
			directParentID = orgRootNodeBasic.UserId
			needUpdateAncestors = true

			log.ZInfo(ctx, "设置组织根节点为用户父节点",
				"organization_id", orgId.Hex(),
				"root_node_id", orgRootNodeID)
		}
	}

	// 创建用户前，先确保所有层级字段都被正确设置
	// 使用事务确保创建用户和层级字段设置的原子性
	err = mongoCli.GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		// 1. 首先创建基础用户
		if err = orgUserDao.Create(sessionCtx, orgUser); err != nil {
			log.ZError(sessionCtx, "创建组织用户失败", err,
				"user_id", orgUser.UserId,
				"organization_id", orgId.Hex())
			return err
		}

		// 2. 立即添加层级字段
		// 确认一下level的有效性
		if level <= 0 {
			log.ZWarn(sessionCtx, "发现层级计算错误，设置为默认值1", nil,
				"user_id", orgUser.UserId,
				"invalid_level", level)
			level = 1
		}

		log.ZInfo(sessionCtx, "设置用户层级字段", nil,
			"user_id", orgUser.UserId,
			"level", level,
			"ancestor_path", ancestorPath,
			"level1_parent", level1Parent,
			"level2_parent", level2Parent,
			"level3_parent", level3Parent)

		updateFields := bson.M{
			"$set": bson.M{
				"level":                 level,
				"ancestor_path":         ancestorPath,
				"level1_parent":         level1Parent,
				"level2_parent":         level2Parent,
				"level3_parent":         level3Parent,
				"user_type":             "USER",
				"team_size":             0,
				"direct_downline_count": 0,
			},
		}

		collection := orgUserDao.Collection
		_, err := collection.UpdateOne(
			sessionCtx,
			bson.M{"user_id": orgUser.UserId, "organization_id": orgUser.OrganizationId},
			updateFields)
		if err != nil {
			log.ZError(sessionCtx, "设置用户层级字段失败", err,
				"user_id", orgUser.UserId,
				"organization_id", orgId.Hex())
			return err
		}

		// 3. 在同一事务中更新所有祖先节点
		if needUpdateAncestors {
			// 更新直接父节点的DirectDownlineCount
			if directParentID != "" {
				_, updateErr := collection.UpdateOne(
					sessionCtx,
					bson.M{"user_id": directParentID, "organization_id": orgId},
					bson.M{"$inc": bson.M{"direct_downline_count": 1}},
				)
				if updateErr != nil {
					log.ZError(sessionCtx, "更新直接上级DirectDownlineCount失败", updateErr,
						"parent_id", directParentID)
					return updateErr
				}
				log.ZInfo(sessionCtx, "更新直接上级DirectDownlineCount成功",
					"parent_id", directParentID)
			}

			// 更新所有祖先节点的TeamSize
			if len(ancestorPath) > 0 {
				log.ZInfo(sessionCtx, "正在更新祖先节点团队规模", nil,
					"ancestor_path", ancestorPath)

				// 修复版解决方案: 避免重复计数，确保每个祖先节点只被更新一次
				// 策略：使用一个映射来跟踪已更新的节点，避免重复更新
				updatedAncestors := make(map[string]bool)

				// 存储所有找到的祖先ID，最后一次性更新
				var allAncestorIDs []string

				// 1. 添加直接父级
				if directParentID != "" {
					// 添加到待更新列表而不是立即更新
					allAncestorIDs = append(allAncestorIDs, directParentID)
					updatedAncestors[directParentID] = true
					log.ZInfo(sessionCtx, "添加直接父级到更新列表", nil,
						"parent_id", directParentID)

					// 2. 构建一个查询，递归地找出所有上级节点
					pipeline := mongo.Pipeline{
						// 第一阶段：从直接父级开始
						bson.D{{"$match", bson.M{
							"user_id":         directParentID,
							"organization_id": orgId,
						}}},
						// 第二阶段：递归查找所有祖先
						bson.D{{"$graphLookup", bson.M{
							"from":             "organization_user", // 集合名称
							"startWith":        "$level1_parent",    // 从level1_parent字段开始查找
							"connectFromField": "level1_parent",     // 当前文档的连接字段
							"connectToField":   "user_id",           // 目标文档的连接字段
							"as":               "all_ancestors",     // 结果字段名
							"maxDepth":         10,                  // 最大递归深度
							"restrictSearchWithMatch": bson.M{ // 限制查询范围
								"organization_id": orgId,
							},
						}}},
						// 第三阶段：展开祖先数组
						bson.D{{"$unwind", bson.M{
							"path":                       "$all_ancestors",
							"preserveNullAndEmptyArrays": false,
						}}},
						// 第四阶段：只提取祖先ID
						bson.D{{"$project", bson.M{
							"ancestor_id": "$all_ancestors.user_id",
							"_id":         0,
						}}},
					}

					// 执行聚合查询
					cursor, aggrErr := collection.Aggregate(sessionCtx, pipeline)
					if aggrErr != nil {
						log.ZError(sessionCtx, "查询所有祖先失败", aggrErr)
					} else {
						defer cursor.Close(sessionCtx)

						// 收集所有祖先ID
						var ancestorDocs []struct {
							AncestorID string `bson:"ancestor_id"`
						}
						if err := cursor.All(sessionCtx, &ancestorDocs); err != nil {
							log.ZError(sessionCtx, "解析祖先ID失败", err)
						} else {
							// 从结果中提取祖先ID并添加到待更新列表
							for _, doc := range ancestorDocs {
								if doc.AncestorID != "" && !updatedAncestors[doc.AncestorID] {
									allAncestorIDs = append(allAncestorIDs, doc.AncestorID)
									updatedAncestors[doc.AncestorID] = true
								}
							}

							if len(ancestorDocs) > 0 {
								log.ZInfo(sessionCtx, "通过递归查询找到了间接祖先", nil,
									"ancestor_count", len(ancestorDocs))
							} else {
								log.ZInfo(sessionCtx, "通过递归查询未找到任何间接祖先")
							}
						}
					}
				}

				// 3. 添加原始祖先路径中的节点(如果尚未添加)
				for _, ancestorID := range ancestorPath {
					if ancestorID != "" && !updatedAncestors[ancestorID] {
						allAncestorIDs = append(allAncestorIDs, ancestorID)
						updatedAncestors[ancestorID] = true
					}
				}

				// 4. 一次性更新所有收集到的祖先节点
				if len(allAncestorIDs) > 0 {
					log.ZInfo(sessionCtx, "最终收集到的所有需要更新的祖先节点", nil,
						"ancestor_count", len(allAncestorIDs),
						"ancestors", allAncestorIDs)

					_, updateErr := collection.UpdateMany(
						sessionCtx,
						bson.M{
							"user_id":         bson.M{"$in": allAncestorIDs},
							"organization_id": orgId,
						},
						bson.M{"$inc": bson.M{"team_size": 1}},
					)

					if updateErr != nil {
						log.ZError(sessionCtx, "批量更新祖先TeamSize失败", updateErr,
							"ancestor_count", len(allAncestorIDs))

						// 如果批量更新失败，尝试逐个更新，但仍确保不重复
						successCount := 0
						for _, ancestorID := range allAncestorIDs {
							_, singleErr := collection.UpdateOne(
								sessionCtx,
								bson.M{
									"user_id":         ancestorID,
									"organization_id": orgId,
								},
								bson.M{"$inc": bson.M{"team_size": 1}},
							)

							if singleErr != nil {
								log.ZError(sessionCtx, "单个更新祖先TeamSize失败", singleErr,
									"ancestor_id", ancestorID)
							} else {
								successCount++
							}
						}

						log.ZInfo(sessionCtx, "单个更新祖先TeamSize结果", nil,
							"total", len(allAncestorIDs),
							"success_count", successCount)
					} else {
						log.ZInfo(sessionCtx, "一次性批量更新所有祖先TeamSize成功", nil,
							"ancestor_count", len(allAncestorIDs))
					}
				} else {
					log.ZWarn(sessionCtx, "没有找到任何需要更新的祖先节点", nil)
				}

				// 5. 单独确保更新组织根节点的团队规模
				orgRootNodeID := OrgRootNodePrefix + orgId.Hex()

				_, rootUpdateErr := collection.UpdateOne(
					sessionCtx,
					bson.M{
						"user_id":         orgRootNodeID,
						"organization_id": orgId,
						"user_type":       "ORGANIZATION",
					},
					bson.M{"$inc": bson.M{"team_size": 1}},
				)

				if rootUpdateErr != nil {
					log.ZWarn(sessionCtx, "更新组织根节点团队规模失败", rootUpdateErr,
						"root_node_id", orgRootNodeID)
				} else {
					log.ZInfo(sessionCtx, "成功更新组织根节点团队规模", nil,
						"root_node_id", orgRootNodeID)
				}
			}
		}

		// 事务成功
		return nil
	})

	// 如果事务失败，返回错误
	if err != nil {
		log.ZError(ctx, "创建用户并设置层级关系事务失败", err,
			"user_id", attr.UserID,
			"organization_id", orgId.Hex())
		return nil, err
	}

	// 记录成功日志
	log.ZInfo(ctx, "用户注册和层级关系设置成功",
		"user_id", attr.UserID,
		"organization_id", orgId.Hex(),
		"level", level,
		"ancestor_path", ancestorPath)
	userInfo := &imapi.OrgUserInfo{
		UserID:         newImServerUserID,
		Nickname:       req.Nickname,
		FaceURL:        req.FaceURL,
		CreateTime:     time.Now().UnixMilli(),
		OrgId:          orgId.Hex(),
		OrgRole:        string(model.OrganizationUserNormalRole),
		CanSendFreeMsg: 0,
	}
	// 注册IM服务器用户信息
	log.ZInfo(ctx, "注册用户到IM服务器", nil,
		"user_id", attr.UserID,
		"im_server_user_id", newImServerUserID)

	err = imApiCaller.RegisterOrgUser(imApiCallerCtx, []*imapi.OrgUserInfo{userInfo})
	if err != nil {
		log.ZError(ctx, "注册IM服务器用户失败", err,
			"user_id", attr.UserID,
			"im_server_user_id", newImServerUserID)
		return nil, freeErrors.SystemErr(fmt.Errorf("注册IM服务器用户失败: %w", err))
	}

	// 注册成功，返回完整响应信息
	log.ZInfo(ctx, "用户注册完成", nil,
		"user_id", attr.UserID,
		"organization_id", orgId.Hex(),
		"im_server_user_id", newImServerUserID,
		"level", level,
		"has_parent", needUpdateAncestors)

	// 删除异步验证，避免重复更新团队规模
	// 我们已经在事务中实现了完整可靠的更新逻辑，不再需要异步验证
	// 异步验证是之前解决方案的一部分，但现在可能导致重复计数
	// 如果主事务中的更新成功，就不需要额外的验证

	return &JoinOrgUsingInvitationCodeResp{
		OrgId:          orgId,
		ImServerUserId: newImServerUserID,
	}, nil
}

type OrganizationUserSvc struct {
}

func NewOrganizationUserService() *OrganizationUserSvc {
	return &OrganizationUserSvc{}
}

type CreateOrganizationBackendAdminReq struct {
	Nickname string `json:"nickname"  binding:"required"`
	FaceURL  string `json:"faceURL"`
	Birth    int64  `json:"birth"`
	Gender   int32  `json:"gender"`
	Account  string `json:"account" binding:"required"`
	Password string `json:"password"  binding:"required"`
}

type ResetOrganizationUserPasswordReq struct {
	UserID      string `json:"userID" binding:"required"`
	NewPassword string `json:"newPassword" binding:"required"`
}

type UpdateOrganizationUserNicknameReq struct {
	UserID   string `json:"userID" binding:"required"`
	Nickname string `json:"nickname" binding:"required"`
}

func orgPasswordIsMD5Hex(s string) bool {
	if len(s) != 32 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

func normalizeOrgPasswordForStorage(p string) string {
	p = strings.TrimSpace(p)
	p = strings.ToLower(p)
	if orgPasswordIsMD5Hex(p) {
		return p
	}
	sum := md5.Sum([]byte(p))
	return hex.EncodeToString(sum[:])
}

func (w *OrganizationUserSvc) CreateOrganizationBackendAdmin(operatorId string, orgId primitive.ObjectID, params CreateOrganizationBackendAdminReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	organizationDao := model.NewOrganizationDao(db)
	orgUserDao := model.NewOrganizationUserDao(db)
	attributeDao := chatModel.NewAttributeDao(db)
	accountDao := chatModel.NewAccountDao(db)
	registerDao := chatModel.NewRegisterDao(db)
	credentialDao := chatModel.NewCredentialDao(db)

	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(context.Background(), constantpb.OperationID, operatorId)
	imApiCallerToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return err
	}
	imApiCallerCtx := mctx.WithApiToken(ctxWithOpID, imApiCallerToken)

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		organization, err := organizationDao.GetByIdAndStatus(sessionCtx, orgId, model.OrganizationStatusPass)
		if err != nil {
			return err
		}

		newUserID, err := utils.NewId()
		if err != nil {
			return err
		}
		newImServerUserID, err := utils.NewId()
		if err != nil {
			return err
		}

		orgCreatorUser, err := attributeDao.Take(sessionCtx, organization.CreatorId)
		if err != nil {
			return err
		}
		params.Account = orgCreatorUser.Account + "_" + params.Account
		attributes, err := attributeDao.FindAccountCaseInsensitive(sessionCtx, []string{params.Account})
		if err != nil {
			return err
		}
		if len(attributes) > 0 {
			return fmt.Errorf("account %s already exists", params.Account)
		}

		orgUser := &model.OrganizationUser{
			OrganizationId: organization.ID,
			UserId:         newUserID,
			Role:           model.OrganizationUserBackendAdminRole,
			Status:         model.OrganizationUserEnableStatus,
			RegisterType:   model.OrganizationUserRegisterTypeBackend,
			ImServerUserId: newImServerUserID,
		}
		if err := orgUserDao.Create(sessionCtx, orgUser); err != nil {
			return err
		}

		// 创建credential记录
		credentials := make([]*chatModel.Credential, 0)
		var registerType int32 = constant.AccountRegister
		if params.Account != "" {
			credentials = append(credentials, &chatModel.Credential{
				UserID:      newUserID,
				Account:     params.Account,
				Type:        constant.CredentialAccount,
				AllowChange: true,
			})
			registerType = constant.AccountRegister
		}

		register := &chatModel.Register{
			UserID:      newUserID,
			DeviceID:    "",
			IP:          "",
			Platform:    pkgConstant.H5PlatformStr,
			AccountType: "",
			Mode:        constant.UserMode,
			CreateTime:  time.Now(),
		}
		account := &chatModel.Account{
			UserID:         newUserID,
			Password:       params.Password,
			OperatorUserID: operatorId,
			ChangeTime:     register.CreateTime,
			CreateTime:     register.CreateTime,
		}

		attribute := &chatModel.Attribute{
			UserID:         newUserID,
			Account:        params.Account,
			PhoneNumber:    "",
			AreaCode:       "",
			Email:          "",
			Nickname:       "",
			FaceURL:        "",
			Gender:         params.Gender,
			BirthTime:      time.UnixMilli(params.Birth),
			ChangeTime:     register.CreateTime,
			CreateTime:     register.CreateTime,
			AllowVibration: constant.DefaultAllowVibration,
			AllowBeep:      constant.DefaultAllowBeep,
			AllowAddFriend: constant.DefaultAllowAddFriend,
			RegisterType:   registerType,
		}

		if err := registerDao.Create(sessionCtx, register); err != nil {
			return err
		}
		if err := accountDao.Create(sessionCtx, account); err != nil {
			return err
		}
		if err := attributeDao.Create(sessionCtx, attribute); err != nil {
			return err
		}
		// 创建credential记录
		if err := credentialDao.Create(sessionCtx, credentials...); err != nil {
			return err
		}

		userInfo := &imapi.OrgUserInfo{
			UserID:         newImServerUserID,
			Nickname:       params.Nickname,
			FaceURL:        params.FaceURL,
			CreateTime:     time.Now().UnixMilli(),
			OrgId:          orgUser.OrganizationId.Hex(),
			OrgRole:        string(orgUser.Role),
			CanSendFreeMsg: 0,
		}

		err = imApiCaller.RegisterOrgUser(imApiCallerCtx, []*imapi.OrgUserInfo{userInfo})
		return err
	})
	return errs.Unwrap(err)
}

func (w *OrganizationUserSvc) ResetOrganizationUserPassword(ctx context.Context, operationID string, orgId primitive.ObjectID, operator *model.OrganizationUser, params ResetOrganizationUserPasswordReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	orgUserDao := model.NewOrganizationUserDao(db)
	accountDao := chatModel.NewAccountDao(db)

	if operator == nil {
		return freeErrors.ForbiddenErr("operator not found")
	}

	targetOrgUser, err := orgUserDao.GetByUserIdAndOrgId(ctx, params.UserID, orgId)
	if err != nil {
		return err
	}

	if operator.Role != model.OrganizationUserSuperAdminRole && targetOrgUser.Role == model.OrganizationUserSuperAdminRole {
		return freeErrors.ForbiddenErr("backend admin cannot reset super admin password")
	}

	if strings.TrimSpace(params.NewPassword) == "" {
		return errs.ErrArgs.WrapMsg("new password must be set")
	}
	password := normalizeOrgPasswordForStorage(params.NewPassword)

	imApiCaller := plugin.ImApiCaller()
	apiCtx := context.WithValue(ctx, constantpb.OperationID, operationID)
	imToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(apiCtx)
	if err != nil {
		return err
	}
	apiCtx = mctx.WithApiToken(apiCtx, imToken)

	if err := accountDao.UpdatePassword(ctx, targetOrgUser.UserId, password); err != nil {
		return err
	}

	if _, err := plugin.AdminClient().InvalidateToken(ctx, &adminpb.InvalidateTokenReq{UserID: targetOrgUser.UserId}); err != nil {
		return err
	}

	if targetOrgUser.ImServerUserId != "" {
		if err := imApiCaller.ForceOffLine(apiCtx, targetOrgUser.ImServerUserId); err != nil {
			return err
		}
	}

	return nil
}

func (w *OrganizationUserSvc) UpdateOrganizationUserNickname(ctx context.Context, operationID string, orgId primitive.ObjectID, operator *model.OrganizationUser, params UpdateOrganizationUserNicknameReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	orgUserDao := model.NewOrganizationUserDao(db)
	userDao := openImModel.NewUserDao(db)

	if operator == nil {
		return freeErrors.ForbiddenErr("operator not found")
	}

	targetOrgUser, err := orgUserDao.GetByUserIdAndOrgId(ctx, params.UserID, orgId)
	if err != nil {
		return err
	}
	if operator.Role != model.OrganizationUserSuperAdminRole && targetOrgUser.Role == model.OrganizationUserSuperAdminRole {
		return freeErrors.ForbiddenErr("backend admin cannot update super admin nickname")
	}

	imUser, err := userDao.Take(ctx, targetOrgUser.ImServerUserId)
	if err != nil {
		return err
	}

	nickname := strings.TrimSpace(params.Nickname)
	if err := ValidateAppUserNickname(ctx, db, orgId, nickname, targetOrgUser.ImServerUserId); err != nil {
		return err
	}

	imApiCaller := plugin.ImApiCaller()
	apiCtx := context.WithValue(ctx, constantpb.OperationID, operationID)
	imToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(apiCtx)
	if err != nil {
		return err
	}
	apiCtx = mctx.WithApiToken(apiCtx, imToken)

	return imApiCaller.UpdateUserInfo(apiCtx, targetOrgUser.ImServerUserId, nickname, imUser.FaceURL)
}

type UpdateWebUserRoleReq struct {
	UserId string                     `json:"user_id"`
	Role   model.OrganizationUserRole `json:"role"`
}

type UpdateUserCanSendFreeMsgReq struct {
	UserId         string `json:"user_id" binding:"required"`
	CanSendFreeMsg int32  `json:"can_send_free_msg" binding:"gte=0,lte=1"` // 0=普通用户需好友验证，1=可跳过消息验证
}

func (w *OrganizationUserSvc) roleCanSendFreeMsg(ctx context.Context, orgId primitive.ObjectID, role model.OrganizationUserRole) (int32, error) {
	orgRolePermissionDao := model.NewOrganizationRolePermissionDao(plugin.MongoCli().GetDB())
	hasPermission, err := orgRolePermissionDao.ExistPermission(ctx, orgId, role, model.PermissionCodeFreePrivateChat)
	if err != nil {
		return 0, err
	}
	if hasPermission {
		return 1, nil
	}
	return 0, nil
}

func (w *OrganizationUserSvc) UpdateWebUserRole(ctx context.Context, operatorId string, orgId primitive.ObjectID, params UpdateWebUserRoleReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	organizationDao := model.NewOrganizationDao(db)
	orgUserDao := model.NewOrganizationUserDao(db)

	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(context.Background(), constantpb.OperationID, operatorId)
	imApiCallerToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return err
	}
	imApiCallerCtx := mctx.WithApiToken(ctxWithOpID, imApiCallerToken)

	orgUser, err := orgUserDao.GetByUserIdAndOrgId(ctx, params.UserId, orgId)
	if err != nil {
		return err
	}

	targetCanSendFreeMsg, err := w.roleCanSendFreeMsg(ctx, orgId, params.Role)
	if err != nil {
		return err
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		_, err := organizationDao.GetByIdAndStatus(sessionCtx, orgId, model.OrganizationStatusPass)
		if err != nil {
			return err
		}

		notAllowedRole := []model.OrganizationUserRole{model.OrganizationUserSuperAdminRole, model.OrganizationUserBackendAdminRole}
		if slices.Contains(notAllowedRole, orgUser.Role) {
			return fmt.Errorf("backend manager user are not allowed to be set as web user role")
		}

		AllowedSetRole := []model.OrganizationUserRole{model.OrganizationUserNormalRole, model.OrganizationUserGroupManagerRole, model.OrganizationUserTermManagerRole}
		if !slices.Contains(AllowedSetRole, params.Role) {
			return fmt.Errorf("web users can only be set as normal, group manager or term manager role")
		}

		err = orgUserDao.UpdateInfoById(sessionCtx, orgUser.ID, model.UpdateInfoByIdField{
			Status: orgUser.Status,
			Role:   params.Role,
		})
		if err != nil {
			return err
		}

		err = imApiCaller.UpdateOrgUserInfo(imApiCallerCtx, orgUser.ImServerUserId, orgUser.OrganizationId.Hex(), string(params.Role))
		if err != nil {
			return err
		}

		err = imApiCaller.UpdateUserCanSendFreeMsg(imApiCallerCtx, orgUser.ImServerUserId, targetCanSendFreeMsg)
		return err
	})

	// 使用协程异步通知用户权限变更(如果配置启用)
	if plugin.ChatCfg().Share.EnablePermissionNotifications {
		go func() {
			if notifyErr := notifyUserPermissionChange(context.Background(), operatorId, orgUser.ImServerUserId, orgId); notifyErr != nil {
				log.ZError(context.Background(), "通知用户权限变更失败", notifyErr,
					"org_id", orgId.Hex(),
					"target_user_id", orgUser.ImServerUserId,
					"new_role", params.Role)
			}
		}()
	}

	return errs.Unwrap(err)
}

func (w *OrganizationUserSvc) UpdateUserCanSendFreeMsg(ctx context.Context, operatorId string, orgId primitive.ObjectID, params UpdateUserCanSendFreeMsgReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	organizationDao := model.NewOrganizationDao(db)
	orgUserDao := model.NewOrganizationUserDao(db)

	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(context.Background(), constantpb.OperationID, operatorId)
	imApiCallerToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return err
	}
	imApiCallerCtx := mctx.WithApiToken(ctxWithOpID, imApiCallerToken)

	orgUser, err := orgUserDao.GetByUserIdAndOrgId(ctx, params.UserId, orgId)
	if err != nil {
		return err
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		_, err := organizationDao.GetByIdAndStatus(sessionCtx, orgId, model.OrganizationStatusPass)
		if err != nil {
			return err
		}
		// 调用 IM API 更新用户的 CanSendFreeMsg 字段
		err = imApiCaller.UpdateUserCanSendFreeMsg(imApiCallerCtx, orgUser.ImServerUserId, params.CanSendFreeMsg)
		if err != nil {
			return err
		}
		return nil
	})

	// 使用协程异步通知用户CanSendFreeMsg状态变更(如果配置启用)
	if plugin.ChatCfg().Share.EnablePermissionNotifications {
		go func() {
			if notifyErr := notifyUserCanSendFreeMsgChange(context.Background(), operatorId, orgUser.ImServerUserId, orgId, params.CanSendFreeMsg); notifyErr != nil {
				log.ZError(context.Background(), "通知用户CanSendFreeMsg状态变更失败", notifyErr,
					"org_id", orgId.Hex(),
					"target_user_id", orgUser.ImServerUserId,
					"new_can_send_free_msg", params.CanSendFreeMsg)
			}
		}()
	}

	return errs.Unwrap(err)
}

type UpdateUserStatusReq struct {
	UserId string                       `json:"user_id"`
	Status model.OrganizationUserStatus `json:"status"`
}

func (w *OrganizationUserSvc) UpdateUserStatus(orgId primitive.ObjectID, params UpdateUserStatusReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	organizationDao := model.NewOrganizationDao(db)
	orgUserDao := model.NewOrganizationUserDao(db)

	err := mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		_, err := organizationDao.GetByIdAndStatus(sessionCtx, orgId, model.OrganizationStatusPass)
		if err != nil {
			return err
		}

		orgUser, err := orgUserDao.GetByUserIdAndOrgId(sessionCtx, params.UserId, orgId)
		if err != nil {
			return err
		}

		err = orgUserDao.UpdateInfoById(sessionCtx, orgUser.ID, model.UpdateInfoByIdField{
			Status: model.OrganizationUserStatus(params.Status),
			Role:   orgUser.Role,
		})
		return err
	})
	return errs.Unwrap(err)
}

func (w *OrganizationUserSvc) GetUserAllOrg(keyword string, userIds []string) (*paginationUtils.ListResp[*dto.OrgUserWithOrgResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	orgUserDao := model.NewOrganizationUserDao(db)
	forbiddenAccountDao := chatModel.NewForbiddenAccountDao(db)

	notInImUserIds, err := forbiddenAccountDao.FindAllIDs(context.TODO())
	if err != nil {
		return nil, err
	}

	orgUser, err := orgUserDao.SelectJoinUser(context.TODO(), primitive.NilObjectID, keyword, userIds, notInImUserIds, nil, nil)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.OrgUserWithOrgResp]{
		Total: int64(len(orgUser)),
		List:  []*dto.OrgUserWithOrgResp{},
	}

	for _, record := range orgUser {
		item, err := dto.NewOrgUserWithOrgResp(context.TODO(), db, record)
		if err != nil {
			return nil, err
		}
		resp.List = append(resp.List, item)
	}

	return resp, err
}

type ChangeOrgUserReq struct {
	OrgId    primitive.ObjectID `json:"org_id"`
	Platform int32              `json:"platform"`
}

type ChangeOrgUserResp struct {
	OrgId          primitive.ObjectID `json:"org_id"`
	ImToken        string             `json:"im_token"`
	ImServerUserId string             `json:"im_server_user_id"`
}

// ChangeOrgUser 切换组织用户
func (w *OrganizationUserSvc) ChangeOrgUser(ctx context.Context, operationID string, userId string, req ChangeOrgUserReq) (*ChangeOrgUserResp, error) {
	mongoCli := plugin.MongoCli()
	orgUserDao := model.NewOrganizationUserDao(mongoCli.GetDB())

	orgUser, err := orgUserDao.GetByUserIdAndOrgId(context.Background(), userId, req.OrgId)
	if err != nil {
		return nil, err
	}

	if orgUser.Status == model.OrganizationUserDisableStatus {
		return nil, freeErrors.ApiErr("the account is not activated")
	}

	org, err := orgCache.NewOrgCacheRedis(plugin.RedisCli(), mongoCli.GetDB()).GetByIdAndStatus(ctx, orgUser.OrganizationId, model.OrganizationStatusPass)
	if err != nil && !dbutil.IsDBNotFound(err) {
		return nil, errs.NewCodeError(freeErrors.ErrSystem, "failed to query org by email")
	}

	// 获取im token
	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return nil, err
	}

	imToken, err := imApiCaller.GetUserToken(mctx.WithApiToken(ctxWithOpID, adminToken), orgUser.ImServerUserId, req.Platform)
	if err != nil {
		return nil, err
	}

	// 记录用户切换组织的登录记录（异步处理）
	go func(imServerUserId string, orgId primitive.ObjectID) {
		changeOrgDao := model.NewChangeOrgUserDao(mongoCli.GetDB())
		err := changeOrgDao.UpsertTodayLoginRecord(ctxWithOpID, imServerUserId, orgId)
		if err != nil {
			log.ZError(ctxWithOpID, "记录用户切换组织失败", err, "im_server_user_id", imServerUserId, "org_id", orgId)
		}
	}(orgUser.ImServerUserId, orgUser.OrganizationId)

	return &ChangeOrgUserResp{
		OrgId:          org.ID,
		ImToken:        imToken,
		ImServerUserId: orgUser.ImServerUserId,
	}, nil
}

type GetOrgByImServerUserIdReq struct {
	ImServerUserId string `json:"im_server_user_id" binding:"required"`
}

type GetOrgByImServerUserIdResp struct {
	OrganizationId primitive.ObjectID         `json:"organization_id"`
	UserId         string                     `json:"user_id"`
	Role           model.OrganizationUserRole `json:"role"`
	ImServerUserId string                     `json:"im_server_user_id"`
	InvitationCode string                     `json:"invitation_code"`
	Inviter        string                     `json:"inviter"`
}

func (w *OrganizationUserSvc) GetOrgByImServerUserId(ctx context.Context, req GetOrgByImServerUserIdReq) (*GetOrgByImServerUserIdResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	orgUserDao := model.NewOrganizationUserDao(db)

	// 根据imServerUserId查找组织用户记录
	orgUser, err := orgUserDao.GetByUserIMServerUserId(ctx, req.ImServerUserId)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource(req.ImServerUserId)
		}
		return nil, err
	}

	return &GetOrgByImServerUserIdResp{
		OrganizationId: orgUser.OrganizationId,
		UserId:         orgUser.UserId,
		Role:           orgUser.Role,
		ImServerUserId: orgUser.ImServerUserId,
		InvitationCode: orgUser.InvitationCode,
		Inviter:        orgUser.Inviter,
	}, nil
}

// intersectOrgUserIDs 两个 user_id 列表求交集（保留 a 中顺序）
func intersectOrgUserIDs(a, b []string) []string {
	if len(a) == 0 {
		return append([]string(nil), b...)
	}
	if len(b) == 0 {
		return append([]string(nil), a...)
	}
	mb := make(map[string]struct{}, len(b))
	for _, x := range b {
		if x != "" {
			mb[x] = struct{}{}
		}
	}
	out := make([]string, 0)
	for _, x := range a {
		if x == "" {
			continue
		}
		if _, ok := mb[x]; ok {
			out = append(out, x)
		}
	}
	return out
}

// applyLoginIPUserIDFilter 按最近登录 IP 子串收窄 req.UserIds；无匹配时返回 (true, nil) 表示应直接空列表。
func applyLoginIPUserIDFilter(ctx context.Context, db *mongo.Database, req *dto.GetOrgUserReq) (empty bool, err error) {
	ip := strings.TrimSpace(req.LoginIP)
	if ip == "" {
		return false, nil
	}
	loginDao := chatModel.NewUserLoginRecordDao(db)
	ipUserIDs, err := loginDao.FindUserIDsByLatestLoginIPContains(ctx, ip)
	if err != nil {
		return false, err
	}
	if len(ipUserIDs) == 0 {
		return true, nil
	}
	if len(req.UserIds) > 0 {
		req.UserIds = intersectOrgUserIDs(req.UserIds, ipUserIDs)
		if len(req.UserIds) == 0 {
			return true, nil
		}
	} else {
		req.UserIds = ipUserIDs
	}
	return false, nil
}

// GetOrgUserWithFilters 带过滤条件的查询组织用户（支持标签筛选）- 深度优化版本
func (w *OrganizationUserSvc) GetOrgUserWithFilters(orgId primitive.ObjectID, req *dto.GetOrgUserReq, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.OrgUserResp], error) {
	// 仅当前端上送非空 account（JSON 字段名 account）时走账号精准查询；否则走 keyword 等原有逻辑
	if trimmedAccount := strings.TrimSpace(req.Account); trimmedAccount != "" {
		cpy := *req
		cpy.Account = trimmedAccount
		return w.GetOrgUserByAccount(orgId, &cpy, page)
	}

	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	ctx := context.TODO()

	skip, err := applyLoginIPUserIDFilter(ctx, db, req)
	if err != nil {
		return nil, err
	}
	if skip {
		return &paginationUtils.ListResp[*dto.OrgUserResp]{Total: 0, List: []*dto.OrgUserResp{}}, nil
	}
	// 创建DAO实例
	forbiddenAccountDao := chatModel.NewForbiddenAccountDao(db)
	superAdminForbiddenDao := adminModel.NewSuperAdminForbiddenDao(db)
	userTagDao := model.NewUserTagDao(db)

	// 🚀 协程优化：并行获取禁用用户列表和组织标签
	type initQueryResult struct {
		notInImUserIds    []string
		forbiddenUserIDs  []string
		tags              []*model.UserTag
		forbiddenErr      error
		superForbiddenErr error
		tagsErr           error
	}

	initResult := &initQueryResult{}
	var initWg sync.WaitGroup

	// 协程1：获取禁用账户列表
	initWg.Add(1)
	go func() {
		defer initWg.Done()
		userIds, err := forbiddenAccountDao.FindAllIDs(ctx)
		initResult.notInImUserIds = userIds
		initResult.forbiddenErr = err
	}()

	// 协程2：获取超级管理员禁用用户列表
	initWg.Add(1)
	go func() {
		defer initWg.Done()
		userIds, err := superAdminForbiddenDao.GetAllForbiddenUserIDs(ctx)
		initResult.forbiddenUserIDs = userIds
		initResult.superForbiddenErr = err
	}()

	// 协程3：获取组织标签信息
	initWg.Add(1)
	go func() {
		defer initWg.Done()
		tags, err := userTagDao.GetByOrgId(ctx, orgId)
		initResult.tags = tags
		initResult.tagsErr = err
	}()

	// 等待所有协程完成
	initWg.Wait()

	// 检查错误
	if initResult.forbiddenErr != nil {
		return nil, fmt.Errorf("failed to get forbidden account IDs: %w", initResult.forbiddenErr)
	}
	if initResult.superForbiddenErr != nil {
		return nil, fmt.Errorf("failed to get super admin forbidden user IDs: %w", initResult.superForbiddenErr)
	}
	if initResult.tagsErr != nil {
		return nil, fmt.Errorf("failed to get organization tags: %w", initResult.tagsErr)
	}

	// 处理标签映射
	tagMap := make(map[string]*dto.UserTagResp, len(initResult.tags))
	for _, tag := range initResult.tags {
		tagMap[tag.ID.Hex()] = dto.NewUserTagResp(tag)
	}

	notInImUserIds := initResult.notInImUserIds
	forbiddenUserIDs := initResult.forbiddenUserIDs

	// 解析时间参数
	startTime := req.ParseStartTime()
	endTime := req.ParseEndTime()

	// 查询组织用户数据（按最近登录 IP / IP 所属地排序时先取全量，服务层排序后再分页）
	orgUserDao := model.NewOrganizationUserDao(db)
	queryPage := page
	needFullDataLoginSort := req.OrderKey == "last_login_record_ip_region" || req.OrderKey == "last_login_record_ip"
	if needFullDataLoginSort {
		queryPage = nil
	}
	total, orgUsers, err := orgUserDao.SelectJoinUserWithTags(
		ctx, orgId, req.Keyword, req.UserIds,
		notInImUserIds, forbiddenUserIDs,
		req.Roles, req.Status, req.TagIds, req.CanSendFreeMsg,
		startTime, endTime, queryPage,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to select organization users: %w", err)
	}

	// 批量查询优化：收集所有需要的ID，一次性查询相关数据
	if len(orgUsers) == 0 {
		return &paginationUtils.ListResp[*dto.OrgUserResp]{
			Total: total,
			List:  []*dto.OrgUserResp{},
		}, nil
	}

	// 最近登录 IP/设备/平台：直读 Mongo user_login_record（各 user_id 最新一条），与列表展示一致；排序同样依赖该数据
	allUserIDs := make([]string, len(orgUsers))
	for i, orgUser := range orgUsers {
		allUserIDs[i] = orgUser.UserId
	}
	loginDao := chatModel.NewUserLoginRecordDao(db)
	loginRowHelper := chatCache.NewLoginRecordCacheRedis(plugin.RedisCli(), db)
	dbLoginRows, err := loginDao.FindLatestByUserIDs(ctx, allUserIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to find login records for %d users: %w", len(allUserIDs), err)
	}
	cacheRecords, err := loginRowHelper.FromDBRecords(ctx, dbLoginRows)
	if err != nil {
		return nil, fmt.Errorf("failed to materialize login records for %d users: %w", len(allUserIDs), err)
	}
	loginRecordMap := make(map[string]*chatCache.UserLoginRecordCache, len(cacheRecords))
	for _, record := range cacheRecords {
		if record != nil {
			loginRecordMap[record.UserID] = record
		}
	}

	// 全量按最近登录 IP 或 IP 所属地排序后再分页，保证分页与排序一致
	if needFullDataLoginSort {
		desc := strings.ToLower(req.OrderDirection) == "desc"
		switch req.OrderKey {
		case "last_login_record_ip_region":
			sort.SliceStable(orgUsers, func(i, j int) bool {
				left := ""
				if lr, ok := loginRecordMap[orgUsers[i].UserId]; ok && lr != nil {
					left = strings.TrimSpace(lr.Region)
				}
				right := ""
				if lr, ok := loginRecordMap[orgUsers[j].UserId]; ok && lr != nil {
					right = strings.TrimSpace(lr.Region)
				}
				if desc {
					return left > right
				}
				return left < right
			})
		case "last_login_record_ip":
			sort.SliceStable(orgUsers, func(i, j int) bool {
				left := ""
				if lr, ok := loginRecordMap[orgUsers[i].UserId]; ok && lr != nil {
					left = strings.TrimSpace(lr.IP)
				}
				right := ""
				if lr, ok := loginRecordMap[orgUsers[j].UserId]; ok && lr != nil {
					right = strings.TrimSpace(lr.IP)
				}
				if desc {
					return left > right
				}
				return left < right
			})
		}
		if page != nil && page.PageSize > 0 {
			start := int((page.Page - 1) * page.PageSize)
			if start >= len(orgUsers) {
				orgUsers = []*model.OrganizationUserWithUser{}
			} else {
				end := start + int(page.PageSize)
				if end > len(orgUsers) {
					end = len(orgUsers)
				}
				orgUsers = orgUsers[start:end]
			}
		}
	}

	// 收集当前页用户ID用于批量查询
	userIds := make([]string, len(orgUsers))
	imServerUserIds := make([]string, len(orgUsers))
	for i, orgUser := range orgUsers {
		userIds[i] = orgUser.UserId
		imServerUserIds[i] = orgUser.ImServerUserId
	}

	// 🚀 协程优化：并行执行多个批量查询，减少总查询时间
	type batchQueryResult struct {
		users      []*openImModel.User
		attributes []*chatModel.Attribute
		registers  []*chatModel.Register
		userErr    error
		attrErr    error
		regErr     error
		loginErr   error
	}

	result := &batchQueryResult{}
	var wg sync.WaitGroup

	// 协程1：批量查询 OpenIM user 表（昵称/头像/非好友发消息等均以该表为准，与 DAO 是否已 $lookup 无关）
	wg.Add(1)
	go func() {
		defer wg.Done()
		userDao := openImModel.NewUserDao(db)
		users, err := userDao.FindByUserIDs(ctx, imServerUserIds)
		result.users = users
		result.userErr = err
	}()

	// 协程2：批量查询 Attribute 表（账号 account 等以库表为准，不经属性 Redis 缓存）
	wg.Add(1)
	go func() {
		defer wg.Done()
		attributeDao := chatModel.NewAttributeDao(db)
		attrs, err := attributeDao.Find(ctx, userIds)
		result.attributes = attrs
		result.attrErr = err
	}()

	// 协程3：批量查询 registers 表（注册 IP）
	wg.Add(1)
	go func() {
		defer wg.Done()
		registerDao := chatModel.NewRegisterDao(db)
		regs, err := registerDao.FindByUserIDs(ctx, userIds)
		result.registers = regs
		result.regErr = err
	}()

	// 等待所有协程完成
	wg.Wait()

	// 检查错误
	if result.userErr != nil {
		return nil, fmt.Errorf("failed to find users for %d imServerUserIds: %w", len(imServerUserIds), result.userErr)
	}
	if result.attrErr != nil {
		return nil, fmt.Errorf("failed to find attributes for %d users: %w", len(userIds), result.attrErr)
	}
	if result.regErr != nil {
		return nil, fmt.Errorf("failed to find registers for %d users: %w", len(userIds), result.regErr)
	}
	if result.loginErr != nil {
		return nil, fmt.Errorf("failed to find login records for %d users: %w", len(userIds), result.loginErr)
	}

	// 构建映射关系（并行查询完成后串行处理，避免竞态条件）
	userMap := make(map[string]*openImModel.User, len(result.users))
	for _, user := range result.users {
		userMap[user.UserID] = user
	}
	for _, orgUser := range orgUsers {
		if u := userMap[orgUser.ImServerUserId]; u != nil {
			orgUser.User = u
		}
	}

	attributeMap := make(map[string]*chatModel.Attribute, len(result.attributes))
	for _, attr := range result.attributes {
		attributeMap[attr.UserID] = attr
	}

	registerIPByUserID := make(map[string]string, len(result.registers))
	for _, reg := range result.registers {
		if reg != nil && reg.UserID != "" {
			registerIPByUserID[reg.UserID] = reg.IP
		}
	}

	// 批量转换：使用精确容量，避免append开销
	orgUserResps := make([]*dto.OrgUserResp, len(orgUsers))
	for i, orgUser := range orgUsers {
		var loginRecord *chatCache.UserLoginRecordCache
		if lr, exists := loginRecordMap[orgUser.UserId]; exists && lr != nil {
			loginRecord = lr
		}

		orgUserResp, err := dto.NewOrgUserRespWithBatchData(orgUser, tagMap, attributeMap, loginRecord, registerIPByUserID[orgUser.UserId])
		if err != nil {
			return nil, fmt.Errorf("failed to convert organization user %s: %w", orgUser.UserId, err)
		}
		orgUserResps[i] = orgUserResp
	}

	// 钱包/补偿金：omit_wallet 时跳过，由前端 wallet_snapshot 按需合并
	if !req.OmitWallet && len(orgUserResps) > 0 {
		var walletWg sync.WaitGroup
		for _, orgUserResp := range orgUserResps {
			walletWg.Add(1)
			go func(userResp *dto.OrgUserResp) {
				defer walletWg.Done()
				fillOrgUserWalletFields(ctx, db, userResp)
			}(orgUserResp)
		}

		// 等待所有钱包查询完成
		walletWg.Wait()
	}

	return &paginationUtils.ListResp[*dto.OrgUserResp]{
		Total: total,
		List:  orgUserResps,
	}, nil
}

// BatchOrgUserWalletSnapshot 仅返回本组织内用户的钱包/补偿金（供列表 omit_wallet 后合并）
func (w *OrganizationUserSvc) BatchOrgUserWalletSnapshot(orgId primitive.ObjectID, req *dto.OrgUserWalletSnapshotReq) (*dto.OrgUserWalletSnapshotResp, error) {
	if len(req.UserIDs) == 0 {
		return &dto.OrgUserWalletSnapshotResp{List: []*dto.OrgUserWalletSnapshotItem{}}, nil
	}
	seen := make(map[string]struct{})
	var ids []string
	for _, id := range req.UserIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
		if len(ids) >= 200 {
			break
		}
	}
	if len(ids) == 0 {
		return &dto.OrgUserWalletSnapshotResp{List: []*dto.OrgUserWalletSnapshotItem{}}, nil
	}

	db := plugin.MongoCli().GetDB()
	ctx := context.TODO()
	orgUserDao := model.NewOrganizationUserDao(db)
	orgUsers, err := orgUserDao.ListByOrgIdAndUserIDs(ctx, orgId, ids)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(orgUsers))
	for _, ou := range orgUsers {
		allowed[ou.UserId] = struct{}{}
	}

	out := make([]*dto.OrgUserWalletSnapshotItem, 0, len(allowed))
	for _, uid := range ids {
		if _, ok := allowed[uid]; !ok {
			continue
		}
		tmp := &dto.OrgUserResp{UserId: uid}
		fillOrgUserWalletFields(ctx, db, tmp)
		out = append(out, &dto.OrgUserWalletSnapshotItem{
			UserID:              uid,
			WalletBalances:      tmp.WalletBalances,
			CompensationBalance: tmp.CompensationBalance,
		})
	}
	return &dto.OrgUserWalletSnapshotResp{List: out}, nil
}

// 用户标签管理相关服务方法

// CreateUserTag 创建标签
func (w *OrganizationUserSvc) CreateUserTag(ctx context.Context, orgId primitive.ObjectID, req *dto.CreateUserTagReq) (*dto.UserTagResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	userTagDao := model.NewUserTagDao(db)

	// 检查标签名是否已存在
	exist, err := userTagDao.ExistByTagNameAndOrgId(ctx, req.TagName, orgId)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	if exist {
		return nil, errs.NewCodeError(freeErrors.ErrInvalidParams, fmt.Sprintf("Tag name '%s' already exists", req.TagName))
	}

	// 创建标签
	tag := &model.UserTag{
		OrganizationId: orgId,
		TagName:        req.TagName,
		Description:    req.Description,
	}

	if err := userTagDao.Create(ctx, tag); err != nil {
		return nil, errs.Wrap(err)
	}

	// 查询创建的标签并返回
	createdTag, err := userTagDao.GetByTagNameAndOrgId(ctx, req.TagName, orgId)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	return dto.NewUserTagResp(createdTag), nil
}

// UpdateUserTag 更新标签
func (w *OrganizationUserSvc) UpdateUserTag(ctx context.Context, orgId primitive.ObjectID, req *dto.UpdateUserTagReq) (*dto.UserTagResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	userTagDao := model.NewUserTagDao(db)

	// 检查标签是否存在
	existingTag, err := userTagDao.GetById(ctx, req.TagId)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, errs.NewCodeError(freeErrors.ErrInvalidParams, "Tag not found")
		}
		return nil, errs.Wrap(err)
	}

	// 检查标签是否属于当前组织
	if existingTag.OrganizationId != orgId {
		return nil, errs.NewCodeError(freeErrors.ErrInvalidParams, "No permission to operate this tag")
	}

	// 如果标签名有变化，检查新名称是否已存在
	if existingTag.TagName != req.TagName {
		exist, err := userTagDao.ExistByTagNameAndOrgId(ctx, req.TagName, orgId)
		if err != nil {
			return nil, errs.Wrap(err)
		}
		if exist {
			return nil, errs.NewCodeError(freeErrors.ErrInvalidParams, fmt.Sprintf("Tag name '%s' already exists", req.TagName))
		}
	}

	// 更新标签
	if err := userTagDao.UpdateById(ctx, req.TagId, req.TagName, req.Description); err != nil {
		return nil, errs.Wrap(err)
	}

	// 查询更新后的标签并返回
	updatedTag, err := userTagDao.GetById(ctx, req.TagId)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	return dto.NewUserTagResp(updatedTag), nil
}

// GetUserTagList 获取标签列表
func (w *OrganizationUserSvc) GetUserTagList(ctx context.Context, orgId primitive.ObjectID, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.UserTagResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	userTagDao := model.NewUserTagDao(db)

	total, tags, err := userTagDao.Select(ctx, orgId, page)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	// 转换为响应格式
	tagResps := make([]*dto.UserTagResp, 0, len(tags))
	for _, tag := range tags {
		tagResps = append(tagResps, dto.NewUserTagResp(tag))
	}

	return &paginationUtils.ListResp[*dto.UserTagResp]{
		Total: total,
		List:  tagResps,
	}, nil
}

// AssignUserTags 给用户打标签（全量更新）
func (w *OrganizationUserSvc) AssignUserTags(ctx context.Context, orgId primitive.ObjectID, req *dto.AssignUserTagsReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	userTagDao := model.NewUserTagDao(db)
	orgUserDao := model.NewOrganizationUserDao(db)

	// 检查用户是否存在于当前组织
	orgUser, err := orgUserDao.GetByUserIMServerUserId(ctx, req.ImUserSeverID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return errs.NewCodeError(freeErrors.ErrInvalidParams, "User not found in current organization")
		}
		return errs.Wrap(err)
	}

	// 检查用户orgid是否是当前组织
	if orgUser.OrganizationId != orgId {
		return errs.NewCodeError(freeErrors.ErrInvalidParams, "User not found in current organization")
	}

	// 验证所有标签都属于当前组织
	if len(req.TagIds) > 0 {
		validTags, err := userTagDao.GetByIdsAndOrgId(ctx, req.TagIds, orgId)
		if err != nil {
			return errs.Wrap(err)
		}
		if len(validTags) != len(req.TagIds) {
			return errs.NewCodeError(freeErrors.ErrInvalidParams, "Invalid tag ID exists")
		}
	}

	// 全量更新：直接用传入的标签替换用户现有的所有标签
	return orgUserDao.UpdateUserTags(ctx, req.ImUserSeverID, orgId, req.TagIds)
}

type StatisticsRegisterCountReq struct {
	Pagination *paginationUtils.DepPagination `json:"pagination"`
}

type OrgRolePermissionSvc struct{}

func NewOrgRolePermissionService() *OrgRolePermissionSvc {
	return &OrgRolePermissionSvc{}
}

func (w *OrgRolePermissionSvc) DetailOrgRolePermission(orgId primitive.ObjectID, role model.OrganizationUserRole) ([]*dto.OrgRolePermissionResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	orgRolePermissionDao := model.NewOrganizationRolePermissionDao(db)

	permissions, err := orgRolePermissionDao.GetByOrgIdAndRole(context.TODO(), orgId, role)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	resp := make([]*dto.OrgRolePermissionResp, 0)
	for _, per := range permissions {
		resp = append(resp, dto.NewOrgRolePermissionResp(context.TODO(), per))
	}

	return resp, nil
}

type UpdateOrgRolePermissionReq struct {
	Role            model.OrganizationUserRole `json:"role" binding:"required"`
	OrgId           primitive.ObjectID         `json:"org_id"`
	PermissionsCode []model.PermissionCode     `json:"permissions_code"`
}

func (w *OrgRolePermissionSvc) UpdateOrgRolePermission(ctx context.Context, req UpdateOrgRolePermissionReq, operationID string) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	orgRolePermissionDao := model.NewOrganizationRolePermissionDao(db)

	oldPermissions, err := orgRolePermissionDao.GetByOrgIdAndRole(ctx, req.OrgId, req.Role)
	if err != nil {
		return err
	}

	for _, code := range req.PermissionsCode {
		if !model.IsValidPermissionCode(code) {
			return fmt.Errorf("invalid permission_code: %s", code)
		}
	}

	roleCanSendFreeMsg := int32(0)
	if slices.Contains(req.PermissionsCode, model.PermissionCodeFreePrivateChat) {
		roleCanSendFreeMsg = 1
	}
	oldRoleCanSendFreeMsg := int32(0)
	for _, permission := range oldPermissions {
		if permission != nil && permission.PermissionCode == model.PermissionCodeFreePrivateChat {
			oldRoleCanSendFreeMsg = 1
			break
		}
	}
	restoreCodes := make([]model.PermissionCode, 0, len(oldPermissions))
	for _, permission := range oldPermissions {
		if permission == nil {
			continue
		}
		restoreCodes = append(restoreCodes, permission.PermissionCode)
	}

	writeRolePermissions := func(sessionCtx context.Context, codes []model.PermissionCode) error {
		err := orgRolePermissionDao.DeleteByOrgIdAndRole(sessionCtx, req.OrgId, req.Role)
		if err != nil {
			return err
		}

		for _, code := range codes {
			err = orgRolePermissionDao.Create(sessionCtx, &model.OrganizationRolePermission{
				OrgId:          req.OrgId,
				Role:           req.Role,
				PermissionCode: code,
			})
			if err != nil {
				return err
			}
		}

		return nil
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		return writeRolePermissions(sessionCtx, req.PermissionsCode)
	})
	if err != nil {
		return err
	}

	// 直接查询该组织下具有指定角色的所有用户的imServerUserId
	orgUserDao := model.NewOrganizationUserDao(db)
	imServerUserIDs, err := orgUserDao.GetIMServerUserIdsByOrgIdAndRole(ctx, req.OrgId, req.Role)
	if err != nil {
		return err
	}
	if len(imServerUserIDs) > 0 {
		imApiCaller := plugin.ImApiCaller()
		apiCtx := context.WithValue(context.Background(), constantpb.OperationID, operationID)
		imToken, tokenErr := imApiCaller.ImAdminTokenWithDefaultAdmin(apiCtx)
		if tokenErr != nil {
			restoreErr := mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
				return writeRolePermissions(sessionCtx, restoreCodes)
			})
			if restoreErr != nil {
				log.ZError(ctx, "角色权限同步失败且回滚数据库失败", restoreErr, "org_id", req.OrgId.Hex(), "role", req.Role)
			}
			return tokenErr
		} else {
			apiCtx = mctx.WithApiToken(apiCtx, imToken)
			syncedUserIDs := make([]string, 0, len(imServerUserIDs))
			for _, imServerUserID := range imServerUserIDs {
				if imServerUserID == "" {
					continue
				}
				if syncErr := imApiCaller.UpdateUserCanSendFreeMsg(apiCtx, imServerUserID, roleCanSendFreeMsg); syncErr != nil {
					restoreErr := mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
						return writeRolePermissions(sessionCtx, restoreCodes)
					})
					if restoreErr != nil {
						log.ZError(ctx, "同步角色私聊权限失败且回滚数据库失败", restoreErr, "org_id", req.OrgId.Hex(), "role", req.Role)
					}
					for _, syncedUserID := range syncedUserIDs {
						if rollbackSyncErr := imApiCaller.UpdateUserCanSendFreeMsg(apiCtx, syncedUserID, oldRoleCanSendFreeMsg); rollbackSyncErr != nil {
							log.ZError(ctx, "同步角色私聊权限失败且回滚IM状态失败", rollbackSyncErr, "org_id", req.OrgId.Hex(), "role", req.Role, "im_server_user_id", syncedUserID)
						}
					}
					return syncErr
				}
				syncedUserIDs = append(syncedUserIDs, imServerUserID)
			}
		}

		// 使用协程异步批量通知用户权限变更(如果配置启用)
		if plugin.ChatCfg().Share.EnablePermissionNotifications {
			go func() {
				if err := notifyUserPermissionChangeBatch(context.Background(), operationID, imServerUserIDs, req.OrgId); err != nil {
					log.ZError(context.Background(), "批量通知用户权限变更失败", err, "org_id", req.OrgId.Hex(), "role", req.Role)
				}
			}()
		}
	}

	return nil
}

// notifyUserPermissionChange 通知单个用户权限变更
func notifyUserPermissionChange(ctx context.Context, operationID, imServerUserID string, orgId primitive.ObjectID) error {
	// 创建通知服务实例
	notificationService := notificationSvc.NewNotificationService()

	// 获取当前时间
	nowUTC := time.Now().UTC()

	// 构建自定义消息的数据内容
	customData := map[string]interface{}{
		"customType": 10011,
		"content":    "UserRoleChanges",
		"timestamp":  nowUTC.Unix(),
		"viewType":   10011,
	}

	// 序列化自定义数据
	customDataJSON, err := json.Marshal(customData)
	if err != nil {
		log.ZError(ctx, "序列化权限变更通知数据失败", err, "org_id", orgId.Hex(), "user_id", imServerUserID)
		return err
	}

	// 构建符合CustomElem结构的消息内容
	customContent := map[string]any{
		"data":        string(customDataJSON),
		"description": "User role permission change notification",
		"extension":   "",
	}

	// 构建消息数据
	msgData := notificationSvc.SendMsg{
		SendID:         depConstant.NOTIFICATION_ADMIN_SEND_ID,
		RecvID:         imServerUserID,
		SenderNickname: "",
		SenderFaceURL:  "",
		ContentType:    constantpb.Custom,
		SessionType:    constantpb.SingleChatType,
		Content:        customContent,
		SendTime:       nowUTC.UnixMilli(),
		IsOnlineOnly:   true,
	}

	// 发送通知
	if err := notificationService.SendNotification(ctx, msgData, operationID); err != nil {
		log.ZError(ctx, "发送权限变更通知失败", err, "org_id", orgId.Hex(), "user_id", imServerUserID)
		return err
	}
	return nil
}

// NotifyUserPermissionChangeBatch 批量通知用户权限变更
func notifyUserPermissionChangeBatch(ctx context.Context, operationID string, imServerUserIDs []string, orgId primitive.ObjectID) error {
	if len(imServerUserIDs) == 0 {
		log.ZWarn(ctx, "批量通知用户权限变更：用户ID列表为空", nil, "org_id", orgId.Hex())
		return nil
	}

	// 创建通知服务实例
	notificationService := notificationSvc.NewNotificationService()

	// 获取当前时间
	nowUTC := time.Now().UTC()

	// 构建自定义消息的数据内容
	customData := map[string]interface{}{
		"customType": 10011,
		"content":    "RolePermissionChanges",
		"timestamp":  nowUTC.Unix(),
		"viewType":   10012,
	}

	// 序列化自定义数据
	customDataJSON, err := json.Marshal(customData)
	if err != nil {
		log.ZError(ctx, "序列化批量权限变更通知数据失败", err, "org_id", orgId.Hex(), "user_count", len(imServerUserIDs))
		return err
	}

	// 构建符合CustomElem结构的消息内容
	customContent := map[string]any{
		"data":        string(customDataJSON),
		"description": "Role permission change notification",
		"extension":   "",
	}

	// 构建批量消息数据
	batchMsgData := notificationSvc.BatchSendMsg{
		SendID:         depConstant.NOTIFICATION_ADMIN_SEND_ID,
		RecvIDs:        imServerUserIDs,
		SenderNickname: "",
		SenderFaceURL:  "",
		ContentType:    constantpb.Custom,
		SessionType:    constantpb.SingleChatType,
		Content:        customContent,
		SendTime:       nowUTC.UnixMilli(),
		IsOnlineOnly:   true,
	}

	// 批量发送通知
	if err := notificationService.BatchSendNotification(ctx, batchMsgData, operationID); err != nil {
		log.ZError(ctx, "批量发送权限变更通知失败", err, "org_id", orgId.Hex(), "user_count", len(imServerUserIDs))
		return err
	}
	return nil
}

// notifyUserCanSendFreeMsgChange 通知用户 CanSendFreeMsg 状态变更
func notifyUserCanSendFreeMsgChange(ctx context.Context, operationID, imServerUserID string, orgId primitive.ObjectID, newCanSendFreeMsg int32) error {
	// 创建通知服务实例
	notificationService := notificationSvc.NewNotificationService()

	// 获取当前时间
	nowUTC := time.Now().UTC()

	// 构建自定义消息的数据内容
	customData := map[string]interface{}{
		"customType":     10011, // 使用不同的customType标识CanSendFreeMsg变更
		"content":        "CanSendFreeMsgChange",
		"timestamp":      nowUTC.Unix(),
		"viewType":       10013,
		"canSendFreeMsg": newCanSendFreeMsg, // 新增字段，表示新的状态值
	}

	// 序列化自定义数据
	customDataJSON, err := json.Marshal(customData)
	if err != nil {
		log.ZError(ctx, "序列化CanSendFreeMsg变更通知数据失败", err, "org_id", orgId.Hex(), "user_id", imServerUserID)
		return err
	}

	// 构建符合CustomElem结构的消息内容
	customContent := map[string]any{
		"data":        string(customDataJSON),
		"description": "User CanSendFreeMsg status change notification",
		"extension":   "",
	}

	// 构建消息数据
	msgData := notificationSvc.SendMsg{
		SendID:         depConstant.NOTIFICATION_ADMIN_SEND_ID,
		RecvID:         imServerUserID,
		SenderNickname: "",
		SenderFaceURL:  "",
		ContentType:    constantpb.Custom,
		SessionType:    constantpb.SingleChatType,
		Content:        customContent,
		SendTime:       nowUTC.UnixMilli(),
		IsOnlineOnly:   true,
	}

	// 发送通知
	if err := notificationService.SendNotification(ctx, msgData, operationID); err != nil {
		log.ZError(ctx, "发送CanSendFreeMsg变更通知失败", err, "org_id", orgId.Hex(), "user_id", imServerUserID, "new_status", newCanSendFreeMsg)
		return err
	}

	log.ZInfo(ctx, "CanSendFreeMsg变更通知发送成功", "org_id", orgId.Hex(), "user_id", imServerUserID, "new_status", newCanSendFreeMsg)
	return nil
}

// GetOrgUserByAccount 通过 account 精准查询组织用户
//
// 功能描述：
// 1. 通过 account 在 attr 表中精准查询，获取 userId
// 2. 根据 userId 和 organizationId 在 organization_user 表中查询 imServerUserId
// 3. 补充 user 表数据，进行 Roles 过滤
// 4. 检查用户是否被禁用（包括普通禁用和超级管理员禁用）
// 5. 返回完整的用户信息，与现有的 GetOrgUserWithFilters 数据结构保持一致
//
// 查询流程：
// attr.account -> attr.userId -> org_user.imServerUserId -> user + loginRecord + tags
//
// 注意事项：
// - 这是精准查询，不做模糊匹配
// - 如果指定了 Roles 过滤条件且用户不匹配，将返回空结果
// - 被禁用的用户不会返回在结果中
// - 使用并发查询优化性能
func (w *OrganizationUserSvc) GetOrgUserByAccount(orgId primitive.ObjectID, req *dto.GetOrgUserReq, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.OrgUserResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	ctx := context.TODO()

	// 🚀 第1步：通过 account 精准查询获取 userId
	attributeDao := chatModel.NewAttributeDao(db)
	attr, err := attributeDao.TakeAccount(ctx, req.Account)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			// 用户不存在，返回空结果
			return &paginationUtils.ListResp[*dto.OrgUserResp]{
				Total: 0,
				List:  []*dto.OrgUserResp{},
			}, nil
		}
		return nil, fmt.Errorf("failed to query user by account: %w", err)
	}

	// 🚀 第2步：通过 userId 和 orgId 查询组织用户信息
	orgUserDao := model.NewOrganizationUserDao(db)
	orgUser, err := orgUserDao.GetByUserIdAndOrgId(ctx, attr.UserID, orgId)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			// 用户不在该组织中，返回空结果
			return &paginationUtils.ListResp[*dto.OrgUserResp]{
				Total: 0,
				List:  []*dto.OrgUserResp{},
			}, nil
		}
		return nil, fmt.Errorf("failed to query organization user: %w", err)
	}

	// 按最近登录 IP 子串筛选（与列表接口 login_ip 一致）
	if trimmed := strings.TrimSpace(req.LoginIP); trimmed != "" {
		loginDao := chatModel.NewUserLoginRecordDao(db)
		lr, lrErr := loginDao.GetByUserId(ctx, attr.UserID)
		if lrErr != nil || lr == nil || strings.TrimSpace(lr.IP) == "" {
			return &paginationUtils.ListResp[*dto.OrgUserResp]{Total: 0, List: []*dto.OrgUserResp{}}, nil
		}
		if !strings.Contains(strings.ToLower(strings.TrimSpace(lr.IP)), strings.ToLower(trimmed)) {
			return &paginationUtils.ListResp[*dto.OrgUserResp]{Total: 0, List: []*dto.OrgUserResp{}}, nil
		}
	}

	// 🚀 第3步：角色过滤检查（提前过滤，避免后续无效查询）
	if len(req.Roles) > 0 {
		roleMatched := false
		for _, role := range req.Roles {
			if orgUser.Role == role {
				roleMatched = true
				break
			}
		}
		if !roleMatched {
			// 角色不匹配，返回空结果
			return &paginationUtils.ListResp[*dto.OrgUserResp]{
				Total: 0,
				List:  []*dto.OrgUserResp{},
			}, nil
		}
	}

	// 🚀 第4步：并发查询所有需要的辅助数据
	type QueryResult struct {
		forbiddenAccount    *chatModel.ForbiddenAccount
		superAdminForbidden bool
		tags                []*model.UserTag
		user                *openImModel.User
		err                 error
	}

	result := &QueryResult{}
	var wg sync.WaitGroup
	errChan := make(chan error, 5) // 5个并发查询

	// 并发查询1：检查禁用状态
	wg.Add(1)
	go func() {
		defer wg.Done()
		forbiddenAccountDao := chatModel.NewForbiddenAccountDao(db)
		exists, err := forbiddenAccountDao.ExistByUserId(ctx, attr.UserID)
		if err != nil {
			errChan <- fmt.Errorf("failed to check forbidden account: %w", err)
			return
		}
		if exists {
			// 如果用户被禁用，直接设置一个标记
			result.forbiddenAccount = &chatModel.ForbiddenAccount{}
		}
	}()

	// 并发查询2：检查超级管理员禁用状态
	wg.Add(1)
	go func() {
		defer wg.Done()
		superAdminForbiddenDao := adminModel.NewSuperAdminForbiddenDao(db)
		exists, err := superAdminForbiddenDao.ExistByUserID(ctx, attr.UserID)
		if err != nil {
			errChan <- fmt.Errorf("failed to check super admin forbidden: %w", err)
			return
		}
		result.superAdminForbidden = exists
	}()

	// 并发查询3：查询组织标签
	wg.Add(1)
	go func() {
		defer wg.Done()
		userTagDao := model.NewUserTagDao(db)
		tags, err := userTagDao.GetByOrgId(ctx, orgId)
		if err != nil {
			errChan <- fmt.Errorf("failed to get organization tags: %w", err)
			return
		}
		result.tags = tags
	}()

	// 并发查询4：查询用户基本信息
	wg.Add(1)
	go func() {
		defer wg.Done()
		userDao := openImModel.NewUserDao(db)
		user, err := userDao.Take(ctx, orgUser.ImServerUserId)
		if err != nil {
			errChan <- fmt.Errorf("failed to query user data: %w", err)
			return
		}
		result.user = user
	}()

	// 等待所有并发查询完成
	wg.Wait()
	close(errChan)

	// 检查是否有错误
	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

	// 🚀 第5步：手动组装数据（避免DTO层额外查询）

	// 构建标签映射
	tagMap := make(map[string]*dto.UserTagResp, len(result.tags))
	for _, tag := range result.tags {
		tagMap[tag.ID.Hex()] = dto.NewUserTagResp(tag)
	}

	// 构建attribute映射
	attributeMap := map[string]*chatModel.Attribute{
		attr.UserID: attr,
	}

	// 构建 OrganizationUserWithUser 对象
	orgUserWithUser := &model.OrganizationUserWithUser{
		OrganizationUser: *orgUser,
		User:             result.user,
	}

	loginDaoSingle := chatModel.NewUserLoginRecordDao(db)
	loginMat := chatCache.NewLoginRecordCacheRedis(plugin.RedisCli(), db)
	var loginRecordCacheData *chatCache.UserLoginRecordCache
	if lr, lrErr := loginDaoSingle.GetByUserId(ctx, attr.UserID); lrErr == nil && lr != nil {
		var convErr error
		loginRecordCacheData, convErr = loginMat.FromDBRecord(ctx, lr)
		if convErr != nil {
			return nil, fmt.Errorf("failed to materialize login record: %w", convErr)
		}
	} else if lrErr != nil && !dbutil.IsDBNotFound(lrErr) {
		return nil, fmt.Errorf("failed to query login record: %w", lrErr)
	}

	registerIP := ""
	registerDao := chatModel.NewRegisterDao(db)
	if reg, regErr := registerDao.GetByUserId(ctx, attr.UserID); regErr == nil && reg != nil {
		registerIP = reg.IP
	} else if regErr != nil && !dbutil.IsDBNotFound(regErr) {
		return nil, fmt.Errorf("failed to query register: %w", regErr)
	}

	// 使用批量数据创建响应
	orgUserResp, err := dto.NewOrgUserRespWithBatchData(
		orgUserWithUser,
		tagMap,
		attributeMap,
		loginRecordCacheData,
		registerIP,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create user response: %w", err)
	}

	if !req.OmitWallet {
		fillOrgUserWalletFields(ctx, db, orgUserResp)
	}

	// 🚀 第6步：返回结果
	return &paginationUtils.ListResp[*dto.OrgUserResp]{
		Total: 1,
		List:  []*dto.OrgUserResp{orgUserResp},
	}, nil
}

// CheckUserHasProtection 检查用户是否拥有官方账号保护权限
// 此方法供内部服务（如Free-IM-Server）调用
func (s *OrganizationSvc) CheckUserHasProtection(ctx context.Context, userID string) (bool, error) {
	db := plugin.MongoCli().GetDB()
	orgUserDao := model.NewOrganizationUserDao(db)

	// 1. 尝试通过 user_id 获取用户组织信息
	orgUser, err := orgUserDao.GetByUserId(ctx, userID)
	if err != nil {
		// 如果通过 user_id 找不到，尝试通过 im_server_user_id 查找
		// 这是因为 Flutter 客户端传入的是 OpenIM 的用户ID
		orgUser, err = orgUserDao.GetByImServerUserId(ctx, userID)
		if err != nil {
			// 两种ID都找不到用户，没有保护权限
			return false, nil
		}
	}

	// 2. 查询该角色是否拥有 official_protection 权限
	// 移除硬编码的角色检查，改为完全依赖权限表配置
	// 任何角色（SuperAdmin/BackendAdmin/GroupManager/Normal）都可以通过权限表配置获得保护
	orgRolePermissionDao := model.NewOrganizationRolePermissionDao(db)
	hasPermission, err := orgRolePermissionDao.ExistPermission(
		ctx,
		orgUser.OrganizationId,
		orgUser.Role,
		model.PermissionCodeOfficialProtection,
	)
	if err != nil {
		return false, err
	}

	return hasPermission, nil
}

type UpdateCheckinRuleReq struct {
	CheckinRuleDescription string `json:"checkin_rule_description"`
}

// CmsUpdateCheckinRuleDescription 更新签到规则说明
func (o *OrganizationSvc) CmsUpdateCheckinRuleDescription(ctx context.Context, userId string, org *model.Organization, req *UpdateCheckinRuleReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	orgUserDao := model.NewOrganizationUserDao(db)

	orgUser, err := orgUserDao.GetByUserIdAndOrgId(ctx, userId, org.ID)
	if err != nil {
		return err
	}

	// 权限检查:只有超级管理员和后台管理员可以修改
	allowRole := []model.OrganizationUserRole{
		model.OrganizationUserSuperAdminRole,
		model.OrganizationUserBackendAdminRole,
	}
	if !slices.Contains(allowRole, orgUser.Role) {
		return freeErrors.ApiErr("the account is not an admin or super admin")
	}

	err = mongoCli.GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		orgCollection := db.Collection(model.Organization{}.TableName())
		updateField := bson.M{"$set": bson.M{
			"checkin_rule_description": req.CheckinRuleDescription,
			"updated_at":               time.Now().UTC(),
		}}
		return mongoutil.UpdateOne(sessionCtx, orgCollection, bson.M{"_id": org.ID}, updateField, false)
	})

	// 更新成功后主动删除Redis缓存，确保下次获取时能返回最新数据
	if err == nil {
		// 构建需要删除的缓存键模式
		// 1. 基本缓存键
		baseKeys := []string{
			fmt.Sprintf("C_ORG_ID:%s", org.ID.Hex()),
			fmt.Sprintf("C_ORG_ID_%s:%s", model.OrganizationStatusPass, org.ID.Hex()),
		}

		// 注意: RocksCache的TagAsDeletedBatch2方法不支持通配符模式匹配
		// 所以我们只删除基本缓存键，暂不处理带版本号的缓存键
		// 由于缓存过期时间已缩短为30秒，此影响很小

		// 使用基本缓存键
		allKeys := baseKeys

		// 获取Redis客户端并创建RocksCacheClient
		rdb := plugin.RedisCli()
		rcClient := cacheRedis.NewRocksCacheClient(rdb)

		// 使用批量删除器删除缓存
		deleter := rcClient.GetBatchDeleter()
		delErr := deleter.ExecDelWithKeys(ctx, allKeys)
		if delErr != nil {
			// 只记录错误，不影响主要业务流程
			log.ZWarn(ctx, "删除组织缓存失败", delErr, "org_id", org.ID.Hex())
		} else {
			log.ZInfo(ctx, "成功删除组织缓存", "org_id", org.ID.Hex(), "deleted_keys", allKeys)
		}
	}

	return errs.Unwrap(err)
}
