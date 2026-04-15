package svc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings" // 用于字符串处理，如strings.Contains
	"time"

	"github.com/openimsdk/chat/freechat/utils/paginationUtils"

	"github.com/redis/go-redis/v9"

	organizationModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/apps/transaction/dto"
	"github.com/openimsdk/chat/freechat/apps/transaction/model"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	walletTransactionRecordModel "github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/model"

	"github.com/openimsdk/chat/freechat/utils"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/openimsdk/chat/freechat/plugin"

	"github.com/google/uuid"
	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// stressCtxKey 压测请求 context 键，用于跳过依赖 OpenIM 的群成员校验，便于压测时对领取链路施压
type stressCtxKey struct{}

var stressSkipGroupCheckKey = &stressCtxKey{}

// WithStressSkipGroupCheck 压测入口调用：在 ctx 中标记「跳过群校验」，仅用于 receive_stress 接口
func WithStressSkipGroupCheck(ctx context.Context) context.Context {
	return context.WithValue(ctx, stressSkipGroupCheckKey, true)
}

func isStressSkipGroupCheck(ctx context.Context) bool {
	v, ok := ctx.Value(stressSkipGroupCheckKey).(bool)
	return ok && v
}

// SafeParseDecimal128 安全地将decimal转换为Decimal128，避免0E-6176问题
func SafeParseDecimal128(amount decimal.Decimal) (primitive.Decimal128, error) {
	if amount.IsZero() {
		// 对于零值，直接使用标准零值
		return primitive.ParseDecimal128("0")
	} else {
		// 对于正常值，使用固定格式（9位小数）
		normalizedAmountStr := amount.StringFixed(9)
		return primitive.ParseDecimal128(normalizedAmountStr)
	}
}

type TransactionService struct {
}

func NewTransactionService() *TransactionService {
	return &TransactionService{}
}

// trimAmountZeros 去除金额字符串末尾的0（仅限小数部分）
func trimAmountZeros(amount string) string {
	if amount == "" || amount == "0" {
		return "0"
	}

	// 只有包含小数点的情况才处理
	if strings.Contains(amount, ".") {
		// 去除末尾的0
		amount = strings.TrimRight(amount, "0")
		// 如果末尾是小数点，也去除
		amount = strings.TrimRight(amount, ".")
	}

	// 防止返回空字符串
	if amount == "" {
		return "0"
	}

	return amount
}

// validateAndConvertWalletType 验证并转换钱包类型
func validateAndConvertWalletType(ownerType string) (walletModel.WalletInfoOwnerType, error) {
	if ownerType == "" {
		return walletModel.WalletInfoOwnerTypeOrdinary, nil
	}

	// 验证钱包类型是否有效
	switch ownerType {
	case string(walletModel.WalletInfoOwnerTypeOrdinary),
		string(walletModel.WalletInfoOwnerTypeOrganization):
		return walletModel.WalletInfoOwnerType(ownerType), nil
	default:
		return "", errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}
}

// validateBasicParams 验证基础参数
func (t *TransactionService) validateBasicParams(ctx context.Context, req *dto.CreateTransactionReq) error {
	// 验证发送者ID
	if req.SenderID == "" {
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	// 验证目标ID
	if req.TargetID == "" {
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	// 验证组织ID
	if req.OrgID == "" {
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	// 群组专属红包特殊验证
	if req.TransactionType == model.TransactionTypeGroupExclusive {
		// 验证专属接收者ID
		if req.ExclusiveReceiverID == "" {
			log.ZError(ctx, "群组专属红包缺少专属接收者ID", nil, "sender_id", req.SenderID, "target_id", req.TargetID)
			return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
		}
		// 强制数量为1
		if req.TotalCount != 1 {
			log.ZError(ctx, "群组专属红包数量必须为1", nil, "sender_id", req.SenderID, "total_count", req.TotalCount)
			return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
		}
	}

	// 群组口令红包特殊验证
	if req.TransactionType == model.TransactionTypePasswordPacket {
		// 验证口令不能为空
		if req.Password == "" {
			log.ZError(ctx, "群组口令红包缺少口令", nil, "sender_id", req.SenderID, "target_id", req.TargetID)
			return errs.NewCodeError(freeErrors.ErrInvalidParams, "Password cannot be empty for password red packet")
		}
		// 验证口令长度不超过100个字符
		if len(req.Password) > 100 {
			log.ZError(ctx, "群组口令红包口令长度超过限制", nil, "sender_id", req.SenderID, "password_length", len(req.Password))
			return errs.NewCodeError(freeErrors.ErrInvalidParams, "Password length cannot exceed 50 characters")
		}
	}

	return nil
}

// validateWalletPermission 验证钱包类型和权限
func (t *TransactionService) validateWalletPermission(ctx context.Context, req *dto.CreateTransactionReq) (*organizationModel.OrganizationUser, string, walletModel.WalletInfoOwnerType, error) {
	mongoCli := plugin.MongoCli().GetDB()

	// 验证并转换钱包类型
	walletType, err := validateAndConvertWalletType(req.WalletInfoOwnerType)
	if err != nil {
		log.ZError(ctx, "无效的钱包类型", err, "wallet_type", req.WalletInfoOwnerType)
		return nil, "", "", err
	}

	ownerId := req.SenderID
	orgUserDao := organizationModel.NewOrganizationUserDao(mongoCli)
	orgUser, err := orgUserDao.GetByUserIdAndOrgID(ctx, req.SenderID, req.OrgID)
	if err != nil {
		log.ZError(ctx, "failed to query org by email", err, "ownerId", req.SenderID)
		return nil, "", "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	if walletType == walletModel.WalletInfoOwnerTypeOrganization {
		if req.PayPassword == "" {
			log.ZError(ctx, "组织账户创建交易参数错误，无支付密码", err, "ownerId", ownerId)
			return nil, "", "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
		}
		if orgUser.Role != organizationModel.OrganizationUserSuperAdminRole {
			return nil, "", "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
		}
		//获取组织账户ID
		ownerId = orgUser.OrganizationId.Hex()
	}

	return orgUser, ownerId, walletType, nil
}

// validatePayPassword 验证支付密码
func (t *TransactionService) validatePayPassword(ctx context.Context, req *dto.CreateTransactionReq, ownerId string, walletType walletModel.WalletInfoOwnerType) (*walletModel.WalletInfo, error) {
	mongoCli := plugin.MongoCli().GetDB()
	walletInfoDao := walletModel.NewWalletInfoDao(mongoCli)

	// 检查钱包是否存在
	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, ownerId, walletType)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, errs.NewCodeError(freeErrors.WalletNotOpenCode, freeErrors.ErrorMessages[freeErrors.WalletNotOpenCode])
		}
		log.ZError(ctx, "获取钱包信息失败", err, "user_id", req.SenderID)
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 验证支付密码，除非是生物识别认证
	if req.PayPassword != "" {
		if !utils.CheckPassword(req.PayPassword, walletInfo.PayPwd) {
			log.ZError(ctx, "支付密码错误", err, "user_id", req.SenderID)
			return nil, errs.NewCodeError(freeErrors.UserPwdErrCode, freeErrors.ErrorMessages[freeErrors.UserPwdErrCode])
		}
	} else {
		// 生物识别认证已在控制器中验证，此处记录日志
		log.ZInfo(ctx, "生物识别认证通过，跳过密码验证", "user_id", req.SenderID)
	}

	return walletInfo, nil
}

// validateAmountAndCount 验证金额和数量
func (t *TransactionService) validateAmountAndCount(ctx context.Context, req *dto.CreateTransactionReq) (decimal.Decimal, primitive.Decimal128, *walletModel.WalletCurrency, error) {
	mongoCli := plugin.MongoCli().GetDB()

	// 验证金额
	amount, err := decimal.NewFromString(req.TotalAmount)
	if err != nil {
		log.ZError(ctx, "金额格式无效", err, "amount", req.TotalAmount)
		return decimal.Zero, primitive.Decimal128{}, nil, errs.NewCodeError(freeErrors.ErrInvalidAmount, freeErrors.ErrorMessages[freeErrors.ErrInvalidAmount])
	}

	//最小可用金额  根据币种详情来
	walletCurrencyDao := walletModel.NewWalletCurrencyDao(mongoCli)
	walletCurrency, err := walletCurrencyDao.GetById(ctx, req.CurrencyId)
	if err != nil {
		log.ZError(ctx, "获取币种精度失败", err, "currency_id", req.CurrencyId)
		return decimal.Zero, primitive.Decimal128{}, nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 根据精度和红包个数计算最小红包总金额
	precision := walletCurrency.Decimals
	// 计算最小单位金额：1 * 10^(-precision)  例如精度2 -> 0.01, 精度6 -> 0.000001
	one := decimal.NewFromInt(1)
	divisor := decimal.NewFromInt(10).Pow(decimal.NewFromInt(int64(precision)))
	minUnitAmount := one.Div(divisor)
	// 交易总金额 > 红包最小总金额 = 红包个数 * 最小单位金额
	minRedAmount := minUnitAmount.Mul(decimal.NewFromInt(int64(req.TotalCount)))
	// 交易总金额 > 最小可用
	minAvailableAmount, err := decimal.NewFromString(walletCurrency.MinAvailableAmount.String())
	if err != nil {
		log.ZError(ctx, "NewFromString", err)
		return decimal.Zero, primitive.Decimal128{}, nil, errs.NewCodeError(freeErrors.ErrInvalidAmount, freeErrors.ErrorMessages[freeErrors.ErrInvalidAmount])
	}

	minAmount := minAvailableAmount
	if amount.LessThan(minRedAmount) || amount.LessThan(minAmount) {
		log.ZError(ctx, "金额小于最小值", nil, "amount", amount.String(), "min_amount", minAmount.String())
		return decimal.Zero, primitive.Decimal128{}, nil, errs.NewCodeError(freeErrors.ErrInvalidAmount, freeErrors.ErrorMessages[freeErrors.ErrInvalidAmount])
	}

	// 如果是普通红包，验证金额能被数量整除
	if req.TransactionType == model.TransactionTypeNormalPacket {
		// 计算每个红包的金额
		countDecimal := decimal.NewFromInt(int64(req.TotalCount))
		perAmount := amount.Div(countDecimal)

		// 检查是否能整除：将结果乘以数量，看是否等于原金额
		reconstructedAmount := perAmount.Mul(countDecimal)
		if !amount.Equal(reconstructedAmount) {
			log.ZWarn(ctx, "普通红包金额不能被数量整除", nil,
				"total_amount", amount.String(),
				"total_count", req.TotalCount,
				"per_amount", perAmount.String(),
				"reconstructed_amount", reconstructedAmount.String())
			return decimal.Zero, primitive.Decimal128{}, nil, errs.NewCodeError(freeErrors.ErrRedPacketAmountNotDivisible, freeErrors.ErrorMessages[freeErrors.ErrRedPacketAmountNotDivisible])
		}
	}

	// 转换为Decimal128，使用安全转换函数避免0E-6176问题
	amountDecimal128, err := SafeParseDecimal128(amount)
	if err != nil {
		log.ZError(ctx, "金额转换为Decimal128失败", err, "amount", amount.String())
		return decimal.Zero, primitive.Decimal128{}, nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 验证数量
	if req.TotalCount < constant.MinRedPacketCount || req.TotalCount > constant.MaxRedPacketCount {
		return decimal.Zero, primitive.Decimal128{}, nil, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}
	// 拼手气/口令红包单包个数上限，避免高并发下单包压力过大（与 constant.MaxLuckyRedPacketCount 保持一致，便于生产未合入 constant 时仍可编译）
	const maxLuckyRedPacketCount = 500
	if (req.TransactionType == model.TransactionTypeLuckyPacket || req.TransactionType == model.TransactionTypePasswordPacket) &&
		req.TotalCount > maxLuckyRedPacketCount {
		return decimal.Zero, primitive.Decimal128{}, nil, errs.NewCodeError(freeErrors.ErrInvalidParams, "拼手气/口令红包单包个数不能超过 "+strconv.Itoa(maxLuckyRedPacketCount))
	}

	return amount, amountDecimal128, walletCurrency, nil
}

// validateUserRelations 验证用户关系（根据交易类型）
func (t *TransactionService) validateUserRelations(ctx context.Context, req *dto.CreateTransactionReq, senderImID string) error {
	verify := NewVerifyService()

	// 根据交易类型进行不同的验证
	if req.TransactionType == model.TransactionTypeNormalPacket || req.TransactionType == model.TransactionTypeLuckyPacket || req.TransactionType == model.TransactionTypePasswordPacket {
		// 对于群红包，验证群组成员是否都在同一个组织中
		if err := verify.CheckGroupOrganizationRelation(ctx, senderImID, req.TargetID); err != nil {
			return err
		}
		// 验证用户在群中和红包数量合法性
		_, err := verify.VerifyGroupRedPacket(ctx, senderImID, req.TargetID, req.TotalCount)
		if err != nil {
			return err
		}
	} else if req.TransactionType == model.TransactionTypeTransfer || req.TransactionType == model.TransactionTypeP2PRedPacket {
		// 禁止自己给自己转账或发红包
		if senderImID == req.TargetID {
			log.ZWarn(ctx, "不允许自己给自己转账或发红包", nil, "sender_im_id", senderImID, "target_id", req.TargetID)
			return errs.NewCodeError(freeErrors.ErrInvalidParams, "Cannot transfer or send red packet to yourself")
		}
		// 对于转账和一对一红包，验证用户是否在同一个组织中
		if err := verify.CheckOrganizationRelation(ctx, senderImID, req.TargetID); err != nil {
			return err
		}
		// 验证好友关系  更新非好友间可以发送红包
		//if err := verify.CheckFriendRelation(ctx, senderImID, req.TargetID); err != nil {
		//	return err
		//}
	} else if req.TransactionType == model.TransactionTypeOrganization {
		// 验证用户与接受方是否在同一个组织内部
		if err := verify.CheckOrganizationRelation(ctx, senderImID, req.TargetID); err != nil {
			return err
		}
	} else if req.TransactionType == model.TransactionTypeGroupExclusive {
		// 禁止自己给自己发专属红包
		if senderImID == req.ExclusiveReceiverID {
			log.ZWarn(ctx, "不允许自己给自己发专属红包", nil, "sender_im_id", senderImID, "exclusive_receiver_id", req.ExclusiveReceiverID)
			return errs.NewCodeError(freeErrors.ErrInvalidParams, "Cannot send exclusive red packet to yourself")
		}
		// 群组专属红包验证
		// 1. 验证群组关系和组织关系
		if err := verify.CheckGroupOrganizationRelation(ctx, senderImID, req.TargetID); err != nil {
			return err
		}
		// 2. 验证发送者是群成员
		if err := verify.CheckGroupMembership(ctx, senderImID, req.TargetID); err != nil {
			log.ZError(ctx, "发送者不在目标群组中", err, "sender_im_id", senderImID, "target_group", req.TargetID)
			return err
		}
		// 3. 验证专属接收者是群成员（exclusive_receiver_id 就是 IM ID）
		if err := verify.CheckGroupMembership(ctx, req.ExclusiveReceiverID, req.TargetID); err != nil {
			log.ZError(ctx, "专属接收者不在目标群组中", err, "exclusive_receiver_im_id", req.ExclusiveReceiverID, "target_group", req.TargetID)
			return errs.NewCodeError(freeErrors.ErrInvalidParams, "Exclusive receiver is not a member of the target group")
		}
		// 4. 验证发送者和专属接收者在同一组织
		if err := verify.CheckOrganizationRelation(ctx, senderImID, req.ExclusiveReceiverID); err != nil {
			return err
		}
	} else {
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	return nil
}

// validateTransactionPermission 验证交易权限（红包和转账）
func (t *TransactionService) validateTransactionPermission(ctx context.Context, req *dto.CreateTransactionReq, orgUser *organizationModel.OrganizationUser) error {
	mongoCli := plugin.MongoCli().GetDB()
	orgRolePermissionDao := organizationModel.NewOrganizationRolePermissionDao(mongoCli)

	orgId, err := primitive.ObjectIDFromHex(req.OrgID)
	if err != nil {
		log.ZError(ctx, "组织ID格式错误", err, "org_id", req.OrgID)
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	// 根据交易类型检查对应权限
	switch req.TransactionType {
	case model.TransactionTypeTransfer:
		// 普通转账需要 transfer 权限
		hasPermission, err := orgRolePermissionDao.ExistPermission(ctx, orgId, orgUser.Role, organizationModel.PermissionCodeTransfer)
		if err != nil {
			log.ZError(ctx, "检查转账权限失败", err, "org_id", req.OrgID, "role", orgUser.Role)
			return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
		}
		if !hasPermission {
			log.ZWarn(ctx, "用户无转账权限", nil, "sender_id", req.SenderID, "role", orgUser.Role)
			return errs.NewCodeError(freeErrors.ErrForbidden, "您没有转账权限，请联系管理员")
		}

	case model.TransactionTypeP2PRedPacket,
		model.TransactionTypeNormalPacket,
		model.TransactionTypeLuckyPacket,
		model.TransactionTypeGroupExclusive,
		model.TransactionTypePasswordPacket:
		// 所有红包类型需要 send_red_packet 权限
		hasPermission, err := orgRolePermissionDao.ExistPermission(ctx, orgId, orgUser.Role, organizationModel.PermissionCodeSendRedPacket)
		if err != nil {
			log.ZError(ctx, "检查红包权限失败", err, "org_id", req.OrgID, "role", orgUser.Role)
			return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
		}
		if !hasPermission {
			log.ZWarn(ctx, "用户无发送红包权限", nil, "sender_id", req.SenderID, "role", orgUser.Role, "transaction_type", req.TransactionType)
			return errs.NewCodeError(freeErrors.ErrForbidden, "您没有发送红包权限，请联系管理员")
		}

	case model.TransactionTypeOrganization,
		model.TransactionTypeOrganizationSignInReward:
		// 组织账户转账和签到奖励不需要权限检查（由系统/管理员控制）
		log.ZDebug(ctx, "组织系统交易，跳过权限检查", "transaction_type", req.TransactionType)

	default:
		log.ZError(ctx, "未知的交易类型", nil, "transaction_type", req.TransactionType)
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	return nil
}

// TransactionProcessor 交易处理器接口
type TransactionProcessor interface {
	Process(ctx context.Context, sessCtx mongo.SessionContext, req *dto.CreateTransactionReq,
		transaction *model.Transaction, amount decimal.Decimal, walletId primitive.ObjectID) error
}

// RedPacketProcessor 红包处理器
type RedPacketProcessor struct {
	walletCurrencyDao *walletModel.WalletCurrencyDao
	walletDao         *walletModel.WalletBalanceDao
}

func NewRedPacketProcessor(mongoCli *mongo.Database) *RedPacketProcessor {
	return &RedPacketProcessor{
		walletCurrencyDao: walletModel.NewWalletCurrencyDao(mongoCli),
		walletDao:         walletModel.NewWalletBalanceDao(mongoCli),
	}
}

func (p *RedPacketProcessor) Process(ctx context.Context, sessCtx mongo.SessionContext, req *dto.CreateTransactionReq,
	transaction *model.Transaction, amount decimal.Decimal, walletId primitive.ObjectID) error {

	currency, err := p.walletCurrencyDao.GetById(sessCtx, req.CurrencyId)
	if err != nil {
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	maxAmount, err := decimal.NewFromString(currency.MaxRedPacketAmount.String())
	if err != nil {
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 检查总金额是否超过：红包个数 × 单个红包最大金额
	maxTotalAmount := maxAmount.Mul(decimal.NewFromInt(int64(req.TotalCount)))
	if amount.GreaterThan(maxTotalAmount) {
		log.ZError(ctx, "总金额超过限制", nil,
			"total_amount", amount.String(),
			"max_total_amount", maxTotalAmount.String(),
			"max_single_amount", maxAmount.String(),
			"total_count", req.TotalCount)
		return errs.NewCodeError(freeErrors.ErrInvalidAmount, freeErrors.ErrorMessages[freeErrors.ErrInvalidAmount])
	}

	// 先减少可用余额
	if err := p.walletDao.UpdateAvailableBalanceAndAddTsRecord(
		sessCtx,
		walletId,
		req.CurrencyId,
		amount.Neg(), // 金额取负值，表示减少
		walletTransactionRecordModel.TsRecordTypeRedPacketExpense,
		"",
		req.Greeting); err != nil {
		return err
	}
	// 再增加红包冻结余额
	if err := p.walletDao.UpdateRedPacketFrozenBalance(sessCtx, walletId, req.CurrencyId, amount); err != nil {
		return err
	}

	return nil
}

// TransferProcessor 转账处理器
type TransferProcessor struct {
	walletDao *walletModel.WalletBalanceDao
}

func NewTransferProcessor(mongoCli *mongo.Database) *TransferProcessor {
	return &TransferProcessor{
		walletDao: walletModel.NewWalletBalanceDao(mongoCli),
	}
}

func (p *TransferProcessor) Process(ctx context.Context, sessCtx mongo.SessionContext, req *dto.CreateTransactionReq,
	transaction *model.Transaction, amount decimal.Decimal, walletId primitive.ObjectID) error {

	// 对于转账，冻结金额到转账冻结中
	if err := p.walletDao.UpdateAvailableBalanceAndAddTsRecord(
		sessCtx,
		walletId,
		req.CurrencyId,
		amount.Neg(), // 金额取负值，表示减少
		walletTransactionRecordModel.TsRecordTypeTransferExpense,
		"",
		req.Greeting); err != nil {
		return err
	}
	// 再增加转账冻结余额
	if err := p.walletDao.UpdateTransferFrozenBalance(sessCtx, walletId, req.CurrencyId, amount); err != nil {
		return err
	}

	return nil
}

// OrganizationProcessor 组织转账处理器
type OrganizationProcessor struct {
	walletInfoDao     *walletModel.WalletInfoDao
	walletDao         *walletModel.WalletBalanceDao
	orgUserDao        *organizationModel.OrganizationUserDao
	walletCurrencyDao *walletModel.WalletCurrencyDao
}

func NewOrganizationProcessor(mongoCli *mongo.Database) *OrganizationProcessor {
	return &OrganizationProcessor{
		walletInfoDao:     walletModel.NewWalletInfoDao(mongoCli),
		walletDao:         walletModel.NewWalletBalanceDao(mongoCli),
		orgUserDao:        organizationModel.NewOrganizationUserDao(mongoCli),
		walletCurrencyDao: walletModel.NewWalletCurrencyDao(mongoCli),
	}
}

func (p *OrganizationProcessor) Process(ctx context.Context, sessCtx mongo.SessionContext, req *dto.CreateTransactionReq,
	transaction *model.Transaction, amount decimal.Decimal, walletId primitive.ObjectID) error {

	//增加判断接受者是不是管理员  根据接受者ID查询是不是管理员
	orgUser, err := p.orgUserDao.GetByUserIMServerUserId(sessCtx, req.TargetID)
	if err != nil {
		log.ZError(ctx, "查询接受者组织用户信息失败", err, "target_id", req.TargetID)
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	if !organizationModel.IsOrgWebElevatedRole(orgUser.Role) {
		log.ZError(ctx, "接受者不是管理员或团队长", nil, "target_id", req.TargetID, "role", orgUser.Role)
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	orgId, err := primitive.ObjectIDFromHex(req.OrgID)
	if err != nil {
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}
	//判断当前币种是不是属于当前组织
	currency, err := p.walletCurrencyDao.ExistByIdAndOrgID(sessCtx, req.CurrencyId, orgId)
	if err != nil {
		return err
	}
	if !currency {
		return freeErrors.ApiErr("currency not found")
	}

	//更新组织账户余额
	// 1. 验证发送者是组织账户
	if req.WalletInfoOwnerType != string(walletModel.WalletInfoOwnerTypeOrganization) {
		log.ZWarn(sessCtx, "发送方不是组织账户", nil, "wallet_type", req.WalletInfoOwnerType)
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	// 2. 获取接收者钱包信息 传主账户ID
	receiverWalletInfo, err := p.walletInfoDao.GetByOwnerIdAndOwnerType(sessCtx, orgUser.UserId, walletModel.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			log.ZWarn(sessCtx, "接收者钱包未开启", nil, "receiver_id", req.TargetID)
			return errs.NewCodeError(freeErrors.WalletNotOpenCode, freeErrors.ErrorMessages[freeErrors.WalletNotOpenCode])
		}
		log.ZError(sessCtx, "获取接收者钱包信息失败", err, "receiver_id", req.TargetID)
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 3. 扣减发送者余额
	if err := p.walletDao.UpdateAvailableBalanceAndAddTsRecord(
		sessCtx,
		walletId,
		req.CurrencyId,
		amount.Neg(), // 金额取负值，表示减少
		walletTransactionRecordModel.TsRecordTypeTransferExpense,
		"",
		req.Greeting); err != nil {
		log.ZError(sessCtx, "扣减发送者余额失败", err, "sender_id", req.SenderID, "amount", amount.Neg().String())
		return err
	}

	// 4. 增加接收者余额
	if err := p.walletDao.UpdateAvailableBalanceAndAddTsRecord(
		sessCtx,
		receiverWalletInfo.ID,
		req.CurrencyId,
		amount,
		walletTransactionRecordModel.TsRecordTypeTransferReceive,
		"",
		req.Greeting); err != nil {
		log.ZError(sessCtx, "增加接收者余额失败", err, "receiver_id", req.TargetID, "amount", amount.String())
		return err
	}

	// 5. 设置交易状态为完成
	transaction.Status = model.TransactionStatusComplete

	return nil
}

// getTransactionProcessor 根据交易类型获取处理器
func (t *TransactionService) getTransactionProcessor(transactionType int, mongoCli *mongo.Database) TransactionProcessor {
	switch transactionType {
	case model.TransactionTypeNormalPacket, model.TransactionTypeLuckyPacket, model.TransactionTypeP2PRedPacket, model.TransactionTypeGroupExclusive, model.TransactionTypePasswordPacket:
		return NewRedPacketProcessor(mongoCli)
	case model.TransactionTypeTransfer:
		return NewTransferProcessor(mongoCli)
	case model.TransactionTypeOrganization:
		return NewOrganizationProcessor(mongoCli)
	default:
		return nil
	}
}

// CreateTransaction 创建交易
func (t *TransactionService) CreateTransaction(ctx context.Context, req *dto.CreateTransactionReq) (string, error) {
	// 创建统一的UTC时间引用，所有时间操作都使用这个变量
	nowUTC := time.Now().UTC()
	mongoCli := plugin.MongoCli().GetDB()
	redisCli := plugin.RedisCli()

	if redisCli.Get(ctx, "CLOSE_TRANSACTION").Val() != "" {
		return "", freeErrors.ApiErr("transaction is closed")
	}

	// 记录开始创建交易的日志
	log.ZInfo(ctx, "开始创建交易",
		"target_id", req.TargetID,
		"transaction_type", req.TransactionType,
		"org_id", req.OrgID)

	// 1. 验证基础参数
	if err := t.validateBasicParams(ctx, req); err != nil {
		log.ZError(ctx, "基础参数验证失败", err,
			"sender_id", req.SenderID,
			"target_id", req.TargetID,
			"transaction_type", req.TransactionType,
			"org_id", req.OrgID)
		return "", err
	}

	// 2. 创建分布式锁
	lockKey := fmt.Sprintf("%slock:%s", constant.TransactionKeyPrefix, req.SenderID)
	// 设置锁的过期时间为10秒，防止死锁
	ok, err := redisCli.SetNX(ctx, lockKey, "1", 10*time.Second).Result()
	if err != nil {
		log.ZError(ctx, "Redis分布式锁操作失败", err,
			"sender_id", req.SenderID,
			"lock_key", lockKey)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	if !ok {
		log.ZWarn(ctx, "获取分布式锁失败，操作过于频繁", nil,
			"sender_id", req.SenderID,
			"lock_key", lockKey)
		return "", errs.NewCodeError(freeErrors.ErrTooFrequent, freeErrors.ErrorMessages[freeErrors.ErrTooFrequent])
	}
	defer redisCli.Del(ctx, lockKey)

	// 3. 验证钱包权限
	orgUser, ownerId, walletType, err := t.validateWalletPermission(ctx, req)
	if err != nil {
		log.ZError(ctx, "钱包权限验证失败", err,
			"sender_id", req.SenderID,
			"org_id", req.OrgID,
			"wallet_owner_type", req.WalletInfoOwnerType)
		return "", err
	}

	// 4. 验证支付密码并获取钱包信息
	walletInfo, err := t.validatePayPassword(ctx, req, ownerId, walletType)
	if err != nil {
		log.ZError(ctx, "支付密码验证失败", err,
			"sender_id", req.SenderID,
			"owner_id", ownerId,
			"wallet_type", walletType,
			"has_password", req.PayPassword != "")
		return "", err
	}

	// 5. 验证金额和数量
	amount, amountDecimal128, _, err := t.validateAmountAndCount(ctx, req)
	if err != nil {
		log.ZError(ctx, "金额和数量验证失败", err,
			"sender_id", req.SenderID,
			"total_amount", req.TotalAmount,
			"total_count", req.TotalCount,
			"currency_id", req.CurrencyId,
			"transaction_type", req.TransactionType)
		return "", err
	}

	// 6. 验证用户关系
	senderImID := orgUser.ImServerUserId
	if err := t.validateUserRelations(ctx, req, senderImID); err != nil {
		log.ZError(ctx, "用户关系验证失败", err,
			"sender_id", req.SenderID,
			"sender_im_id", senderImID,
			"target_id", req.TargetID,
			"transaction_type", req.TransactionType)
		return "", err
	}

	// 6.5. 验证交易权限（红包和转账）
	if err := t.validateTransactionPermission(ctx, req, orgUser); err != nil {
		log.ZError(ctx, "交易权限验证失败", err,
			"sender_id", req.SenderID,
			"org_id", req.OrgID,
			"role", orgUser.Role,
			"transaction_type", req.TransactionType)
		return "", err
	}

	// 7. 生成交易ID
	transactionID := fmt.Sprintf("%d%s%s", req.TransactionType, nowUTC.Format("20060102150405"), strings.Replace(uuid.New().String(), "-", "", -1)[:8])

	// 8. 创建交易记录
	transaction := &model.Transaction{
		TransactionID:         transactionID,
		SenderID:              req.SenderID,
		SenderImID:            senderImID,
		TargetImID:            req.TargetID,
		TransactionType:       req.TransactionType,
		OrgID:                 req.OrgID,
		TotalAmount:           amountDecimal128,
		TotalCount:            req.TotalCount,
		RemainingAmount:       amountDecimal128,
		RemainingCount:        req.TotalCount,
		WalletID:              walletInfo.ID,
		CurrencyId:            req.CurrencyId,
		Greeting:              req.Greeting,
		Password:              req.Password,
		ExclusiveReceiverImID: req.ExclusiveReceiverID,
		Status:                model.TransactionStatusPending,
		CreatedAt:             nowUTC, // 使用统一的UTC时间
	}

	// 9. 执行事务
	transactionDao := model.NewTransactionDao(mongoCli)
	session, err := mongoCli.Client().StartSession()
	if err != nil {
		log.ZError(ctx, "创建MongoDB会话失败", err,
			"sender_id", req.SenderID,
			"transaction_id", transactionID)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	defer session.EndSession(ctx)

	// 在事务中执行操作
	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		// 使用处理器模式处理不同类型的交易
		processor := t.getTransactionProcessor(req.TransactionType, mongoCli)
		if processor == nil {
			log.ZError(sessCtx, "无效的交易类型", nil,
				"transaction_type", req.TransactionType,
				"sender_id", req.SenderID,
				"transaction_id", transactionID)
			return nil, errs.NewCodeError(freeErrors.ErrUnknownTransactionType, freeErrors.ErrorMessages[freeErrors.ErrUnknownTransactionType])
		}

		if err := processor.Process(ctx, sessCtx, req, transaction, amount, walletInfo.ID); err != nil {
			log.ZError(sessCtx, "交易处理器执行失败", err,
				"transaction_type", req.TransactionType,
				"sender_id", req.SenderID,
				"transaction_id", transactionID,
				"amount", amount.String(),
				"wallet_id", walletInfo.ID.Hex())
			return nil, err
		}

		// 创建交易记录
		if err := transactionDao.Create(sessCtx, transaction); err != nil {
			log.ZError(sessCtx, "创建交易记录失败", err,
				"transaction_id", transactionID,
				"sender_id", req.SenderID,
				"target_id", req.TargetID,
				"amount", amount.String())
			return nil, err
		}

		// 如果是组织转账，直接完成
		if transaction.Status == model.TransactionStatusComplete {
			log.ZInfo(ctx, "组织转账直接完成",
				"transaction_id", transactionID,
				"sender_id", req.SenderID,
				"target_id", req.TargetID,
				"amount", amount.String())
			return transactionID, nil
		}

		// 保存到Redis
		redisKey := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, transactionID)
		transactionMap := map[string]interface{}{
			"type":             req.TransactionType,
			"total_amount":     amount.String(),
			"remaining_amount": amount.String(),
			"total_count":      req.TotalCount,
			"remaining_count":  req.TotalCount,
			"created_at":       nowUTC.Unix(), // 使用统一的UTC时间戳
		}

		if err := redisCli.HMSet(sessCtx, redisKey, transactionMap).Err(); err != nil {
			log.ZError(sessCtx, "保存交易到Redis失败", err,
				"transaction_id", transactionID,
				"redis_key", redisKey,
				"sender_id", req.SenderID)
			return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
		}

		// 记录交易创建日志
		log.ZInfo(ctx, "交易创建成功",
			"transaction_id", transactionID,
			"sender_id", req.SenderID,
			"target_im_id", req.TargetID,
			"amount", amount.String(),
			"transaction_type", req.TransactionType,
			"time", nowUTC.Format(time.RFC3339))

		return transactionID, nil
	})

	if err != nil {
		log.ZError(ctx, "MongoDB事务执行失败", err,
			"sender_id", req.SenderID,
			"transaction_id", transactionID,
			"target_id", req.TargetID,
			"amount", amount.String())
		return "", err
	}

	// 为红包交易类型初始化Redis槽位管理
	if req.TransactionType == model.TransactionTypeNormalPacket ||
		req.TransactionType == model.TransactionTypeLuckyPacket ||
		req.TransactionType == model.TransactionTypeGroupExclusive ||
		req.TransactionType == model.TransactionTypePasswordPacket ||
		req.TransactionType == model.TransactionTypeP2PRedPacket {

		// 尝试创建槽位管理器并初始化Redis
		slotManager, err := NewRedPacketSlotManager(redisCli)
		if err != nil {
			log.ZWarn(ctx, "创建红包槽位管理器失败", err, "transaction_id", transactionID)
			// 不返回错误，因为Redis初始化失败不影响主流程
		} else {
			// 初始化Redis键
			if err := slotManager.InitializeRedPacket(ctx, transactionID, req.TotalCount); err != nil {
				log.ZWarn(ctx, "初始化Redis红包计数器失败", err, "transaction_id", transactionID)
				// 不返回错误，因为Redis初始化失败不影响主流程
			} else {
				log.ZInfo(ctx, "已成功初始化Redis红包计数器", "transaction_id", transactionID, "total_count", req.TotalCount)
			}
		}
	}

	return transactionID, nil
}

// ================================= 接收交易 =======================================
//
// 拼手气红包（type=3/6）与 Redis：
//   - 金额并非提前算好放入 Redis。Redis 只存 total_amount、remaining_amount、total_count、remaining_count 等，
//     每人领取时在 Lua 内用「二倍均值法」现场计算当次金额。
//   - 原实现中拼手气在 remaining_count>1 时随机 [1, 2*均值]，且 math.floor(math.random()*max) 实际为 [0,max-1]，
//     再与 min=1 结合后，容易出现很多人只抽到 1，导致 remaining_count 先减到 0（个数领完）而 remaining_amount 仍>0（钱没分完）。
//   - 修复：当前人最多拿 remaining_amount - (remaining_count-1)，保证后续每人至少 1 单位；随机区间改为 [1, max] 使钱在个数领完时被分完。
//
// Lua脚本常量
const (
	// 接收交易的 Lua 脚本
	ReceiveTransactionLuaScript = `-- Lua脚本：处理交易接收逻辑
-- KEYS 说明:
-- KEYS[1]: 交易信息的key (transaction:transaction_id)
-- KEYS[2]: 交易接收者集合的key (transaction_receivers:transaction_id)
-- ARGV 说明:
-- ARGV[1]: 接收者ID
-- ARGV[2]: 当前时间戳
-- ARGV[3]: 交易过期时间(秒)
-- ARGV[4]: 币种精度

-- 获取交易信息
local transaction = redis.call('HGETALL', KEYS[1])
if #transaction == 0 then
    return '{"status":"error","err":"TRANSACTION_NOT_FOUND","receive_amount":"0","remaining_count":"0"}'
end

-- 将HGETALL的结果转换为Lua表
local transaction_info = {}
for i = 1, #transaction, 2 do
    transaction_info[transaction[i]] = transaction[i + 1]
end

-- 检查交易是否过期 (created_at + expire_time < 当前时间)
local created_at = tonumber(transaction_info.created_at)
local expire_time = tonumber(ARGV[3])
local current_time = tonumber(ARGV[2])

if created_at + expire_time < current_time then
    return '{"status":"error","err":"TRANSACTION_EXPIRED","receive_amount":"0","remaining_count":"0"}'
end

-- 检查接收者是否已经领取过
local is_received = redis.call('SISMEMBER', KEYS[2], ARGV[1])
if is_received == 1 then
    return '{"status":"error","err":"ALREADY_RECEIVED","receive_amount":"0","remaining_count":"' .. (transaction_info.remaining_count or "0") .. '"}'
end

-- 获取剩余金额和个数
local remaining_amount = tonumber(transaction_info.remaining_amount)
local remaining_count = tonumber(transaction_info.remaining_count)

-- 检查是否还有剩余
if remaining_count <= 0 then
    return '{"status":"error","err":"NO_REMAINING","receive_amount":"0","remaining_count":"0"}'
end

-- 获取币种精度并计算缩放因子
local precision = tonumber(ARGV[4])
local scale_factor = 10^precision

-- 将金额转为整数处理，避免浮点误差
local remaining_amount_int = math.floor(remaining_amount * scale_factor + 0.5)
local receive_amount_int = 0
local transaction_type = tonumber(transaction_info.type)

-- 根据交易类型计算接收金额
if transaction_type == 0 then
    -- 转账: 剩余金额全部给接收者
    receive_amount_int = remaining_amount_int
elseif transaction_type == 1 or transaction_type == 2 or transaction_type == 5 then
    -- 一对一红包或普通红包
    if transaction_type == 2 then
        -- 普通红包（等额红包）：确保每个红包金额相同
        -- 从交易信息获取总金额和总数量
        local total_amount = tonumber(transaction_info.total_amount)
        local total_count = tonumber(transaction_info.total_count)
        -- 转换为整数处理
        local total_amount_int = math.floor(total_amount * scale_factor + 0.5)
        -- 计算每个红包固定金额（直接除法不会四舍五入）
        local per_amount_int = math.floor(total_amount_int / total_count + 0.5)
        -- 每个人领取固定等额红包
        receive_amount_int = per_amount_int
    else
        -- 其他类型红包使用平均分配
        receive_amount_int = math.floor(remaining_amount_int / remaining_count)
    end
elseif transaction_type == 3 or transaction_type == 6 then
    -- 拼手气红包: 二倍均值法，并保证「个数领完时钱也分完」（避免剩余人数为0但剩余金额>0）
    if remaining_count == 1 then
        receive_amount_int = remaining_amount_int
    else
        local mean_int = math.floor(remaining_amount_int / remaining_count)
        local two_mean_int = mean_int * 2
        -- 保证后续 remaining_count-1 人每人至少 1 单位，故当前人最多拿 remaining_amount_int - (remaining_count - 1)
        local cap_int = remaining_amount_int - (remaining_count - 1)
        local max_amount_int = math.max(1, math.min(two_mean_int, cap_int))
        math.randomseed(current_time + string.byte(ARGV[1], 1))
        -- [1, max_amount_int] 均匀随机（原式 math.floor(math.random()*max) 为 [0,max-1]，+1 得 [1,max]）
        receive_amount_int = math.floor(math.random() * max_amount_int) + 1
    end
end

-- 更新剩余金额和个数
local new_remaining_amount_int = remaining_amount_int - receive_amount_int
local new_remaining_count = remaining_count - 1

-- 将整数金额转回原精度小数
local receive_amount = receive_amount_int / scale_factor
local new_remaining_amount = new_remaining_amount_int / scale_factor

-- 将用户添加到已接收集合
redis.call('SADD', KEYS[2], ARGV[1])

-- 更新交易信息中的剩余金额和个数
redis.call('HSET', KEYS[1], 'remaining_amount', tostring(new_remaining_amount))
redis.call('HSET', KEYS[1], 'remaining_count', tostring(new_remaining_count))

-- 返回JSON字符串
return '{"status":"success","err":"","receive_amount":"' .. tostring(receive_amount) .. '","remaining_count":"' .. tostring(new_remaining_count) .. '","remaining_amount":"' .. tostring(new_remaining_amount) .. '"}'`

	// 回滚 Lua 脚本
	RollbackTransactionLuaScript = `-- 回滚脚本：恢复交易状态
-- KEYS[1]: 交易信息的key (transaction:transaction_id)
-- KEYS[2]: 交易接收者集合的key (transaction_receivers:transaction_id)
-- ARGV[1]: 接收者ID
-- ARGV[2]: 接收金额
-- ARGV[3]: 接收前的剩余数量
-- ARGV[4]: 币种精度

-- 将用户从已接收集合中移除
redis.call('SREM', KEYS[2], ARGV[1])

-- 获取当前剩余金额
local current_amount = tonumber(redis.call('HGET', KEYS[1], 'remaining_amount'))
if current_amount == nil then
    return '{"status":"error","err":"TRANSACTION_NOT_FOUND"}'
end

-- 获取币种精度并计算缩放因子
local precision = tonumber(ARGV[4])
local scale_factor = 10^precision

-- 转换为整数计算
local current_amount_int = math.floor(current_amount * scale_factor + 0.5)
local restore_amount_int = math.floor(tonumber(ARGV[2]) * scale_factor + 0.5)

-- 恢复剩余金额和数量
local restored_amount = (current_amount_int + restore_amount_int) / scale_factor
local restored_count = tonumber(ARGV[3])

-- 更新交易信息
redis.call('HSET', KEYS[1], 'remaining_amount', tostring(restored_amount))
redis.call('HSET', KEYS[1], 'remaining_count', tostring(restored_count))

return '{"status":"success","err":""}'`

	// 释放锁的 Lua 脚本
	ReleaseLockLuaScript = `if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end`
)

// ReceiveTransactionContext 接收交易的上下文
type ReceiveTransactionContext struct {
	Req                 *dto.ReceiveTransactionReq
	Transaction         *model.Transaction
	NowUTC              time.Time
	LockKey             string
	LockValue           string
	RedisTransactionKey string
	RedisReceiversKey   string
	CurrencyPrecision   int32
	ImServerUserID      string
	WalletInfo          *walletModel.WalletInfo
}

// LuaScriptResult Lua脚本执行结果
type LuaScriptResult struct {
	Status          string `json:"status"`
	Err             string `json:"err"`
	ReceiveAmount   string `json:"receive_amount"`
	RemainingCount  string `json:"remaining_count"`
	RemainingAmount string `json:"remaining_amount"`
}

// ReceiveTransactionValidator 接收交易验证器
type ReceiveTransactionValidator struct {
	mongoDB *mongo.Database
}

// ReceiveTransactionLockManager 分布式锁管理器
type ReceiveTransactionLockManager struct {
	redisClient redis.UniversalClient
}

// ReceiveTransactionRedisProcessor Redis处理器
type ReceiveTransactionRedisProcessor struct {
	redisClient redis.UniversalClient
}

// ReceiveTransactionDBProcessor 数据库处理器
type ReceiveTransactionDBProcessor struct {
	mongoDB          *mongo.Database
	transactionDao   *model.TransactionDao
	receiveRecordDao *model.ReceiveRecordDao
	walletDao        *walletModel.WalletBalanceDao
	walletInfoDao    *walletModel.WalletInfoDao
}

// ReceiveTransactionManager 接收交易管理器
type ReceiveTransactionManager struct {
	validator      *ReceiveTransactionValidator
	lockManager    *ReceiveTransactionLockManager
	redisProcessor *ReceiveTransactionRedisProcessor
	dbProcessor    *ReceiveTransactionDBProcessor
}

// ReceiveTransaction 领取交易（重构后的主入口）
func (t *TransactionService) ReceiveTransaction(ctx context.Context, req *dto.ReceiveTransactionReq) (string, error) {
	// 获取数据库连接
	mongoDB := plugin.MongoCli().GetDB()
	redisClient := plugin.RedisCli()
	if mongoDB == nil || redisClient == nil {
		log.ZError(ctx, "获取数据库连接失败", nil, "receiver_id", req.ReceiverID, "transaction_id", req.TransactionID)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	log.ZInfo(ctx, "开始处理红包领取请求", "receiver_id", req.ReceiverID, "transaction_id", req.TransactionID, "使用混合模式", true)

	// 入口处将 receiver_id 统一为 organization_user.user_id（若传入的是 im_server_user_id 则先解析），保证后续 ReserveSlot/Redis/校验全流程使用同一 ID
	if err := normalizeReceiverIDToOrgUserID(ctx, mongoDB, req); err != nil {
		log.ZError(ctx, "归一化 receiver_id 失败", err, "receiver_id", req.ReceiverID, "org_id", req.OrgID)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 检查RedisClient是否可用
	_, redisErr := redisClient.Ping(ctx).Result()
	if redisErr != nil {
		log.ZError(ctx, "Redis连接失败，降级使用传统模式", redisErr, "receiver_id", req.ReceiverID, "transaction_id", req.TransactionID)
		manager := NewReceiveTransactionManager(mongoDB, redisClient)
		return manager.ProcessReceiveTransaction(ctx, req)
	}

	// 使用混合模式处理红包领取
	managerV2, err := NewReceiveTransactionManagerV2(mongoDB, redisClient)
	if err != nil {
		log.ZError(ctx, "创建V2交易管理器失败，降级使用传统模式", err, "receiver_id", req.ReceiverID, "transaction_id", req.TransactionID)
		// 降级使用传统模式
		manager := NewReceiveTransactionManager(mongoDB, redisClient)
		return manager.ProcessReceiveTransaction(ctx, req)
	}

	// 【高并发优化】使用原子操作检查并初始化 Redis 元数据
	// 关键：Lua 脚本检查的是 transKey 的 status 字段，必须确保 status 字段存在后才能调用 Lua
	transKey := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, req.TransactionID)
	countKey := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, req.TransactionID)

	// 使用 HSETNX 原子检查 status 是否已设置（返回 1 表示成功设置，0 表示已存在）
	wasSet, hsetnxErr := redisClient.HSetNX(ctx, transKey, "status", -2).Result() // -2 表示"正在初始化"的临时状态
	if hsetnxErr != nil {
		log.ZWarn(ctx, "HSETNX 检查失败，跳过预同步", hsetnxErr, "transaction_id", req.TransactionID)
	} else if wasSet {
		// 当前请求获得了初始化权，从 MongoDB 加载数据
		transactionDao := model.NewTransactionDao(mongoDB)
		transaction, err := transactionDao.GetByTransactionID(ctx, req.TransactionID)
		if err != nil || transaction == nil {
			// 初始化失败，删除临时状态让其他请求可以重试
			redisClient.HDel(ctx, transKey, "status")
			log.ZWarn(ctx, "从MongoDB获取交易信息失败", err,
				"transaction_id", req.TransactionID,
				"receiver_id", req.ReceiverID)
		} else {
			log.ZInfo(ctx, "获得初始化权，从MongoDB初始化Redis",
				"transaction_id", req.TransactionID,
				"remaining_count", transaction.RemainingCount)

			// 使用 Pipeline 原子设置所有字段（含 total_amount/remaining_amount，避免 Hash 不完整导致 Lua 误判）
			pipe := redisClient.Pipeline()
			pipe.HSet(ctx, transKey, "status", transaction.Status)
			pipe.HSet(ctx, transKey, "total_count", transaction.TotalCount)
			pipe.HSet(ctx, transKey, "remaining_count", transaction.RemainingCount)
			pipe.HSet(ctx, transKey, "total_amount", transaction.TotalAmount.String())
			pipe.HSet(ctx, transKey, "remaining_amount", transaction.RemainingAmount.String())
			pipe.Expire(ctx, transKey, 24*time.Hour)
			pipe.SetNX(ctx, countKey, transaction.RemainingCount, 24*time.Hour)

			if _, pipeErr := pipe.Exec(ctx); pipeErr != nil {
				log.ZWarn(ctx, "初始化Redis元数据失败", pipeErr, "transaction_id", req.TransactionID)
			}

			// 智能判断是否需要同步接收者集合（仅初始化时同步一次）
			shouldSync, _, _ := managerV2.slotManager.ShouldSyncReceivers(ctx, req.TransactionID)
			if shouldSync {
				_ = managerV2.slotManager.SyncReceiversFromMongoDB(ctx, mongoDB, req.TransactionID)
			}
		}
	} else {
		// status 字段已存在，但可能是 -2（正在初始化）或有效值
		// 【性能优化】减少等待时间到最多 200ms（每 20ms 检查一次）
		statusVal, _ := redisClient.HGet(ctx, transKey, "status").Int()
		if statusVal == -2 {
			// 另一个请求正在初始化，短暂等待
			for i := 0; i < 10; i++ {
				time.Sleep(20 * time.Millisecond)
				statusVal, _ = redisClient.HGet(ctx, transKey, "status").Int()
				if statusVal != -2 {
					break
				}
			}
			// 即使超时也继续，让 Lua 脚本返回 INITIALIZING 并在那里处理
		}
	}

	log.ZInfo(ctx, "成功创建混合模式管理器，开始处理交易", "receiver_id", req.ReceiverID, "transaction_id", req.TransactionID)

	// 使用混合模式处理交易
	result, err := managerV2.ProcessReceiveTransaction(ctx, req)
	if err != nil {
		log.ZError(ctx, "混合模式处理失败", err, "receiver_id", req.ReceiverID, "transaction_id", req.TransactionID)
		return "", err
	}

	log.ZInfo(ctx, "混合模式处理成功", "receiver_id", req.ReceiverID, "transaction_id", req.TransactionID, "amount", result)
	return result, nil
}

// CheckUserReceived 查询用户是否接收了交易
func (t *TransactionService) CheckUserReceived(ctx context.Context, req *dto.CheckUserReceivedReq) (*dto.CheckUserReceivedResp, error) {
	// 使用正确的Redis键格式查询用户是否接收了交易
	// 修正: 使用与Lua脚本一致的格式 dep_transaction:{id}:receivers
	redisReceiversKey := fmt.Sprintf("%s%s:receivers", constant.TransactionKeyPrefix, req.TransactionID)

	// 获取Redis客户端
	redisCli := plugin.RedisCli()
	if redisCli == nil {
		log.ZError(ctx, "获取Redis客户端失败", nil, "transaction_id", req.TransactionID)
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 检查Redis中是否存在该交易的接收者集合
	exists, err := redisCli.Exists(ctx, redisReceiversKey).Result()
	if err != nil {
		log.ZError(ctx, "查询Redis交易接收者集合失败", err,
			"transaction_id", req.TransactionID,
			"receivers_key", redisReceiversKey)

		// 如果Redis查询失败，尝试直接查询MongoDB
		return t.checkUserReceivedFromMongoDB(ctx, req)
	}

	// 如果Redis中不存在该交易的接收者集合，表示交易可能已过期或已领取完毕
	// 也可能是使用的是旧的键格式，尝试检查旧格式
	if exists == 0 {
		// 尝试使用旧的键格式
		oldRedisReceiversKey := fmt.Sprintf("%s%s", constant.TransactionReceiversPrefix, req.TransactionID)
		oldExists, oldErr := redisCli.Exists(ctx, oldRedisReceiversKey).Result()

		if oldErr == nil && oldExists > 0 {
			// 如果旧格式的键存在，使用旧格式查询
			log.ZWarn(ctx, "发现旧格式的接收者集合键", nil,
				"transaction_id", req.TransactionID,
				"old_key", oldRedisReceiversKey)

			// 查询用户是否在旧格式接收者集合中
			isMember, err := redisCli.SIsMember(ctx, oldRedisReceiversKey, req.UserID).Result()
			if err == nil && isMember {
				// 如果用户在旧格式接收者集合中，返回已接收
				log.ZInfo(ctx, "用户在旧格式接收者集合中",
					"transaction_id", req.TransactionID,
					"user_id", req.UserID)

				// 查询MongoDB获取接收金额
				return t.getReceiveAmountFromMongoDB(ctx, req)
			}
		}

		// 如果Redis中没有相关键，则查询MongoDB
		return t.checkUserReceivedFromMongoDB(ctx, req)
	}

	// 查询用户是否在接收者集合中
	isMember, err := redisCli.SIsMember(ctx, redisReceiversKey, req.UserID).Result()
	if err != nil {
		log.ZError(ctx, "查询用户是否在Redis接收者集合中失败", err,
			"transaction_id", req.TransactionID,
			"user_id", req.UserID)

		// 如果Redis查询失败，尝试直接查询MongoDB
		return t.checkUserReceivedFromMongoDB(ctx, req)
	}

	// 如果用户不在接收者集合中，表示未接收
	if !isMember {
		return &dto.CheckUserReceivedResp{
			Received: false,
		}, nil
	}

	// 用户在接收者集合中，表示已接收，需要查询接收金额
	return t.getReceiveAmountFromMongoDB(ctx, req)
}

// checkUserReceivedFromMongoDB 从MongoDB中查询用户是否接收了交易
// 当Redis查询失败或不存在接收者集合时使用
func (t *TransactionService) checkUserReceivedFromMongoDB(ctx context.Context, req *dto.CheckUserReceivedReq) (*dto.CheckUserReceivedResp, error) {
	log.ZInfo(ctx, "尝试从MongoDB直接查询用户接收记录",
		"transaction_id", req.TransactionID,
		"user_id", req.UserID)

	// 获取MongoDB客户端
	mongoDB := plugin.MongoCli().GetDB()
	if mongoDB == nil {
		log.ZError(ctx, "获取MongoDB客户端失败", nil, "transaction_id", req.TransactionID)
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 查询MongoDB
	receiveDao := model.NewReceiveRecordDao(mongoDB)
	record, err := receiveDao.GetByTransactionAndUser(ctx, req.TransactionID, req.UserID)

	// 如果查询出错或记录不存在，表示未接收
	if err != nil || record == nil {
		if err != nil {
			log.ZWarn(ctx, "MongoDB中查询用户接收记录失败", err,
				"transaction_id", req.TransactionID,
				"user_id", req.UserID)
		} else {
			log.ZInfo(ctx, "MongoDB中未找到用户接收记录",
				"transaction_id", req.TransactionID,
				"user_id", req.UserID)
		}

		return &dto.CheckUserReceivedResp{
			Received: false,
		}, nil
	}

	// 找到记录，表示已接收
	log.ZInfo(ctx, "在MongoDB中找到用户接收记录",
		"transaction_id", req.TransactionID,
		"user_id", req.UserID,
		"amount", record.Amount.String())

	return &dto.CheckUserReceivedResp{
		Received: true,
		Amount:   record.Amount.String(),
	}, nil
}

// getReceiveAmountFromMongoDB 从MongoDB获取用户接收的金额
func (t *TransactionService) getReceiveAmountFromMongoDB(ctx context.Context, req *dto.CheckUserReceivedReq) (*dto.CheckUserReceivedResp, error) {
	// 查询MongoDB获取接收金额
	receiveDao := model.NewReceiveRecordDao(plugin.MongoCli().GetDB())
	record, err := receiveDao.GetByTransactionAndUser(ctx, req.TransactionID, req.UserID)
	if err != nil {
		log.ZWarn(ctx, "MongoDB中查询用户接收记录失败", err,
			"transaction_id", req.TransactionID,
			"user_id", req.UserID)

		// 虽然查询金额失败，但确实已接收，返回已接收但无金额信息
		return &dto.CheckUserReceivedResp{
			Received: true,
		}, nil
	}

	if record == nil {
		// 这种情况可能是Redis和MongoDB数据不一致
		// Redis中显示用户已接收，但MongoDB中没有记录
		log.ZWarn(ctx, "Redis接收记录与MongoDB不一致",
			nil,
			"transaction_id", req.TransactionID,
			"user_id", req.UserID)

		// 保守返回已接收
		return &dto.CheckUserReceivedResp{
			Received: true,
		}, nil
	}

	return &dto.CheckUserReceivedResp{
		Received: true,
		Amount:   record.Amount.String(),
	}, nil
}

// GetTransactionReceiveDetails 查询交易接收详情
func (t *TransactionService) GetTransactionReceiveDetails(ctx context.Context, req *dto.QueryTransactionReceiveDetailsReq) (*dto.TransactionReceiveDetailsResp, error) {
	// 首先检查交易是否存在
	transactionDao := model.NewTransactionDao(plugin.MongoCli().GetDB())
	transaction, err := transactionDao.GetByTransactionID(ctx, req.TransactionID)
	if err != nil {
		log.ZError(ctx, "查询交易信息失败", err)
		return nil, err
	}

	// 查询该交易的接收记录（仅包含有写入领取记录的真实领取，压测领取不会写入此表）
	receiveDao := model.NewReceiveRecordDao(plugin.MongoCli().GetDB())
	var records []*model.ReceiveRecord
	if req.PageNum > 0 && req.PageSize > 0 {
		records, err = receiveDao.GetByTransactionIDPaged(ctx, req.TransactionID, req.PageNum, req.PageSize)
	} else {
		records, err = receiveDao.GetByTransactionID(ctx, req.TransactionID)
	}
	if err != nil {
		log.ZError(ctx, "查询交易接收记录失败", err)
		return nil, err
	}

	// 获取总金额
	totalAmountDecimal, err := decimal.NewFromString(transaction.TotalAmount.String())
	if err != nil {
		log.ZWarn(ctx, "总金额格式转换失败", err, "total_amount", transaction.TotalAmount.String())
		totalAmountDecimal = decimal.NewFromInt(0)
	}

	// 已领个数与已领金额以 transaction 为准，与 remaining 一致（压测等未写 receive_record 的领取也会被统计）
	receivedCountFromTransaction := transaction.TotalCount - transaction.RemainingCount
	if receivedCountFromTransaction < 0 {
		receivedCountFromTransaction = 0
	}
	remainingAmountDecimal, _ := decimal.NewFromString(transaction.RemainingAmount.String())
	receivedAmountFromTransaction := totalAmountDecimal.Sub(remainingAmountDecimal)
	if receivedAmountFromTransaction.IsNegative() {
		receivedAmountFromTransaction = decimal.NewFromInt(0)
	}

	// 根据交易状态和实际领取数量确定红包状态
	var statusStr string
	switch transaction.Status {
	case model.TransactionStatusComplete:
		statusStr = "completed"
	case model.TransactionStatusExpired:
		statusStr = "expired"
	default:
		// 进行中状态，但需要检查是否实际已领完
		if receivedCountFromTransaction >= transaction.TotalCount {
			statusStr = "completed"
		} else {
			statusStr = "pending"
		}
	}

	// 构造响应（整包统计维度）
	resp := &dto.TransactionReceiveDetailsResp{
		Records:        make([]*dto.ReceiveRecordResp, 0, len(records)),
		TotalAmount:    trimAmountZeros(totalAmountDecimal.String()),
		ReceivedAmount: trimAmountZeros(receivedAmountFromTransaction.String()),
		TotalCount:     transaction.TotalCount,
		ReceivedCount:  receivedCountFromTransaction,
		Status:         statusStr,
		TotalRecords:   receivedCountFromTransaction, // 领取记录总条数（含可能未写库的压测记录）
	}

	// 转换领取记录列表（可能少于实际领取数，例如压测未写库）
	for _, record := range records {
		resp.Records = append(resp.Records, &dto.ReceiveRecordResp{
			TransactionID:   record.TransactionID,
			ReceiverID:      record.UserID,
			ReceiverIMID:    record.UserImID,
			Amount:          trimAmountZeros(record.Amount.String()),
			ReceivedAt:      record.ReceivedAt,
			TransactionType: fmt.Sprintf("%d", record.TransactionType),
		})
	}

	// 如果当前上下文中包含登录用户，则补充该用户自身的领取信息，便于前端展示“我领到多少”
	// 优先用 token 中的操作人 ID，为空时用客户端请求头 X-User-IM-ID（重装/多端后 token 可能与库中 user_id/user_im_id 不一致时兜底）
	// 使用 user_id 或 user_im_id 双条件查询：领取时写入的是 org user_id，而 token 里可能是 IM user_id，若只按 user_id 查会漏掉已领用户
	selfUserID := mctx.GetOpUserID(ctx)
	if selfUserID == "" && req.OpUserImID != "" {
		selfUserID = req.OpUserImID
	}
	if selfUserID != "" {
		if selfRecord, err := receiveDao.GetByTransactionAndUserOrImID(ctx, req.TransactionID, selfUserID); err == nil && selfRecord != nil {
			resp.SelfReceived = true
			resp.SelfAmount = trimAmountZeros(selfRecord.Amount.String())
		} else {
			// 未领取时显式返回 0，前端可直接展示 0.00 而不是总金额，避免误解
			resp.SelfReceived = false
			resp.SelfAmount = "0"
		}
	}

	return resp, nil
}

// FindExpiredTransactions 查找需要处理的过期交易
func (s *TransactionService) FindExpiredTransactions(ctx context.Context) ([]*model.Transaction, error) {
	// 创建统一的UTC时间引用
	nowUTC := time.Now().UTC()
	//log.ZInfo(ctx, "开始查找过期交易", "time", nowUTC.Format(time.RFC3339))

	// 查找创建时间在24小时前且状态为"待领取"的交易
	expireTime := nowUTC.Add(-time.Duration(constant.TransactionExpireTime) * time.Second)

	// 查询已过期但状态仍为"待领取"的交易
	filter := bson.M{
		"status":     model.TransactionStatusPending,
		"created_at": bson.M{"$lt": expireTime},
	}

	// 创建TransactionDao实例
	transactionDao := model.NewTransactionDao(plugin.MongoCli().GetDB())

	// 执行查询
	transactions, err := transactionDao.FindTransactions(ctx, filter)
	if err != nil {
		log.ZError(ctx, "查询过期交易失败", err)
		return nil, err
	}
	return transactions, nil
}

// CalculateRemainingAmountFromDB 从数据库计算剩余金额和数量
func (s *TransactionService) CalculateRemainingAmountFromDB(ctx context.Context, transaction *model.Transaction) (string, string, error) {
	mongoCli := plugin.MongoCli().GetDB()

	// 使用ReceiveRecordDao查询该交易的所有接收记录
	receiveRecordDao := model.NewReceiveRecordDao(mongoCli)

	// 根据交易ID获取所有接收记录
	records, err := receiveRecordDao.GetByTransactionID(ctx, transaction.TransactionID)
	if err != nil {
		log.ZError(ctx, "查询接收记录失败", err, "transaction_id", transaction.TransactionID)
		return "", "", err
	}

	// 计算已接收的总金额和数量
	receivedAmount := decimal.Zero
	receivedCount := len(records)
	failedParseCount := 0

	for _, record := range records {
		// 转换接收金额
		recordAmountStr := record.Amount.String()
		recordAmount, err := decimal.NewFromString(recordAmountStr)
		if err != nil {
			log.ZError(ctx, "解析接收记录金额失败", err, "record_amount", recordAmountStr, "transaction_id", transaction.TransactionID)
			failedParseCount++
			continue
		}
		receivedAmount = receivedAmount.Add(recordAmount)
	}

	// 如果有解析失败的记录，记录警告
	if failedParseCount > 0 {
		log.ZWarn(ctx, "部分接收记录金额解析失败", nil,
			"transaction_id", transaction.TransactionID,
			"failed_count", failedParseCount,
			"total_records", len(records))
	}

	// 计算剩余金额和数量
	originalAmountStr := transaction.TotalAmount.String()
	originalAmount, err := decimal.NewFromString(originalAmountStr)
	if err != nil {
		log.ZError(ctx, "解析原始交易金额失败", err, "amount", originalAmountStr, "transaction_id", transaction.TransactionID)
		return "", "", err
	}

	// 计算剩余金额
	remainingAmount := originalAmount.Sub(receivedAmount)

	// 计算剩余数量
	remainingCount := transaction.TotalCount - receivedCount

	// 确保剩余金额和数量不为负数
	if remainingAmount.IsNegative() {
		log.ZWarn(ctx, "计算出的剩余金额为负数，设置为0", nil,
			"transaction_id", transaction.TransactionID,
			"calculated_remaining", remainingAmount.String())
		remainingAmount = decimal.Zero
	}
	if remainingCount < 0 {
		log.ZWarn(ctx, "计算出的剩余数量为负数，设置为0", nil,
			"transaction_id", transaction.TransactionID,
			"calculated_remaining_count", remainingCount)
		remainingCount = 0
	}

	log.ZInfo(ctx, "从数据库计算退款信息",
		"transaction_id", transaction.TransactionID,
		"original_amount", originalAmount.String(),
		"received_amount", receivedAmount.String(),
		"remaining_amount", remainingAmount.String(),
		"original_count", transaction.TotalCount,
		"received_count", receivedCount,
		"remaining_count", remainingCount,
		"failed_parse_count", failedParseCount)

	return remainingAmount.String(), fmt.Sprintf("%d", remainingCount), nil
}

// GetUserReceiveHistoryLast24Hours 获取用户24小时内接收/发送完成的交易记录
func (t *TransactionService) GetUserReceiveHistoryLast24Hours(ctx context.Context, req *dto.QueryUserReceiveHistoryReq) (*dto.QueryUserReceiveHistoryResp, error) {
	// 从上下文获取用户ID
	userID := req.UserID
	orgID := req.OrgID
	// 创建数据访问对象
	mongo := plugin.MongoCli().GetDB()
	receiveDao := model.NewReceiveRecordDao(mongo)
	transactionDao := model.NewTransactionDao(mongo)
	orgUserDao := organizationModel.NewOrganizationUserDao(mongo)
	//获取子账户ID
	orgUser, err := orgUserDao.GetByUserIdAndOrgID(ctx, userID, orgID)
	if err != nil {
		log.ZError(ctx, "获取组织用户信息失败", err, "user_id", userID, "org_id", orgID)
		return nil, errs.NewCodeError(freeErrors.ErrSystem, "Failed to get user organization information")
	}

	// 检查orgUser是否为空
	if orgUser == nil {
		log.ZWarn(ctx, "组织用户信息不存在", nil, "user_id", userID, "org_id", orgID)
		return nil, errs.NewCodeError(freeErrors.UserNotFoundCode, "User is not in the organization")
	}

	// 获取用户24小时内的接收记录
	receiveRecords, err := receiveDao.GetUserReceiveHistoryLast24Hours(ctx, orgUser.ImServerUserId)
	if err != nil {
		log.ZError(ctx, "查询用户24小时内接收记录失败", err, "user_id", userID)
		return nil, err
	}

	// 获取用户24小时内发起且完成的交易
	sentTransactions, err := transactionDao.GetUserSentTransactionsLast24Hours(ctx, orgUser.ImServerUserId)
	if err != nil {
		log.ZError(ctx, "查询用户24小时内发起的交易失败", err, "user_id", userID)
		return nil, err
	}

	// 使用map存储已有的TransactionID，用于去重
	transactionMap := make(map[string]bool)

	// 构造响应对象
	resp := &dto.QueryUserReceiveHistoryResp{
		Records: make([]*dto.ReceiveRecordResp, 0, len(receiveRecords)+len(sentTransactions)),
	}

	// 一次性添加所有不重复的交易记录
	for i := 0; i < len(receiveRecords) || i < len(sentTransactions); i++ {
		// 添加接收记录（如果存在且未重复）
		if i < len(receiveRecords) {
			record := receiveRecords[i]
			if _, exists := transactionMap[record.TransactionID]; !exists {
				resp.Records = append(resp.Records, &dto.ReceiveRecordResp{
					TransactionID:   record.TransactionID,
					ReceiverID:      record.UserID,
					ReceiverIMID:    record.UserImID,
					Amount:          record.Amount.String(),
					ReceivedAt:      record.ReceivedAt,
					TransactionType: fmt.Sprintf("%d", record.TransactionType),
				})
				transactionMap[record.TransactionID] = true
			}
		}

		// 添加发起记录（如果存在且未重复）
		if i < len(sentTransactions) {
			tx := sentTransactions[i]
			if _, exists := transactionMap[tx.TransactionID]; !exists {
				resp.Records = append(resp.Records, &dto.ReceiveRecordResp{
					TransactionID:   tx.TransactionID,
					ReceiverIMID:    tx.SenderImID,
					ReceiverID:      tx.SenderID,
					Amount:          tx.TotalAmount.String(),
					ReceivedAt:      tx.CreatedAt,
					TransactionType: fmt.Sprintf("%d", tx.TransactionType),
				})
				transactionMap[tx.TransactionID] = true
			}
		}
	}

	// 更新总记录数
	resp.Total = len(resp.Records)

	return resp, nil
}

// CheckTransactionCompleted 根据交易ID检查交易是否已完成
func (t *TransactionService) CheckTransactionCompleted(ctx context.Context, req *dto.CheckTransactionCompletedReq) (bool, bool, error) {
	// 使用通道并发查询
	type completedResult struct {
		completed bool
		err       error
	}

	type receivedResult struct {
		received bool
		err      error
	}

	// 并发查询交易是否完成和用户是否领取过
	completedChan := make(chan completedResult, 1)
	receivedChan := make(chan receivedResult, 1)

	// 协程1: 判断交易是否已完成（优先以数据库中交易状态/剩余金额为准，Redis 作为兜底加速）
	go func() {
		// 1）先查 Mongo 中交易记录，按交易状态/剩余金额判断是否已结束
		mongoCli := plugin.MongoCli().GetDB()
		transactionDao := model.NewTransactionDao(mongoCli)
		transaction, err := transactionDao.GetByTransactionID(ctx, req.TransactionID)
		if err != nil && !dbutil.IsDBNotFound(err) {
			log.ZError(ctx, "查询交易记录失败", err, "transaction_id", req.TransactionID)
			completedChan <- completedResult{false, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])}
			return
		}

		// 未找到交易记录，视为已结束（兼容历史数据被清理的情况）
		if transaction == nil {
			completedChan <- completedResult{true, nil}
			return
		}

		// 交易状态为“已完成”或“已过期”，视为整体已结束
		if transaction.Status == model.TransactionStatusComplete || transaction.Status == model.TransactionStatusExpired {
			completedChan <- completedResult{true, nil}
			return
		}

		// 若存在 RemainingAmount / RemainingCount 字段，任何一个为 0 也视为已结束
		//（注意：这里不关心 SelfReceived，仅表示红包整体是否还有可领金额/个数）
		remainingAmountDecimal, _ := decimal.NewFromString(transaction.RemainingAmount.String())
		if transaction.RemainingCount == 0 || !remainingAmountDecimal.GreaterThan(decimal.Zero) {
			completedChan <- completedResult{true, nil}
			return
		}

		// 2）兜底：再看 Redis 是否已经删除该交易 Key（老逻辑行为保持）
		redisCli := plugin.RedisCli()
		redisTransactionKey := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, req.TransactionID)
		exists, err := redisCli.Exists(ctx, redisTransactionKey).Result()
		if err != nil {
			log.ZWarn(ctx, "查询Redis交易信息失败, 使用DB结果", err, "transaction_id", req.TransactionID)
			completedChan <- completedResult{false, nil}
			return
		}

		if exists == 0 {
			completedChan <- completedResult{true, nil}
			return
		}

		// 剩余金额/个数不为 0，Redis 也仍然存在，认为交易尚未结束
		completedChan <- completedResult{false, nil}
	}()

	// 协程2: 直接查库判断用户是否领取
	go func() {
		if req.UserID == "" {
			receivedChan <- receivedResult{false, nil}
			return
		}

		// 直接查询MongoDB中用户是否有该交易的领取记录
		mongoCli := plugin.MongoCli().GetDB()
		receiveRecordDao := model.NewReceiveRecordDao(mongoCli)
		_, err := receiveRecordDao.GetByTransactionAndUser(ctx, req.TransactionID, req.UserID)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				// 用户未领取过
				receivedChan <- receivedResult{false, nil}
				return
			}
			// 其他错误，仍然返回未领取但不抛出错误
			log.ZWarn(ctx, "查询用户领取记录失败", err, "transaction_id", req.TransactionID, "user_id", req.UserID)
			receivedChan <- receivedResult{false, nil}
			return
		}

		// 找到领取记录，用户已领取
		receivedChan <- receivedResult{true, nil}
	}()

	// 等待两个协程完成并获取结果
	completedRes := <-completedChan
	receivedRes := <-receivedChan

	// 处理错误情况
	if completedRes.err != nil {
		return false, false, completedRes.err
	}

	// 返回结果
	return completedRes.completed, receivedRes.received, nil
}

// getReceiveTransactionType 根据交易类型获取接收记录类型
func getReceiveTransactionType(transactionType int) walletTransactionRecordModel.TsRecordType {
	switch transactionType {
	case model.TransactionTypeNormalPacket, model.TransactionTypeLuckyPacket, model.TransactionTypeP2PRedPacket, model.TransactionTypeGroupExclusive, model.TransactionTypePasswordPacket:
		return walletTransactionRecordModel.TsRecordTypeRedPacketReceive
	case model.TransactionTypeTransfer:
		return walletTransactionRecordModel.TsRecordTypeTransferReceive
	default:
		return walletTransactionRecordModel.TsRecordTypeTransferReceive
	}
}

// NewReceiveTransactionValidator 创建接收交易验证器
func NewReceiveTransactionValidator(mongoDB *mongo.Database) *ReceiveTransactionValidator {
	return &ReceiveTransactionValidator{
		mongoDB: mongoDB,
	}
}

// ValidateBasicParams 验证基础参数
func (v *ReceiveTransactionValidator) ValidateBasicParams(ctx context.Context, req *dto.ReceiveTransactionReq) error {
	// 验证交易ID
	if req.TransactionID == "" {
		log.ZError(ctx, "交易ID不能为空", nil, "receiver_id", req.ReceiverID)
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	// 验证接收者ID
	if req.ReceiverID == "" {
		log.ZError(ctx, "接收者ID不能为空", nil, "transaction_id", req.TransactionID)
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	// 验证组织ID
	if req.OrgID == "" {
		log.ZError(ctx, "组织ID不能为空", nil, "receiver_id", req.ReceiverID, "transaction_id", req.TransactionID)
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	return nil
}

// ValidateTransactionStatus 验证交易状态
func (v *ReceiveTransactionValidator) ValidateTransactionStatus(ctx context.Context, rtCtx *ReceiveTransactionContext) error {
	transactionDao := model.NewTransactionDao(v.mongoDB)

	// 根据交易ID获取交易记录
	transaction, err := transactionDao.GetByTransactionID(ctx, rtCtx.Req.TransactionID)
	if err != nil {
		log.ZError(ctx, "获取交易记录失败", err, "receiver_id", rtCtx.Req.ReceiverID, "transaction_id", rtCtx.Req.TransactionID)
		return errs.Wrap(err)
	}

	if transaction == nil {
		log.ZWarn(ctx, "交易记录不存在", nil, "receiver_id", rtCtx.Req.ReceiverID, "transaction_id", rtCtx.Req.TransactionID)
		return errs.NewCodeError(freeErrors.ErrTransactionNotFound, freeErrors.ErrorMessages[freeErrors.ErrTransactionNotFound])
	}

	if transaction.Status == model.TransactionStatusExpired {
		log.ZWarn(ctx, "交易已过期", nil, "receiver_id", rtCtx.Req.ReceiverID, "transaction_id", rtCtx.Req.TransactionID)
		return errs.NewCodeError(freeErrors.ErrTransactionExpired, freeErrors.ErrorMessages[freeErrors.ErrTransactionExpired])
	}

	if transaction.Status == model.TransactionStatusComplete {
		log.ZWarn(ctx, "交易已结束", nil, "receiver_id", rtCtx.Req.ReceiverID, "transaction_id", rtCtx.Req.TransactionID)
		return errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
	}

	// 如果交易类型为口令红包，则需要验证口令
	if transaction.TransactionType == model.TransactionTypePasswordPacket {
		if rtCtx.Req.Password == "" {
			log.ZWarn(ctx, "口令红包口令不能为空", nil, "receiver_id", rtCtx.Req.ReceiverID, "transaction_id", rtCtx.Req.TransactionID)
			return errs.NewCodeError(freeErrors.ErrPasswordCannotBeEmpty, freeErrors.ErrorMessages[freeErrors.ErrPasswordCannotBeEmpty])
		}
	}

	// 设置到上下文中
	rtCtx.Transaction = transaction
	return nil
}

// normalizeReceiverIDToOrgUserID 在入口处将 req.ReceiverID 统一为 organization_user.user_id（若传入的是 im_server_user_id 则解析并覆盖），保证 ReserveSlot/Redis/校验全流程使用同一 ID
func normalizeReceiverIDToOrgUserID(ctx context.Context, mongoDB *mongo.Database, req *dto.ReceiveTransactionReq) error {
	if req.ReceiverID == "" || req.OrgID == "" {
		return nil
	}
	orgUserDao := organizationModel.NewOrganizationUserDao(mongoDB)
	_, err := orgUserDao.GetByUserIdAndOrgID(ctx, req.ReceiverID, req.OrgID)
	if err == nil {
		return nil // 已是 organization_user.user_id，无需变更
	}
	if !errors.Is(err, mongo.ErrNoDocuments) && errs.Unwrap(err) != mongo.ErrNoDocuments {
		return err
	}
	// 按 im_server_user_id 查询，再校验 org_id（兼容生产环境无 GetByImServerUserIdAndOrgID 的 organization 版本）
	orgUser, imErr := orgUserDao.GetByUserIMServerUserId(ctx, req.ReceiverID)
	if imErr != nil || orgUser == nil {
		return nil // 保持原值，后续 ValidateReceiverInfo 会返回 10114
	}
	if orgUser.OrganizationId.Hex() != req.OrgID {
		return nil
	}
	req.ReceiverID = orgUser.UserId
	return nil
}

// ValidateReceiverInfo 验证接收者信息（调用前 req.ReceiverID 已在入口归一化为 organization_user.user_id）
func (v *ReceiveTransactionValidator) ValidateReceiverInfo(ctx context.Context, rtCtx *ReceiveTransactionContext) error {
	orgUserDao := organizationModel.NewOrganizationUserDao(v.mongoDB)

	// 1. 获取接收者的组织用户信息
	receiverOrgUser, err := orgUserDao.GetByUserIdAndOrgID(ctx, rtCtx.Req.ReceiverID, rtCtx.Req.OrgID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) || errs.Unwrap(err) == mongo.ErrNoDocuments {
			log.ZWarn(ctx, "接收者不属于该组织(查无此人)", nil, "receiver_id", rtCtx.Req.ReceiverID, "org_id", rtCtx.Req.OrgID)
			return errs.NewCodeError(freeErrors.ErrReceiverNotInOrganization, freeErrors.ErrorMessages[freeErrors.ErrReceiverNotInOrganization])
		}
		log.ZError(ctx, "获取接收者组织用户信息失败", err, "receiver_id", rtCtx.Req.ReceiverID, "org_id", rtCtx.Req.OrgID)
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 2. 检查接收者是否属于该组织
	if receiverOrgUser == nil {
		log.ZWarn(ctx, "接收者不属于该组织", nil, "receiver_id", rtCtx.Req.ReceiverID, "org_id", rtCtx.Req.OrgID)
		return errs.NewCodeError(freeErrors.ErrReceiverNotInOrganization, freeErrors.ErrorMessages[freeErrors.ErrReceiverNotInOrganization])
	}

	// 3. 获取发送者的组织用户信息
	senderOrgUser, err := orgUserDao.GetByUserIdAndOrgID(ctx, rtCtx.Transaction.SenderID, rtCtx.Transaction.OrgID)
	if err != nil {
		log.ZError(ctx, "获取发送者组织用户信息失败", err, "sender_id", rtCtx.Transaction.SenderID, "org_id", rtCtx.Transaction.OrgID)
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 4. 检查接收者和发送者是否在同一个组织
	if receiverOrgUser.OrganizationId.Hex() != senderOrgUser.OrganizationId.Hex() {
		log.ZWarn(ctx, "接收者和发送者不在同一组织", nil,
			"receiver_org", receiverOrgUser.OrganizationId.Hex(),
			"sender_org", senderOrgUser.OrganizationId.Hex())
		return errs.NewCodeError(freeErrors.ErrUserNotInSameOrganization, freeErrors.ErrorMessages[freeErrors.ErrUserNotInSameOrganization])
	}

	// 5. 根据交易类型检查发送者和接收者关系
	if receiverOrgUser.ImServerUserId == senderOrgUser.ImServerUserId {
		// 红包类型允许发送者领取自己的红包
		if rtCtx.Transaction.TransactionType == model.TransactionTypeNormalPacket ||
			rtCtx.Transaction.TransactionType == model.TransactionTypeLuckyPacket ||
			rtCtx.Transaction.TransactionType == model.TransactionTypeP2PRedPacket ||
			rtCtx.Transaction.TransactionType == model.TransactionTypeGroupExclusive ||
			rtCtx.Transaction.TransactionType == model.TransactionTypePasswordPacket {
		} else {
			// 转账类型不允许发送者接收
			log.ZWarn(ctx, "转账类型不允许发送者接收", nil, "user_im_id", receiverOrgUser.ImServerUserId, "transaction_type", rtCtx.Transaction.TransactionType)
			return errs.NewCodeError(freeErrors.ErrCannotReceiveOwnTransfer, freeErrors.ErrorMessages[freeErrors.ErrCannotReceiveOwnTransfer])
		}
	}

	// 6. 根据交易类型进行特定的关系验证
	verify := NewVerifyService()
	receiverImID := receiverOrgUser.ImServerUserId
	//senderImID := senderOrgUser.ImServerUserId
	skipGroupCheck := isStressSkipGroupCheck(ctx) // 压测接口会设置，跳过依赖 OpenIM 的群校验以施压

	switch rtCtx.Transaction.TransactionType {
	case model.TransactionTypeNormalPacket, model.TransactionTypeLuckyPacket:
		// 群红包：验证接收者是否为群成员（压测时跳过）
		if !skipGroupCheck {
			if err := verify.CheckGroupMembership(ctx, receiverImID, rtCtx.Transaction.TargetImID); err != nil {
				log.ZWarn(ctx, "接收者不在目标群组中", err, "receiver_im_id", receiverImID, "target_group", rtCtx.Transaction.TargetImID)
				return errs.NewCodeError(freeErrors.ErrNotInGroup, freeErrors.ErrorMessages[freeErrors.ErrNotInGroup])
			}
		}
	case model.TransactionTypePasswordPacket:
		// 群组口令红包：验证接收者是否为群成员 + 验证口令（压测时跳过群成员校验）
		if !skipGroupCheck {
			if err := verify.CheckGroupMembership(ctx, receiverImID, rtCtx.Transaction.TargetImID); err != nil {
				log.ZWarn(ctx, "接收者不在目标群组中", err, "receiver_im_id", receiverImID, "target_group", rtCtx.Transaction.TargetImID)
				return errs.NewCodeError(freeErrors.ErrNotInGroup, freeErrors.ErrorMessages[freeErrors.ErrNotInGroup])
			}
		}
		// 验证口令是否正确（严格区分大小写）
		if rtCtx.Req.Password != rtCtx.Transaction.Password {
			log.ZWarn(ctx, "口令红包密码错误", nil, "receiver_im_id", receiverImID, "transaction_id", rtCtx.Transaction.TransactionID)
			return errs.NewCodeError(freeErrors.ErrIncorrectPassword, freeErrors.ErrorMessages[freeErrors.ErrIncorrectPassword])
		}
	case model.TransactionTypeTransfer, model.TransactionTypeP2PRedPacket:
		// 转账和一对一红包：验证好友关系，并检查接收者是否为目标用户
		if receiverImID != rtCtx.Transaction.TargetImID {
			log.ZWarn(ctx, "接收者不是交易的目标用户", nil, "receiver_im_id", receiverImID, "target_im_id", rtCtx.Transaction.TargetImID)
			return errs.NewCodeError(freeErrors.ErrReceiverNotTargetUser, freeErrors.ErrorMessages[freeErrors.ErrReceiverNotTargetUser])
		}

		// 验证好友关系  好友关系不做验证
		//if err := verify.CheckFriendRelation(ctx, senderImID, receiverImID); err != nil {
		//	log.ZWarn(ctx, "发送者和接收者不是好友关系", err, "sender_im_id", senderImID, "receiver_im_id", receiverImID)
		//	return errs.NewCodeError(freeErrors.ErrNotFriend, "Sender and receiver are not friends")
		//}
	case model.TransactionTypeOrganization:
		// 组织转账：验证接收者是否为目标用户，且必须是管理员
		if receiverImID != rtCtx.Transaction.TargetImID {
			log.ZWarn(ctx, "接收者不是交易的目标用户", nil, "receiver_im_id", receiverImID, "target_im_id", rtCtx.Transaction.TargetImID)
			return errs.NewCodeError(freeErrors.ErrReceiverNotTargetUser, freeErrors.ErrorMessages[freeErrors.ErrReceiverNotTargetUser])
		}
		// 验证接收者是否为管理员
		if !organizationModel.IsOrgWebElevatedRole(receiverOrgUser.Role) {
			log.ZWarn(ctx, "组织转账的接收者必须是管理员或团队长", nil, "receiver_role", receiverOrgUser.Role)
			return errs.NewCodeError(freeErrors.ErrOrgTransferReceiverMustBeAdmin, freeErrors.ErrorMessages[freeErrors.ErrOrgTransferReceiverMustBeAdmin])
		}
	case model.TransactionTypeGroupExclusive:
		// 群组专属红包：验证接收者是否为专属接收者，且在群组中（压测时跳过群成员校验）
		if receiverImID != rtCtx.Transaction.ExclusiveReceiverImID {
			log.ZWarn(ctx, "接收者不是专属红包的指定接收者", nil, "receiver_im_id", receiverImID, "exclusive_receiver_im_id", rtCtx.Transaction.ExclusiveReceiverImID)
			return errs.NewCodeError(freeErrors.ErrReceiverNotExclusiveReceiver, freeErrors.ErrorMessages[freeErrors.ErrReceiverNotExclusiveReceiver])
		}
		if !skipGroupCheck {
			if err := verify.CheckGroupMembership(ctx, receiverImID, rtCtx.Transaction.TargetImID); err != nil {
				log.ZWarn(ctx, "专属接收者不在目标群组中", err, "receiver_im_id", receiverImID, "target_group", rtCtx.Transaction.TargetImID)
				return errs.NewCodeError(freeErrors.ErrNotInGroup, freeErrors.ErrorMessages[freeErrors.ErrNotInGroup])
			}
		}
	default:
		log.ZWarn(ctx, "未知的交易类型", nil, "transaction_type", rtCtx.Transaction.TransactionType)
		return errs.NewCodeError(freeErrors.ErrUnknownTransactionType, freeErrors.ErrorMessages[freeErrors.ErrUnknownTransactionType])
	}

	// 设置到上下文中
	rtCtx.ImServerUserID = receiverImID
	log.ZInfo(ctx, "接收者信息校验通过", "receiver_id", rtCtx.Req.ReceiverID, "transaction_id", rtCtx.Req.TransactionID, "transaction_type", rtCtx.Transaction.TransactionType)
	return nil
}

// ValidateWalletStatus 验证钱包状态
func (v *ReceiveTransactionValidator) ValidateWalletStatus(ctx context.Context, rtCtx *ReceiveTransactionContext) error {
	walletInfoDao := walletModel.NewWalletInfoDao(v.mongoDB)

	// 检查接收者钱包是否已开启
	isOpen, err := walletInfoDao.ExistByOwnerIdAndOwnerType(ctx, rtCtx.Req.ReceiverID, walletModel.WalletInfoOwnerTypeOrdinary)
	if !isOpen {
		if dbutil.IsDBNotFound(err) {
			log.ZWarn(ctx, "接收者钱包未开启", nil, "receiver_id", rtCtx.Req.ReceiverID)
			return errs.NewCodeError(freeErrors.WalletNotOpenCode, freeErrors.ErrorMessages[freeErrors.WalletNotOpenCode])
		}
		log.ZError(ctx, "获取接收者钱包信息失败", err, "receiver_id", rtCtx.Req.ReceiverID)
		return errs.NewCodeError(freeErrors.WalletNotOpenCode, freeErrors.ErrorMessages[freeErrors.WalletNotOpenCode])
	}

	// 获取钱包信息并设置到上下文
	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, rtCtx.Req.ReceiverID, walletModel.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		log.ZError(ctx, "获取接收者钱包信息失败", err, "receiver_id", rtCtx.Req.ReceiverID)
		return errs.NewCodeError(freeErrors.WalletNotOpenCode, freeErrors.ErrorMessages[freeErrors.WalletNotOpenCode])
	}

	rtCtx.WalletInfo = walletInfo
	return nil
}

// ValidateCurrencyPrecision 验证币种精度
func (v *ReceiveTransactionValidator) ValidateCurrencyPrecision(ctx context.Context, rtCtx *ReceiveTransactionContext) error {
	walletCurrencyDao := walletModel.NewWalletCurrencyDao(v.mongoDB)

	walletCurrency, err := walletCurrencyDao.GetById(ctx, rtCtx.Transaction.CurrencyId)
	if err != nil {
		log.ZError(ctx, "获取币种精度失败", err, "transaction_id", rtCtx.Req.TransactionID, "receiver_id", rtCtx.Req.ReceiverID, "currency_id", rtCtx.Transaction.CurrencyId)
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	rtCtx.CurrencyPrecision = int32(walletCurrency.Decimals)
	return nil
}

// NewReceiveTransactionLockManager 创建分布式锁管理器
func NewReceiveTransactionLockManager(redisClient redis.UniversalClient) *ReceiveTransactionLockManager {
	return &ReceiveTransactionLockManager{
		redisClient: redisClient,
	}
}

// AcquireLock 获取分布式锁
func (l *ReceiveTransactionLockManager) AcquireLock(ctx context.Context, rtCtx *ReceiveTransactionContext) error {
	rtCtx.LockKey = fmt.Sprintf("lock:transaction:%s", rtCtx.Req.TransactionID)
	rtCtx.LockValue = fmt.Sprintf("%d", rtCtx.NowUTC.UnixNano())

	// 【关键修复】使用独立的 context，避免请求 context 被取消导致锁获取失败
	// 设置5秒超时用于Redis操作，锁本身30秒过期避免死锁
	lockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	success, err := l.redisClient.SetNX(lockCtx, rtCtx.LockKey, rtCtx.LockValue, 30*time.Second).Result()
	if err != nil {
		log.ZError(ctx, "获取Redis分布式锁失败", err, "receiver_id", rtCtx.Req.ReceiverID, "transaction_id", rtCtx.Req.TransactionID)
		return errs.Wrap(err)
	}
	if !success {
		log.ZWarn(ctx, "交易正在被其他请求处理", nil, "receiver_id", rtCtx.Req.ReceiverID, "transaction_id", rtCtx.Req.TransactionID)
		return errs.NewCodeError(freeErrors.ErrDistributedLock, freeErrors.ErrorMessages[freeErrors.ErrDistributedLock])
	}

	return nil
}

// ReleaseLock 释放分布式锁
func (l *ReceiveTransactionLockManager) ReleaseLock(ctx context.Context, rtCtx *ReceiveTransactionContext) {
	// 使用Lua脚本保证原子性释放锁
	l.redisClient.Eval(ctx, ReleaseLockLuaScript, []string{rtCtx.LockKey}, rtCtx.LockValue)
}

// NewReceiveTransactionRedisProcessor 创建Redis处理器
func NewReceiveTransactionRedisProcessor(redisClient redis.UniversalClient) *ReceiveTransactionRedisProcessor {
	return &ReceiveTransactionRedisProcessor{
		redisClient: redisClient,
	}
}

// InitializeKeys 初始化Redis键名
func (r *ReceiveTransactionRedisProcessor) InitializeKeys(rtCtx *ReceiveTransactionContext) {
	rtCtx.RedisTransactionKey = fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, rtCtx.Req.TransactionID)
	rtCtx.RedisReceiversKey = fmt.Sprintf("%s%s", constant.TransactionReceiversPrefix, rtCtx.Req.TransactionID)
}

// ProcessTransaction 处理交易逻辑
func (r *ReceiveTransactionRedisProcessor) ProcessTransaction(ctx context.Context, rtCtx *ReceiveTransactionContext) (*LuaScriptResult, error) {
	// 添加超时控制，避免长时间等待
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 执行Lua脚本
	result, err := r.redisClient.Eval(ctxWithTimeout, ReceiveTransactionLuaScript, []string{
		rtCtx.RedisTransactionKey,
		rtCtx.RedisReceiversKey,
	}, []interface{}{
		rtCtx.Req.ReceiverID,
		rtCtx.NowUTC.Unix(),
		constant.TransactionExpireTime,
		rtCtx.CurrencyPrecision,
	}).Result()

	if err != nil {
		log.ZError(ctx, "执行Lua脚本失败", err, "transaction_id", rtCtx.Req.TransactionID, "receiver_id", rtCtx.Req.ReceiverID, "precision", rtCtx.CurrencyPrecision)
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 解析脚本结果
	resultStr, ok := result.(string)
	if !ok {
		log.ZError(ctx, "Lua脚本返回结果类型错误", nil, "result_type", fmt.Sprintf("%T", result), "result", result)
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	var resultData LuaScriptResult
	if err := json.Unmarshal([]byte(resultStr), &resultData); err != nil {
		log.ZError(ctx, "解析Lua脚本返回结果失败", err, "result", resultStr, "transaction_id", rtCtx.Req.TransactionID)
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 检查是否有错误
	if resultData.Status == "error" && resultData.Err != "" {
		log.ZWarn(ctx, "Lua脚本执行返回错误", nil, "lua_error", resultData.Err)
		switch resultData.Err {
		case "TRANSACTION_NOT_FOUND":
			return nil, errs.NewCodeError(freeErrors.ErrTransactionNotFound, freeErrors.ErrorMessages[freeErrors.ErrTransactionNotFound])
		case "TRANSACTION_EXPIRED":
			return nil, errs.NewCodeError(freeErrors.ErrTransactionExpired, freeErrors.ErrorMessages[freeErrors.ErrTransactionExpired])
		case "ALREADY_RECEIVED":
			return nil, errs.NewCodeError(freeErrors.ErrAlreadyReceived, freeErrors.ErrorMessages[freeErrors.ErrAlreadyReceived])
		case "NO_REMAINING":
			return nil, errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
		default:
			return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
		}
	}

	// 确保接收金额有效
	if resultData.ReceiveAmount == "" || resultData.ReceiveAmount == "nil" || resultData.ReceiveAmount == "0" {
		log.ZError(ctx, "接收金额无效", nil, "receive_amount", resultData.ReceiveAmount)
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	return &resultData, nil
}

// RollbackTransaction 回滚交易
func (r *ReceiveTransactionRedisProcessor) RollbackTransaction(ctx context.Context, rtCtx *ReceiveTransactionContext, receiveAmount, remainingCount string) {
	// 执行回滚脚本
	_, rollbackErr := r.redisClient.Eval(ctx, RollbackTransactionLuaScript, []string{
		rtCtx.RedisTransactionKey,
		rtCtx.RedisReceiversKey,
	}, []interface{}{
		rtCtx.Req.ReceiverID,
		receiveAmount,
		remainingCount,
		rtCtx.CurrencyPrecision,
	}).Result()

	if rollbackErr != nil {
		log.ZError(ctx, "Redis回滚操作失败", rollbackErr, "transaction_id", rtCtx.Req.TransactionID, "receiver_id", rtCtx.Req.ReceiverID)
	} else {
		log.ZInfo(ctx, "Redis状态已成功回滚", "transaction_id", rtCtx.Req.TransactionID, "receiver_id", rtCtx.Req.ReceiverID)
	}
}

// CleanupRedisKeys 清理Redis键
func (r *ReceiveTransactionRedisProcessor) CleanupRedisKeys(ctx context.Context, rtCtx *ReceiveTransactionContext, remainingCount string) {
	// 如果剩余个数为0，删除Redis数据结构
	remainingCountInt := 0
	fmt.Sscanf(remainingCount, "%d", &remainingCountInt)
	if remainingCountInt == 0 {
		if err := r.redisClient.Del(ctx, rtCtx.RedisTransactionKey, rtCtx.RedisReceiversKey).Err(); err != nil {
			log.ZWarn(ctx, "删除交易Redis键失败", err, "transaction_id", rtCtx.Req.TransactionID, "keys", []string{rtCtx.RedisTransactionKey, rtCtx.RedisReceiversKey})
		}
	}
}

// NewReceiveTransactionDBProcessor 创建数据库处理器
func NewReceiveTransactionDBProcessor(mongoDB *mongo.Database) *ReceiveTransactionDBProcessor {
	return &ReceiveTransactionDBProcessor{
		mongoDB:          mongoDB,
		transactionDao:   model.NewTransactionDao(mongoDB),
		receiveRecordDao: model.NewReceiveRecordDao(mongoDB),
		walletDao:        walletModel.NewWalletBalanceDao(mongoDB),
		walletInfoDao:    walletModel.NewWalletInfoDao(mongoDB),
	}
}

// ProcessDBTransaction 处理数据库事务
func (d *ReceiveTransactionDBProcessor) ProcessDBTransaction(ctx context.Context, rtCtx *ReceiveTransactionContext, result *LuaScriptResult) error {
	session, err := d.mongoDB.Client().StartSession()
	if err != nil {
		log.ZError(ctx, "开启MongoDB会话失败", err, "receiver_id", rtCtx.Req.ReceiverID, "transaction_id", rtCtx.Req.TransactionID)
		return errs.Wrap(err)
	}
	defer session.EndSession(ctx)

	// 在事务中执行操作
	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		// 将金额字符串转换为Decimal128
		amountDecimal, err := decimal.NewFromString(result.ReceiveAmount)
		if err != nil {
			log.ZError(sessCtx, "金额格式转换失败", err, "amount", result.ReceiveAmount)
			return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
		}

		// 使用安全转换函数避免0E-6176问题
		amountDecimal128, err := SafeParseDecimal128(amountDecimal)
		if err != nil {
			log.ZError(sessCtx, "金额转换为Decimal128失败", err, "amount", amountDecimal.String())
			return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
		}

		// 创建领取记录
		receiveRecord := &model.ReceiveRecord{
			TransactionID:   rtCtx.Req.TransactionID,
			UserID:          rtCtx.Req.ReceiverID,
			UserImID:        rtCtx.ImServerUserID,
			Amount:          amountDecimal128,
			ReceivedAt:      rtCtx.NowUTC,
			TransactionType: rtCtx.Transaction.TransactionType,
		}

		// 减少发送者冻结余额
		if err := d.updateSenderBalance(sessCtx, rtCtx, amountDecimal); err != nil {
			return nil, err
		}

		// 增加接收者可用余额
		if err := d.updateReceiverBalance(sessCtx, rtCtx, amountDecimal); err != nil {
			return nil, err
		}

		// 插入领取记录
		if err = d.receiveRecordDao.Create(sessCtx, receiveRecord); err != nil {
			log.ZError(sessCtx, "插入领取记录失败", err, "receiver_id", rtCtx.Req.ReceiverID, "transaction_id", rtCtx.Req.TransactionID)
			return nil, errs.Wrap(err)
		}

		// 【关键修复-高并发安全】使用原子递减操作更新剩余金额和个数
		// 而不是使用 Redis 返回的值直接 $set，避免高并发下的覆盖问题
		amountDecrement128, err := SafeParseDecimal128(amountDecimal)
		if err != nil {
			log.ZError(sessCtx, "领取金额转换为Decimal128失败", err, "amount", amountDecimal.String())
			return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
		}

		// 使用原子递减（$inc）更新，而不是 $set
		success, newRemainingCount, err := d.transactionDao.DecrementRemainingAmountAndCount(sessCtx, rtCtx.Req.TransactionID, amountDecrement128)
		if err != nil {
			log.ZError(sessCtx, "原子递减交易剩余金额和个数失败", err, "transaction_id", rtCtx.Req.TransactionID)
			return nil, err
		}

		if !success {
			// 更新失败（可能 remaining_count 已经是 0）
			log.ZWarn(sessCtx, "原子递减失败，可能 remaining_count 已为0",
				nil, "transaction_id", rtCtx.Req.TransactionID, "receiver_id", rtCtx.Req.ReceiverID)
			// 但领取记录和余额已经更新，这种情况需要特殊处理
			// 检查是否应该标记为完成
		}

		// 如果递减后剩余数量为 0，更新交易状态为完成
		if newRemainingCount == 0 {
			if err := d.transactionDao.UpdateTransactionComplete(sessCtx, rtCtx.Req.TransactionID); err != nil {
				log.ZWarn(sessCtx, "更新交易状态为完成失败（非关键错误）", err, "transaction_id", rtCtx.Req.TransactionID)
				// 不返回错误，因为领取已经成功
			} else {
				log.ZInfo(sessCtx, "交易已完成，剩余个数为0", "transaction_id", rtCtx.Req.TransactionID)
			}
		}

		return nil, nil
	})

	return err
}

// updateSenderBalance 更新发送者余额
func (d *ReceiveTransactionDBProcessor) updateSenderBalance(sessCtx mongo.SessionContext, rtCtx *ReceiveTransactionContext, amountDecimal decimal.Decimal) error {
	switch rtCtx.Transaction.TransactionType {
	case model.TransactionTypeNormalPacket, model.TransactionTypeLuckyPacket, model.TransactionTypeP2PRedPacket, model.TransactionTypeGroupExclusive, model.TransactionTypePasswordPacket:
		// 红包类型，减少红包冻结余额
		if err := d.walletDao.UpdateRedPacketFrozenBalance(sessCtx, rtCtx.Transaction.WalletID, rtCtx.Transaction.CurrencyId, amountDecimal.Neg()); err != nil {
			log.ZError(sessCtx, "更新发送者红包冻结余额失败", err, "user_id", rtCtx.Transaction.SenderID, "amount", amountDecimal.Neg().String())
			return err
		}
	case model.TransactionTypeTransfer:
		// 转账类型，减少转账冻结余额
		if err := d.walletDao.UpdateTransferFrozenBalance(sessCtx, rtCtx.Transaction.WalletID, rtCtx.Transaction.CurrencyId, amountDecimal.Neg()); err != nil {
			log.ZError(sessCtx, "更新发送者转账冻结余额失败", err, "user_id", rtCtx.Transaction.SenderID, "amount", amountDecimal.Neg().String())
			return err
		}
	}
	return nil
}

// updateReceiverBalance 更新接收者余额
func (d *ReceiveTransactionDBProcessor) updateReceiverBalance(sessCtx mongo.SessionContext, rtCtx *ReceiveTransactionContext, amountDecimal decimal.Decimal) error {
	// 增加接收者可用余额
	return d.walletDao.UpdateAvailableBalanceAndAddTsRecord(
		sessCtx,
		rtCtx.WalletInfo.ID,
		rtCtx.Transaction.CurrencyId,
		amountDecimal,
		getReceiveTransactionType(rtCtx.Transaction.TransactionType),
		rtCtx.Transaction.SenderID,
		rtCtx.Transaction.Greeting)
}

// NewReceiveTransactionManager 创建接收交易管理器
func NewReceiveTransactionManager(mongoDB *mongo.Database, redisClient redis.UniversalClient) *ReceiveTransactionManager {
	return &ReceiveTransactionManager{
		validator:      NewReceiveTransactionValidator(mongoDB),
		lockManager:    NewReceiveTransactionLockManager(redisClient),
		redisProcessor: NewReceiveTransactionRedisProcessor(redisClient),
		dbProcessor:    NewReceiveTransactionDBProcessor(mongoDB),
	}
}

// ProcessReceiveTransaction 处理接收交易的完整流程
func (m *ReceiveTransactionManager) ProcessReceiveTransaction(ctx context.Context, req *dto.ReceiveTransactionReq) (string, error) {
	// 创建上下文
	rtCtx := &ReceiveTransactionContext{
		Req:    req,
		NowUTC: time.Now().UTC(),
	}

	// 记录开始处理交易
	log.ZInfo(ctx, "开始处理交易", "receiver_id", req.ReceiverID, "transaction_id", req.TransactionID, "time", rtCtx.NowUTC.Format(time.RFC3339))

	// 1. 基础参数校验
	if err := m.validator.ValidateBasicParams(ctx, req); err != nil {
		return "", err
	}

	// 2. 获取分布式锁（与业务一致，保证每次领取串行写库：领取记录、transaction 剩余、发送方冻结）
	if err := m.lockManager.AcquireLock(ctx, rtCtx); err != nil {
		return "", err
	}
	defer m.lockManager.ReleaseLock(ctx, rtCtx)

	// 3. 验证交易状态
	if err := m.validator.ValidateTransactionStatus(ctx, rtCtx); err != nil {
		return "", err
	}

	// 4. 验证接收者信息
	if err := m.validator.ValidateReceiverInfo(ctx, rtCtx); err != nil {
		return "", err
	}
	// 5. 验证钱包状态
	if err := m.validator.ValidateWalletStatus(ctx, rtCtx); err != nil {
		return "", err
	}

	// 6. 验证币种精度
	if err := m.validator.ValidateCurrencyPrecision(ctx, rtCtx); err != nil {
		return "", err
	}

	// 7. 初始化Redis键名
	m.redisProcessor.InitializeKeys(rtCtx)

	// 8. 处理Redis交易逻辑
	result, err := m.redisProcessor.ProcessTransaction(ctx, rtCtx)
	if err != nil {
		return "", err
	}

	// 9. 处理数据库
	if err := m.dbProcessor.ProcessDBTransaction(ctx, rtCtx, result); err != nil {
		log.ZError(ctx, "事务执行失败", err, "receiver_id", req.ReceiverID, "transaction_id", req.TransactionID)
		m.redisProcessor.RollbackTransaction(ctx, rtCtx, result.ReceiveAmount, result.RemainingCount)
		return "", err
	}

	// 10. 清理Redis键（如果交易完成）
	m.redisProcessor.CleanupRedisKeys(ctx, rtCtx, result.RemainingCount)

	log.ZInfo(ctx, "交易处理成功", "receiver_id", req.ReceiverID, "transaction_id", req.TransactionID, "amount", result.ReceiveAmount, "time", rtCtx.NowUTC.Format(time.RFC3339))

	// 发送领取通知（异步）
	sendRedPacketNotification(ctx, rtCtx, result.ReceiveAmount)

	return result.ReceiveAmount, nil
}

// CreateTransactionRecord 创建交易记录的公共方法
func (t *TransactionService) CreateTransactionRecord(ctx context.Context, sessCtx mongo.SessionContext,
	transactionID, senderID, senderImID, targetImID, orgID string, currencyId primitive.ObjectID, greeting string,
	transactionType int, amount decimal.Decimal, walletID primitive.ObjectID) error {

	mongoCli := plugin.MongoCli().GetDB()
	transactionDao := model.NewTransactionDao(mongoCli)
	nowUTC := time.Now().UTC()

	// 使用安全转换函数避免0E-6176问题
	amountDecimal128, err := SafeParseDecimal128(amount)
	if err != nil {
		log.ZError(ctx, "金额转换为Decimal128失败", err, "amount", amount.String())
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	zero, _ := primitive.ParseDecimal128("0")
	// 创建交易记录
	transaction := &model.Transaction{
		TransactionID:   transactionID,
		SenderID:        senderID,
		SenderImID:      senderImID,
		TargetImID:      targetImID,
		TransactionType: transactionType,
		OrgID:           orgID,
		TotalAmount:     amountDecimal128,
		TotalCount:      1,    // 组织转账固定为1
		RemainingAmount: zero, // 已完成，剩余为0
		RemainingCount:  0,
		WalletID:        walletID,
		CurrencyId:      currencyId,
		Greeting:        greeting,
		Status:          model.TransactionStatusComplete, // 组织转账直接完成
		CreatedAt:       nowUTC,
	}

	// 创建交易记录
	if err := transactionDao.Create(sessCtx, transaction); err != nil {
		log.ZError(ctx, "创建交易记录失败", err, "transaction_id", transactionID)
		return err
	}

	return nil
}

// InternalOrganizationSignInRewardTransfer 内部组织签到奖励转账方法
func (t *TransactionService) InternalOrganizationSignInRewardTransfer(ctx context.Context, orgID, receiverImID, currencyId, amount, greeting string) (string, error) {
	mongoCli := plugin.MongoCli().GetDB()
	nowUTC := time.Now().UTC()

	log.ZInfo(ctx, "开始内部组织签到奖励转账",
		"org_id", orgID,
		"receiver_im_id", receiverImID,
		"currency_id", currencyId,
		"amount", amount)

	// 1. 验证基础参数
	if orgID == "" || receiverImID == "" || currencyId == "" || amount == "" {
		log.ZError(ctx, "内部组织签到奖励转账参数不完整", nil, "org_id", orgID, "receiver_im_id", receiverImID, "currency_id", currencyId, "amount", amount)
		return "", errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	// 2. 验证金额格式
	amountDecimal, err := decimal.NewFromString(amount)
	if err != nil || amountDecimal.LessThanOrEqual(decimal.Zero) {
		log.ZError(ctx, "金额格式无效", err, "amount", amount)
		return "", errs.NewCodeError(freeErrors.ErrInvalidAmount, freeErrors.ErrorMessages[freeErrors.ErrInvalidAmount])
	}

	// 3. 转换组织ID
	orgId, err := primitive.ObjectIDFromHex(orgID)
	if err != nil {
		log.ZError(ctx, "组织ID格式无效", err, "org_id", orgID)
		return "", errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	// 转换币种ID
	currencyObjectId, err := primitive.ObjectIDFromHex(currencyId)
	if err != nil {
		log.ZError(ctx, "币种ID格式无效", err, "currency_id", currencyId)
		return "", errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	// 4. 获取接收方的组织用户信息
	orgUserDao := organizationModel.NewOrganizationUserDao(mongoCli)
	receiverOrgUser, err := orgUserDao.GetByUserIMServerUserId(ctx, receiverImID)
	if err != nil {
		log.ZError(ctx, "获取接收方组织用户信息失败", err, "receiver_im_id", receiverImID)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 5. 验证接收方是否属于指定组织
	if receiverOrgUser.OrganizationId != orgId {
		log.ZError(ctx, "接收方不属于指定组织", nil,
			"receiver_org_id", receiverOrgUser.OrganizationId.Hex(),
			"target_org_id", orgID)
		return "", errs.NewCodeError(freeErrors.ErrInvalidParams, "接收方不属于指定组织")
	}

	// 6. 验证币种是否属于当前组织
	walletCurrencyDao := walletModel.NewWalletCurrencyDao(mongoCli)
	currencyExists, err := walletCurrencyDao.ExistByIdAndOrgID(ctx, currencyObjectId, orgId)
	if err != nil {
		log.ZError(ctx, "验证币种失败", err, "currency_id", currencyId, "org_id", orgID)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	if !currencyExists {
		log.ZError(ctx, "币种不属于当前组织", nil, "currency_id", currencyId, "org_id", orgID)
		return "", errs.NewCodeError(freeErrors.ErrInvalidParams, "币种不属于当前组织")
	}

	// 7. 获取组织钱包信息
	walletInfoDao := walletModel.NewWalletInfoDao(mongoCli)
	orgWalletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, orgID, walletModel.WalletInfoOwnerTypeOrganization)
	if err != nil {
		log.ZError(ctx, "获取组织钱包信息失败", err, "org_id", orgID)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 8. 获取接收方钱包信息
	receiverWalletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, receiverOrgUser.UserId, walletModel.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			log.ZWarn(ctx, "接收方钱包未开启", nil, "receiver_id", receiverOrgUser.UserId)
			return "", errs.NewCodeError(freeErrors.WalletNotOpenCode, freeErrors.ErrorMessages[freeErrors.WalletNotOpenCode])
		}
		log.ZError(ctx, "获取接收方钱包信息失败", err, "receiver_id", receiverOrgUser.UserId)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 9. 生成交易ID
	transactionID := fmt.Sprintf("%d%s%s", model.TransactionTypeOrganizationSignInReward, nowUTC.Format("20060102150405"), strings.Replace(uuid.New().String(), "-", "", -1)[:8])

	// 10. 执行转账事务
	session, err := mongoCli.Client().StartSession()
	if err != nil {
		log.ZError(ctx, "创建MongoDB会话失败", err, "transaction_id", transactionID)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		walletDao := walletModel.NewWalletBalanceDao(mongoCli)

		// 扣减组织账户余额 - 使用签到奖励支出类型
		if err := walletDao.UpdateAvailableBalanceAndAddTsRecord(
			sessCtx,
			orgWalletInfo.ID,
			currencyObjectId,
			amountDecimal.Neg(), // 金额取负值，表示减少
			walletTransactionRecordModel.TsRecordTypeSignInRewardExpense,
			"",
			greeting); err != nil {
			log.ZError(sessCtx, "扣减组织账户余额失败", err, "org_id", orgID, "amount", amountDecimal.Neg().String())
			return nil, err
		}

		// 增加接收方余额 - 使用签到奖励领取类型
		if err := walletDao.UpdateAvailableBalanceAndAddTsRecord(
			sessCtx,
			receiverWalletInfo.ID,
			currencyObjectId,
			amountDecimal,
			walletTransactionRecordModel.TsRecordTypeSignInRewardReceive,
			"",
			greeting); err != nil {
			log.ZError(sessCtx, "增加接收方余额失败", err, "receiver_id", receiverOrgUser.UserId, "amount", amountDecimal.String())
			return nil, err
		}

		// 查询组织的第一个超级管理员作为发送者
		senderOrgUser, err := orgUserDao.GetFirstSuperAdminByOrgId(ctx, orgId)
		if err != nil {
			log.ZError(sessCtx, "查询组织超级管理员失败", err, "org_id", orgID)
			return nil, err
		}

		// 创建交易记录 - 使用组织签到奖励转账类型
		if err := t.CreateTransactionRecord(ctx, sessCtx,
			transactionID, senderOrgUser.UserId, senderOrgUser.ImServerUserId, receiverImID, orgID, currencyObjectId, greeting,
			model.TransactionTypeOrganizationSignInReward, amountDecimal, orgWalletInfo.ID); err != nil {
			return nil, err
		}

		log.ZInfo(ctx, "内部组织签到奖励转账完成",
			"transaction_id", transactionID,
			"org_id", orgID,
			"receiver_im_id", receiverImID,
			"amount", amountDecimal.String())

		return transactionID, nil
	})

	if err != nil {
		log.ZError(ctx, "内部组织签到奖励转账事务执行失败", err, "transaction_id", transactionID)
		return "", err
	}

	return transactionID, nil
}

// InternalOrganizationSignInRewardReversal 签到奖励冲回：从用户扣减金额并加回组织（用于错误发放的阶段奖励清理）
func (t *TransactionService) InternalOrganizationSignInRewardReversal(ctx context.Context, orgID string, receiverImID string, currencyId string, amount string) error {
	mongoDB := plugin.MongoCli().GetDB()
	if orgID == "" || receiverImID == "" || currencyId == "" || amount == "" {
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}
	amountDecimal, err := decimal.NewFromString(amount)
	if err != nil || amountDecimal.LessThanOrEqual(decimal.Zero) {
		return errs.NewCodeError(freeErrors.ErrInvalidAmount, freeErrors.ErrorMessages[freeErrors.ErrInvalidAmount])
	}
	currencyObjectId, err := primitive.ObjectIDFromHex(currencyId)
	if err != nil {
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	orgUserDao := organizationModel.NewOrganizationUserDao(mongoDB)
	receiverOrgUser, err := orgUserDao.GetByImServerUserIdAndOrgID(ctx, receiverImID, orgID)
	if err != nil {
		return err
	}

	walletInfoDao := walletModel.NewWalletInfoDao(mongoDB)
	orgWalletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, orgID, walletModel.WalletInfoOwnerTypeOrganization)
	if err != nil {
		return err
	}
	receiverWalletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, receiverOrgUser.UserId, walletModel.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return freeErrors.WalletNotOpenErr
		}
		return err
	}

	session, err := mongoDB.Client().StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		walletDao := walletModel.NewWalletBalanceDao(mongoDB)
		// 用户扣减（冲回）
		if err := walletDao.UpdateAvailableBalanceAndAddTsRecord(
			sessCtx,
			receiverWalletInfo.ID,
			currencyObjectId,
			amountDecimal.Neg(),
			walletTransactionRecordModel.TsRecordTypeSignInRewardRefund,
			"checkin_reward_cleanup",
			"签到奖励冲回"); err != nil {
			return nil, err
		}
		// 组织加回
		if err := walletDao.UpdateAvailableBalanceAndAddTsRecord(
			sessCtx,
			orgWalletInfo.ID,
			currencyObjectId,
			amountDecimal,
			walletTransactionRecordModel.TsRecordTypeSignInRewardRefundIncome,
			"checkin_reward_cleanup",
			"签到奖励冲回"); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

func (s *TransactionService) GetOrgTransactionList(ctx context.Context, req *dto.ListOrgTransactions) (*paginationUtils.ListResp[*dto.TransactionOrgListResp], error) {
	transactionDao := model.NewTransactionDao(plugin.MongoCli().GetDB())

	total, transactions, err := transactionDao.ListTransactions(ctx, req)
	if err != nil {
		log.ZError(ctx, "获取组织交易列表失败", err, "org_id", req.OrgID)
		return &paginationUtils.ListResp[*dto.TransactionOrgListResp]{}, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	if total == 0 {
		return &paginationUtils.ListResp[*dto.TransactionOrgListResp]{}, nil
	}
	result := &paginationUtils.ListResp[*dto.TransactionOrgListResp]{
		Total: total,
		List:  []*dto.TransactionOrgListResp{},
	}
	for _, transaction := range transactions {
		result.List = append(result.List, &dto.TransactionOrgListResp{
			TransactionID:   transaction.Base.TransactionID,   // 交易ID
			Status:          transaction.Base.Status,          // 交易状态
			Currency:        transaction.Currency,             // 币种
			Sender:          transaction.SenderName,           // 发送者名称
			Receiver:        transaction.ReceiverName,         // 接收者名称
			Greeting:        transaction.Base.Greeting,        // 交易信息
			TotalAmount:     transaction.Base.TotalAmount,     // 交易金额
			RemainingAmount: transaction.Base.RemainingAmount, // 剩余金额
			TotalCount:      transaction.Base.TotalCount,      // 交易个数
			RemainingCount:  transaction.Base.RemainingCount,  // 剩余个数
			CreatedAt:       transaction.Base.CreatedAt,       // 创建时间
		})

	}
	return result, nil
}

// QueryTransactionRecords 查询交易记录（带用户信息，根据组织过滤）
func (w *TransactionService) QueryTransactionRecords(ctx context.Context, req *dto.TransactionRecordQueryReq, orgID string) (*dto.TransactionRecordQueryResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	transactionDao := model.NewTransactionDao(db)

	// 解析时间参数
	var startTime, endTime *time.Time
	if req.StartTime != "" {

		timeInt, err := strconv.ParseInt(req.StartTime, 10, 64)
		if err != nil {
			return nil, freeErrors.ParameterInvalidErr
		}
		t := time.Unix(timeInt, 0).UTC()
		startTime = &t
	}
	if req.EndTime != "" {

		timeInt, err := strconv.ParseInt(req.EndTime, 10, 64)
		if err != nil {
			return nil, freeErrors.ParameterInvalidErr
		}
		t := time.Unix(timeInt, 0).UTC()
		endTime = &t
	}

	// 使用嵌入的分页参数
	page := &req.DepPagination

	// 查询交易记录
	total, transactions, err := transactionDao.QueryTransactionRecordsWithUserInfo(ctx, orgID, req.Keyword, startTime, endTime, req.Status, req.TransactionType, page)
	if err != nil {
		return nil, err
	}

	// 转换为响应格式
	records := make([]*dto.TransactionRecordWithUserInfo, 0, len(transactions))
	for _, transaction := range transactions {
		// 计算到期时间：创建时间 + 24小时
		expireAt := transaction.CreatedAt.Add(time.Duration(constant.TransactionExpireTime) * time.Second)

		// 确保完成状态的交易剩余金额和数量为0，解决高并发下的数据不一致问题
		remainingAmount := transaction.RemainingAmount
		remainingCount := transaction.RemainingCount

		// 如果交易状态为已完成(status=1)，则强制设置剩余金额和数量为0
		if transaction.Status == 1 {
			zeroAmount, _ := primitive.ParseDecimal128("0")
			remainingAmount = zeroAmount
			remainingCount = 0
		}

		record := &dto.TransactionRecordWithUserInfo{
			TransactionID:   transaction.TransactionID,
			Status:          transaction.Status,
			TransactionType: transaction.TransactionType,
			TotalAmount:     trimAmountZeros(transaction.TotalAmount.String()),
			RemainingAmount: trimAmountZeros(remainingAmount.String()),
			TotalCount:      transaction.TotalCount,
			RemainingCount:  remainingCount,
			Greeting:        transaction.Greeting,
			CreatedAt:       transaction.CreatedAt,
			UpdatedAt:       transaction.UpdatedAt,
			ExpireAt:        expireAt,
		}

		// 提取货币信息
		if currency, ok := transaction.Currency["name"]; ok {
			if currencyName, ok := currency.(string); ok {
				record.Currency = currencyName
			}
		}

		// 提取发送者信息
		//record.Sender = &dto.TransactionUserInfo{}
		//if senderUser, ok := transaction.SenderUser["user_id"]; ok {
		//	if userId, ok := senderUser.(string); ok {
		//		record.Sender.ImServerID = userId
		//	}
		//}
		//if senderUser, ok := transaction.SenderUser["nickname"]; ok {
		//	if nickname, ok := senderUser.(string); ok {
		//		record.Sender.Nickname = nickname
		//	}
		//}
		//if senderUser, ok := transaction.SenderUser["face_url"]; ok {
		//	if faceUrl, ok := senderUser.(string); ok {
		//		record.Sender.FaceURL = faceUrl
		//	}
		//}
		//if senderAttr, ok := transaction.SenderAttr["account"]; ok {
		//	if account, ok := senderAttr.(string); ok {
		//		record.Sender.Account = account
		//	}
		//}
		//if senderAttr, ok := transaction.SenderAttr["user_id"]; ok {
		//	if userId, ok := senderAttr.(string); ok {
		//		record.Sender.UserID = userId
		//	}
		//}

		// 提取接收者信息（如果是转账）
		//if transaction.TransactionType == 1 { // 假设1是转账类型
		//	record.Receiver = &dto.TransactionUserInfo{}
		//	if receiverUser, ok := transaction.ReceiverUser["user_id"]; ok {
		//		if userId, ok := receiverUser.(string); ok {
		//			record.Receiver.ImServerID = userId
		//		}
		//	}
		//	if receiverUser, ok := transaction.ReceiverUser["nickname"]; ok {
		//		if nickname, ok := receiverUser.(string); ok {
		//			record.Receiver.Nickname = nickname
		//		}
		//	}
		//	if receiverUser, ok := transaction.ReceiverUser["face_url"]; ok {
		//		if faceUrl, ok := receiverUser.(string); ok {
		//			record.Receiver.FaceURL = faceUrl
		//		}
		//	}
		//	// 处理ReceiverAttr（通过子查询获取）
		//	if receiverAttrData, ok := transaction.ReceiverAttr["account"]; ok {
		//		if account, ok := receiverAttrData.(string); ok {
		//			record.Receiver.Account = account
		//		}
		//	}
		//	if receiverAttrUserId, ok := transaction.ReceiverAttr["user_id"]; ok {
		//		if userId, ok := receiverAttrUserId.(string); ok {
		//			record.Receiver.UserID = userId
		//		}
		//	}
		//}

		records = append(records, record)
	}

	return &dto.TransactionRecordQueryResp{
		Total: int(total),
		List:  records,
	}, nil
}

// 查询用户领取详情（带用户信息，根据组织过滤）
func (w *TransactionService) QueryReceiveRecords(ctx context.Context, req *dto.ReceiveRecordQueryReq, orgID string) (*dto.ReceiveRecordQueryResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	receiveRecordDao := model.NewReceiveRecordDao(db)
	walletCurrencyDao := walletModel.NewWalletCurrencyDao(db)

	// 解析时间参数
	var startTime, endTime *time.Time
	if req.StartTime != "" {

		timeInt, err := strconv.ParseInt(req.StartTime, 10, 64)
		if err != nil {
			return nil, freeErrors.ParameterInvalidErr
		}
		t := time.Unix(timeInt, 0).UTC()
		startTime = &t
	}
	if req.EndTime != "" {

		timeInt, err := strconv.ParseInt(req.EndTime, 10, 64)
		if err != nil {
			return nil, freeErrors.ParameterInvalidErr
		}
		t := time.Unix(timeInt, 0).UTC()
		endTime = &t
	}

	// 使用嵌入的分页参数
	page := &req.DepPagination

	// 查询领取记录
	total, receiveRecords, err := receiveRecordDao.QueryReceiveRecordsWithUserInfo(ctx, orgID, req.SenderKeyword, req.ReceiverKeyword, startTime, endTime, req.TransactionID, req.TransactionType, page)
	if err != nil {
		return nil, err
	}

	// 从聚合结果中收集本页涉及的 currency_id，批量查币种名称，避免 N+1
	currencyIDSet := make(map[primitive.ObjectID]struct{})
	for _, r := range receiveRecords {
		if r.Transaction == nil {
			continue
		}
		v, ok := r.Transaction["currency_id"]
		if !ok {
			continue
		}
		if oid, ok := v.(primitive.ObjectID); ok && !oid.IsZero() {
			currencyIDSet[oid] = struct{}{}
		}
	}
	currencyIDs := make([]primitive.ObjectID, 0, len(currencyIDSet))
	for id := range currencyIDSet {
		currencyIDs = append(currencyIDs, id)
	}
	currencies, err := walletCurrencyDao.GetByIds(ctx, currencyIDs)
	if err != nil {
		return nil, err
	}
	currencyNameByID := make(map[primitive.ObjectID]string)
	for _, c := range currencies {
		if c != nil {
			currencyNameByID[c.ID] = c.Name
		}
	}

	// 转换为响应格式（币种从聚合 Transaction 与上面批量结果取）
	records := make([]*dto.ReceiveRecordWithUserInfo, 0, len(receiveRecords))
	for _, receiveRecord := range receiveRecords {
		record := &dto.ReceiveRecordWithUserInfo{
			TransactionID:   receiveRecord.TransactionID,
			TransactionType: receiveRecord.TransactionType,
			Amount:          trimAmountZeros(receiveRecord.Amount.String()),
			ReceivedAt:      receiveRecord.ReceivedAt,
		}
		if receiveRecord.Transaction != nil {
			if v, ok := receiveRecord.Transaction["currency_id"]; ok {
				if oid, ok := v.(primitive.ObjectID); ok {
					record.Currency = currencyNameByID[oid]
				}
			}
		}

		// 提取发送者信息
		record.Sender = &dto.TransactionUserInfo{}
		if senderUser, ok := receiveRecord.SenderUser["user_id"]; ok {
			if userId, ok := senderUser.(string); ok {
				record.Sender.ImServerID = userId
			}
		}
		if senderUser, ok := receiveRecord.SenderUser["nickname"]; ok {
			if nickname, ok := senderUser.(string); ok {
				record.Sender.Nickname = nickname
			}
		}
		if senderUser, ok := receiveRecord.SenderUser["face_url"]; ok {
			if faceUrl, ok := senderUser.(string); ok {
				record.Sender.FaceURL = faceUrl
			}
		}
		if senderAttr, ok := receiveRecord.SenderAttr["account"]; ok {
			if account, ok := senderAttr.(string); ok {
				record.Sender.Account = account
			}
		}
		if senderAttr, ok := receiveRecord.SenderAttr["user_id"]; ok {
			if userId, ok := senderAttr.(string); ok {
				record.Sender.UserID = userId
			}
		}

		// 提取领取者信息
		record.Receiver = &dto.TransactionUserInfo{}
		if receiverUserId, ok := receiveRecord.ReceiverUser["user_id"]; ok {
			if userId, ok := receiverUserId.(string); ok {
				record.Receiver.ImServerID = userId
			}
		}
		if receiverNickname, ok := receiveRecord.ReceiverUser["nickname"]; ok {
			if nickname, ok := receiverNickname.(string); ok {
				record.Receiver.Nickname = nickname
			}
		}
		if receiverFaceUrl, ok := receiveRecord.ReceiverUser["face_url"]; ok {
			if faceUrl, ok := receiverFaceUrl.(string); ok {
				record.Receiver.FaceURL = faceUrl
			}
		}
		if receiverAccount, ok := receiveRecord.ReceiverAttr["account"]; ok {
			if account, ok := receiverAccount.(string); ok {
				record.Receiver.Account = account
			}
		}
		if receiverUserId2, ok := receiveRecord.ReceiverAttr["user_id"]; ok {
			if userId, ok := receiverUserId2.(string); ok {
				record.Receiver.UserID = userId
			}
		}

		records = append(records, record)
	}

	return &dto.ReceiveRecordQueryResp{
		Total: int(total),
		List:  records,
	}, nil
}

// ========================== 红包槽位预留机制（混合模式）==========================

// ReceiveTransactionContextExtended 扩展的接收交易上下文
type ReceiveTransactionContextExtended struct {
	*ReceiveTransactionContext
	ReservationID string // 预留ID
}

// ReceiveTransactionManagerV2 接收交易管理器（混合模式）
type ReceiveTransactionManagerV2 struct {
	validator      *ReceiveTransactionValidator
	lockManager    *ReceiveTransactionLockManager
	redisProcessor *ReceiveTransactionRedisProcessor
	dbProcessor    *ReceiveTransactionDBProcessor
	slotManager    *RedPacketSlotManager // 新增：红包槽位管理器
}

// NewReceiveTransactionManagerV2 创建接收交易管理器（混合模式）
func NewReceiveTransactionManagerV2(mongoDB *mongo.Database, redisClient redis.UniversalClient) (*ReceiveTransactionManagerV2, error) {
	slotManager, err := NewRedPacketSlotManager(redisClient)
	if err != nil {
		return nil, fmt.Errorf("创建红包槽位管理器失败: %v", err)
	}

	return &ReceiveTransactionManagerV2{
		validator:      NewReceiveTransactionValidator(mongoDB),
		lockManager:    NewReceiveTransactionLockManager(redisClient),
		redisProcessor: NewReceiveTransactionRedisProcessor(redisClient),
		dbProcessor:    NewReceiveTransactionDBProcessor(mongoDB),
		slotManager:    slotManager,
	}, nil
}

// ProcessReceiveTransaction 处理接收交易的完整流程（混合模式）
func (m *ReceiveTransactionManagerV2) ProcessReceiveTransaction(ctx context.Context, req *dto.ReceiveTransactionReq) (string, error) {
	// 创建上下文
	rtCtx := &ReceiveTransactionContext{
		Req:    req,
		NowUTC: time.Now().UTC(),
	}

	// 记录开始处理交易
	log.ZInfo(ctx, "开始处理交易", "receiver_id", req.ReceiverID, "transaction_id", req.TransactionID, "time", rtCtx.NowUTC.Format(time.RFC3339))

	// 1. 基础参数校验
	if err := m.validator.ValidateBasicParams(ctx, req); err != nil {
		return "", err
	}

	// 【性能优化】先检查 Redis，只有在 Redis 检查不确定时才查询 MongoDB
	// 2. 快速检查用户是否已领取过（Redis）- 移到最前面
	received, err := m.slotManager.CheckUserReceived(ctx, req.TransactionID, req.ReceiverID)
	if err != nil {
		log.ZWarn(ctx, "Redis检查用户是否已领取失败，将进行MongoDB检查", err, "transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)
	} else if received {
		log.ZInfo(ctx, "用户已领取过该红包（Redis）", "transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)
		return "", errs.NewCodeError(freeErrors.ErrAlreadyReceived, freeErrors.ErrorMessages[freeErrors.ErrAlreadyReceived])
	}

	receiveRecordDao := model.NewReceiveRecordDao(m.dbProcessor.mongoDB)

	// 【关键优化-幂等性保障】只有 Redis 检查失败或不确定时才查询 MongoDB
	// 这里的 MongoDB 幂等检查是为了防止 Redis 数据丢失的极端情况
	if err != nil {
		// Redis 检查失败，必须从 MongoDB 确认
		existingRecord, existErr := receiveRecordDao.GetByTransactionAndUser(ctx, req.TransactionID, req.ReceiverID)
		if existErr == nil && existingRecord != nil {
			log.ZInfo(ctx, "用户已领取过该红包（MongoDB幂等检查）",
				"transaction_id", req.TransactionID,
				"receiver_id", req.ReceiverID,
				"existing_amount", existingRecord.Amount.String())
			go m.ensureRedisConsistencyAsync(context.Background(), req.TransactionID, req.ReceiverID)
			return "", errs.NewCodeError(freeErrors.ErrAlreadyReceived, freeErrors.ErrorMessages[freeErrors.ErrAlreadyReceived])
		}
	}

	// 2.1 从MongoDB检查交易状态
	transactionDao := model.NewTransactionDao(m.dbProcessor.mongoDB)
	transaction, err := transactionDao.GetByTransactionID(ctx, req.TransactionID)
	if err != nil {
		log.ZWarn(ctx, "从MongoDB获取交易信息失败", err, "transaction_id", req.TransactionID)
	} else if transaction != nil {
		// 【性能优化】只查询计数而不是获取全部记录
		actualReceivedCount, recErr := receiveRecordDao.CountByTransactionID(ctx, req.TransactionID)
		if recErr == nil && int(actualReceivedCount) >= transaction.TotalCount {
			log.ZInfo(ctx, "红包已领完（基于实际领取记录）", "transaction_id", req.TransactionID,
				"total_count", transaction.TotalCount, "actual_received", actualReceivedCount)
			return "", errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
		}

		// 检查交易状态
		if transaction.Status == model.TransactionStatusComplete {
			log.ZInfo(ctx, "红包交易已完成", "transaction_id", req.TransactionID)
			return "", errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
		}
		if transaction.Status == model.TransactionStatusExpired {
			log.ZInfo(ctx, "红包交易已过期", "transaction_id", req.TransactionID)
			return "", errs.NewCodeError(freeErrors.ErrTransactionExpired, freeErrors.ErrorMessages[freeErrors.ErrTransactionExpired])
		}

		// 【增强】基于实际领取数计算真实剩余数量
		trueRemainingCount := transaction.TotalCount - int(actualReceivedCount)
		if trueRemainingCount <= 0 {
			log.ZInfo(ctx, "红包已无剩余（基于实际领取记录）", "transaction_id", req.TransactionID,
				"true_remaining", trueRemainingCount)
			return "", errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
		}

		// 【增强】智能修复Redis计数器
		countKey := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, req.TransactionID)
		redisCount, redisErr := m.redisProcessor.redisClient.Get(ctx, countKey).Int64()
		if redisErr != nil && redisErr != redis.Nil {
			log.ZWarn(ctx, "获取Redis计数器失败", redisErr, "transaction_id", req.TransactionID)
		} else {
			// 检查是否有进行中的预留
			reservationsKey := fmt.Sprintf("%s%s:reservations", constant.TransactionKeyPrefix, req.TransactionID)
			pendingCount, _ := m.redisProcessor.redisClient.HLen(ctx, reservationsKey).Result()

			// 只有当Redis计数明显异常（<=0）且实际有剩余，且没有进行中的预留时才修复
			if redisCount <= 0 && trueRemainingCount > 0 && pendingCount == 0 {
				// 使用安全的修复值：真实剩余 - 预留数（这里pendingCount=0）
				safeCount := trueRemainingCount
				if safeCount > 0 {
					err = m.redisProcessor.redisClient.Set(ctx, countKey, safeCount, 24*time.Hour).Err()
					if err != nil {
						log.ZWarn(ctx, "修复Redis计数器失败", err, "transaction_id", req.TransactionID)
					} else {
						log.ZWarn(ctx, "已修复Redis计数器",
							nil,
							"transaction_id", req.TransactionID,
							"redis_count_before", redisCount,
							"new_count", safeCount,
							"true_remaining", trueRemainingCount,
							"db_remaining", transaction.RemainingCount)
					}
				}
			}
		}
	}

	// 3. 尝试使用槽位预留模式（拼手气红包使用动态预留过期时间）
	var reservationResult *ReservationResult
	var reservationID string
	if transaction != nil && transaction.TransactionType == model.TransactionTypeLuckyPacket {
		expirySec := m.getDynamicReservationExpiryForLuckyPacket(ctx, req.TransactionID)
		reservationResult, reservationID, err = m.slotManager.ReserveSlotWithExpiry(ctx, req.TransactionID, req.ReceiverID, expirySec)
	} else {
		reservationResult, reservationID, err = m.slotManager.ReserveSlot(ctx, req.TransactionID, req.ReceiverID)
	}

	// 优化的错误处理和降级策略
	if err != nil {
		// 判断错误类型是否允许降级
		shouldFallback := false

		// 尝试将错误转换为ReservationError
		if resErr, ok := err.(*ReservationError); ok {
			// 只有连接错误才降级
			shouldFallback = resErr.ShouldFallback()

			log.ZWarn(ctx, "预留名额失败，错误类型分析",
				resErr.Err,
				"transaction_id", req.TransactionID,
				"receiver_id", req.ReceiverID,
				"error_type", resErr.Type,
				"should_fallback", shouldFallback)
		} else {
			// 未知错误类型，查看错误内容
			errStr := err.Error()
			if strings.Contains(errStr, "Redis连接") ||
				strings.Contains(errStr, "connection") {
				shouldFallback = true
				log.ZWarn(ctx, "预留名额失败，未分类连接错误，降级到传统锁模式",
					err,
					"transaction_id", req.TransactionID,
					"receiver_id", req.ReceiverID)
			} else {
				shouldFallback = false
				log.ZError(ctx, "预留名额失败，非连接错误，直接返回错误",
					err,
					"transaction_id", req.TransactionID,
					"receiver_id", req.ReceiverID)
			}
		}

		// 根据错误类型决定是降级还是直接返回错误
		if shouldFallback {
			// 仅Redis连接问题才降级到传统模式
			log.ZInfo(ctx, "由于Redis连接问题，降级到传统锁模式",
				"transaction_id", req.TransactionID,
				"receiver_id", req.ReceiverID)

			return m.processWithDistributedLock(ctx, rtCtx)
		} else {
			// 非连接问题直接返回错误
			return "", errs.NewCodeError(freeErrors.ErrSystem, fmt.Sprintf("预留处理错误: %v", err))
		}
	}

	// 检查预留结果
	if reservationResult == nil || reservationResult.Status != "SUCCESS" {
		// 预留结果为空，这是一种异常情况
		if reservationResult == nil {
			// 这不应该发生，因为脚本要么返回结果要么出错，所以这里直接返回错误
			log.ZError(ctx, "预留结果为空（异常情况）", nil,
				"transaction_id", req.TransactionID,
				"receiver_id", req.ReceiverID)
			return "", errs.NewCodeError(freeErrors.ErrSystem, "红包预留异常，请稍后重试")
		}

		// 记录业务逻辑拒绝的详细信息
		log.ZWarn(ctx, "预留名额失败（业务拒绝）", nil,
			"transaction_id", req.TransactionID,
			"receiver_id", req.ReceiverID,
			"status", reservationResult.Status,
			"reason", reservationResult.Reason)

		// 处理业务逻辑错误 - 这些是预期内的状态，直接返回对应错误码，不降级
		switch reservationResult.Reason {
		case "ALREADY_RECEIVED":
			// 用户已领取
			return "", errs.NewCodeError(freeErrors.ErrAlreadyReceived, freeErrors.ErrorMessages[freeErrors.ErrAlreadyReceived])

		case "RESERVATION_EXISTS":
			// 用户有进行中的预留，告知客户端稍后重试
			return "", errs.NewCodeError(freeErrors.ErrOperationTooFrequent, "操作过于频繁，请稍后再试")

		case "PACKET_EMPTY", "PACKET_EMPTY_FINAL", "PACKET_FULL_BY_RECEIVERS", "PACKET_FULL_BY_RECEIVERS_AND_PENDING":
			// 红包已领完（Redis 视角），为了保证与 MongoDB 中交易状态/剩余数量一致，在高并发或异常场景下做一次兜底校验
			mongoCli := plugin.MongoCli().GetDB()
			tsDao := model.NewTransactionDao(mongoCli)
			tx, txErr := tsDao.GetByTransactionID(ctx, req.TransactionID)
			if txErr != nil {
				// 查询失败视为系统异常，避免错误地提示“红包抢完了”
				log.ZError(ctx, "Redis 报 PACKET_EMPTY 但查询交易失败", txErr,
					"transaction_id", req.TransactionID,
					"receiver_id", req.ReceiverID)
				return "", errs.NewCodeError(freeErrors.ErrSystem, "红包状态异常，请稍后重试")
			}

			// 如果交易在数据库中已经是「已完成」或「已过期」，或者剩余个数为 0，则认定为真正抢完/不可再抢
			if tx.Status == model.TransactionStatusComplete ||
				tx.Status == model.TransactionStatusExpired ||
				tx.RemainingCount <= 0 {
				return "", errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
			}

			// 走到这里说明：Redis 认为红包抢完了，但 MongoDB 里交易仍为进行中并且 RemainingCount > 0
			// 这类场景又可以细分两种：
			// A) Mongo 中实际领取人数已经达到/超过总个数，但交易状态/剩余个数尚未更新（应视为「已抢完」，并修正交易状态）
			// B) Mongo 中实际领取人数 < 总个数（确有剩余），属于高并发下 Redis 计数器提前归零等数据不一致，需要纠正 Redis 计数并提示客户端重试

			// 复用当前上下文中的交易信息 tx，仅新增一个用于统计领取记录的 DAO
			rrDao := model.NewReceiveRecordDao(plugin.MongoCli().GetDB())

			// 1）统计 MongoDB 中实际已领取的数量（使用 Count 而不是获取全部记录，性能更优）
			receivedCount64, recErr := rrDao.CountByTransactionID(ctx, req.TransactionID)
			if recErr != nil {
				log.ZError(ctx, "Redis 报 PACKET_EMPTY 且统计领取记录失败", recErr,
					"transaction_id", req.TransactionID,
					"receiver_id", req.ReceiverID)
				return "", errs.NewCodeError(freeErrors.ErrSystem, "红包状态异常，请稍后重试")
			}

			receivedCount := int(receivedCount64)

			// 1-A）如果已领取数量 >= 总个数，说明这笔红包在业务上已经完全领完（优先信任实际领取记录数）
			// 【关键修复】remaining_count 可能因预留过期、更新延迟等不准确，应以实际领取记录数为准
			// 如果 receivedCount >= tx.TotalCount，无论 remaining_count 是多少，都应该更新为完成
			if receivedCount >= tx.TotalCount {
				log.ZWarn(ctx, "检测到已领取数量达到总个数，自动将交易置为完成状态",
					nil,
					"transaction_id", req.TransactionID,
					"receiver_id", req.ReceiverID,
					"received_count", receivedCount,
					"total_count", tx.TotalCount,
					"old_status", tx.Status,
					"old_remaining_count", tx.RemainingCount)

				// 将交易更新为完成状态，剩余金额/个数归零
				if err := tsDao.UpdateTransactionComplete(ctx, req.TransactionID); err != nil {
					log.ZError(ctx, "自动修正交易状态为完成失败", err,
						"transaction_id", req.TransactionID)
					return "", errs.NewCodeError(freeErrors.ErrSystem, "红包状态异常，请稍后重试")
				}

				// 视为红包已真实抢完，对客户端返回「已抢完」
				return "", errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
			}

			// 1-B）否则，说明 Mongo 中也确实还有剩余，只是 Redis 计数器/接收者集合出了问题
			// 此时不应误导用户为“抢完了”，而是：
			// 1）尝试按 DB 剩余个数纠正 Redis 计数器；2）提示客户端稍后重试，下一次会基于修正后的计数继续抢

			// 【P0修复-拼手气红包】仅对拼手气红包应用安全纠正（避免覆盖正在处理的预留）
			isLuckyPacket := tx.TransactionType == model.TransactionTypeLuckyPacket
			if isLuckyPacket {
				// 安全纠正：传入已有的数据，避免重复查询 MongoDB
				if err := m.safeCorrectCounterForLuckyPacket(ctx, req.TransactionID, tx.TotalCount, receivedCount, tx.RemainingCount); err != nil {
					log.ZWarn(ctx, "拼手气红包安全纠正计数器失败", err,
						"transaction_id", req.TransactionID,
						"receiver_id", req.ReceiverID)
				}
				// 同时纠正 Hash，避免 Lua 仍读到 remaining_count=0
				transKey := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, req.TransactionID)
				_ = m.redisProcessor.redisClient.HSet(ctx, transKey, "remaining_count", tx.RemainingCount).Err()
				_ = m.redisProcessor.redisClient.HSet(ctx, transKey, "remaining_amount", tx.RemainingAmount.String()).Err()
			} else {
				// 非拼手气红包：纠正计数器并同步 Hash，避免下次仍因 Hash=0 误判领完
				redisCli := plugin.RedisCli()
				transKey := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, req.TransactionID)
				countKey := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, req.TransactionID)
				monitorPrefix := fmt.Sprintf("dep_transaction_monitor:%s:", req.TransactionID)

				pipe := redisCli.Pipeline()
				pipe.Set(ctx, countKey, tx.RemainingCount, 24*time.Hour)
				pipe.HSet(ctx, transKey, "remaining_count", tx.RemainingCount)
				pipe.HSet(ctx, transKey, "remaining_amount", tx.RemainingAmount.String())
				pipe.Incr(ctx, monitorPrefix+"corrections")
				correctionField := fmt.Sprintf("user:%s", req.ReceiverID)
				correctionValue := fmt.Sprintf("fix_to=%d_at=%d", tx.RemainingCount, time.Now().Unix())
				pipe.HSet(ctx, monitorPrefix+"correction_details", correctionField, correctionValue)

				if _, pipeErr := pipe.Exec(ctx); pipeErr != nil {
					log.ZError(ctx, "纠正 Redis 计数器失败", pipeErr,
						"transaction_id", req.TransactionID,
						"receiver_id", req.ReceiverID,
						"db_remaining_count", tx.RemainingCount,
						"count_key", countKey)
				} else {
					log.ZWarn(ctx, "Redis 报 PACKET_EMPTY 但 MongoDB 显示仍有剩余，已按 DB 剩余个数纠正 Redis 计数器",
						nil,
						"transaction_id", req.TransactionID,
						"receiver_id", req.ReceiverID,
						"tx_status", tx.Status,
						"db_remaining_count", tx.RemainingCount,
						"count_key", countKey)
				}
			}

			// 【关键优化】纠正完计数器后立即重试预留，而不是返回错误让客户端重试
			// 这样可以减少一次网络往返，提高成功率
			log.ZInfo(ctx, "计数器已纠正，立即重试预留",
				"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)

			var retryResult *ReservationResult
			var retryID string
			if isLuckyPacket {
				expirySec := m.getDynamicReservationExpiryForLuckyPacket(ctx, req.TransactionID)
				retryResult, retryID, err = m.slotManager.ReserveSlotWithExpiry(ctx, req.TransactionID, req.ReceiverID, expirySec)
			} else {
				retryResult, retryID, err = m.slotManager.ReserveSlot(ctx, req.TransactionID, req.ReceiverID)
			}

			if err != nil {
				log.ZWarn(ctx, "纠正后重试预留失败", err,
					"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)
				return "", errs.NewCodeError(freeErrors.ErrSystem, "系统繁忙，请稍后重试")
			}

			if retryResult != nil && retryResult.Status == "SUCCESS" {
				reservationResult = retryResult
				reservationID = retryID
				log.ZInfo(ctx, "纠正后重试预留成功",
					"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)
				break // 跳出 switch，继续处理交易
			}

			// 重试仍失败，根据原因返回
			if retryResult != nil {
				switch retryResult.Reason {
				case "ALREADY_RECEIVED":
					return "", errs.NewCodeError(freeErrors.ErrAlreadyReceived, freeErrors.ErrorMessages[freeErrors.ErrAlreadyReceived])
				case "PACKET_EMPTY", "PACKET_EMPTY_FINAL":
					// 计数器或元数据显示红包已空，返回已领完
					return "", errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
				case "PACKET_FULL_BY_RECEIVERS", "PACKET_FULL_BY_RECEIVERS_AND_PENDING":
					// 【关键修复】这种情况是 Redis receivers 与 MongoDB 不一致导致的
					// 我们已经验证 MongoDB 中还有剩余，不应该返回"已领完"
					// 返回"系统繁忙"让用户重试，下一次可能就成功了
					log.ZWarn(ctx, "纠正后重试仍返回 PACKET_FULL，可能存在竞态条件",
						nil,
						"transaction_id", req.TransactionID,
						"receiver_id", req.ReceiverID,
						"reason", retryResult.Reason)
					return "", errs.NewCodeError(freeErrors.ErrOperationTooFrequent, "系统繁忙，请稍后重试")
				}
			}
			return "", errs.NewCodeError(freeErrors.ErrSystem, "系统繁忙，请稍后重试")

		case "INVALID_REMAINING_COUNT":
			// 红包数量异常
			return "", errs.NewCodeError(freeErrors.ErrNoRemaining, "红包数据异常")

		case "INITIALIZING":
			// 另一个请求正在初始化 Redis 元数据，等待后重试
			log.ZInfo(ctx, "Redis 正在初始化中，等待后重试",
				"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)

			// 【性能优化】等待初始化完成（最多 200ms，每 20ms 检查一次）
			var statusVal int
			transKeyForCheck := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, req.TransactionID)
			for i := 0; i < 10; i++ {
				time.Sleep(20 * time.Millisecond)
				statusVal, _ = m.redisProcessor.redisClient.HGet(ctx, transKeyForCheck, "status").Int()
				if statusVal != -2 {
					break
				}
			}

			if statusVal == -2 || statusVal == -1 {
				// 初始化仍未完成或被清除了，自己尝试初始化
				log.ZWarn(ctx, "等待初始化超时或状态异常，尝试自己初始化", nil,
					"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID, "status", statusVal)

				// 尝试获取初始化权（使用原子操作）
				// 如果 status 仍然是 -2，我们尝试直接设置有效值
				txDao := model.NewTransactionDao(m.dbProcessor.mongoDB)
				tx, txErr := txDao.GetByTransactionID(ctx, req.TransactionID)
				if txErr != nil || tx == nil {
					return "", errs.NewCodeError(freeErrors.ErrInvalidTransaction, "交易不存在")
				}

				if tx.Status == model.TransactionStatusComplete || tx.RemainingCount <= 0 {
					return "", errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
				}
				if tx.Status == model.TransactionStatusExpired {
					return "", errs.NewCodeError(freeErrors.ErrTransactionExpired, freeErrors.ErrorMessages[freeErrors.ErrTransactionExpired])
				}

				// 强制初始化（覆盖 -2 状态），补全 total_amount/remaining_amount
				initCtx, initCancel := context.WithTimeout(context.Background(), 2*time.Second)
				countKeyForInit := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, req.TransactionID)
				pipe := m.redisProcessor.redisClient.Pipeline()
				pipe.HSet(initCtx, transKeyForCheck, "status", tx.Status)
				pipe.HSet(initCtx, transKeyForCheck, "total_count", tx.TotalCount)
				pipe.HSet(initCtx, transKeyForCheck, "remaining_count", tx.RemainingCount)
				pipe.HSet(initCtx, transKeyForCheck, "total_amount", tx.TotalAmount.String())
				pipe.HSet(initCtx, transKeyForCheck, "remaining_amount", tx.RemainingAmount.String())
				pipe.Expire(initCtx, transKeyForCheck, 24*time.Hour)
				pipe.SetNX(initCtx, countKeyForInit, tx.RemainingCount, 24*time.Hour)
				_, _ = pipe.Exec(initCtx)
				initCancel()
			}

			// 初始化完成（或自己完成了初始化），重试预留
			log.ZInfo(ctx, "初始化完成，重试预留",
				"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)

			var retryResult *ReservationResult
			var retryID string
			// 获取交易类型以决定使用哪种预留方式
			txDaoForRetry := model.NewTransactionDao(m.dbProcessor.mongoDB)
			txForRetry, _ := txDaoForRetry.GetByTransactionID(ctx, req.TransactionID)
			if txForRetry != nil && txForRetry.TransactionType == model.TransactionTypeLuckyPacket {
				expirySec := m.getDynamicReservationExpiryForLuckyPacket(ctx, req.TransactionID)
				retryResult, retryID, err = m.slotManager.ReserveSlotWithExpiry(ctx, req.TransactionID, req.ReceiverID, expirySec)
			} else {
				retryResult, retryID, err = m.slotManager.ReserveSlot(ctx, req.TransactionID, req.ReceiverID)
			}

			if err != nil {
				return "", errs.NewCodeError(freeErrors.ErrSystem, "系统繁忙，请稍后重试")
			}

			if retryResult != nil && retryResult.Status == "SUCCESS" {
				reservationResult = retryResult
				reservationID = retryID
				break // 跳出 switch，继续处理
			}

			// 重试仍失败
			if retryResult != nil {
				switch retryResult.Reason {
				case "PACKET_EMPTY", "PACKET_EMPTY_FINAL":
					return "", errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
				case "ALREADY_RECEIVED":
					return "", errs.NewCodeError(freeErrors.ErrAlreadyReceived, freeErrors.ErrorMessages[freeErrors.ErrAlreadyReceived])
				}
			}
			return "", errs.NewCodeError(freeErrors.ErrSystem, "系统繁忙，请稍后重试")

		case "NOT_INITIALIZED":
			// 【关键修复】Redis 中交易元数据尚未初始化，这在高并发下很常见
			// 从 MongoDB 获取交易信息并初始化 Redis，然后重试预留
			log.ZInfo(ctx, "Redis 交易元数据未初始化，开始初始化",
				"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)

			txDao := model.NewTransactionDao(m.dbProcessor.mongoDB)
			tx, txErr := txDao.GetByTransactionID(ctx, req.TransactionID)
			if txErr != nil || tx == nil {
				log.ZWarn(ctx, "NOT_INITIALIZED 且无法从 MongoDB 获取交易", txErr,
					"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)
				return "", errs.NewCodeError(freeErrors.ErrInvalidTransaction, "交易不存在")
			}

			// 检查交易状态
			if tx.Status == model.TransactionStatusComplete || tx.RemainingCount <= 0 {
				return "", errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
			}
			if tx.Status == model.TransactionStatusExpired {
				return "", errs.NewCodeError(freeErrors.ErrTransactionExpired, freeErrors.ErrorMessages[freeErrors.ErrTransactionExpired])
			}

			// 初始化 Redis 元数据（使用独立 context 避免被取消），补全 total_amount/remaining_amount
			initCtx, initCancel := context.WithTimeout(context.Background(), 3*time.Second)
			transKey := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, req.TransactionID)
			countKey := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, req.TransactionID)

			pipe := m.redisProcessor.redisClient.Pipeline()
			pipe.HSet(initCtx, transKey, "status", tx.Status)
			pipe.HSet(initCtx, transKey, "total_count", tx.TotalCount)
			pipe.HSet(initCtx, transKey, "remaining_count", tx.RemainingCount)
			pipe.HSet(initCtx, transKey, "total_amount", tx.TotalAmount.String())
			pipe.HSet(initCtx, transKey, "remaining_amount", tx.RemainingAmount.String())
			pipe.Expire(initCtx, transKey, 24*time.Hour)
			// 同时初始化计数器
			pipe.SetNX(initCtx, countKey, tx.RemainingCount, 24*time.Hour)

			if _, pipeErr := pipe.Exec(initCtx); pipeErr != nil {
				initCancel()
				log.ZWarn(ctx, "NOT_INITIALIZED 初始化 Redis 失败", pipeErr,
					"transaction_id", req.TransactionID)
				return "", errs.NewCodeError(freeErrors.ErrSystem, "系统繁忙，请稍后重试")
			}
			initCancel()

			log.ZInfo(ctx, "NOT_INITIALIZED 已初始化 Redis，重试预留",
				"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID,
				"remaining_count", tx.RemainingCount)

			// 重试预留
			var retryResult *ReservationResult
			var retryID string
			if tx.TransactionType == model.TransactionTypeLuckyPacket {
				expirySec := m.getDynamicReservationExpiryForLuckyPacket(ctx, req.TransactionID)
				retryResult, retryID, err = m.slotManager.ReserveSlotWithExpiry(ctx, req.TransactionID, req.ReceiverID, expirySec)
			} else {
				retryResult, retryID, err = m.slotManager.ReserveSlot(ctx, req.TransactionID, req.ReceiverID)
			}

			if err != nil {
				log.ZWarn(ctx, "NOT_INITIALIZED 重试预留失败", err,
					"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)
				return "", errs.NewCodeError(freeErrors.ErrSystem, "系统繁忙，请稍后重试")
			}

			if retryResult != nil && retryResult.Status == "SUCCESS" {
				reservationResult = retryResult
				reservationID = retryID
				log.ZInfo(ctx, "NOT_INITIALIZED 重试预留成功",
					"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)
				break // 跳出 switch，继续走步骤 4 处理交易逻辑
			}

			// 重试仍失败，根据原因返回
			if retryResult != nil {
				switch retryResult.Reason {
				case "PACKET_EMPTY", "PACKET_EMPTY_FINAL":
					return "", errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
				case "ALREADY_RECEIVED":
					return "", errs.NewCodeError(freeErrors.ErrAlreadyReceived, freeErrors.ErrorMessages[freeErrors.ErrAlreadyReceived])
				}
			}
			return "", errs.NewCodeError(freeErrors.ErrSystem, "系统繁忙，请稍后重试")

		case "TRANSACTION_INVALID":
			// Redis 中 dep_transaction:{id} 的 status 非 0（可能未初始化或已过期），先查 MongoDB 再决定
			txDao := model.NewTransactionDao(m.dbProcessor.mongoDB)
			tx, txErr := txDao.GetByTransactionID(ctx, req.TransactionID)
			if txErr != nil || tx == nil {
				log.ZWarn(ctx, "TRANSACTION_INVALID 且无法从 MongoDB 获取交易", txErr,
					"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)
				return "", errs.NewCodeError(freeErrors.ErrInvalidTransaction, "交易无效或已过期")
			}
			if tx.Status == model.TransactionStatusComplete || tx.RemainingCount <= 0 {
				return "", errs.NewCodeError(freeErrors.ErrNoRemaining, freeErrors.ErrorMessages[freeErrors.ErrNoRemaining])
			}
			if tx.Status == model.TransactionStatusExpired {
				return "", errs.NewCodeError(freeErrors.ErrTransactionExpired, freeErrors.ErrorMessages[freeErrors.ErrTransactionExpired])
			}
			// 交易在 DB 中有效（进行中且有剩余），补齐 Redis 元数据（含 total_amount/remaining_amount）并重试一次预留
			transKey := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, req.TransactionID)
			pipe := m.redisProcessor.redisClient.Pipeline()
			pipe.HSet(ctx, transKey, "status", tx.Status)
			pipe.HSet(ctx, transKey, "total_count", tx.TotalCount)
			pipe.HSet(ctx, transKey, "remaining_count", tx.RemainingCount)
			pipe.HSet(ctx, transKey, "total_amount", tx.TotalAmount.String())
			pipe.HSet(ctx, transKey, "remaining_amount", tx.RemainingAmount.String())
			pipe.Expire(ctx, transKey, 24*time.Hour)
			if _, pipeErr := pipe.Exec(ctx); pipeErr != nil {
				log.ZWarn(ctx, "TRANSACTION_INVALID 时初始化 Redis 交易哈希失败", pipeErr,
					"transaction_id", req.TransactionID)
				return "", errs.NewCodeError(freeErrors.ErrSystem, "红包状态异常，请稍后重试")
			}
			log.ZInfo(ctx, "TRANSACTION_INVALID 已补齐 Redis 元数据，重试预留",
				"transaction_id", req.TransactionID, "receiver_id", req.ReceiverID)
			var retryResult *ReservationResult
			var retryID string
			if tx.TransactionType == model.TransactionTypeLuckyPacket {
				expirySec := m.getDynamicReservationExpiryForLuckyPacket(ctx, req.TransactionID)
				retryResult, retryID, err = m.slotManager.ReserveSlotWithExpiry(ctx, req.TransactionID, req.ReceiverID, expirySec)
			} else {
				retryResult, retryID, err = m.slotManager.ReserveSlot(ctx, req.TransactionID, req.ReceiverID)
			}
			if err != nil {
				return "", errs.NewCodeError(freeErrors.ErrSystem, "红包状态异常，请稍后重试")
			}
			if retryResult != nil && retryResult.Status == "SUCCESS" {
				reservationResult = retryResult
				reservationID = retryID
				break // 跳出 switch，继续走步骤 4 处理交易逻辑
			}
			// 重试仍失败（如 PACKET_EMPTY 等），按原因返回
			if retryResult != nil && retryResult.Reason == "TRANSACTION_INVALID" {
				return "", errs.NewCodeError(freeErrors.ErrInvalidTransaction, "交易无效或已过期")
			}
			return "", errs.NewCodeError(freeErrors.ErrSystem, "红包状态异常，请稍后重试")

		default:
			// 未知原因，记录日志并返回通用错误
			log.ZWarn(ctx, "预留失败原因未知，返回通用错误", nil,
				"transaction_id", req.TransactionID,
				"reason", reservationResult.Reason)
			return "", errs.NewCodeError(freeErrors.ErrSystem, "红包领取失败: "+reservationResult.Reason)
		}
	}

	// 4. 处理交易逻辑（无需获取全局锁）
	extCtx := &ReceiveTransactionContextExtended{
		ReceiveTransactionContext: rtCtx,
		ReservationID:             reservationID,
	}

	result, err := m.processWithReservation(ctx, extCtx)
	if err != nil {
		// 取消预留
		m.slotManager.CancelReservation(ctx, req.TransactionID, req.ReceiverID, reservationID)
		return "", err
	}

	return result, nil
}

// processWithDistributedLock 使用分布式锁处理交易（传统模式）
func (m *ReceiveTransactionManagerV2) processWithDistributedLock(ctx context.Context, rtCtx *ReceiveTransactionContext) (string, error) {
	log.ZInfo(ctx, "使用分布式锁模式处理交易", "transaction_id", rtCtx.Req.TransactionID, "receiver_id", rtCtx.Req.ReceiverID)

	// 获取分布式锁
	if err := m.lockManager.AcquireLock(ctx, rtCtx); err != nil {
		return "", err
	}
	defer m.lockManager.ReleaseLock(ctx, rtCtx)

	// 验证交易状态
	if err := m.validator.ValidateTransactionStatus(ctx, rtCtx); err != nil {
		return "", err
	}

	if err := m.validator.ValidateReceiverInfo(ctx, rtCtx); err != nil {
		return "", err
	}
	if err := m.validator.ValidateWalletStatus(ctx, rtCtx); err != nil {
		return "", err
	}

	// 验证币种精度
	if err := m.validator.ValidateCurrencyPrecision(ctx, rtCtx); err != nil {
		return "", err
	}

	// 初始化Redis键名
	m.redisProcessor.InitializeKeys(rtCtx)

	// 处理Redis交易逻辑
	result, err := m.redisProcessor.ProcessTransaction(ctx, rtCtx)
	if err != nil {
		return "", err
	}

	// 处理MongoDB持久化
	var amount string
	err = m.dbProcessor.ProcessDBTransaction(ctx, rtCtx, result)
	if err != nil {
		return "", err
	}
	amount = result.ReceiveAmount

	// 发送领取通知（异步）
	sendRedPacketNotification(ctx, rtCtx, amount)

	return amount, nil
}

// processWithReservation 使用预留机制处理交易（优化模式）
func (m *ReceiveTransactionManagerV2) processWithReservation(ctx context.Context, extCtx *ReceiveTransactionContextExtended) (string, error) {
	rtCtx := extCtx.ReceiveTransactionContext
	isLuckyPacket := rtCtx.Transaction != nil && rtCtx.Transaction.TransactionType == model.TransactionTypeLuckyPacket

	log.ZInfo(ctx, "使用预留模式处理交易",
		"transaction_id", rtCtx.Req.TransactionID,
		"receiver_id", rtCtx.Req.ReceiverID,
		"reservation_id", extCtx.ReservationID,
		"is_lucky_packet", isLuckyPacket)

	// 验证交易状态（这里不需要锁，因为已经通过预留机制保证了原子性）
	if err := m.validator.ValidateTransactionStatus(ctx, rtCtx); err != nil {
		if isLuckyPacket {
			m.slotManager.CancelReservation(ctx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID, extCtx.ReservationID)
		}
		return "", err
	}

	if err := m.validator.ValidateReceiverInfo(ctx, rtCtx); err != nil {
		if isLuckyPacket {
			m.slotManager.CancelReservation(ctx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID, extCtx.ReservationID)
		}
		return "", err
	}
	if err := m.validator.ValidateWalletStatus(ctx, rtCtx); err != nil {
		if isLuckyPacket {
			m.slotManager.CancelReservation(ctx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID, extCtx.ReservationID)
		}
		return "", err
	}

	// 验证币种精度
	if err := m.validator.ValidateCurrencyPrecision(ctx, rtCtx); err != nil {
		if isLuckyPacket {
			m.slotManager.CancelReservation(ctx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID, extCtx.ReservationID)
		}
		return "", err
	}

	// 初始化Redis键名
	m.redisProcessor.InitializeKeys(rtCtx)

	// 处理Redis交易逻辑
	result, err := m.redisProcessor.ProcessTransaction(ctx, rtCtx)
	if err != nil {
		if isLuckyPacket {
			m.slotManager.CancelReservation(ctx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID, extCtx.ReservationID)
		}
		return "", err
	}
	amount := result.ReceiveAmount

	// 【P0修复-拼手气红包优化】仅对拼手气红包应用优化流程
	if isLuckyPacket {
		// 1. 幂等性检查：先检查是否已存在领取记录
		receiveRecordDao := model.NewReceiveRecordDao(m.dbProcessor.mongoDB)
		existing, err := receiveRecordDao.GetByTransactionAndUser(ctx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID)
		if err == nil && existing != nil {
			// 已存在，幂等返回成功（但需要确认预留）
			log.ZInfo(ctx, "拼手气红包领取记录已存在，幂等返回成功",
				"transaction_id", rtCtx.Req.TransactionID,
				"receiver_id", rtCtx.Req.ReceiverID,
				"existing_amount", existing.Amount.String())
			// 确认预留（可能已过期，但不影响幂等返回）
			_ = m.slotManager.ConfirmReservation(ctx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID, extCtx.ReservationID, existing.Amount.String(), true)
			return existing.Amount.String(), nil
		}

		// 2. 【关键修复】先确认预留，再写MongoDB（避免MongoDB写入成功但Redis确认失败导致数据不一致）
		if err := m.slotManager.ConfirmReservation(ctx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID, extCtx.ReservationID, amount, true); err != nil {
			log.ZError(ctx, "拼手气红包确认预留失败，取消预留", err,
				"transaction_id", rtCtx.Req.TransactionID,
				"receiver_id", rtCtx.Req.ReceiverID,
				"reservation_id", extCtx.ReservationID)
			m.slotManager.CancelReservation(ctx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID, extCtx.ReservationID)
			return "", err
		}

		// 3. MongoDB 持久化（P2 可选：异步写入时先返回金额，再后台落库）
		if EnableLuckyPacketAsyncDBWrite {
			go m.asyncWriteLuckyPacketDB(context.Background(), rtCtx, result, amount)
			sendRedPacketNotification(ctx, rtCtx, amount)
			return amount, nil
		}

		// 3. 处理MongoDB持久化（P1：拼手气红包带重试，最多3次指数退避）
		err = m.processDBTransactionWithRetryForLuckyPacket(ctx, rtCtx, result)
		if err != nil {
			// 【关键修复】MongoDB写入失败，需要补偿（回滚Redis）
			log.ZError(ctx, "拼手气红包MongoDB写入失败，执行补偿机制", err,
				"transaction_id", rtCtx.Req.TransactionID,
				"receiver_id", rtCtx.Req.ReceiverID)
			if compErr := m.compensateLuckyPacketReservation(ctx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID); compErr != nil {
				log.ZError(ctx, "拼手气红包补偿机制执行失败", compErr,
					"transaction_id", rtCtx.Req.TransactionID,
					"receiver_id", rtCtx.Req.ReceiverID)
			}
			return "", err
		}
	} else {
		// 非拼手气红包：保持原有流程（先写MongoDB，再确认预留）
		err = m.dbProcessor.ProcessDBTransaction(ctx, rtCtx, result)
		if err != nil {
			// 【关键修复】MongoDB写入失败，需要取消预留（回滚Redis）
			log.ZError(ctx, "非拼手气红包MongoDB写入失败，取消预留", err,
				"transaction_id", rtCtx.Req.TransactionID,
				"receiver_id", rtCtx.Req.ReceiverID)
			if cancelErr := m.slotManager.CancelReservation(ctx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID, extCtx.ReservationID); cancelErr != nil {
				log.ZError(ctx, "取消预留失败", cancelErr,
					"transaction_id", rtCtx.Req.TransactionID,
					"receiver_id", rtCtx.Req.ReceiverID)
			}
			return "", err
		}
		// 确认预留
		if err := m.slotManager.ConfirmReservation(ctx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID, extCtx.ReservationID, amount, true); err != nil {
			log.ZWarn(ctx, "确认预留失败，但交易已成功处理", err, "transaction_id", rtCtx.Req.TransactionID, "receiver_id", rtCtx.Req.ReceiverID)
			// 这里不返回错误，因为交易已经成功处理
		}
	}

	// 只进行安全性检查，避免过度同步导致问题
	// 从MongoDB读取最新的剩余数量
	transactionDao := model.NewTransactionDao(m.dbProcessor.mongoDB)
	transaction, err := transactionDao.GetByTransactionID(ctx, rtCtx.Req.TransactionID)
	if err != nil {
		log.ZWarn(ctx, "数据库处理后无法获取最新交易信息", err,
			"transaction_id", rtCtx.Req.TransactionID,
			"receiver_id", rtCtx.Req.ReceiverID)
		return amount, nil
	}

	if transaction != nil {
		// 【高并发优化】领取成功后回写 Redis Hash，保持 remaining_count/remaining_amount 与 MongoDB 一致
		transKey := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, rtCtx.Req.TransactionID)
		if setErr := m.redisProcessor.redisClient.HSet(ctx, transKey, "remaining_count", transaction.RemainingCount).Err(); setErr != nil {
			log.ZWarn(ctx, "回写Redis remaining_count失败", setErr, "transaction_id", rtCtx.Req.TransactionID)
		}
		if setErr := m.redisProcessor.redisClient.HSet(ctx, transKey, "remaining_amount", transaction.RemainingAmount.String()).Err(); setErr != nil {
			log.ZWarn(ctx, "回写Redis remaining_amount失败", setErr, "transaction_id", rtCtx.Req.TransactionID)
		}
		m.redisProcessor.redisClient.Expire(ctx, transKey, 24*time.Hour)

		// 获取当前Redis计数器值
		countKey := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, rtCtx.Req.TransactionID)
		currentCount, redisErr := m.redisProcessor.redisClient.Get(ctx, countKey).Int64()

		if redisErr != nil {
			log.ZWarn(ctx, "获取当前Redis计数器失败", redisErr,
				"transaction_id", rtCtx.Req.TransactionID)
			return amount, nil
		}

		// 将MongoDB的RemainingCount转换为int64，与Redis计数器类型一致
		mongoCount := int64(transaction.RemainingCount)

		// 安全检查：仅当MongoDB的计数小于Redis计数器时才更新
		// 这防止了将Redis计数器意外增加的情况
		if mongoCount < currentCount {
			log.ZInfo(ctx, "检测到Redis计数器大于MongoDB计数，进行安全同步",
				"transaction_id", rtCtx.Req.TransactionID,
				"redis_count", currentCount,
				"mongo_count", mongoCount)

			err = m.redisProcessor.redisClient.Set(ctx, countKey, mongoCount, 24*time.Hour).Err()
			if err != nil {
				log.ZWarn(ctx, "安全同步Redis计数器失败", err,
					"transaction_id", rtCtx.Req.TransactionID)
			} else {
				log.ZInfo(ctx, "已完成安全同步Redis计数器",
					"transaction_id", rtCtx.Req.TransactionID,
					"new_count", mongoCount)
			}
		} else if mongoCount > currentCount && currentCount <= 0 {
			// 只有当Redis计数器已为0且MongoDB还有剩余时才允许增加Redis计数器
			// 这防止了超额领取，同时修复了计数器被过度减少的情况
			log.ZInfo(ctx, "检测到Redis计数器为0但MongoDB还有剩余，进行修复",
				"transaction_id", rtCtx.Req.TransactionID,
				"redis_count", currentCount,
				"mongo_count", mongoCount)

			// 只将计数器设置为1，避免过度增加
			err = m.redisProcessor.redisClient.Set(ctx, countKey, 1, 24*time.Hour).Err()
			if err != nil {
				log.ZWarn(ctx, "修复Redis计数器失败", err,
					"transaction_id", rtCtx.Req.TransactionID)
			} else {
				log.ZInfo(ctx, "已修复Redis计数器",
					"transaction_id", rtCtx.Req.TransactionID,
					"new_count", 1)
			}
		}
	}

	// 预留已在前面确认

	// 发送领取通知（异步）
	sendRedPacketNotification(ctx, rtCtx, amount)

	return amount, nil
}

// compensateLuckyPacketReservation 补偿机制：拼手气红包MongoDB写入失败时回滚Redis状态
// 【重要】使用Lua脚本保证原子性，避免部分回滚导致数据不一致
func (m *ReceiveTransactionManagerV2) compensateLuckyPacketReservation(ctx context.Context, transactionID string, userID string) error {
	redisCli := m.redisProcessor.redisClient

	log.ZInfo(ctx, "开始执行拼手气红包补偿机制（原子化版本）",
		"transaction_id", transactionID,
		"user_id", userID)

	// 使用Lua脚本原子执行补偿操作，避免部分成功导致数据不一致
	compensateScript := redis.NewScript(`
		-- 补偿脚本：原子回滚Redis状态
		-- KEYS[1]: receivers集合 dep_transaction:{id}:receivers
		-- KEYS[2]: 计数器 dep_transaction:{id}:counter
		-- KEYS[3]: results哈希 dep_transaction:{id}:results
		-- KEYS[4]: 监控键前缀
		-- ARGV[1]: 用户ID
		
		local receivers_key = KEYS[1]
		local counter_key = KEYS[2]
		local results_key = KEYS[3]
		local monitor_prefix = KEYS[4]
		local user_id = ARGV[1]
		
		-- 检查用户是否在receivers集合中
		local was_in_receivers = redis.call('SISMEMBER', receivers_key, user_id)
		
		-- 原子执行所有补偿操作
		local removed = redis.call('SREM', receivers_key, user_id)
		local new_count = 0
		
		-- 只有当用户确实在receivers中时才恢复计数器，避免重复补偿导致计数器异常增加
		if removed > 0 then
			new_count = redis.call('INCR', counter_key)
		end
		
		-- 删除results中的记录
		redis.call('HDEL', results_key, user_id)
		
		-- 记录监控指标
		redis.call('INCR', monitor_prefix .. "compensations")
		if removed > 0 then
			redis.call('INCR', monitor_prefix .. "compensation_success")
		else
			redis.call('INCR', monitor_prefix .. "compensation_skipped")
		end
		
		return {was_in_receivers, removed, new_count}
	`)

	receiversKey := fmt.Sprintf("%s%s:receivers", constant.TransactionKeyPrefix, transactionID)
	countKey := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, transactionID)
	resultsKey := fmt.Sprintf("%s%s:results", constant.TransactionKeyPrefix, transactionID)
	monitorPrefix := fmt.Sprintf("dep_transaction_monitor:%s:", transactionID)

	// 【关键修复】补偿机制带重试，最多3次，避免瞬时网络问题导致补偿失败
	var result interface{}
	var err error
	const maxCompensateRetries = 3
	for i := 0; i < maxCompensateRetries; i++ {
		// 使用独立的 context 避免被上游取消
		compensateCtx := context.Background()
		if i > 0 {
			// 指数退避等待
			time.Sleep(time.Duration(1<<uint(i-1)) * 100 * time.Millisecond) // 100ms, 200ms, 400ms
		}
		result, err = compensateScript.Run(compensateCtx, redisCli,
			[]string{receiversKey, countKey, resultsKey, monitorPrefix},
			userID).Result()
		if err == nil {
			if i > 0 {
				log.ZInfo(ctx, "拼手气红包补偿机制重试成功",
					"transaction_id", transactionID,
					"user_id", userID,
					"retry_count", i)
			}
			break
		}
		log.ZWarn(ctx, "拼手气红包补偿机制执行失败，准备重试", err,
			"transaction_id", transactionID,
			"user_id", userID,
			"retry", i+1,
			"max_retries", maxCompensateRetries)
	}

	if err != nil {
		log.ZError(ctx, "拼手气红包补偿机制最终执行失败", err,
			"transaction_id", transactionID,
			"user_id", userID)
		// 补偿失败时记录告警，后续可接入告警系统
		m.recordLuckyPacketMetric(ctx, "compensation_failed")
		return err
	}

	// 解析结果
	if resultArr, ok := result.([]interface{}); ok && len(resultArr) >= 3 {
		wasInReceivers := resultArr[0]
		removed := resultArr[1]
		newCount := resultArr[2]
		log.ZInfo(ctx, "拼手气红包补偿机制执行完成",
			"transaction_id", transactionID,
			"user_id", userID,
			"was_in_receivers", wasInReceivers,
			"removed", removed,
			"new_counter", newCount)
	} else {
		log.ZInfo(ctx, "拼手气红包补偿机制执行完成",
			"transaction_id", transactionID,
			"user_id", userID,
			"result", result)
	}

	return nil
}

// safeCorrectCounterForLuckyPacket 安全纠正计数器和receivers集合（拼手气红包专用）
// 【增强版】同时纠正计数器和清理receivers集合中多余的用户
// totalCount: 红包总个数
// actualReceivedCount: MongoDB 中实际已领取记录数
// dbRemainingCount: 交易表中的剩余数量（可能不准确，仅作参考）
func (m *ReceiveTransactionManagerV2) safeCorrectCounterForLuckyPacket(ctx context.Context, transactionID string, totalCount int, actualReceivedCount int, dbRemainingCount int) error {
	redisCli := m.redisProcessor.redisClient

	// 计算真正应该剩余的数量（基于实际领取记录）
	trueRemainingCount := totalCount - actualReceivedCount
	if trueRemainingCount < 0 {
		trueRemainingCount = 0
	}

	// 构建Redis键名
	reservationsKey := fmt.Sprintf("%s%s:reservations", constant.TransactionKeyPrefix, transactionID)
	countKey := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, transactionID)
	receiversKey := fmt.Sprintf("%s%s:receivers", constant.TransactionKeyPrefix, transactionID)
	monitorPrefix := fmt.Sprintf("dep_transaction_monitor:%s:", transactionID)

	// 【关键修复】检查 Redis receivers 集合是否与 MongoDB 实际记录数不一致
	// 如果 Redis receivers 数量 > MongoDB 实际记录数，需要同步清理
	redisReceiversCount, _ := redisCli.SCard(ctx, receiversKey).Result()
	if int(redisReceiversCount) > actualReceivedCount {
		log.ZWarn(ctx, "检测到 Redis receivers 与 MongoDB 不一致，需要同步清理",
			nil,
			"transaction_id", transactionID,
			"redis_receivers", redisReceiversCount,
			"mongodb_received", actualReceivedCount)

		// 从 MongoDB 获取实际已领取的用户ID列表
		rrDao := model.NewReceiveRecordDao(m.dbProcessor.mongoDB)
		records, err := rrDao.GetByTransactionID(ctx, transactionID)
		if err == nil && records != nil {
			// 构建实际已领取用户的列表
			actualReceiverIDs := make([]interface{}, 0, len(records))
			for _, record := range records {
				actualReceiverIDs = append(actualReceiverIDs, record.UserID)
			}

			// 使用 Lua 脚本原子地重建 receivers 集合
			// 这避免了逐个删除导致的竞态条件
			rebuildScript := redis.NewScript(`
				local receivers_key = KEYS[1]
				local monitor_prefix = KEYS[2]
				
				-- 获取当前 receivers 数量（清理前）
				local old_count = redis.call('SCARD', receivers_key)
				
				-- 删除旧的 receivers 集合
				redis.call('DEL', receivers_key)
				
				-- 重新添加实际的接收者
				local new_count = 0
				for i = 1, #ARGV do
					if ARGV[i] ~= "" then
						redis.call('SADD', receivers_key, ARGV[i])
						new_count = new_count + 1
					end
				end
				
				-- 记录清理操作
				redis.call('INCR', monitor_prefix .. "receivers_rebuilt")
				redis.call('HSET', monitor_prefix .. "rebuild_details", 
					tostring(redis.call('TIME')[1]),
					"old=" .. old_count .. ",new=" .. new_count)
				
				return {old_count, new_count}
			`)

			result, rebuildErr := rebuildScript.Run(ctx, redisCli,
				[]string{receiversKey, monitorPrefix},
				actualReceiverIDs...).Result()

			if rebuildErr != nil {
				log.ZError(ctx, "重建 receivers 集合失败", rebuildErr,
					"transaction_id", transactionID)
			} else {
				log.ZWarn(ctx, "已重建 Redis receivers 集合",
					nil,
					"transaction_id", transactionID,
					"result", result)
			}
		}
	}

	// 使用Lua脚本原子性地纠正计数器（考虑进行中的预留）
	correctScript := redis.NewScript(`
		local count_key = KEYS[1]
		local receivers_key = KEYS[2]
		local reservations_key = KEYS[3]
		local monitor_prefix = KEYS[4]
		
		local total_count = tonumber(ARGV[1])
		local true_remaining = tonumber(ARGV[2])
		local db_remaining = tonumber(ARGV[3])
		
		local current_count = tonumber(redis.call('GET', count_key) or "0")
		local redis_receivers = redis.call('SCARD', receivers_key) or 0
		local pending_reservations = redis.call('HLEN', reservations_key) or 0
		
		-- 计算Redis认为的已领取数量（receivers + pending）
		local redis_claimed = redis_receivers + pending_reservations
		
		-- 计算Redis应该的剩余数量
		local expected_remaining = total_count - redis_claimed
		if expected_remaining < 0 then
			expected_remaining = 0
		end
		
		-- 安全的纠正值：取真实剩余、期望剩余、DB剩余中的最小值，减去正在处理的预留数
		local safe_count = math.min(true_remaining, expected_remaining, db_remaining) - pending_reservations
		if safe_count < 0 then
			safe_count = 0
		end
		
		-- 只有当当前计数明显异常时才纠正（计数为0或负数，且应该有剩余）
		if current_count <= 0 and safe_count > 0 then
			redis.call('SET', count_key, safe_count)
			redis.call('INCR', monitor_prefix .. "corrections")
			redis.call('HSET', monitor_prefix .. "correction_details", 
				tostring(os.time and os.time() or 0),
				"old=" .. current_count .. ",new=" .. safe_count .. 
				",true_rem=" .. true_remaining .. ",pending=" .. pending_reservations)
			return {"CORRECTED", current_count, safe_count, pending_reservations}
		elseif pending_reservations > 0 then
			-- 有预留进行中，跳过纠正
			return {"SKIPPED_PENDING", current_count, safe_count, pending_reservations}
		else
			return {"SKIPPED_OK", current_count, safe_count, pending_reservations}
		end
	`)

	result, err := correctScript.Run(ctx, redisCli,
		[]string{countKey, receiversKey, reservationsKey, monitorPrefix},
		totalCount, trueRemainingCount, dbRemainingCount).Result()
	if err != nil {
		log.ZError(ctx, "拼手气红包安全纠正计数器失败", err, "transaction_id", transactionID)
		return err
	}

	// 解析结果并记录详细日志
	if resultArr, ok := result.([]interface{}); ok && len(resultArr) >= 4 {
		status := resultArr[0]
		oldCount := resultArr[1]
		newCount := resultArr[2]
		pending := resultArr[3]

		if status == "CORRECTED" {
			m.recordLuckyPacketMetric(ctx, "safe_correct_success")
			log.ZWarn(ctx, "拼手气红包计数器已纠正",
				nil,
				"transaction_id", transactionID,
				"old_count", oldCount,
				"new_count", newCount,
				"pending_reservations", pending,
				"total_count", totalCount,
				"actual_received", actualReceivedCount,
				"true_remaining", trueRemainingCount,
				"db_remaining", dbRemainingCount)
		} else {
			m.recordLuckyPacketMetric(ctx, "safe_correct_skipped")
			log.ZInfo(ctx, "拼手气红包计数器纠正已跳过",
				"transaction_id", transactionID,
				"status", status,
				"current_count", oldCount,
				"calculated_safe_count", newCount,
				"pending_reservations", pending)
		}
	}

	return nil
}

// 拼手气红包动态预留过期时间：基础60秒，最大300秒
const (
	luckyPacketReservationBaseSec   = 60
	luckyPacketReservationSafetySec = 30
	luckyPacketReservationMaxSec    = 300
	luckyPacketReservationMinSec    = 60
)

// ensureRedisConsistencyAsync 异步确保Redis状态与MongoDB一致
// 当发现MongoDB有记录但Redis可能不一致时调用，用于修复脏数据
func (m *ReceiveTransactionManagerV2) ensureRedisConsistencyAsync(ctx context.Context, transactionID string, userID string) {
	defer func() {
		if r := recover(); r != nil {
			log.ZError(ctx, "ensureRedisConsistencyAsync panic recovered", nil,
				"transaction_id", transactionID,
				"user_id", userID,
				"panic", r)
		}
	}()

	redisCli := m.redisProcessor.redisClient
	receiversKey := fmt.Sprintf("%s%s:receivers", constant.TransactionKeyPrefix, transactionID)

	// 检查用户是否在Redis receivers集合中
	isMember, err := redisCli.SIsMember(ctx, receiversKey, userID).Result()
	if err != nil {
		log.ZWarn(ctx, "检查Redis receivers失败", err,
			"transaction_id", transactionID,
			"user_id", userID)
		return
	}

	// 如果不在集合中，添加进去（修复不一致）
	if !isMember {
		if err := redisCli.SAdd(ctx, receiversKey, userID).Err(); err != nil {
			log.ZWarn(ctx, "修复Redis receivers失败", err,
				"transaction_id", transactionID,
				"user_id", userID)
		} else {
			log.ZInfo(ctx, "已修复Redis receivers（添加缺失用户）",
				"transaction_id", transactionID,
				"user_id", userID)

			// 同时检查并可能修正计数器
			m.syncCounterWithMongoDB(ctx, transactionID)
		}
	}
}

// syncCounterWithMongoDB 同步Redis计数器与MongoDB实际数据
func (m *ReceiveTransactionManagerV2) syncCounterWithMongoDB(ctx context.Context, transactionID string) {
	// 获取交易信息
	txDao := model.NewTransactionDao(m.dbProcessor.mongoDB)
	tx, err := txDao.GetByTransactionID(ctx, transactionID)
	if err != nil || tx == nil {
		return
	}

	// 获取实际领取记录数
	rrDao := model.NewReceiveRecordDao(m.dbProcessor.mongoDB)
	records, err := rrDao.GetByTransactionID(ctx, transactionID)
	if err != nil {
		return
	}

	actualReceived := len(records)
	trueRemaining := tx.TotalCount - actualReceived
	if trueRemaining < 0 {
		trueRemaining = 0
	}

	// 检查Redis计数器
	redisCli := m.redisProcessor.redisClient
	countKey := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, transactionID)
	currentCount, err := redisCli.Get(ctx, countKey).Int64()
	if err != nil && err != redis.Nil {
		return
	}

	// 如果计数器与真实剩余差距较大，进行修正
	if int64(trueRemaining) != currentCount {
		// 检查是否有进行中的预留
		reservationsKey := fmt.Sprintf("%s%s:reservations", constant.TransactionKeyPrefix, transactionID)
		pendingCount, _ := redisCli.HLen(ctx, reservationsKey).Result()

		// 只有没有预留时才修正
		if pendingCount == 0 {
			if err := redisCli.Set(ctx, countKey, trueRemaining, 24*time.Hour).Err(); err == nil {
				log.ZWarn(ctx, "已同步Redis计数器与MongoDB",
					nil,
					"transaction_id", transactionID,
					"old_count", currentCount,
					"new_count", trueRemaining,
					"actual_received", actualReceived,
					"total_count", tx.TotalCount)
			}
		}
	}
}

// P2：拼手气红包全局监控 Redis 键前缀（用于告警/大盘）
const luckyPacketMonitorPrefix = "dep_transaction_monitor:lucky_packet:"

// P2：是否启用拼手气红包异步 MongoDB 写入（默认关闭，开启后先返回金额再异步落库，失败则补偿）
const EnableLuckyPacketAsyncDBWrite = false

// asyncWriteLuckyPacketDB P2：拼手气红包异步落库（仅在 EnableLuckyPacketAsyncDBWrite 为 true 时使用）
func (m *ReceiveTransactionManagerV2) asyncWriteLuckyPacketDB(bgCtx context.Context, rtCtx *ReceiveTransactionContext, result *LuaScriptResult, amount string) {
	err := m.processDBTransactionWithRetryForLuckyPacket(bgCtx, rtCtx, result)
	if err != nil {
		log.ZError(bgCtx, "拼手气红包异步MongoDB写入失败，执行补偿", err,
			"transaction_id", rtCtx.Req.TransactionID,
			"receiver_id", rtCtx.Req.ReceiverID)
		_ = m.compensateLuckyPacketReservation(bgCtx, rtCtx.Req.TransactionID, rtCtx.Req.ReceiverID)
	}
}

// recordLuckyPacketMetric P2：记录拼手气红包监控指标到 Redis（供告警/大盘使用）
func (m *ReceiveTransactionManagerV2) recordLuckyPacketMetric(ctx context.Context, metric string) {
	key := luckyPacketMonitorPrefix + metric
	if err := m.redisProcessor.redisClient.Incr(ctx, key).Err(); err != nil {
		log.ZWarn(ctx, "拼手气红包监控指标写入失败", err, "key", key)
		return
	}
	m.redisProcessor.redisClient.Expire(ctx, key, 24*time.Hour)
}

// processDBTransactionWithRetryForLuckyPacket 拼手气红包MongoDB写入带重试（最多3次，指数退避）
func (m *ReceiveTransactionManagerV2) processDBTransactionWithRetryForLuckyPacket(ctx context.Context, rtCtx *ReceiveTransactionContext, result *LuaScriptResult) error {
	const maxRetries = 3
	baseDelay := time.Second
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		lastErr = m.dbProcessor.ProcessDBTransaction(ctx, rtCtx, result)
		if lastErr == nil {
			if i > 0 {
				m.recordLuckyPacketMetric(ctx, "retry_success")
				log.ZInfo(ctx, "拼手气红包MongoDB写入重试成功",
					"transaction_id", rtCtx.Req.TransactionID,
					"receiver_id", rtCtx.Req.ReceiverID,
					"retry_count", i)
			}
			return nil
		}
		if i < maxRetries-1 {
			delay := baseDelay * time.Duration(1<<uint(i))
			log.ZWarn(ctx, "拼手气红包MongoDB写入失败，准备重试",
				lastErr,
				"transaction_id", rtCtx.Req.TransactionID,
				"receiver_id", rtCtx.Req.ReceiverID,
				"retry", i+1,
				"max_retries", maxRetries,
				"delay", delay)
			time.Sleep(delay)
		}
	}
	log.ZError(ctx, "拼手气红包MongoDB写入最终失败，已重试所有次数",
		lastErr,
		"transaction_id", rtCtx.Req.TransactionID,
		"receiver_id", rtCtx.Req.ReceiverID,
		"retry_count", maxRetries)
	return lastErr
}

// getDynamicReservationExpiryForLuckyPacket 根据近期MongoDB写入耗时计算拼手气红包预留过期时间（秒）
func (m *ReceiveTransactionManagerV2) getDynamicReservationExpiryForLuckyPacket(ctx context.Context, transactionID string) int {
	monitorPrefix := fmt.Sprintf("dep_transaction_monitor:%s:", transactionID)
	times, err := m.redisProcessor.redisClient.LRange(ctx, monitorPrefix+"processing_times", 0, 99).Result()
	if err != nil || len(times) == 0 {
		return luckyPacketReservationBaseSec
	}
	var sum int64
	count := 0
	for _, t := range times {
		if val, parseErr := strconv.ParseInt(t, 10, 64); parseErr == nil && val >= 0 {
			sum += val
			count++
		}
	}
	if count == 0 {
		return luckyPacketReservationBaseSec
	}
	avgSec := sum / int64(count)
	expiry := luckyPacketReservationBaseSec + int(avgSec) + luckyPacketReservationSafetySec
	if expiry > luckyPacketReservationMaxSec {
		expiry = luckyPacketReservationMaxSec
	}
	if expiry < luckyPacketReservationMinSec {
		expiry = luckyPacketReservationMinSec
	}
	log.ZInfo(ctx, "拼手气红包动态预留过期时间",
		"transaction_id", transactionID,
		"avg_processing_sec", avgSec,
		"expiry_sec", expiry)
	return expiry
}

// sendRedPacketNotification 发送红包领取通知（通知发送者和接收者）
// 已关闭：不再向发送者/领取者发 IM 通知，仅依赖用户点击红包查看领取结果，减轻客户端与服务端数据量及 message too large 风险。
// 线索：此前客户端会出现「自己给自己发消息、内容为暂不支持的消息类型」，即因本条会向 recvID=发送者 与 recvID=领取者 各发一条单聊（contentType 101），
// 客户端未解析则展示为「暂不支持的消息类型」；关闭后该现象已消除。
func sendRedPacketNotification(ctx context.Context, rtCtx *ReceiveTransactionContext, amount string) {
	// 不再发送「xxx 领取了您的红包」「您领取了红包 xxx」等 IM 消息，用户通过点击红包查看详情即可
	_ = ctx
	_ = rtCtx
	_ = amount
}

// RepairTransactionConsistency 修复红包交易数据一致性
// 当Redis与MongoDB数据不一致时调用，基于MongoDB实际领取记录修复：
// 1. 修复交易表的remaining_count（基于实际领取记录数）
// 2. 修复Redis计数器
// 3. 同步Redis receivers集合
func (t *TransactionService) RepairTransactionConsistency(ctx context.Context, transactionID string) (map[string]interface{}, error) {
	mongoDB := plugin.MongoCli().GetDB()
	redisCli := plugin.RedisCli()

	result := make(map[string]interface{})

	// 1. 获取交易信息
	txDao := model.NewTransactionDao(mongoDB)
	tx, err := txDao.GetByTransactionID(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("获取交易信息失败: %v", err)
	}
	if tx == nil {
		return nil, fmt.Errorf("交易不存在: %s", transactionID)
	}

	result["transaction_id"] = transactionID
	result["total_count"] = tx.TotalCount
	result["db_remaining_count_before"] = tx.RemainingCount
	result["db_status_before"] = tx.Status

	// 2. 获取实际领取记录数（最可靠的数据源）
	rrDao := model.NewReceiveRecordDao(mongoDB)
	records, err := rrDao.GetByTransactionID(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("获取领取记录失败: %v", err)
	}

	actualReceivedCount := len(records)
	result["actual_received_count"] = actualReceivedCount

	// 3. 计算真正的剩余数量
	trueRemainingCount := tx.TotalCount - actualReceivedCount
	if trueRemainingCount < 0 {
		trueRemainingCount = 0
	}
	result["true_remaining_count"] = trueRemainingCount

	// 4. 计算实际领取总金额
	totalReceivedAmount := decimal.Zero
	receiverIDs := make([]string, 0, len(records))
	for _, rec := range records {
		amountStr := rec.Amount.String()
		if amt, parseErr := decimal.NewFromString(amountStr); parseErr == nil {
			totalReceivedAmount = totalReceivedAmount.Add(amt)
		}
		receiverIDs = append(receiverIDs, rec.UserID)
	}

	// 计算真正的剩余金额
	totalAmountStr := tx.TotalAmount.String()
	totalAmount, _ := decimal.NewFromString(totalAmountStr)
	trueRemainingAmount := totalAmount.Sub(totalReceivedAmount)
	if trueRemainingAmount.IsNegative() {
		trueRemainingAmount = decimal.Zero
	}

	result["total_amount"] = totalAmountStr
	result["total_received_amount"] = totalReceivedAmount.String()
	result["true_remaining_amount"] = trueRemainingAmount.String()

	// 5. 修复MongoDB交易表
	if tx.RemainingCount != trueRemainingCount {
		trueRemainingAmountDecimal128, _ := primitive.ParseDecimal128(trueRemainingAmount.StringFixed(9))

		// 根据是否还有剩余决定状态
		if trueRemainingCount == 0 {
			// 已领完，更新为完成状态
			if err := txDao.UpdateTransactionComplete(ctx, transactionID); err != nil {
				log.ZError(ctx, "修复：更新交易为完成状态失败", err, "transaction_id", transactionID)
			} else {
				result["db_fix_action"] = "updated_to_complete"
			}
		} else {
			// 还有剩余，更新剩余数量和金额
			if err := txDao.UpdateRemainingAmountAndCount(ctx, transactionID, trueRemainingAmountDecimal128, trueRemainingCount); err != nil {
				log.ZError(ctx, "修复：更新交易剩余数量失败", err, "transaction_id", transactionID)
			} else {
				result["db_fix_action"] = "updated_remaining"
			}
		}
		result["db_remaining_count_after"] = trueRemainingCount
	} else {
		result["db_fix_action"] = "no_change_needed"
	}

	// 6. 修复Redis计数器
	countKey := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, transactionID)
	oldRedisCount, _ := redisCli.Get(ctx, countKey).Int64()
	result["redis_counter_before"] = oldRedisCount

	if err := redisCli.Set(ctx, countKey, trueRemainingCount, 24*time.Hour).Err(); err != nil {
		log.ZError(ctx, "修复：设置Redis计数器失败", err, "transaction_id", transactionID)
	} else {
		result["redis_counter_after"] = trueRemainingCount
	}

	// 7. 修复Redis receivers集合（清空后重新添加所有已领取用户）
	receiversKey := fmt.Sprintf("%s%s:receivers", constant.TransactionKeyPrefix, transactionID)
	oldReceiversCount, _ := redisCli.SCard(ctx, receiversKey).Result()
	result["redis_receivers_before"] = oldReceiversCount

	// 使用Pipeline原子操作
	pipe := redisCli.Pipeline()
	pipe.Del(ctx, receiversKey)
	if len(receiverIDs) > 0 {
		// 分批添加避免单次命令过大
		for i := 0; i < len(receiverIDs); i += 100 {
			end := i + 100
			if end > len(receiverIDs) {
				end = len(receiverIDs)
			}
			batch := receiverIDs[i:end]
			args := make([]interface{}, len(batch))
			for j, id := range batch {
				args[j] = id
			}
			pipe.SAdd(ctx, receiversKey, args...)
		}
	}
	pipe.Expire(ctx, receiversKey, 24*time.Hour)

	if _, pipeErr := pipe.Exec(ctx); pipeErr != nil {
		log.ZError(ctx, "修复：同步Redis receivers失败", pipeErr, "transaction_id", transactionID)
	} else {
		result["redis_receivers_after"] = len(receiverIDs)
	}

	// 8. 清理预留表（避免脏数据影响后续领取）
	reservationsKey := fmt.Sprintf("%s%s:reservations", constant.TransactionKeyPrefix, transactionID)
	reservationsTimeKey := fmt.Sprintf("%s%s:reservations:time", constant.TransactionKeyPrefix, transactionID)
	pendingCount, _ := redisCli.HLen(ctx, reservationsKey).Result()
	result["redis_reservations_before"] = pendingCount

	if pendingCount > 0 {
		redisCli.Del(ctx, reservationsKey, reservationsTimeKey)
		result["redis_reservations_cleared"] = true
	}

	// 9. 更新Redis交易元数据（含 remaining_amount，避免 Hash 不完整）
	transKey := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, transactionID)
	newStatus := tx.Status
	if trueRemainingCount == 0 {
		newStatus = model.TransactionStatusComplete
	}
	redisCli.HSet(ctx, transKey, "status", newStatus)
	redisCli.HSet(ctx, transKey, "remaining_count", trueRemainingCount)
	redisCli.HSet(ctx, transKey, "remaining_amount", trueRemainingAmount.String())
	redisCli.HSet(ctx, transKey, "total_count", tx.TotalCount)
	redisCli.HSet(ctx, transKey, "total_amount", tx.TotalAmount.String())
	redisCli.Expire(ctx, transKey, 24*time.Hour)

	log.ZWarn(ctx, "红包数据一致性修复完成", nil,
		"transaction_id", transactionID,
		"result", result)

	return result, nil
}
