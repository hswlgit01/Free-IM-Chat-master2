package freeErrors

import (
	"fmt"

	"github.com/openimsdk/tools/errs"
)

// 错误码定义
// error code为1开头的5位数,openIM的错误code是四位数,Im-chat的错误code是2开头5位数
const (
	// Common errors: 10000-10099
	ErrSystem            = 10000 // 系统错误
	ErrInvalidParams     = 10001 // 无效的参数
	ErrUnauthorized      = 10002 // 未授权的访问
	ErrForbidden         = 10003 // 禁止访问
	ErrTooFrequent       = 10004 // 操作太频繁
	ErrNotFoundCode      = 10005 // 资源未找到
	ErrInvalidPageParams = 10007 // 分页参数错误
	ErrLiveKitUrl        = 10010 // live kit地址错误

	ErrApiCode     = 10090 // api公共错误
	ErrCaptchaCode = 10091 // 数字验证码错误
	ErrVerifyCode  = 10092 // 验证码错误

	// Transaction errors: 10100-10199
	ErrInsufficientBalance                  = 10100 // 余额不足
	ErrTransactionExpired                   = 10101 // 交易已过期
	ErrTransactionNotFound                  = 10102 // 交易不存在
	ErrInvalidAmount                        = 10103 // 无效的金额
	ErrAlreadyReceived                      = 10104 // 已领取过该交易
	ErrNotInGroup                           = 10105 // 不在群组中
	ErrNotFriend                            = 10106 // 不是好友关系
	WalletNotOpenCode                       = 10107 // 钱包未开通
	WalletOpenedCode                        = 10108 // 钱包已开通
	ErrNoRemaining                          = 10109 // 没有剩余可领取
	ErrDistributedLock                      = 10110 // 获取分布式锁失败
	ErrRedPacketCountExceedGroupMemberCount = 10111 // 红包数量超过群成员数
	ErrRedPacketAmountNotDivisible          = 10112 // 普通红包总额不能被总数整除
	WalletBalanceNotOpenCode                = 10113 // 钱包余额未开通

	// 新增红包交易错误码: 10130-10140
	ErrOperationTooFrequent = 10130 // 操作过于频繁
	ErrInvalidTransaction   = 10131 // 交易无效或已过期

	// ValidateReceiverInfo 相关错误码: 10114-10130
	ErrReceiverNotInOrganization      = 10114 // 接收者不属于该组织
	ErrUserNotInSameOrganization      = 10115 // 接收者和发送者不在同一组织
	ErrCannotReceiveOwnTransfer       = 10116 // 转账类型不允许发送者接收
	ErrReceiverNotTargetUser          = 10117 // 接收者不是交易的目标用户
	ErrOrgTransferReceiverMustBeAdmin = 10118 // 组织转账的接收者必须是管理员
	ErrReceiverNotExclusiveReceiver   = 10119 // 接收者不是专属红包的指定接收者
	ErrUnknownTransactionType         = 10120 // 未知的交易类型
	ErrIncorrectPassword              = 10121 // 口令红包密码错误
	ErrPasswordCannotBeEmpty          = 10122 // 口令红包密码不能为空

	// user errors: 10300-10399
	ErrDeviceRegisterNumExceed  = 10300 // 超过设备注册上限
	ErrImportUserExcelRowExceed = 10301 // 导入用户Excel行数超出限制

	// organization errors: 10400-10499
	ErrInvalidInvitationCodeCode = 10400 // 无效的邀请码

	// account errors: 11000-11500
	UserAccountErrCode = 11002
	UserNotFoundCode   = 11003
	UserPwdErrCode     = 11004
	ErrEmailInUse      = 11005 // 邮箱已被使用
	ErrAccountExists   = 11006 // 账户已存在

	// livestream errors: 11500-12000
	ErrLiveStreamRoomNotFound                     = 11501 // 直播间不存在
	ErrLiveStreamRoomExecutePermissionErrCode     = 11510 // 执行者权限不足
	ErrLiveStreamRoomParticipantPermissionErrCode = 11511 // 参与者权限不足
	ErrLiveStreamParticipantBlocked               = 11512 // 参与者已被屏蔽
	ErrLiveStreamSystemErr                        = 11520 // 直播系统错误

	// lottery errors: 12000-12099
	ErrLotteryRewardInUse = 12001 // 抽奖奖品正在使用中
	ErrLotteryNameExists  = 12002 // 抽奖活动名称已存在

	// 验证码相关错误: 20000-20099
	ErrVerifyCodeNotMatch = 20006 // 验证码不匹配

)

// common errors
var (
	SystemErr = func(err error) error {
		if err == nil {
			return nil
		}
		return errs.NewCodeError(ErrSystem, fmt.Errorf("system error: %v", err).Error())
	}
	ApiErr = func(detail string) error {
		return errs.NewCodeError(ErrApiCode, fmt.Sprintf("api error: %s", detail))
	}

	CaptchaErr = func(detail string) error {
		return errs.NewCodeError(ErrCaptchaCode, fmt.Sprintf("captcha error: %s", detail))
	}

	ForbiddenErr = func(msg string) error {
		if msg == "" {
			msg = "Access forbidden!"
		}
		return errs.NewCodeError(ErrForbidden, msg)
	}

	NotFoundErr             = errs.NewCodeError(ErrNotFoundCode, "Resource not found")
	NotFoundErrWithResource = func(resource string) error {
		return errs.NewCodeError(ErrNotFoundCode, fmt.Sprintf("Resource not found: %s", resource))
	}

	ParameterInvalidErr     = errs.NewCodeError(ErrInvalidParams, "Invalid parameters")
	PageParameterInvalidErr = errs.NewCodeError(ErrInvalidPageParams, "Invalid pagination parameters")
	VerifyCodeNotMatchErr   = errs.NewCodeError(ErrVerifyCodeNotMatch, "Verify code not match")
)

// transaction errors
var (
	WalletNotOpenErr             = errs.NewCodeError(WalletNotOpenCode, "Wallet not opened")
	WalletOpenedErr              = errs.NewCodeError(WalletOpenedCode, "Wallet already opened")
	WalletInsufficientBalanceErr = errs.NewCodeError(ErrInsufficientBalance, "Insufficient wallet balance")
)

// organization errors
var (
	InvitationCodeErr = func(code string) error {
		return errs.NewCodeError(ErrInvalidInvitationCodeCode, "Invalid invitation code: "+code)
	}
)

// user errors
var (
	DeviceRegisterNumExceedErr = func(deviceCode string, num int) error {
		return errs.NewCodeError(ErrDeviceRegisterNumExceed, fmt.Sprintf("Device: %s register number exceeded: %d", deviceCode, num))
	}
	ImportUserExcelRowExceedErr = func(num int) error {
		return errs.NewCodeError(ErrImportUserExcelRowExceed, fmt.Sprintf("Import user excel row exceeded: %d", num))
	}
)

// account errors
var (
	UserAccountErr   = errs.NewCodeError(UserAccountErrCode, "User verification failed")
	UserNotFoundErr  = errs.NewCodeError(UserNotFoundCode, "User not found")
	UserPwdErrErr    = errs.NewCodeError(UserPwdErrCode, "Incorrect password")
	EmailInUseErr    = errs.NewCodeError(ErrEmailInUse, "Email already in use")
	AccountExistsErr = errs.NewCodeError(ErrAccountExists, "Account already exists")
)

// liveKit errors
var (
	LiveKitUrlErr             = errs.NewCodeError(ErrLiveKitUrl, "live url error")
	LiveStreamRoomNotFoundErr = func(roomName string) error {
		return errs.NewCodeError(ErrLiveStreamRoomNotFound, "Live stream room not found! room name: "+roomName)
	}
	LiveStreamSystemErr = func(err error) error {
		if err == nil {
			return nil
		}
		return errs.NewCodeError(ErrLiveStreamSystemErr, fmt.Sprintf("Live stream system error: %v", err))
	}
	LiveStreamRoomExecutePermissionErr     = errs.NewCodeError(ErrLiveStreamRoomExecutePermissionErrCode, "Execute permission denied")
	LiveStreamRoomParticipantPermissionErr = errs.NewCodeError(ErrLiveStreamRoomParticipantPermissionErrCode, "Participant permission denied")
	LiveStreamParticipantBlockedErr        = func(identity string) error {
		return errs.NewCodeError(ErrLiveStreamParticipantBlocked, "Participant blocked! identity: "+identity)
	}
)

// lottery errors
var (
	LotteryRewardInUseErr = errs.NewCodeError(ErrLotteryRewardInUse, "This reward is currently being used by lottery activities and cannot be deleted")
	LotteryNameExistsErr  = errs.NewCodeError(ErrLotteryNameExists, "Lottery name already exists")
)

// ErrorMessages 错误信息映射
var ErrorMessages = map[int]string{
	// System errors
	ErrSystem:        "System error",
	ErrInvalidParams: "Invalid parameters",
	ErrUnauthorized:  "Unauthorized access",
	ErrTooFrequent:   "Operation too frequent",
	// Transaction errors
	ErrInsufficientBalance:                  "Insufficient balance",
	ErrTransactionExpired:                   "Transaction expired",
	ErrTransactionNotFound:                  "Transaction not found",
	ErrInvalidAmount:                        "Invalid amount",
	ErrAlreadyReceived:                      "Transaction already received",
	ErrNotInGroup:                           "Not in group",
	ErrNotFriend:                            "Not friend with target user",
	WalletNotOpenCode:                       "Wallet not opened",
	WalletBalanceNotOpenCode:                "Wallet balance not opened",
	WalletOpenedCode:                        "Wallet already opened",
	ErrNoRemaining:                          "No remaining amount or count",
	ErrDistributedLock:                      "Failed to acquire distributed lock",
	ErrRedPacketCountExceedGroupMemberCount: "Red packet count exceeds group member count",
	ErrRedPacketAmountNotDivisible:          "Normal red packet amount must be divisible by count",
	ErrOperationTooFrequent:                 "Operation too frequent, please try again later",
	ErrInvalidTransaction:                   "Transaction is invalid or expired",

	// ValidateReceiverInfo 相关错误信息
	ErrReceiverNotInOrganization:      "Receiver is not in the organization",
	ErrUserNotInSameOrganization:      "Receiver and sender are not in the same organization",
	ErrCannotReceiveOwnTransfer:       "Cannot receive your own transfer",
	ErrReceiverNotTargetUser:          "Receiver is not the target user of this transaction",
	ErrOrgTransferReceiverMustBeAdmin: "Organization transfer receiver must be an administrator",
	ErrReceiverNotExclusiveReceiver:   "Receiver is not the exclusive receiver of this red packet",
	ErrUnknownTransactionType:         "Unknown transaction type",
	ErrIncorrectPassword:              "Incorrect password for password red packet",
	ErrPasswordCannotBeEmpty:          "Password cannot be empty for password red packet",

	// Account errors
	UserAccountErrCode: "User account error",
	UserNotFoundCode:   "User not found",
	UserPwdErrCode:     "Password error",
	ErrEmailInUse:      "Email already in use",
	ErrAccountExists:   "Account already exists",

	// Lottery errors
	ErrLotteryRewardInUse: "This reward is currently being used by lottery activities and cannot be deleted",
	ErrLotteryNameExists:  "Lottery name already exists",

	// Verify code errors
	ErrVerifyCodeNotMatch: "Verify code not match",
}
