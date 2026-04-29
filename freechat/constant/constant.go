package constant

import "go.mongodb.org/mongo-driver/bson/primitive"

// Redis相关常量

const client = "dep_transaction"

const (
	// TransactionKeyPrefix 交易信息发起key
	TransactionKeyPrefix = client + ":"
	// TransactionLockPrefix 交易锁key前缀
	TransactionLockPrefix = client + "_lock:"
	// TransactionReceiversPrefix 交易接收者集合key前缀
	TransactionReceiversPrefix = client + "_receivers:"
)

// 业务相关常量
const (
	MaxRedPacketCount = 1000 // 最大红包数
	MinRedPacketCount = 1    // 最小红包数

	// 交易过期时间（秒）
	TransactionExpireTime = 86400 // 24小时 = 86400秒

	// ============= 货币精度限制相关常量 =============
	//
	// Lua脚本精度限制：Redis中的Lua脚本使用IEEE 754双精度浮点数
	// 关键计算：remaining_amount_int = math.floor(remaining_amount * 10^precision + 0.5)
	// 安全条件：amount × 10^precision ≤ 2^53 (9,007,199,254,740,992)
	//
	MaxCurrencyDecimals = 9 // 货币最大精度位数：9位小数
	MinCurrencyDecimals = 0 // 货币最小精度位数：0位小数

	// 红包金额限制
	MaxCreateRedPacketAmount  = "5000000" // 创建红包最大金额：500万元
	DefaultMaxRedPacketAmount = "100000"  // 默认最大红包金额：10万元
)

// 数据库相关常量
const (
	// MongoDB集合名
	CollectionTransaction                = "transaction_record"
	CollectionReceive                    = "transaction_receive_record"
	CollectionRefund                     = "transaction_refund_record"
	CollectionWalletBalance              = "wallet_balance"
	CollectionWalletCurrency             = "wallet_currency"
	CollectionWalletInfo                 = "wallet_info"
	CollectionWalletTsRecord             = "wallet_transaction_record"
	CollectionWalletSettings             = "wallet_settings"
	CollectionUserKeys                   = "user_keys"
	CollectionLivestreamStatistics       = "livestream_statistics"
	CollectionExchangeRate               = "exchange_rate" // 汇率表
	CollectionOrganization               = "organization"
	CollectionOrganizationRolePermission = "organization_role_permission"
	CollectionOrganizationUser           = "organization_user"
	CollectionGroup                      = "group"
	CollectionUser                       = "user"
	CollectionGroupOperationLog          = "group_operation_log" // 群操作日志表
	CollectionOperationLog               = "operation_log"       // 群操作日志表
	CollectionAppLog                     = "app_log"             // App 客户端日志表

	CollectionChangeOrgRecord = "user_change_org"

	CollectionCheckin                  = "checkin"
	CollectionCheckinRewardConfig      = "checkin_reward_config"
	CollectionCheckinReward            = "checkin_reward"
	CollectionDailyCheckinRewardConfig = "daily_checkin_reward_config"

	CollectionLottery           = "lottery"
	CollectionLotteryConfig     = "lottery_config"
	CollectionLotteryUserTicket = "lottery_user_ticket"
	CollectionLotteryReward     = "lottery_reward"
	CollectionLotteryUserRecord = "lottery_user_record"

	CollectionWebhook        = "webhook"
	CollectionWebhookTrigger = "webhook_trigger"

	CollectionPoints  = "points"
	CollectionArticle = "article"

	// 超管封禁相关表
	CollectionSuperAdminForbidden       = "super_admin_forbidden"        // 超管封禁主表
	CollectionSuperAdminForbiddenDetail = "super_admin_forbidden_detail" // 超管封禁详情表
	CollectionSuperAdminForbiddenRecord = "super_admin_forbidden_record" // 超管封禁解封记录表
)

// 通知系统默认配置
const (
	//系统通知用户的ID
	NOTIFICATION_ADMIN_SEND_ID = "imAdmin"
	//系统通知用户的ID
	NOTIFICATION_ADMIN_PAYMENT_SEND_ID = "001"
	// 默认通知名称
	DefaultNotificationName = "Payment"
	// 默认通知头像URL
	DefaultNotificationFaceURL = "https://cweb.chatcdn.org/api/object/07965083964418762527/msg_picture_2d7095971b5cbe01af1e42e2db355b43.png"
)

// 运行模式
const (
	ModeProd = "prod"
	ModeTest = "test"
)

func GetSystemCreatorId() primitive.ObjectID {
	return primitive.NilObjectID
}
