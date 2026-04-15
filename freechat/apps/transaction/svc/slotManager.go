package svc

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/openimsdk/chat/freechat/apps/transaction/scripts"
	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/tools/log"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	// 脚本文件名
	ReserveSlotScriptFile        = "reserve_slot.lua"
	ConfirmReservationScriptFile = "confirm_reservation.lua"

	// 默认预留超时时间(秒)
	DefaultReservationExpiry = 60
)

// ReservationResult 预留结果
type ReservationResult struct {
	Status    string `json:"status"`
	Reason    string `json:"reason"`
	Remaining int64  `json:"remaining,omitempty"`
}

// RedPacketSlotManager 红包槽位管理器
type RedPacketSlotManager struct {
	redisClient       redis.UniversalClient
	reserveScript     *redis.Script
	confirmScript     *redis.Script
	reservationExpiry int // 预留超时时间(秒)
}

// NewRedPacketSlotManager 创建红包槽位管理器
func NewRedPacketSlotManager(redisClient redis.UniversalClient) (*RedPacketSlotManager, error) {
	// 设置预留超时时间(秒)
	reservationExpiry := DefaultReservationExpiry

	// 使用Go embed嵌入的Lua脚本
	var reserveScriptContent, confirmScriptContent []byte
	var loadError error

	// 记录尝试加载脚本的信息
	log.ZInfo(context.Background(), "开始从嵌入资源加载红包槽位预留Lua脚本")

	// 从嵌入资源中加载预留脚本
	reserveScriptContent, loadError = scripts.GetReserveSlotScript()
	if loadError != nil {
		log.ZError(context.Background(), "无法从嵌入资源加载预留脚本", loadError)
		return nil, loadError
	}

	// 从嵌入资源中加载确认脚本
	confirmScriptContent, loadError = scripts.GetConfirmReservationScript()
	if loadError != nil {
		log.ZError(context.Background(), "无法从嵌入资源加载确认脚本", loadError)
		return nil, loadError
	}

	log.ZInfo(context.Background(), "成功从嵌入资源加载Lua脚本")

	// 以下是内联硬编码脚本的后备方案，使用嵌入式后不需要
	if false { // 使用embed特性后不再需要此分支
		log.ZWarn(context.Background(), "无法从文件加载Lua脚本，使用内联脚本", loadError)

		// 内联脚本（与文件内容一致）
		reserveScriptContent = []byte(`-- 预留红包名额脚本
-- KEYS[1]: 红包元数据键 dep_transaction:{id}
-- KEYS[2]: 红包计数器 dep_transaction:{id}:counter
-- KEYS[3]: 已领用户集 dep_transaction:{id}:receivers
-- KEYS[4]: 预留状态表 dep_transaction:{id}:reservations
-- ARGV[1]: 用户ID
-- ARGV[2]: 预留ID (UUID)
-- ARGV[3]: 当前时间戳
-- ARGV[4]: 预留超时时间(秒)
-- 返回: {status, reason, ...其他数据}

-- 1. 检查用户是否已领取过
if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then
    return {status="FAILED", reason="ALREADY_RECEIVED"}
end

-- 2. 检查用户是否已有进行中的预留
local reservation = redis.call('HGET', KEYS[4], ARGV[1])
if reservation then
    -- 由于我们使用的是简单字符串而非JSON，这里简化检查
    -- 实际生产环境中应解析JSON并检查超时
    return {status="FAILED", reason="RESERVATION_EXISTS"}
end

-- 3. 检查红包是否已领完
local remaining = tonumber(redis.call('GET', KEYS[2]) or "0")
if remaining <= 0 then
    return {status="FAILED", reason="PACKET_EMPTY"}
end

-- 4. 原子减少计数器并预留名额
redis.call('DECR', KEYS[2])
redis.call('HSET', KEYS[4], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[4], tonumber(ARGV[4]))

-- 5. 返回预留成功
return {
    status = "SUCCESS",
    reason = "RESERVED",
    remaining = remaining - 1
}`)

		confirmScriptContent = []byte(`-- 确认预留脚本
-- KEYS[1]: 红包计数器 dep_transaction:{id}:counter
-- KEYS[2]: 已领用户集 dep_transaction:{id}:receivers
-- KEYS[3]: 预留状态表 dep_transaction:{id}:reservations
-- KEYS[4]: 领取结果集 dep_transaction:{id}:results
-- ARGV[1]: 用户ID
-- ARGV[2]: 预留ID
-- ARGV[3]: 确认结果 (SUCCESS|FAILED)
-- ARGV[4]: 领取金额 (成功时)或失败原因(失败时)

-- 1. 检查预留是否存在且匹配
local reservation = redis.call('HGET', KEYS[3], ARGV[1])
if not reservation then
    return {status="FAILED", reason="RESERVATION_NOT_FOUND"}
end

if reservation ~= ARGV[2] then
    return {status="FAILED", reason="RESERVATION_ID_MISMATCH"}
end

-- 2. 根据确认结果处理
if ARGV[3] == "SUCCESS" then
    -- 成功领取：添加到已领用户集合，删除预留，记录结果
    redis.call('SADD', KEYS[2], ARGV[1])
    redis.call('HDEL', KEYS[3], ARGV[1])
    redis.call('HSET', KEYS[4], ARGV[1], ARGV[4])

    return {status="SUCCESS", reason="CONFIRMED"}
else
    -- 失败：恢复计数器，删除预留
    redis.call('INCR', KEYS[1])
    redis.call('HDEL', KEYS[3], ARGV[1])

    return {status="SUCCESS", reason="CANCELLED"}
end`)
	}

	// 初始化Lua脚本，确保并发安全性
	reserveScript := redis.NewScript(string(reserveScriptContent))
	confirmScript := redis.NewScript(string(confirmScriptContent))

	return &RedPacketSlotManager{
		redisClient:       redisClient,
		reserveScript:     reserveScript,
		confirmScript:     confirmScript,
		reservationExpiry: reservationExpiry,
	}, nil
}

// InitializeRedPacket 初始化红包相关的Redis键
func (rm *RedPacketSlotManager) InitializeRedPacket(ctx context.Context, transactionID string, totalCount int) error {
	// 设置红包计数器（等于总数量）
	countKey := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, transactionID)
	if err := rm.redisClient.Set(ctx, countKey, totalCount, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("设置红包计数器失败: %v", err)
	}

	// 创建已领取用户集合（初始为空）
	// 修正: 使用与Lua脚本一致的格式
	receiversKey := fmt.Sprintf("%s%s:receivers", constant.TransactionKeyPrefix, transactionID)
	rm.redisClient.Del(ctx, receiversKey)
	rm.redisClient.Expire(ctx, receiversKey, 24*time.Hour)

	// 创建预留表（初始为空）
	reservationsKey := fmt.Sprintf("%s%s:reservations", constant.TransactionKeyPrefix, transactionID)
	rm.redisClient.Del(ctx, reservationsKey)
	rm.redisClient.Expire(ctx, reservationsKey, 24*time.Hour)

	// 创建结果集（初始为空）
	resultsKey := fmt.Sprintf("%s%s:results", constant.TransactionKeyPrefix, transactionID)
	rm.redisClient.Del(ctx, resultsKey)
	rm.redisClient.Expire(ctx, resultsKey, 24*time.Hour)

	// 创建监控键（初始化基本指标）
	monitorPrefix := fmt.Sprintf("dep_transaction_monitor:%s:", transactionID)
	pipe := rm.redisClient.Pipeline()

	// 初始化基本统计
	pipe.Set(ctx, monitorPrefix+"created_at", time.Now().Unix(), 72*time.Hour)
	pipe.Set(ctx, monitorPrefix+"total_count", totalCount, 72*time.Hour)
	pipe.Set(ctx, monitorPrefix+"attempts", 0, 72*time.Hour)
	pipe.Set(ctx, monitorPrefix+"successful_reservations", 0, 72*time.Hour)
	pipe.Set(ctx, monitorPrefix+"confirm_success", 0, 72*time.Hour)
	pipe.Set(ctx, monitorPrefix+"confirm_cancelled", 0, 72*time.Hour)
	pipe.Set(ctx, monitorPrefix+"corrections", 0, 72*time.Hour)

	// 设置监控键过期时间（72小时，比红包键更长以便于问题排查）
	pipe.Expire(ctx, monitorPrefix, 72*time.Hour)

	// 执行管道
	_, err := pipe.Exec(ctx)
	if err != nil {
		log.ZWarn(ctx, "初始化监控键失败", err,
			"transaction_id", transactionID,
			"monitor_prefix", monitorPrefix)
		// 不返回错误，监控失败不应阻止红包创建
	}

	return nil
}

// CheckUserReceived 检查用户是否已领取过红包
func (rm *RedPacketSlotManager) CheckUserReceived(ctx context.Context, transactionID string, userID string) (bool, error) {
	// 修正: 使用与Lua脚本一致的格式
	key := fmt.Sprintf("%s%s:receivers", constant.TransactionKeyPrefix, transactionID)
	return rm.redisClient.SIsMember(ctx, key, userID).Result()
}

// GetRemainingCount 获取红包剩余数量
func (rm *RedPacketSlotManager) GetRemainingCount(ctx context.Context, transactionID string) (int64, error) {
	key := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, transactionID)
	count, err := rm.redisClient.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return count, err
}

// ReservationErrorType 预留错误类型
type ReservationErrorType int

const (
	// ErrTypeConnection Redis连接错误，允许降级到传统模式
	ErrTypeConnection ReservationErrorType = iota
	// ErrTypeLuaExecution Lua脚本执行错误，不应降级到传统模式
	ErrTypeLuaExecution
	// ErrTypeOther 其他错误，通常不应降级
	ErrTypeOther
)

// ReservationError 预留错误
type ReservationError struct {
	Type    ReservationErrorType
	Message string
	Err     error
}

// Error 实现error接口
func (e *ReservationError) Error() string {
	return e.Message
}

// ShouldFallback 是否应该降级到传统锁模式
func (e *ReservationError) ShouldFallback() bool {
	return e.Type == ErrTypeConnection
}

// Unwrap 解包底层错误
func (e *ReservationError) Unwrap() error {
	return e.Err
}

// ReserveSlot 预留红包名额（使用默认过期时间）
func (rm *RedPacketSlotManager) ReserveSlot(ctx context.Context, transactionID string, userID string) (*ReservationResult, string, error) {
	return rm.reserveSlotWithExpiry(ctx, transactionID, userID, rm.reservationExpiry)
}

// ReserveSlotWithExpiry 预留红包名额（指定过期时间，用于拼手气红包动态过期时间）
// expirySec 预留过期时间(秒)；若 <= 0 则使用 DefaultReservationExpiry
func (rm *RedPacketSlotManager) ReserveSlotWithExpiry(ctx context.Context, transactionID string, userID string, expirySec int) (*ReservationResult, string, error) {
	if expirySec <= 0 {
		expirySec = DefaultReservationExpiry
	}
	return rm.reserveSlotWithExpiry(ctx, transactionID, userID, expirySec)
}

// reserveSlotWithExpiry 内部实现：按指定过期时间预留
func (rm *RedPacketSlotManager) reserveSlotWithExpiry(ctx context.Context, transactionID string, userID string, expirySec int) (*ReservationResult, string, error) {
	// 生成预留ID
	reservationID := uuid.New().String()

	log.ZInfo(ctx, "开始预留红包名额", "transaction_id", transactionID, "user_id", userID, "reservation_id", reservationID, "expiry_sec", expirySec)

	// 检查Redis连接是否正常 - 这里使用isRedisAvailable而不是简单的Ping
	if !rm.isRedisAvailable(ctx) {
		err := &ReservationError{
			Type:    ErrTypeConnection,
			Message: "Redis连接失败，无法预留名额",
			Err:     fmt.Errorf("Redis连接不可用"),
		}
		log.ZError(ctx, err.Message, err, "transaction_id", transactionID, "user_id", userID)
		return nil, "", err
	}

	// 高并发优化：仅当计数器 key 不存在时初始化，存在时绝不覆盖，避免抵消 Lua 的 DECR
	countKey := fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, transactionID)

	// 【关键修复】使用独立的 context 进行 Redis 操作，避免请求 context 取消导致失败
	redisCtx, redisCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer redisCancel()

	// 1. 尝试从 Redis 获取计数器值
	redisCount, countErr := rm.redisClient.Get(redisCtx, countKey).Int()
	if countErr != nil && countErr != redis.Nil {
		log.ZError(ctx, "获取Redis计数器失败", countErr, "transaction_id", transactionID, "key", countKey)
		redisCount = -1
	}
	counterKeyExists := (countErr == nil)

	// 2. 仅当计数器不存在时，从哈希表或默认值初始化，并写入 Redis
	if !counterKeyExists {
		transKey := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, transactionID)
		hashCount, hashErr := rm.redisClient.HGet(redisCtx, transKey, "remaining_count").Int()
		if hashErr != nil && hashErr != redis.Nil {
			hashCount = -1
		}
		finalCount := 1
		if redisCount >= 0 {
			finalCount = redisCount
		} else if hashCount >= 0 {
			finalCount = hashCount
		}
		if finalCount <= 0 {
			finalCount = 1
		}
		if setErr := rm.redisClient.Set(redisCtx, countKey, finalCount, 24*time.Hour).Err(); setErr != nil {
			log.ZWarn(ctx, "初始化Redis计数器失败", setErr, "transaction_id", transactionID)
		} else {
			log.ZInfo(ctx, "计数器不存在已初始化", "transaction_id", transactionID, "count", finalCount)
		}
	}

	// 准备脚本参数
	keys := []string{
		fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, transactionID),
		fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, transactionID),
		fmt.Sprintf("%s%s:receivers", constant.TransactionKeyPrefix, transactionID), // 修正: 使用与Lua脚本一致的格式
		fmt.Sprintf("%s%s:reservations", constant.TransactionKeyPrefix, transactionID),
		fmt.Sprintf("dep_transaction_monitor:%s:", transactionID), // 增加监控键前缀
	}

	args := []interface{}{
		userID,
		reservationID,
		time.Now().Unix(),
		expirySec,
	}

	// 打印详细的调用信息
	log.ZInfo(ctx, "准备执行Lua脚本", "transaction_id", transactionID, "user_id", userID,
		"keys", keys, "reservation_id", reservationID, "expiry", expirySec)

	// 使用带重试的脚本执行方法
	result, err := rm.executeScriptWithRetry(ctx, rm.reserveScript, keys, args...)
	if err != nil {
		// 错误已在executeScriptWithRetry中包装为ReservationError，保留原始类型
		return nil, "", err
	}

	log.ZInfo(ctx, "预留脚本执行完成", "transaction_id", transactionID, "user_id", userID, "result_type", fmt.Sprintf("%T", result))

	// 解析结果 - 处理各种可能的返回格式
	log.ZInfo(ctx, "开始解析预留脚本返回结果", "transaction_id", transactionID, "user_id", userID, "result_type", fmt.Sprintf("%T", result), "result", fmt.Sprintf("%+v", result))

	var status, reason string = "FAILED", "INVALID_RESPONSE"
	var remaining int64 = 0

	// 基于redis-go实现，Lua脚本的返回值可能是多种类型
	switch v := result.(type) {
	case map[interface{}]interface{}:
		// 处理正常的map返回
		log.ZInfo(ctx, "脚本返回了map类型结果", "transaction_id", transactionID)

		// 提取状态和原因
		if statusVal, ok := v["status"]; ok {
			if statusStr, ok := statusVal.(string); ok {
				status = statusStr
			}
		}

		if reasonVal, ok := v["reason"]; ok {
			if reasonStr, ok := reasonVal.(string); ok {
				reason = reasonStr
			}
		}

		// 提取剩余数量
		if remainingVal, ok := v["remaining"]; ok {
			if remainingInt, ok := remainingVal.(int64); ok {
				remaining = remainingInt
			} else if remainingStr, ok := remainingVal.(string); ok {
				remaining, _ = strconv.ParseInt(remainingStr, 10, 64)
			}
		}

	case []interface{}:
		// 处理数组返回 - 有些Redis版本的Lua实现会返回数组形式
		log.ZWarn(ctx, "脚本返回了数组类型结果，尝试解析", nil, "transaction_id", transactionID, "len", len(v))

		if len(v) == 0 {
			// 空数组，可能是脚本错误或Redis版本问题
			status = "FAILED"
			reason = "EMPTY_RESPONSE"
		} else if len(v) >= 3 {
			// 假设数组内容是 [status, reason, remaining]
			if statusStr, ok := v[0].(string); ok {
				status = statusStr
			}

			if reasonStr, ok := v[1].(string); ok {
				reason = reasonStr
			}

			// 尝试获取第三个元素作为remaining
			if remVal := v[2]; remVal != nil {
				if remainingInt, ok := remVal.(int64); ok {
					remaining = remainingInt
				} else if remainingStr, ok := remVal.(string); ok {
					remaining, _ = strconv.ParseInt(remainingStr, 10, 64)
				}
			}
		}

	case string:
		// 如果返回的是JSON字符串，尝试解析
		log.ZWarn(ctx, "脚本返回了字符串结果，尝试解析", nil, "transaction_id", transactionID, "result", v)

		if v == "ALREADY_RECEIVED" {
			status = "FAILED"
			reason = "ALREADY_RECEIVED"
		} else if v == "PACKET_EMPTY" {
			status = "FAILED"
			reason = "PACKET_EMPTY"
		} else if v == "SUCCESS" {
			status = "SUCCESS"
			reason = "RESERVED"
		} else {
			status = "FAILED"
			reason = "UNKNOWN: " + v
		}

	default:
		// 其他类型，设置为失败
		log.ZError(ctx, "无法处理的脚本返回类型", nil,
			"transaction_id", transactionID,
			"user_id", userID,
			"type", fmt.Sprintf("%T", result),
			"result", fmt.Sprintf("%+v", result))

		status = "FAILED"
		reason = "UNSUPPORTED_RESPONSE_TYPE"
	}

	// 日志记录解析结果
	log.ZInfo(ctx, "解析脚本结果完成",
		"transaction_id", transactionID,
		"user_id", userID,
		"status", status,
		"reason", reason,
		"remaining", remaining)

	reserveResult := &ReservationResult{
		Status:    status,
		Reason:    reason,
		Remaining: remaining,
	}

	// 记录日志
	if status == "SUCCESS" {
		log.ZInfo(ctx, "成功预留红包名额", "transaction_id", transactionID, "user_id", userID,
			"reservation_id", reservationID, "remaining", remaining)
	} else {
		log.ZWarn(ctx, "预留红包名额失败", nil, "transaction_id", transactionID, "user_id", userID,
			"status", status, "reason", reason)
	}

	return reserveResult, reservationID, nil
}

// ConfirmReservation 确认预留
func (rm *RedPacketSlotManager) ConfirmReservation(ctx context.Context, transactionID string, userID string, reservationID string, amount string, success bool) error {
	// 记录操作开始
	log.ZInfo(ctx, "开始确认/取消预留",
		"transaction_id", transactionID,
		"user_id", userID,
		"reservation_id", reservationID,
		"success", success,
		"amount", amount)

	// 检查Redis连接 - 使用增强的可用性检查
	if !rm.isRedisAvailable(ctx) {
		log.ZError(ctx, "Redis连接失败，无法确认/取消预留", nil,
			"transaction_id", transactionID,
			"user_id", userID)
		// 不返回错误，预留确认失败是可接受的
		return nil
	}

	// 准备脚本参数
	keys := []string{
		fmt.Sprintf("%s%s:counter", constant.TransactionKeyPrefix, transactionID),
		fmt.Sprintf("%s%s:receivers", constant.TransactionKeyPrefix, transactionID), // 修正: 使用与Lua脚本一致的格式
		fmt.Sprintf("%s%s:reservations", constant.TransactionKeyPrefix, transactionID),
		fmt.Sprintf("%s%s:results", constant.TransactionKeyPrefix, transactionID),
		fmt.Sprintf("dep_transaction_monitor:%s:", transactionID), // 增加监控键前缀
	}

	var status, value string
	if success {
		status = "SUCCESS"
		value = amount
	} else {
		status = "FAILED"
		value = "DATABASE_ERROR"
	}

	// 获取当前时间戳用于监控
	nowTs := time.Now().Unix()

	args := []interface{}{
		userID,
		reservationID,
		status,
		value,
		nowTs, // 增加当前时间戳参数
	}

	// 记录详细的调用信息
	log.ZInfo(ctx, "准备执行确认脚本",
		"transaction_id", transactionID,
		"user_id", userID,
		"keys", keys,
		"status", status,
		"value", value)

	// 使用带重试的脚本执行方法
	result, err := rm.executeScriptWithRetry(ctx, rm.confirmScript, keys, args...)
	if err != nil {
		// 由于确认预留失败是可接受的，所以即使出错也不中断流程
		log.ZWarn(ctx, "执行确认脚本失败，但不影响主流程", err,
			"transaction_id", transactionID,
			"user_id", userID,
			"reservation_id", reservationID)
		// 预留确认失败是可接受的，不会影响主流程
		return nil
	}

	log.ZInfo(ctx, "确认脚本执行完成",
		"transaction_id", transactionID,
		"user_id", userID,
		"result_type", fmt.Sprintf("%T", result))

	// 解析结果 - 处理各种可能的返回格式
	log.ZInfo(ctx, "开始解析确认脚本返回结果", "transaction_id", transactionID, "user_id", userID, "result_type", fmt.Sprintf("%T", result), "result", fmt.Sprintf("%+v", result))

	var resultStatus, resultReason string = "FAILED", "INVALID_RESPONSE"

	// 基于redis-go实现，Lua脚本的返回值可能是多种类型
	switch v := result.(type) {
	case map[interface{}]interface{}:
		// 处理正常的map返回
		log.ZInfo(ctx, "确认脚本返回了map类型结果", "transaction_id", transactionID)

		// 提取状态和原因
		if statusVal, ok := v["status"]; ok {
			if statusStr, ok := statusVal.(string); ok {
				resultStatus = statusStr
			}
		}

		if reasonVal, ok := v["reason"]; ok {
			if reasonStr, ok := reasonVal.(string); ok {
				resultReason = reasonStr
			}
		}

	case []interface{}:
		// 处理数组返回 - 有些Redis版本的Lua实现会返回数组形式
		log.ZWarn(ctx, "确认脚本返回了数组类型结果，尝试解析", nil, "transaction_id", transactionID, "len", len(v))

		if len(v) == 0 {
			// 空数组，可能是脚本错误或Redis版本问题
			resultStatus = "FAILED"
			resultReason = "EMPTY_RESPONSE"
		} else if len(v) >= 2 {
			// 假设数组内容是 [status, reason]
			if statusStr, ok := v[0].(string); ok {
				resultStatus = statusStr
			}

			if reasonStr, ok := v[1].(string); ok {
				resultReason = reasonStr
			}
		}

	case string:
		// 如果返回的是字符串，尝试解析
		log.ZWarn(ctx, "确认脚本返回了字符串结果，尝试解析", nil, "transaction_id", transactionID, "result", v)

		if v == "CONFIRMED" || v == "SUCCESS" {
			resultStatus = "SUCCESS"
			resultReason = "CONFIRMED"
		} else if v == "CANCELLED" {
			resultStatus = "SUCCESS"
			resultReason = "CANCELLED"
		} else {
			resultStatus = "FAILED"
			resultReason = "UNKNOWN: " + v
		}

	default:
		// 其他类型，设置为失败但不中断流程
		log.ZError(ctx, "无法处理的确认脚本返回类型", nil,
			"transaction_id", transactionID,
			"user_id", userID,
			"type", fmt.Sprintf("%T", result),
			"result", fmt.Sprintf("%+v", result))

		resultStatus = "FAILED"
		resultReason = "UNSUPPORTED_RESPONSE_TYPE"
	}

	// 日志记录解析结果
	log.ZInfo(ctx, "解析确认脚本结果完成",
		"transaction_id", transactionID,
		"user_id", userID,
		"status", resultStatus,
		"reason", resultReason)

	// 记录日志
	if resultStatus == "SUCCESS" {
		if success {
			log.ZInfo(ctx, "成功确认预留",
				"transaction_id", transactionID,
				"user_id", userID,
				"reservation_id", reservationID,
				"amount", amount)
		} else {
			log.ZInfo(ctx, "成功取消预留",
				"transaction_id", transactionID,
				"user_id", userID,
				"reservation_id", reservationID)
		}
	} else {
		log.ZWarn(ctx, "确认预留失败，但不影响主流程", nil,
			"transaction_id", transactionID,
			"user_id", userID,
			"reservation_id", reservationID,
			"reason", resultReason)
		// 预留确认失败是可接受的
	}

	return nil
}

// CancelReservation 取消预留
func (rm *RedPacketSlotManager) CancelReservation(ctx context.Context, transactionID string, userID string, reservationID string) error {
	return rm.ConfirmReservation(ctx, transactionID, userID, reservationID, "", false)
}

// SyncReceiversFromMongoDB 从MongoDB同步已领取用户到Redis接收者集合
// 重要：这个方法用于修复Redis接收者集合与MongoDB不一致的问题
func (rm *RedPacketSlotManager) SyncReceiversFromMongoDB(ctx context.Context, mongoDB *mongo.Database, transactionID string) error {
	// 0. 首先检查Redis接收者集合是否已存在，避免不必要的同步
	receiversKey := fmt.Sprintf("%s%s:receivers", constant.TransactionKeyPrefix, transactionID)

	// 先获取Redis中的接收者数量
	redisCount, err := rm.redisClient.SCard(ctx, receiversKey).Result()
	if err != nil && err != redis.Nil {
		log.ZWarn(ctx, "获取Redis接收者集合大小失败", err, "transaction_id", transactionID)
		// 出错时继续同步，不返回错误
	}

	// 获取交易总数及 remaining_count，用于判断是否存在「接收者数量=总数但仍有剩余个数」的矛盾场景
	transactionKey := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, transactionID)
	totalCountStr, err := rm.redisClient.HGet(ctx, transactionKey, "total_count").Result()
	if err != nil && err != redis.Nil {
		log.ZWarn(ctx, "获取交易总数失败", err, "transaction_id", transactionID)
		// 出错时继续同步，不返回错误
	}
	totalCount, _ := strconv.ParseInt(totalCountStr, 10, 64)

	remainingStr, remErr := rm.redisClient.HGet(ctx, transactionKey, "remaining_count").Result()
	var remaining int64
	if remErr == nil {
		remaining, _ = strconv.ParseInt(remainingStr, 10, 64)
	}

	// 如果接收者数量大于0，且接收者数量正好等于交易的总数量
	if redisCount > 0 && totalCount > 0 && redisCount == totalCount {
		if remaining > 0 {
			// 出现「接收者数量=总数但仍有剩余个数」的矛盾，强制执行同步，以 MongoDB 为准修复 Redis
			log.ZWarn(ctx, "Redis接收者数量已达总数但 remaining_count>0，强制从MongoDB同步接收者集合",
				nil,
				"transaction_id", transactionID,
				"redis_count", redisCount,
				"total_count", totalCount,
				"remaining_count", remaining)
		} else {
			// 正常场景：红包确实已经领完，无需同步
			log.ZInfo(ctx, "Redis接收者数量已达到总数且无剩余个数，无需同步",
				"transaction_id", transactionID,
				"redis_count", redisCount,
				"total_count", totalCount,
				"remaining_count", remaining)
			return nil
		}
	}

	// 1. 查询MongoDB中已领取的记录
	startTime := time.Now()
	collection := mongoDB.Collection("transaction_receive_record")

	// 使用MongoDB聚合获取所有已领取的用户ID和计数
	pipeline := []bson.M{
		{"$match": bson.M{"transaction_id": transactionID}},
		{"$group": bson.M{
			"_id":      nil,
			"user_ids": bson.M{"$push": "$user_id"},
			"count":    bson.M{"$sum": 1},
		}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		log.ZError(ctx, "查询已领取记录失败", err, "transaction_id", transactionID)
		return fmt.Errorf("查询已领取记录失败: %v", err)
	}
	defer cursor.Close(ctx)

	// 2. 解析聚合结果
	var result struct {
		UserIDs []string `bson:"user_ids"`
		Count   int      `bson:"count"`
	}

	if !cursor.Next(ctx) {
		log.ZInfo(ctx, "MongoDB中未找到领取记录", "transaction_id", transactionID)

		// 如果MongoDB中没有记录，但Redis中有记录，可能是测试数据或错误数据
		// 此时不清空Redis集合，避免误删
		if redisCount > 0 {
			log.ZWarn(ctx, "MongoDB中无记录但Redis中有接收者记录，保留Redis数据", nil,
				"transaction_id", transactionID,
				"redis_count", redisCount)
		}
		return nil
	}

	if err := cursor.Decode(&result); err != nil {
		log.ZError(ctx, "解析领取记录失败", err, "transaction_id", transactionID)
		return fmt.Errorf("解析领取记录失败: %v", err)
	}

	// 3. 比较MongoDB和Redis的数量，确定是否需要同步
	mongoCount := int64(result.Count)

	// 3.1 如果MongoDB和Redis数量一致，检查是否需要详细比对
	if mongoCount == redisCount {
		// 当数量相等时，只在数量较小时进行详细比对以提高性能
		if mongoCount <= 20 { // 小于20个用户时才进行详细比对
			// 获取Redis中所有用户
			redisUsers, err := rm.redisClient.SMembers(ctx, receiversKey).Result()
			if err != nil {
				log.ZWarn(ctx, "获取Redis用户列表失败，将进行全量同步", err, "transaction_id", transactionID)
			} else {
				// 构建Redis用户集合用于快速查找
				redisUserSet := make(map[string]struct{}, len(redisUsers))
				for _, user := range redisUsers {
					redisUserSet[user] = struct{}{}
				}

				// 检查MongoDB中的用户是否都在Redis中
				needSync := false
				for _, mongoUser := range result.UserIDs {
					if _, exists := redisUserSet[mongoUser]; !exists {
						needSync = true
						log.ZInfo(ctx, "发现不一致的用户ID",
							"transaction_id", transactionID,
							"user_id", mongoUser)
						break
					}
				}

				if !needSync {
					log.ZInfo(ctx, "MongoDB和Redis接收者数量一致且内容相同，无需同步",
						"transaction_id", transactionID,
						"count", mongoCount)
					return nil
				}
			}
		} else {
			// 数量较多时，假设数量相等就意味着内容相同，避免不必要的网络IO
			log.ZInfo(ctx, "MongoDB和Redis接收者数量一致，假定内容相同，无需同步",
				"transaction_id", transactionID,
				"count", mongoCount)
			return nil
		}
	}

	// 4. 需要同步：更新Redis接收者集合
	if len(result.UserIDs) > 0 {
		// 使用新的键格式
		log.ZInfo(ctx, "开始同步MongoDB接收者数据到Redis",
			"transaction_id", transactionID,
			"mongo_count", mongoCount,
			"redis_count", redisCount)

		// 批量操作，根据数据量选择不同的策略
		if len(result.UserIDs) > 500 {
			// 大量数据时使用替换策略
			// 先清空现有集合
			rm.redisClient.Del(ctx, receiversKey)

			// 分批处理以避免单次命令过大
			batchSize := 100
			for i := 0; i < len(result.UserIDs); i += batchSize {
				end := i + batchSize
				if end > len(result.UserIDs) {
					end = len(result.UserIDs)
				}

				batch := result.UserIDs[i:end]
				args := make([]interface{}, len(batch))
				for j, userID := range batch {
					args[j] = userID
				}

				// 执行添加操作
				if err := rm.redisClient.SAdd(ctx, receiversKey, args...).Err(); err != nil {
					log.ZError(ctx, "批量添加接收者失败", err,
						"transaction_id", transactionID,
						"batch", i/batchSize)
					// 继续处理其他批次，不中断
				}
			}

			// 设置过期时间
			rm.redisClient.Expire(ctx, receiversKey, 24*time.Hour)
		} else {
			// 数量较少时使用管道
			pipe := rm.redisClient.Pipeline()

			// 先清空现有集合
			pipe.Del(ctx, receiversKey)

			// 批量添加所有用户ID
			for _, userID := range result.UserIDs {
				pipe.SAdd(ctx, receiversKey, userID)
			}
			pipe.Expire(ctx, receiversKey, 24*time.Hour)

			// 执行管道
			if _, err := pipe.Exec(ctx); err != nil {
				log.ZError(ctx, "更新Redis接收者集合失败", err, "transaction_id", transactionID)
				return fmt.Errorf("更新Redis接收者集合失败: %v", err)
			}
		}

		// 5. 记录同步完成信息和耗时
		elapsed := time.Since(startTime)
		log.ZInfo(ctx, "已同步接收者集合",
			"transaction_id", transactionID,
			"count", len(result.UserIDs),
			"first_few_users", result.UserIDs[:min(5, len(result.UserIDs))],
			"elapsed_ms", elapsed.Milliseconds())

		// 6. 更新监控数据
		monitorKey := fmt.Sprintf("dep_transaction_monitor:%s:sync_count", transactionID)
		rm.redisClient.Incr(ctx, monitorKey)

		monitorLatestKey := fmt.Sprintf("dep_transaction_monitor:%s:last_sync", transactionID)
		rm.redisClient.Set(ctx, monitorLatestKey, time.Now().Unix(), 72*time.Hour)

		monitorLatestKey = fmt.Sprintf("dep_transaction_monitor:%s:last_sync_duration_ms", transactionID)
		rm.redisClient.Set(ctx, monitorLatestKey, elapsed.Milliseconds(), 72*time.Hour)
	}

	return nil
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ShouldSyncReceivers 智能判断是否需要同步接收者数据
// 根据多种条件判断是否需要进行MongoDB到Redis的同步操作
func (rm *RedPacketSlotManager) ShouldSyncReceivers(ctx context.Context, transactionID string) (bool, string, error) {
	monitorPrefix := fmt.Sprintf("dep_transaction_monitor:%s:", transactionID)
	receiversKey := fmt.Sprintf("%s%s:receivers", constant.TransactionKeyPrefix, transactionID)

	// 1. 检查Redis接收者集合是否存在
	redisCount, err := rm.redisClient.SCard(ctx, receiversKey).Result()
	if err == redis.Nil || redisCount == 0 {
		return true, "Redis接收者集合不存在或为空", nil
	}
	if err != nil {
		log.ZWarn(ctx, "获取Redis接收者数量失败", err, "transaction_id", transactionID)
		// Redis错误时建议同步，保守策略
		return true, fmt.Sprintf("Redis错误: %v", err), nil
	}

	// 2. 检查交易状态
	transKey := fmt.Sprintf("%s%s", constant.TransactionKeyPrefix, transactionID)
	status, err := rm.redisClient.HGet(ctx, transKey, "status").Int()
	if err != nil && err != redis.Nil {
		log.ZWarn(ctx, "获取交易状态失败", err, "transaction_id", transactionID)
	} else if status != 0 {
		// 非激活状态的交易，低频率同步
		// 只有上次同步超过10分钟才再次同步
		lastSyncTime, err := rm.redisClient.Get(ctx, monitorPrefix+"last_sync").Int64()
		if err == nil && lastSyncTime > 0 {
			if time.Now().Unix()-lastSyncTime < 600 { // 10分钟 = 600秒
				return false, "非激活交易且最近已同步", nil
			}
		}
	}

	// 3. 检查总数量和已领取数量
	totalCount, err := rm.redisClient.HGet(ctx, transKey, "total_count").Int64()
	if err == nil && totalCount > 0 && redisCount == totalCount {
		// 进一步读取 remaining_count，用于判断是否存在「接收者数量=总数但仍有剩余个数」的矛盾场景
		remainingStr, remErr := rm.redisClient.HGet(ctx, transKey, "remaining_count").Result()
		if remErr == nil {
			if remaining, parseErr := strconv.ParseInt(remainingStr, 10, 64); parseErr == nil && remaining > 0 {
				// 这里表示：Redis 认为接收者数量已经达到总数，但交易仍有剩余个数
				// 这是 Redis 接收者集合与 MongoDB/交易状态不一致的强信号，需要强制同步修复
				return true, "接收者数量等于总数但 remaining_count>0，强制同步接收者集合", nil
			}
		}

		// 正常场景：接收者数量已达到总数且 remaining_count<=0，认为红包已抢完，无需同步
		return false, "接收者数量已达到总数", nil
	}

	// 4. 获取上一次同步时间，避免频繁同步
	lastSyncTime, err := rm.redisClient.Get(ctx, monitorPrefix+"last_sync").Int64()
	if err == nil && lastSyncTime > 0 {
		nowTs := time.Now().Unix()
		timeSinceLastSync := nowTs - lastSyncTime

		// 根据交易状态和数量设置不同的同步频率
		var minSyncInterval int64

		if status != 0 {
			// 非激活状态的交易，低频率同步
			minSyncInterval = 600 // 10分钟
		} else if redisCount < 10 {
			// 数量很少，可能正在活跃领取中，高频率同步
			minSyncInterval = 5 // 5秒
		} else if totalCount > 0 && float64(redisCount)/float64(totalCount) > 0.9 {
			// 接近领完，中频率同步
			minSyncInterval = 30 // 30秒
		} else {
			// 一般情况，中频率同步
			minSyncInterval = 60 // 1分钟
		}

		if timeSinceLastSync < minSyncInterval {
			return false, fmt.Sprintf("距上次同步时间不足%d秒", minSyncInterval), nil
		}
	}

	// 5. 检查最近的预留尝试次数，如果活跃度高则提高同步频率
	recentAttempts, err := rm.redisClient.Get(ctx, monitorPrefix+"attempts").Int64()
	lastCheckedAttempts, err2 := rm.redisClient.Get(ctx, monitorPrefix+"last_checked_attempts").Int64()
	if err == nil && err2 == nil && recentAttempts > 0 {
		// 如果有新的预留尝试，则增加同步概率
		if recentAttempts > lastCheckedAttempts {
			// 记录当前检查的尝试次数
			rm.redisClient.Set(ctx, monitorPrefix+"last_checked_attempts", recentAttempts, 24*time.Hour)

			// 尝试次数增加较多时，建议同步
			if recentAttempts-lastCheckedAttempts > 5 {
				return true, fmt.Sprintf("有新的预留尝试: %d -> %d", lastCheckedAttempts, recentAttempts), nil
			}
		}
	}

	// 6. 默认行为：建议同步
	// 为了确保数据一致性，如果没有明确的不同步理由，我们选择同步
	return true, "默认同步策略", nil
}

// executeScriptWithRetry 执行Lua脚本并支持重试
// 针对特定的临时性错误进行有限次数的重试
func (rm *RedPacketSlotManager) executeScriptWithRetry(ctx context.Context, script *redis.Script, keys []string, args ...interface{}) (interface{}, error) {
	maxRetries := 2
	var lastErr error
	var result interface{}

	for i := 0; i <= maxRetries; i++ {
		// 如果不是第一次尝试，记录重试信息
		if i > 0 {
			log.ZInfo(ctx, "重试执行Lua脚本",
				"attempt", i+1,
				"max_retries", maxRetries+1,
				"last_error", lastErr)
		}

		// 【关键修复】使用 context.Background() 作为父 context
		// 避免请求 context 被取消时导致 Redis 操作失败
		execCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		result, lastErr = script.Run(execCtx, rm.redisClient, keys, args...).Result()
		cancel()

		// 如果成功，直接返回结果
		if lastErr == nil {
			if i > 0 {
				// 记录重试成功
				log.ZInfo(ctx, "Lua脚本重试执行成功",
					"attempts", i+1,
					"keys", fmt.Sprintf("%v", keys))
			}
			return result, nil
		}

		// 分析错误类型，决定是否重试
		errorStr := lastErr.Error()

		// 判断是否为临时性错误
		isTemporary := false
		if strings.Contains(errorStr, "BUSY") || // Redis服务器忙
			strings.Contains(errorStr, "timeout") || // 超时
			strings.Contains(errorStr, "connection") || // 连接问题
			strings.Contains(errorStr, "try again") || // 临时失败
			strings.Contains(errorStr, "OOM") { // 内存不足
			isTemporary = true
		}

		if !isTemporary {
			// 对于非临时性错误，不再重试
			log.ZWarn(ctx, "Lua脚本执行失败（非临时性错误）", lastErr,
				"keys", fmt.Sprintf("%v", keys))
			return nil, &ReservationError{
				Type:    ErrTypeLuaExecution,
				Message: "Lua脚本执行失败（非临时性错误）: " + lastErr.Error(),
				Err:     lastErr,
			}
		}

		// 临时错误，等待后重试
		backoffTime := 10 * time.Millisecond * time.Duration(1<<uint(i)) // 指数退避
		time.Sleep(backoffTime)
	}

	// 所有重试都失败
	log.ZError(ctx, "Lua脚本执行失败，超过最大重试次数", lastErr,
		"max_retries", maxRetries,
		"keys", fmt.Sprintf("%v", keys))

	return nil, &ReservationError{
		Type:    ErrTypeLuaExecution,
		Message: fmt.Sprintf("Lua脚本执行失败，已重试%d次: %v", maxRetries, lastErr),
		Err:     lastErr,
	}
}

func (rm *RedPacketSlotManager) isRedisAvailable(ctx context.Context) bool {
	// 【优化】简化健康检查，只使用Ping命令
	// 原来的实现在高并发下每次都创建UUID并写入/读取/删除，会导致大量误报
	// Ping 已足够检测Redis连接是否可用

	// 【关键修复】使用 context.Background() 而不是请求的 ctx
	// 因为请求 ctx 可能在高并发时被取消（客户端断开/超时），导致误报 Redis 不可用
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, pingErr := rm.redisClient.Ping(timeoutCtx).Result()
	if pingErr != nil {
		log.ZWarn(ctx, "Redis Ping失败", pingErr)
		return false
	}

	return true
}

// GetTransactionMonitorMetrics 获取交易的监控指标
func (rm *RedPacketSlotManager) GetTransactionMonitorMetrics(ctx context.Context, transactionID string) (map[string]string, error) {
	monitorPrefix := fmt.Sprintf("dep_transaction_monitor:%s:", transactionID)

	// 定义要获取的指标列表
	metrics := []string{
		"created_at",
		"total_count",
		"attempts",
		"successful_reservations",
		"confirm_success",
		"confirm_cancelled",
		"corrections",
		"failed:packet_full",
		"failed:packet_empty",
		"failed:already_received",
		"failed:reservation_exists",
		"failed:invalid_status",
		"expired_reservations",
	}

	// 使用管道批量获取所有指标
	pipe := rm.redisClient.Pipeline()
	cmds := make(map[string]*redis.StringCmd)

	for _, metric := range metrics {
		cmds[metric] = pipe.Get(ctx, monitorPrefix+metric)
	}

	// 获取每小时的预留统计
	hourlyStatsCmd := pipe.HGetAll(ctx, monitorPrefix+"hourly_stats")

	// 获取修正详情
	correctionDetailsCmd := pipe.HGetAll(ctx, monitorPrefix+"correction_details")

	// 获取取消原因
	cancelReasonsCmd := pipe.HGetAll(ctx, monitorPrefix+"cancel_reasons")

	// 执行管道
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		log.ZError(ctx, "获取监控指标失败", err, "transaction_id", transactionID)
		return nil, fmt.Errorf("获取监控指标失败: %v", err)
	}

	// 构建结果映射
	result := make(map[string]string)

	// 处理基础指标
	for metric, cmd := range cmds {
		val, err := cmd.Result()
		if err == nil {
			result[metric] = val
		} else if err != redis.Nil {
			result[metric] = "error: " + err.Error()
		} else {
			result[metric] = "0" // 没有数据的指标默认为0
		}
	}

	// 处理每小时统计
	hourlyStats, err := hourlyStatsCmd.Result()
	if err == nil && len(hourlyStats) > 0 {
		result["hourly_stats"] = fmt.Sprintf("%v", hourlyStats)
	} else {
		result["hourly_stats"] = "{}"
	}

	// 处理修正详情
	correctionDetails, err := correctionDetailsCmd.Result()
	if err == nil && len(correctionDetails) > 0 {
		result["correction_details"] = fmt.Sprintf("%v", correctionDetails)
	} else {
		result["correction_details"] = "{}"
	}

	// 处理取消原因
	cancelReasons, err := cancelReasonsCmd.Result()
	if err == nil && len(cancelReasons) > 0 {
		result["cancel_reasons"] = fmt.Sprintf("%v", cancelReasons)
	} else {
		result["cancel_reasons"] = "{}"
	}

	// 添加状态摘要
	attempts, _ := strconv.ParseInt(result["attempts"], 10, 64)
	successes, _ := strconv.ParseInt(result["successful_reservations"], 10, 64)
	// 以下变量虽然声明但未直接使用，但在fmt.Sprintf中引用了对应的结果字典值
	// confirms, _ := strconv.ParseInt(result["confirm_success"], 10, 64)
	// cancels, _ := strconv.ParseInt(result["confirm_cancelled"], 10, 64)
	// corrections, _ := strconv.ParseInt(result["corrections"], 10, 64)

	if attempts > 0 {
		successRate := float64(successes) / float64(attempts) * 100
		result["success_rate"] = fmt.Sprintf("%.2f%%", successRate)
	} else {
		result["success_rate"] = "0.00%"
	}

	result["summary"] = fmt.Sprintf(
		"尝试: %s, 成功: %s, 确认: %s, 取消: %s, 纠正: %s, 成功率: %s",
		result["attempts"],
		result["successful_reservations"],
		result["confirm_success"],
		result["confirm_cancelled"],
		result["corrections"],
		result["success_rate"],
	)

	return result, nil
}
