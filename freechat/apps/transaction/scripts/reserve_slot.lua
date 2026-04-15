-- 预留红包名额脚本（增强版）
-- KEYS[1]: 红包元数据键 dep_transaction:{id}
-- KEYS[2]: 红包计数器 dep_transaction:{id}:counter
-- KEYS[3]: 已领用户集 dep_transaction:{id}:receivers
-- KEYS[4]: 预留状态表 dep_transaction:{id}:reservations
-- KEYS[5]: 监控键前缀 dep_transaction_monitor:{id}:
-- ARGV[1]: 用户ID
-- ARGV[2]: 预留ID (UUID)
-- ARGV[3]: 当前时间戳
-- ARGV[4]: 预留超时时间(秒)

-- 0. 记录尝试次数（用于监控）
local monitor_prefix = KEYS[5] or "dep_transaction_monitor:"
redis.call('INCR', monitor_prefix .. "attempts")

-- 1. 检查交易状态
local status_str = redis.call('HGET', KEYS[1], "status")
local status = tonumber(status_str or "-1")

-- 【关键修复】status 为 -1 表示 Redis 中尚未初始化交易元数据
-- 这在高并发场景下很常见（请求先于初始化到达），应返回特殊状态让 Go 代码去初始化
if status == -1 then
    redis.call('INCR', monitor_prefix .. "failed:not_initialized")
    return {"FAILED", "NOT_INITIALIZED", 0}
end

-- status 为 -2 表示另一个请求正在初始化，返回特殊状态让调用方等待重试
if status == -2 then
    redis.call('INCR', monitor_prefix .. "failed:initializing")
    return {"FAILED", "INITIALIZING", 0}
end

if status == 1 then
    -- 状态为1表示交易已完成（所有红包已领取完）
    redis.call('INCR', monitor_prefix .. "failed:packet_complete")
    return {"FAILED", "PACKET_EMPTY", 0}
elseif status ~= 0 then
    -- 其他非法状态（2等，但不包括-1, -2）
    redis.call('INCR', monitor_prefix .. "failed:invalid_status")
    return {"FAILED", "TRANSACTION_INVALID", 0}
end

-- 2. 检查用户是否已领取过
if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then
    redis.call('INCR', monitor_prefix .. "failed:already_received")
    return {"FAILED", "ALREADY_RECEIVED", 0}
end

-- 3. 检查用户是否已有进行中的预留
local reservation = redis.call('HGET', KEYS[4], ARGV[1])
if reservation then
    -- 检查预留是否已过期（超过60秒未处理）
    local reservation_time = redis.call('HGET', KEYS[4] .. ":time", ARGV[1])
    if reservation_time and tonumber(ARGV[3]) - tonumber(reservation_time) > 60 then
        -- 预留已过期，清理并继续
        redis.call('HDEL', KEYS[4], ARGV[1])
        redis.call('HDEL', KEYS[4] .. ":time", ARGV[1])
        redis.call('INCR', monitor_prefix .. "expired_reservations")
        -- 【关键修复】恢复计数器：预留时减少了1，过期后应该加回来
        redis.call('INCR', KEYS[2])
        redis.call('INCR', monitor_prefix .. "counter_restored_from_expired_self")
    else
        -- 预留仍然有效
        redis.call('INCR', monitor_prefix .. "failed:reservation_exists")
        return {"FAILED", "RESERVATION_EXISTS", 0}
    end
end

-- 4. 获取红包基本信息
local total_count = tonumber(redis.call('HGET', KEYS[1], "total_count") or "0")
local meta_remaining = tonumber(redis.call('HGET', KEYS[1], "remaining_count") or "0")
local receivers_count = redis.call('SCARD', KEYS[3]) or 0

-- 4.5 【关键优化】在检查 PACKET_FULL 之前，先清理所有过期的预留
-- 这避免了过期预留导致误判红包已领完
local current_time = tonumber(ARGV[3])
local expiry_seconds = tonumber(ARGV[4]) or 60
local reservations_time_key = KEYS[4] .. ":time"

-- 获取所有预留及其时间
local all_reservations = redis.call('HGETALL', KEYS[4])
local expired_users = {}
local valid_pending_count = 0

for i = 1, #all_reservations, 2 do
    local user_id = all_reservations[i]
    local reservation_time_str = redis.call('HGET', reservations_time_key, user_id)
    local reservation_time = tonumber(reservation_time_str or "0")

    -- 预留过期条件：
    -- 1. 时间存在且已超过过期时间
    -- 2. 时间不存在或为0（异常预留，也需要清理）
    if reservation_time == 0 or (current_time - reservation_time) > expiry_seconds then
        -- 预留已过期或异常
        table.insert(expired_users, user_id)
    else
        -- 预留仍有效
        valid_pending_count = valid_pending_count + 1
    end
end

-- 清理过期预留并恢复计数器
if #expired_users > 0 then
    for _, user_id in ipairs(expired_users) do
        redis.call('HDEL', KEYS[4], user_id)
        redis.call('HDEL', reservations_time_key, user_id)
    end
    -- 恢复计数器（每个过期预留对应一个名额）
    redis.call('INCRBY', KEYS[2], #expired_users)
    redis.call('INCRBY', monitor_prefix .. "expired_reservations_batch", #expired_users)
    redis.call('INCRBY', monitor_prefix .. "counter_restored_from_expired", #expired_users)
end

-- 使用清理后的有效预留数
local pending_count = valid_pending_count

-- 5. 安全检查：已领取+预留人数检查
-- 注意：receivers_count 可能因为补偿失败等原因与 MongoDB 实际记录不一致
-- 当返回 PACKET_FULL_BY_RECEIVERS_AND_PENDING 时，Go 代码应该验证 MongoDB 实际记录
if (receivers_count + pending_count) >= total_count then
    redis.call('INCR', monitor_prefix .. "failed:packet_full")
    -- 返回额外信息以便 Go 代码进行验证
    return {"FAILED", "PACKET_FULL_BY_RECEIVERS_AND_PENDING", receivers_count, pending_count, total_count}
end

-- 6. 严格检查红包是否已领完（计数器方式）
-- 【高并发优化】counter<=0 时不以 Hash 的 meta_remaining 为唯一依据，避免 Hash 被错误写成 0 时误判「已领完」
local remaining = tonumber(redis.call('GET', KEYS[2]) or "0")
if remaining <= 0 then
    local theoretical_remaining = total_count - receivers_count
    local available = math.max(0, theoretical_remaining - pending_count)

    -- 安全校正值：若 Hash 的 meta_remaining>0 则取 min(meta, available)，否则以理论剩余 available 为准，避免 Hash=0 误杀
    local safe_correction
    if meta_remaining and meta_remaining > 0 then
        safe_correction = math.min(meta_remaining, available)
    else
        safe_correction = available
    end

    if safe_correction <= 0 then
        redis.call('INCR', monitor_prefix .. "failed:packet_empty")
        return {"FAILED", "PACKET_EMPTY", 0}
    end

    -- 计数器纠正
    redis.call('SET', KEYS[2], safe_correction)
    redis.call('INCR', monitor_prefix .. "corrections")
    redis.call('HSET', monitor_prefix .. "correction_details", ARGV[3],
        "meta:" .. meta_remaining ..
        ",theo:" .. theoretical_remaining ..
        ",safe:" .. safe_correction)

    remaining = safe_correction
end

-- 7. 再次进行最终检查，确保真的有剩余
if remaining <= 0 then
    redis.call('INCR', monitor_prefix .. "failed:packet_empty_final")
    return {"FAILED", "PACKET_EMPTY_FINAL", 0}
end

-- 8. 原子减少计数器并预留名额
redis.call('DECR', KEYS[2])
redis.call('HSET', KEYS[4], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[4] .. ":time", ARGV[1], ARGV[3])
redis.call('EXPIRE', KEYS[4], tonumber(ARGV[4]))
redis.call('EXPIRE', KEYS[4] .. ":time", tonumber(ARGV[4]))

-- 9. 更新预留统计
redis.call('INCR', monitor_prefix .. "successful_reservations")
-- 使用时间戳代替os.date (不使用os模块，避免Redis报错)
local timestamp = tonumber(ARGV[3])
local hour_key = math.floor(timestamp / 3600)
redis.call('HINCRBY', monitor_prefix .. "hourly_stats", hour_key, 1)

-- 10. 返回预留成功，使用数组格式 [status, reason, remaining]
return {"SUCCESS", "RESERVED", remaining - 1}