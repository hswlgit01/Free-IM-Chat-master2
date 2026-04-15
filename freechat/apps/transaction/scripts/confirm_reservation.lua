-- 确认预留脚本（增强版）
-- KEYS[1]: 红包计数器 dep_transaction:{id}:counter
-- KEYS[2]: 已领用户集 dep_transaction:{id}:receivers
-- KEYS[3]: 预留状态表 dep_transaction:{id}:reservations
-- KEYS[4]: 领取结果集 dep_transaction:{id}:results
-- KEYS[5]: 监控键前缀 dep_transaction_monitor:{id}:
-- ARGV[1]: 用户ID
-- ARGV[2]: 预留ID
-- ARGV[3]: 确认结果 (SUCCESS|FAILED)
-- ARGV[4]: 领取金额 (成功时)或失败原因(失败时)
-- ARGV[5]: 当前时间戳

-- 0. 记录尝试次数（用于监控）
local monitor_prefix = KEYS[5] or "dep_transaction_monitor:"
redis.call('INCR', monitor_prefix .. "confirm_attempts")

-- 1. 检查预留是否存在且匹配
local reservation = redis.call('HGET', KEYS[3], ARGV[1])
if not reservation then
    redis.call('INCR', monitor_prefix .. "confirm_failed:reservation_not_found")
    return {"FAILED", "RESERVATION_NOT_FOUND"}
end

if reservation ~= ARGV[2] then
    redis.call('INCR', monitor_prefix .. "confirm_failed:reservation_id_mismatch")
    return {"FAILED", "RESERVATION_ID_MISMATCH"}
end

-- 1.5 清理预留时间记录（如果存在）
redis.call('HDEL', KEYS[3] .. ":time", ARGV[1])

-- 2. 根据确认结果处理
if ARGV[3] == "SUCCESS" then
    -- 检查用户是否已在接收者集合中（异常情况检测）
    if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
        redis.call('INCR', monitor_prefix .. "confirm_anomaly:already_in_receivers")
        -- 仍然删除预留，避免悬挂的预留记录
        redis.call('HDEL', KEYS[3], ARGV[1])
        return {"FAILED", "ALREADY_IN_RECEIVERS"}
    end

    -- 成功领取：添加到已领用户集合，删除预留，记录结果
    redis.call('SADD', KEYS[2], ARGV[1])
    redis.call('HDEL', KEYS[3], ARGV[1])
    redis.call('HSET', KEYS[4], ARGV[1], ARGV[4])

    -- 更新确认统计
    redis.call('INCR', monitor_prefix .. "confirm_success")
    -- 使用时间戳代替os.date (不使用os模块，避免Redis报错)
    local timestamp = tonumber(ARGV[5] or redis.call('TIME')[1])
    local hour_key = math.floor(timestamp / 3600)
    redis.call('HINCRBY', monitor_prefix .. "confirm_hourly_stats", hour_key, 1)

    -- 记录完成时间与耗时（如果有预留时间记录）
    local reservation_time = redis.call('HGET', KEYS[3] .. ":time", ARGV[1])
    if reservation_time then
        -- 使用TIME命令代替os.time
        local current_time = tonumber(ARGV[5] or redis.call('TIME')[1])
        local elapsed = current_time - tonumber(reservation_time)
        redis.call('LPUSH', monitor_prefix .. "processing_times", elapsed)
        redis.call('LTRIM', monitor_prefix .. "processing_times", 0, 999)  -- 保留最近1000条
    end

    return {"SUCCESS", "CONFIRMED"}
else
    -- 失败：恢复计数器，删除预留，记录原因
    redis.call('INCR', KEYS[1])
    redis.call('HDEL', KEYS[3], ARGV[1])

    -- 更新取消统计
    redis.call('INCR', monitor_prefix .. "confirm_cancelled")
    redis.call('HINCRBY', monitor_prefix .. "cancel_reasons", ARGV[4], 1)

    return {"SUCCESS", "CANCELLED"}
end