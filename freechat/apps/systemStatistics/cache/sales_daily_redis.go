package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/openimsdk/chat/freechat/apps/systemStatistics/model"
	"github.com/redis/go-redis/v9"
)

const (
	// SalesDailyRedisHashKey 全组织共用一个 Hash，不按 org 拼 Redis key 后缀；field 为 HashField(org, day)
	SalesDailyRedisHashKey = "sales_daily_stats"
	// 缓存字段保留约 31 天，定时任务会修剪
	salesDailyRetentionDays = 31
)

// SalesDailyRetentionDays 对外暴露保留天数（与定时修剪一致）
func SalesDailyRetentionDays() int { return salesDailyRetentionDays }

// HashField Hash 内字段：orgObjectIdHex + ":" + 自然日 20060102（多租户隔离，便于按日期 HMGET）
func HashField(orgHex, dayYYYYMMDD string) string {
	return orgHex + ":" + dayYYYYMMDD
}

func SalesDailyRedisKey(_ string) string {
	return SalesDailyRedisHashKey
}

// WriteDays 按日期分片写入 Hash；field 为 20060102，value 为当日各业务员维度行 JSON 数组
func WriteDays(ctx context.Context, rdb redis.UniversalClient, orgHex string, rows []*model.SystemStatistics) error {
	return WriteDaysForRange(ctx, rdb, orgHex, nil, rows)
}

// WriteDaysForRange 在 dateList 不为空时，为其中每个日期都写入一项（无数据则写 []），避免前端查询整段日期时缓存永远缺 key
func WriteDaysForRange(ctx context.Context, rdb redis.UniversalClient, orgHex string, dateList []string, rows []*model.SystemStatistics) error {
	byDay := make(map[string][]*model.SystemStatistics)
	for _, r := range rows {
		if r == nil || r.Date == "" {
			continue
		}
		byDay[r.Date] = append(byDay[r.Date], r)
	}
	key := SalesDailyRedisKey(orgHex)
	pipe := rdb.Pipeline()
	if len(dateList) > 0 {
		for _, day := range dateList {
			list := byDay[day]
			if list == nil {
				list = []*model.SystemStatistics{}
			}
			b, err := json.Marshal(list)
			if err != nil {
				return err
			}
			pipe.HSet(ctx, key, HashField(orgHex, day), string(b))
		}
	} else {
		for day, list := range byDay {
			b, err := json.Marshal(list)
			if err != nil {
				return err
			}
			pipe.HSet(ctx, key, HashField(orgHex, day), string(b))
		}
	}
	pipe.Expire(ctx, key, time.Duration(salesDailyRetentionDays+10)*24*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

// TryGetMerged 按日期列表读取并合并；若任一日期缺失则返回 ok=false
func TryGetMerged(ctx context.Context, rdb redis.UniversalClient, orgHex string, dateKeys []string) ([]*model.SystemStatistics, bool, error) {
	if len(dateKeys) == 0 {
		return nil, true, nil
	}
	key := SalesDailyRedisKey(orgHex)
	fields := make([]string, len(dateKeys))
	for i, d := range dateKeys {
		fields[i] = HashField(orgHex, d)
	}
	vals, err := rdb.HMGet(ctx, key, fields...).Result()
	if err != nil {
		return nil, false, err
	}
	out := make([]*model.SystemStatistics, 0, 256)
	for i, v := range vals {
		if v == nil {
			return nil, false, nil
		}
		s, ok := v.(string)
		if !ok || s == "" {
			return nil, false, nil
		}
		var chunk []*model.SystemStatistics
		if err := json.Unmarshal([]byte(s), &chunk); err != nil {
			return nil, false, fmt.Errorf("unmarshal sales_daily day %s: %w", dateKeys[i], err)
		}
		out = append(out, chunk...)
	}
	return out, true, nil
}

// TrimOlderThan 删除本组织早于 cutoffDateStr（20060102）的 field（field 形如 orgHex:YYYYMMDD）
func TrimOlderThan(ctx context.Context, rdb redis.UniversalClient, orgHex string, cutoffDateStr string) error {
	key := SalesDailyRedisKey(orgHex)
	fields, err := rdb.HKeys(ctx, key).Result()
	if err != nil {
		return err
	}
	prefix := orgHex + ":"
	var toDel []string
	for _, f := range fields {
		if !strings.HasPrefix(f, prefix) {
			continue
		}
		datePart := f[len(prefix):]
		if len(datePart) == 8 && datePart < cutoffDateStr {
			toDel = append(toDel, f)
		}
	}
	if len(toDel) == 0 {
		return nil
	}
	return rdb.HDel(ctx, key, toDel...).Err()
}
