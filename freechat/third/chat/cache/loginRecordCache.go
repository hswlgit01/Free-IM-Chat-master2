package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/lionsoul2014/ip2region/binding/golang/xdb"
	cacheRedis "github.com/openimsdk/chat/freechat/cache/redis"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	"github.com/openimsdk/chat/freechat/utils/ip2regionUtils"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
)

// CacheExpireTime
const (
	loginRecordCacheExpireTime = time.Second * 60 * 60 * 12
	loginRecordKeyPrefix       = "C_LOGIN_RECORD_USER_ID"
)

// UserLoginRecordCache
type UserLoginRecordCache struct {
	UserID    string    `json:"user_id"`
	LoginTime time.Time `json:"login_time"`
	IP        string    `json:"ip"`
	DeviceID  string    `json:"device_id"`
	Platform  string    `json:"platform"`
	Region    string    `json:"region"`
}

// LoginRecordCacheRedis
type LoginRecordCacheRedis struct {
	rdb        redis.UniversalClient
	expireTime time.Duration
	rcClient   *cacheRedis.RocksCacheClient
	Dao        *chatModel.UserLoginRecordDao
}

// NewLoginRecordCacheRedis
func NewLoginRecordCacheRedis(rdb redis.UniversalClient, db *mongo.Database) *LoginRecordCacheRedis {
	rc := cacheRedis.NewRocksCacheClient(rdb)
	return &LoginRecordCacheRedis{
		rdb:        rdb,
		expireTime: loginRecordCacheExpireTime,
		rcClient:   rc,
		Dao:        chatModel.NewUserLoginRecordDao(db),
	}
}

// getCacheKey
func (u *LoginRecordCacheRedis) getCacheKey(userID string) string {
	return fmt.Sprintf("%s:{%s}", loginRecordKeyPrefix, userID)
}

// convertToCache converts a database model to a cache model, including IP region parsing.
func (u *LoginRecordCacheRedis) convertToCache(ctx context.Context, record *chatModel.UserLoginRecord) (*UserLoginRecordCache, error) {
	cache := &UserLoginRecordCache{
		UserID:    record.UserID,
		LoginTime: record.LoginTime,
		IP:        record.IP,
		DeviceID:  record.DeviceID,
		Platform:  record.Platform,
		Region:    "",
	}
	if record.IP != "" {
		region, err := u.parseIPRegion(record.IP)
		if err == nil {
			cache.Region = region
		}
	}
	return cache, nil
}

// FromDBRecord 将单条 user_login_record 文档转为列表用结构（IP 与库表一致，归属地解析逻辑与 Redis 缓存路径相同）
func (u *LoginRecordCacheRedis) FromDBRecord(ctx context.Context, record *chatModel.UserLoginRecord) (*UserLoginRecordCache, error) {
	if record == nil {
		return nil, nil
	}
	return u.convertToCache(ctx, record)
}

// FromDBRecords 批量将 user_login_record 查询结果转为列表用结构（管理端用户列表等直读库表、不经登录缓存读取）
func (u *LoginRecordCacheRedis) FromDBRecords(ctx context.Context, records []*chatModel.UserLoginRecord) ([]*UserLoginRecordCache, error) {
	if len(records) == 0 {
		return nil, nil
	}
	out := make([]*UserLoginRecordCache, 0, len(records))
	for _, r := range records {
		if r == nil {
			continue
		}
		c, err := u.convertToCache(ctx, r)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// parseIPRegion parses an IP address to region information.
func (u *LoginRecordCacheRedis) parseIPRegion(ip string) (string, error) {
	searcher, err := xdb.NewWithBuffer(ip2regionUtils.Ip2RegionDB)
	if err != nil {
		return "", err
	}
	defer searcher.Close()
	region, err := searcher.SearchByStr(ip)
	if err != nil {
		return "", err
	}
	format, err := ip2regionUtils.FormatSearchResp(region)
	if err != nil {
		return "", err
	}
	return format.String(), nil
}

// GetByUserId retrieves a user's login record from cache or database.
func (u *LoginRecordCacheRedis) GetByUserId(ctx context.Context, userId string) (*UserLoginRecordCache, error) {
	return cacheRedis.GetCache(ctx, u.rcClient, u.getCacheKey(userId), u.expireTime, func(ctx context.Context) (*UserLoginRecordCache, error) {
		record, err := u.Dao.GetByUserId(ctx, userId)
		if err != nil {
			return nil, err
		}
		return u.convertToCache(ctx, record)
	})
}

// FindLatestByUserIDs retrieves the latest login records for multiple users from cache or database.
func (u *LoginRecordCacheRedis) FindLatestByUserIDs(ctx context.Context, userIDs []string) ([]*UserLoginRecordCache, error) {
	return cacheRedis.BatchGetCache2(ctx, u.rcClient, u.expireTime, userIDs, func(userId string) string {
		return u.getCacheKey(userId)
	}, func(v *UserLoginRecordCache) string {
		return v.UserID
	}, func(ctx context.Context, userIDs []string) ([]*UserLoginRecordCache, error) {
		records, err := u.Dao.FindLatestByUserIDs(ctx, userIDs)
		if err != nil {
			return nil, err
		}
		cacheRecords := make([]*UserLoginRecordCache, 0, len(records))
		for _, record := range records {
			cacheRecord, err := u.convertToCache(ctx, record)
			if err != nil {
				return nil, err
			}
			cacheRecords = append(cacheRecords, cacheRecord)
		}
		return cacheRecords, nil
	})
}

// DelCache
func (u *LoginRecordCacheRedis) DelCache(ctx context.Context, userID string) error {
	if userID == "" {
		return fmt.Errorf("userID cannot be empty")
	}
	if u.rcClient.Disable() {
		return nil
	}
	key := u.getCacheKey(userID)
	_, err := u.rdb.Del(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to delete cache for userID %s: %w", userID, err)
	}
	return nil
}

// DelCaches
func (u *LoginRecordCacheRedis) DelCaches(ctx context.Context, userIDs ...string) error {
	if len(userIDs) == 0 || u.rcClient.Disable() {
		return nil
	}
	keys := make([]string, 0, len(userIDs))
	for _, userID := range userIDs {
		if userID == "" {
			continue
		}
		keys = append(keys, u.getCacheKey(userID))
	}
	if len(keys) == 0 {
		return nil
	}
	_, err := u.rdb.Del(ctx, keys...).Result()
	if err != nil {
		return fmt.Errorf("failed to delete caches for userIDs: %w", err)
	}
	return nil
}
