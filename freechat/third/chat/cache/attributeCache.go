package cache

import (
	"context"
	"fmt"
	cacheRedis "github.com/openimsdk/chat/freechat/cache/redis"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

const (
	attributeCacheExpireTime = time.Second * 60 * 5
	attributeKeyPrefix       = "C_ATTRIBUTE_USER_ID"
)

type AttributeCacheRedis struct {
	rdb        redis.UniversalClient
	expireTime time.Duration
	rcClient   *cacheRedis.RocksCacheClient
	Dao        *chatModel.AttributeDao
}

func NewAttributeCacheRedis(rdb redis.UniversalClient, db *mongo.Database) *AttributeCacheRedis {
	rc := cacheRedis.NewRocksCacheClient(rdb)
	return &AttributeCacheRedis{
		rdb:        rdb,
		expireTime: attributeCacheExpireTime,
		rcClient:   rc,
		Dao:        chatModel.NewAttributeDao(db),
	}
}

func (u *AttributeCacheRedis) getCacheKey(userID string) string {
	return fmt.Sprintf("%s:{%s}", attributeKeyPrefix, userID)
}

func (u *AttributeCacheRedis) Take(ctx context.Context, userId string) (*chatModel.Attribute, error) {
	return cacheRedis.GetCache(ctx, u.rcClient, u.getCacheKey(userId), u.expireTime, func(ctx context.Context) (*chatModel.Attribute, error) {
		return u.Dao.Take(ctx, userId)
	})
}

func (u *AttributeCacheRedis) Find(ctx context.Context, userIDs []string) ([]*chatModel.Attribute, error) {
	return cacheRedis.BatchGetCache2(ctx, u.rcClient, u.expireTime, userIDs, func(userId string) string {
		return u.getCacheKey(userId)
	}, func(v *chatModel.Attribute) string {
		return v.UserID
	}, u.Dao.Find)
}

// DelCache
func (u *AttributeCacheRedis) DelCache(ctx context.Context, userID string) error {
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
