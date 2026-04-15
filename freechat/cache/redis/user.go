package redis

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/cache"
	"github.com/openimsdk/chat/freechat/cache/cachekey"
	"github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/redis/go-redis/v9"
)

const (
	userExpireTime            = time.Second * 60 * 60 * 12
	userOlineStatusExpireTime = time.Second * 60 * 60 * 24
	statusMod                 = 501
)

type UserCacheRedis struct {
	rdb        redis.UniversalClient
	expireTime time.Duration
	rcClient   *RocksCacheClient
	userDao    *model.UserDao
}

func NewUserCacheRedis(rdb redis.UniversalClient) cache.UserCache {
	rc := NewRocksCacheClient(rdb)
	return &UserCacheRedis{
		rdb:        rdb,
		expireTime: userExpireTime,
		rcClient:   rc,
	}
}

func (u *UserCacheRedis) getUserID(user *model.User) string {
	return user.UserID
}

func (u *UserCacheRedis) GetUserInfo(ctx context.Context, userID string) (userInfo *model.User, err error) {
	return GetCache(ctx, u.rcClient, u.getUserInfoKey(userID), u.expireTime, func(ctx context.Context) (*model.User, error) {
		return u.userDao.Take(ctx, userID)
	})
}

func (u *UserCacheRedis) GetUsersInfo(ctx context.Context, userIDs []string) ([]*model.User, error) {
	return BatchGetCache2(ctx, u.rcClient, u.expireTime, userIDs, u.getUserInfoKey, u.getUserID, u.userDao.Find)
}

func (u *UserCacheRedis) getUserInfoKey(userID string) string {
	return cachekey.GetUserInfoKey(userID)
}

func (u *UserCacheRedis) getUserGlobalRecvMsgOptKey(userID string) string {
	return cachekey.GetUserGlobalRecvMsgOptKey(userID)
}

func (u *UserCacheRedis) getUserAESKeyKey(userID string) string {
	return cachekey.GetUserAESKeyKey(userID)
}
