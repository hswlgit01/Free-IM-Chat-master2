package cache

import (
	"context"
	"fmt"
	"github.com/openimsdk/chat/freechat/apps/organization/model"
	cacheRedis "github.com/openimsdk/chat/freechat/cache/redis"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

const (
	// 降低缓存过期时间，确保数据的及时性
	// 原过期时间为120秒(2分钟)，现缩短为30秒
	CacheExpireTime = time.Second * 30
)

type OrgCacheRedis struct {
	rdb        redis.UniversalClient
	expireTime time.Duration
	rcClient   *cacheRedis.RocksCacheClient
	Dao        *model.OrganizationDao
}

func NewOrgCacheRedis(rdb redis.UniversalClient, db *mongo.Database) *OrgCacheRedis {
	rc := cacheRedis.NewRocksCacheClient(rdb)
	return &OrgCacheRedis{
		rdb:        rdb,
		expireTime: CacheExpireTime,
		rcClient:   rc,
		Dao:        model.NewOrganizationDao(db),
	}
}

func (u *OrgCacheRedis) GetById(ctx context.Context, id primitive.ObjectID) (*model.Organization, error) {
	// 支持查询参数中传入版本号
	version := ""
	if v := ctx.Value("cache_version"); v != nil {
		if vStr, ok := v.(string); ok && vStr != "" {
			version = vStr
		}
	}

	// 构建缓存键，支持版本号
	cacheKey := fmt.Sprintf("C_ORG_ID:%s", id.Hex())
	if version != "" {
		cacheKey = fmt.Sprintf("%s:v%s", cacheKey, version)
	}

	return cacheRedis.GetCache(ctx, u.rcClient, cacheKey, u.expireTime, func(ctx context.Context) (*model.Organization, error) {
		return u.Dao.GetById(ctx, id)
	})
}

func (u *OrgCacheRedis) GetByIdAndStatus(ctx context.Context, id primitive.ObjectID, status model.OrganizationStatus) (*model.Organization, error) {
	// 支持查询参数中传入版本号
	version := ""
	if v := ctx.Value("cache_version"); v != nil {
		if vStr, ok := v.(string); ok && vStr != "" {
			version = vStr
		}
	}

	// 构建缓存键，支持版本号
	cacheKey := fmt.Sprintf("C_ORG_ID_%s:%s", status, id.Hex())
	if version != "" {
		cacheKey = fmt.Sprintf("%s:v%s", cacheKey, version)
	}

	return cacheRedis.GetCache(ctx, u.rcClient, cacheKey, u.expireTime, func(ctx context.Context) (*model.Organization, error) {
		return u.Dao.GetByIdAndStatus(ctx, id, status)
	})
}
