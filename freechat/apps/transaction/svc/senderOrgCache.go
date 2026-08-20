package svc

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	organizationModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"go.mongodb.org/mongo-driver/mongo"
)

// 红包发送者的组织用户信息缓存。
//
// 【背景】压测实测：抢红包路径上 organization_user 每次尝试要读 2.5 次，平均 89ms。
// 其中一次查的是**发送者**——同一个红包的几百个抢包人查的是同一条记录，
// 取回来的是同一个文档。
//
// 【为什么只缓存发送者、不缓存接收者】
// 接收者那次查询是对**请求发起人**的授权校验（此人是否属于该组织），
// 缓存授权结论意味着「已被移出组织的人在 TTL 内仍可领红包」，不划算。
// 而发送者这次查询不是对请求方的授权：红包创建时发送者的归属就已经定下来了，
// 这里取它只是为了和接收者比对组织是否一致。缓存它没有授权层面的副作用。
var (
	senderOrgCacheMu sync.RWMutex
	senderOrgCache   = map[string]senderOrgCacheEntry{}
)

type senderOrgCacheEntry struct {
	orgUser  *organizationModel.OrganizationUser
	expireAt time.Time
}

// senderOrgCacheTTL 默认 60 秒，可用 TRANSACTION_SENDER_ORG_CACHE_TTL_SEC 覆盖；
// 设为 0 关闭缓存，回到每次查库的老行为。
func senderOrgCacheTTL() time.Duration {
	if v := os.Getenv("TRANSACTION_SENDER_ORG_CACHE_TTL_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 60 * time.Second
}

// getSenderOrgUserCached 取红包发送者的组织用户信息，命中缓存就不查库。
func getSenderOrgUserCached(ctx context.Context, db *mongo.Database, senderID, orgID string) (*organizationModel.OrganizationUser, error) {
	ttl := senderOrgCacheTTL()
	key := senderID + "|" + orgID

	if ttl > 0 {
		senderOrgCacheMu.RLock()
		entry, ok := senderOrgCache[key]
		senderOrgCacheMu.RUnlock()
		if ok && time.Now().Before(entry.expireAt) {
			return entry.orgUser, nil
		}
	}

	orgUser, err := organizationModel.NewOrganizationUserDao(db).GetByUserIdAndOrgID(ctx, senderID, orgID)
	if err != nil {
		return nil, err
	}

	if ttl > 0 && orgUser != nil {
		senderOrgCacheMu.Lock()
		// 顺手清掉过期条目，避免长期运行下 map 无限增长
		if len(senderOrgCache) > 4096 {
			now := time.Now()
			for k, v := range senderOrgCache {
				if now.After(v.expireAt) {
					delete(senderOrgCache, k)
				}
			}
		}
		senderOrgCache[key] = senderOrgCacheEntry{orgUser: orgUser, expireAt: time.Now().Add(ttl)}
		senderOrgCacheMu.Unlock()
	}
	return orgUser, nil
}
