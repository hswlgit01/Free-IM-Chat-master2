package svc

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// 币种精度缓存。
//
// 【背景】压测实测：抢红包路径上 ValidateCurrencyPrecision 每次都要查一遍
// wallet_currency，2 分钟内查了 304 次，平均耗时 392ms —— 而这张表在测试环境
// 里总共只有 1 行。单行主键查询要 392ms，说明时间全花在 Mongo 排队上，
// 这类静态数据根本不该出现在高并发路径里。
//
// 币种的 decimals 属于建好就基本不动的配置，用短 TTL 的进程内缓存足够：
// 即使运营真的改了精度，最多 currencyCacheTTL 之后生效。
// 需要立刻生效时可以调 InvalidateCurrencyCache。
var (
	currencyCacheMu sync.RWMutex
	currencyCache   = map[string]currencyCacheEntry{}
)

type currencyCacheEntry struct {
	currency *walletModel.WalletCurrency
	expireAt time.Time
}

// currencyCacheTTL 默认 5 分钟，可用 TRANSACTION_CURRENCY_CACHE_TTL_SEC 覆盖；
// 设为 0 表示关闭缓存（回到每次查库的老行为，便于对照排查）。
func currencyCacheTTL() time.Duration {
	if v := os.Getenv("TRANSACTION_CURRENCY_CACHE_TTL_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 5 * time.Minute
}

// getCurrencyCached 取币种信息，命中缓存就不查库。
func getCurrencyCached(ctx context.Context, db *mongo.Database, id primitive.ObjectID) (*walletModel.WalletCurrency, error) {
	ttl := currencyCacheTTL()
	key := id.Hex()

	if ttl > 0 {
		currencyCacheMu.RLock()
		entry, ok := currencyCache[key]
		currencyCacheMu.RUnlock()
		if ok && time.Now().Before(entry.expireAt) {
			return entry.currency, nil
		}
	}

	currency, err := walletModel.NewWalletCurrencyDao(db).GetById(ctx, id)
	if err != nil {
		return nil, err
	}

	if ttl > 0 && currency != nil {
		currencyCacheMu.Lock()
		currencyCache[key] = currencyCacheEntry{currency: currency, expireAt: time.Now().Add(ttl)}
		currencyCacheMu.Unlock()
	}
	return currency, nil
}

// InvalidateCurrencyCache 清空币种缓存。运营改了币种配置后可以调用它立刻生效。
func InvalidateCurrencyCache() {
	currencyCacheMu.Lock()
	currencyCache = map[string]currencyCacheEntry{}
	currencyCacheMu.Unlock()
}
