package model

import (
	"context"
	"errors"
	"github.com/openimsdk/chat/freechat/utils/netUtils"
	"github.com/openimsdk/tools/log"
	"github.com/redis/go-redis/v9"
	"time"
)

type LivestreamUrlDao struct {
	redisCli redis.UniversalClient
}

const LivestreamUrlKey = "LIVESTREAM_URL"

func NewLivestreamUrlDao(redisCli redis.UniversalClient) *LivestreamUrlDao {
	return &LivestreamUrlDao{
		redisCli: redisCli,
	}
}

func (r *LivestreamUrlDao) AutomaticallySearchUrl(ctx context.Context, urls []string) (string, error) {
	key := LivestreamUrlKey
	res, err := r.redisCli.Get(ctx, key).Result()
	//if err != nil && !errors.Is(err, redis.Nil) {
	//	return "", err
	//}
	//
	//if err == nil {
	//	return res, nil
	//}

	if err != nil {
		// 异常错误处理
		if !errors.Is(err, redis.Nil) {
			return "", err
		}
		// 没有获取过值,获取值
		value := ""
		for _, url := range urls {
			_, host, port, err := netUtils.ParseURL(url)
			if err != nil {
				log.ZError(ctx, "解析url失败", err, "url", url)
				continue
			}

			ok := netUtils.PingTCP(host, port, time.Second*2)
			if ok {
				value = url
				break
			}
		}
		if value == "" {
			return "", errors.New("没有可用的直播url")
		}

		_, err = r.redisCli.SetEx(ctx, key, value, time.Second*90).Result()
		return value, err
	}

	return res, nil

}
