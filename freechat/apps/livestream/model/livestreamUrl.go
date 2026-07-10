package model

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/openimsdk/chat/freechat/utils/netUtils"
	"github.com/openimsdk/tools/log"
	"github.com/redis/go-redis/v9"
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

// AutomaticallySearchPublicUrl verifies that at least one internal LiveKit
// endpoint is reachable, then returns the address that clients should use.
// In cloud deployments the server commonly probes 127.0.0.1 while mobile
// clients must receive a public IP/domain; conflating the two produces an
// unusable localhost/0.0.0.0 serverUrl.
func (r *LivestreamUrlDao) AutomaticallySearchPublicUrl(
	ctx context.Context,
	internalUrls []string,
	publicUrl string,
) (string, error) {
	internalUrl, err := r.AutomaticallySearchUrl(ctx, internalUrls)
	if err != nil {
		return "", err
	}
	return selectPublicUrl(internalUrl, publicUrl)
}

func selectPublicUrl(internalUrl, publicUrl string) (string, error) {
	publicUrl = strings.TrimSpace(publicUrl)
	if publicUrl == "" {
		return internalUrl, nil
	}
	parsed, err := url.Parse(publicUrl)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return "", fmt.Errorf("unsupported public LiveKit URL scheme %q", parsed.Scheme)
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return "", errors.New("public LiveKit URL has no host")
	}
	ip := net.ParseIP(host)
	if host == "localhost" || (ip != nil && (ip.IsLoopback() || ip.IsUnspecified())) {
		return "", fmt.Errorf("public LiveKit URL is not client-routable: %q", host)
	}
	return publicUrl, nil
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
