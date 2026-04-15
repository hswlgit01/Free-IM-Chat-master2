package plugin

import (
	"github.com/redis/go-redis/v9"
)

var redisCli redis.UniversalClient

func RedisCli() redis.UniversalClient {
	return redisCli
}

func InitRedisCli(cli redis.UniversalClient) {
	redisCli = cli
}
