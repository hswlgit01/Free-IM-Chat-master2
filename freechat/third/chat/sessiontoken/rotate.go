package sessiontoken

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/openimsdk/tools/log"
	"github.com/redis/go-redis/v9"
)

const (
	rotationLockTTL   = 30 * time.Second
	rotationWait      = 10 * time.Second
	rotationRetry     = 25 * time.Millisecond
	rotationKeyPrefix = "chat:login_rotate:"
)

var releaseLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

// Rotate serializes InvalidateToken -> CreateToken across App and H5 login
// paths. Without the per-user Redis lock, two concurrent logins can both
// delete first and then each add a valid ChatToken.
func Rotate(
	ctx context.Context,
	rdb redis.UniversalClient,
	userID string,
	invalidate func(context.Context) error,
	create func(context.Context) (string, error),
) (string, error) {
	if rdb == nil {
		return "", fmt.Errorf("rotate chat token: redis client is nil")
	}
	release, err := acquire(ctx, rdb, rotationKeyPrefix+"{"+userID+"}")
	if err != nil {
		return "", err
	}
	defer release()

	if err := invalidate(ctx); err != nil {
		return "", err
	}
	return create(ctx)
}

func acquire(ctx context.Context, rdb redis.UniversalClient, key string) (func(), error) {
	waitCtx, cancel := context.WithTimeout(ctx, rotationWait)
	value := uuid.NewString()
	for {
		if err := waitCtx.Err(); err != nil {
			cancel()
			return nil, fmt.Errorf("acquire chat token rotation lock: %w", err)
		}
		ok, err := rdb.SetNX(waitCtx, key, value, rotationLockTTL).Result()
		if err != nil {
			cancel()
			return nil, fmt.Errorf("acquire chat token rotation lock: %w", err)
		}
		if ok {
			cancel()
			return func() {
				releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
				defer releaseCancel()
				if err := releaseLockScript.Run(releaseCtx, rdb, []string{key}, value).Err(); err != nil {
					log.ZWarn(ctx, "release chat token rotation lock failed", err, "userID", userIDFromKey(key))
				}
			}, nil
		}

		timer := time.NewTimer(rotationRetry)
		select {
		case <-waitCtx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func userIDFromKey(key string) string {
	start := len(rotationKeyPrefix) + 1
	if len(key) > start && key[len(key)-1] == '}' {
		return key[start : len(key)-1]
	}
	return ""
}
