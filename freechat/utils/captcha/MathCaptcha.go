package captcha

import (
	"context"
	"github.com/mojocn/base64Captcha"
	"github.com/redis/go-redis/v9"
	"time"
)

var mathCaptchaStore = base64Captcha.DefaultMemStore

const CaptchaMathKeyPrefix = "CAPTCHA_MATH_"

func GenerateMathCaptcha(ctx context.Context, redisCli redis.UniversalClient, expiration time.Duration) (string, string, error) {
	var driver base64Captcha.Driver

	driver = &base64Captcha.DriverMath{
		Height:          80,
		Width:           240,
		NoiseCount:      1,
		ShowLineOptions: 4,
		BgColor:         nil,
		Fonts:           []string{"3Dumb.ttf"},
	}
	c := base64Captcha.NewCaptcha(driver, mathCaptchaStore)
	id, b64s, answer, err := c.Generate()
	if err != nil {
		return "", "", err
	}
	// 清理原始store数据
	_ = mathCaptchaStore.Get(id, true)

	key := CaptchaMathKeyPrefix + id
	cmdRes := redisCli.Set(ctx, key, answer, expiration)
	if cmdRes.Err() != nil {
		return "", "", cmdRes.Err()
	}

	return id, b64s, nil
}

func VerifyMathCaptcha(ctx context.Context, redisCli redis.UniversalClient, id, answer string) bool {
	key := CaptchaMathKeyPrefix + id
	cmdRes := redisCli.Get(ctx, key)
	if cmdRes.Err() != nil {
		return false
	}
	redisAnswer, _ := cmdRes.Result()
	if redisAnswer != answer {
		return false
	}

	if err := redisCli.Del(ctx, key).Err(); err != nil {
		return false
	}
	return true
}
