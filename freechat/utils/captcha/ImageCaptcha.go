package captcha

import (
	"context"
	"github.com/mojocn/base64Captcha"
	"github.com/redis/go-redis/v9"
	"time"
)

var store = base64Captcha.DefaultMemStore

const CaptchaKeyPrefix = "CAPTCHA_IMAGE_"

func GenerateImageCaptcha(ctx context.Context, redisCli redis.UniversalClient, expiration time.Duration) (string, string, error) {
	var driver base64Captcha.Driver

	driver = &base64Captcha.DriverDigit{
		Height:   120,
		Width:    240,
		MaxSkew:  0.1,
		DotCount: 5,
		Length:   4,
	}
	c := base64Captcha.NewCaptcha(driver, store)
	id, b64s, answer, err := c.Generate()
	if err != nil {
		return "", "", err
	}
	// 清理原始store数据
	_ = store.Get(id, true)

	key := CaptchaKeyPrefix + id
	cmdRes := redisCli.Set(ctx, key, answer, expiration)
	if cmdRes.Err() != nil {
		return "", "", cmdRes.Err()
	}

	return id, b64s, nil
}

func VerifyImageCaptcha(ctx context.Context, redisCli redis.UniversalClient, id, answer string) bool {
	key := CaptchaKeyPrefix + id
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
