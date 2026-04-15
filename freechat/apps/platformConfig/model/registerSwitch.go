package model

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
)

type RegisterSwitchDao struct {
	redisCli redis.UniversalClient
}

const RegisterSwitchKey = "CLOSE_REGISTER"

func NewRegisterSwitchDao(redisCli redis.UniversalClient) *RegisterSwitchDao {
	return &RegisterSwitchDao{
		redisCli: redisCli,
	}
}

func (r *RegisterSwitchDao) CloseRegister(ctx context.Context) error {
	_, err := r.redisCli.Set(ctx, RegisterSwitchKey, "1", 0).Result()
	if err != nil {
		return err
	}
	return nil
}

func (r *RegisterSwitchDao) OpenRegister(ctx context.Context) error {
	_, err := r.redisCli.Del(ctx, RegisterSwitchKey).Result()
	if err != nil {
		return err
	}
	return nil
}

func (r *RegisterSwitchDao) IsOpenRegister(ctx context.Context) (bool, error) {
	res, err := r.redisCli.Get(ctx, RegisterSwitchKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}

	return res == "", nil
}
