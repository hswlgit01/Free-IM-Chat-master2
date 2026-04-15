package model

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"strconv"
)

type DeviceRegisterNumDao struct {
	redisCli redis.UniversalClient
}

const DeviceRegisterKeyPrefix = "REGISTER_DEVICE_NUM_"

func NewDeviceRegisterNumDao(redisCli redis.UniversalClient) *DeviceRegisterNumDao {
	return &DeviceRegisterNumDao{
		redisCli: redisCli,
	}
}

func (r *DeviceRegisterNumDao) Add(ctx context.Context, deviceId string) error {
	key := DeviceRegisterKeyPrefix + deviceId
	err := r.redisCli.Incr(ctx, key).Err()
	if err != nil {
		return err
	}
	//ttl, err := r.redisCli.TTL(ctx, key).Result()
	//if err != nil {
	//	return err
	//}
	//
	//if int(ttl) < 0 {
	//	r.redisCli.Expire(ctx, key, time.Hour * 24 * 7)
	//}
	return nil
}

func (r *DeviceRegisterNumDao) Get(ctx context.Context, deviceId string) (int, error) {
	key := DeviceRegisterKeyPrefix + deviceId
	res, err := r.redisCli.Get(ctx, key).Result()

	if err != nil {
		if !errors.Is(err, redis.Nil) {
			return 0, err
		}
		return 0, nil
	}

	deviceRegisterNum, err := strconv.Atoi(res)
	if err != nil {
		return 0, err
	}
	return deviceRegisterNum, nil
}
