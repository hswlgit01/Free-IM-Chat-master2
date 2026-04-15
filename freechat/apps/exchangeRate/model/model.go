package model

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openimsdk/chat/freechat/plugin"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ExchangeRate 表示货币汇率信息
type ExchangeRate struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Base      string             `bson:"base" json:"base"`             // 基准货币
	Timestamp int64              `bson:"timestamp" json:"timestamp"`   // 汇率时间戳
	Rates     map[string]float64 `bson:"rates" json:"rates"`           // 汇率数据
	UpdatedAt time.Time          `bson:"updated_at" json:"updated_at"` // 更新时间
	CreatedAt time.Time          `bson:"created_at" json:"created_at"` // 创建时间
}

func (ExchangeRate) TableName() string {
	return constant.CollectionExchangeRate
}

// ExchangeRateDao 汇率数据访问对象
type ExchangeRateDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

// NewExchangeRateDao 创建汇率DAO实例
func NewExchangeRateDao(db *mongo.Database) *ExchangeRateDao {
	return &ExchangeRateDao{
		DB:         db,
		Collection: db.Collection(ExchangeRate{}.TableName()),
	}
}

// getExchangeRateCacheKey 获取汇率缓存键
func (o *ExchangeRateDao) getExchangeRateCacheKey(base string) string {
	return fmt.Sprintf("exchange_rate:latest:%s", base)
}

// UpsertLatestRates 更新或插入最新汇率数据
func (o *ExchangeRateDao) UpsertLatestRates(ctx context.Context, rate *ExchangeRate) error {
	rate.UpdatedAt = time.Now().UTC()
	if rate.CreatedAt.IsZero() {
		rate.CreatedAt = time.Now().UTC()
	}

	// 从时间戳获取日期
	currentDate := time.Unix(rate.Timestamp, 0).UTC()
	startOfDay := time.Date(currentDate.Year(), currentDate.Month(), currentDate.Day(), 0, 0, 0, 0, time.UTC)
	endOfDay := startOfDay.Add(24 * time.Hour)

	// 查找同一天同一基准货币的数据
	filter := bson.M{
		"base": rate.Base,
		"timestamp": bson.M{
			"$gte": startOfDay.Unix(),
			"$lt":  endOfDay.Unix(),
		},
	}

	// 使用upsert选项，如果找到同一天的记录则更新，否则插入新记录
	update := bson.M{"$set": rate}
	opts := options.Update().SetUpsert(true)

	_, err := o.Collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("更新汇率数据失败: %w", err)
	}

	// 更新Redis缓存
	redis := plugin.RedisCli()
	cacheKey := o.getExchangeRateCacheKey(rate.Base)
	rateJSON, marshalErr := json.Marshal(rate)
	if marshalErr == nil {
		// 设置缓存，不设置有效期
		redis.Set(ctx, cacheKey, rateJSON, 0)
	}

	return nil
}

// GetLatestRates 获取最新汇率数据
func (o *ExchangeRateDao) GetLatestRates(ctx context.Context, base string) (*ExchangeRate, error) {
	// 先尝试从Redis缓存获取
	redis := plugin.RedisCli()
	cacheKey := o.getExchangeRateCacheKey(base)
	cachedData, err := redis.Get(ctx, cacheKey).Result()
	if err == nil {
		var rate ExchangeRate
		if json.Unmarshal([]byte(cachedData), &rate) == nil {
			return &rate, nil
		}
	}

	// 从数据库查询指定基准货币的最新汇率数据
	filter := bson.M{"base": base}
	opts := options.FindOne().SetSort(bson.M{"timestamp": -1})

	result, err := mongoutil.FindOne[*ExchangeRate](ctx, o.Collection, filter, opts)
	if err != nil {
		return nil, err
	}

	// 将结果存入Redis缓存
	if result != nil {
		cacheKey := o.getExchangeRateCacheKey(base)
		rateJSON, marshalErr := json.Marshal(result)
		if marshalErr == nil {
			// 设置缓存，不设置有效期
			redis.Set(ctx, cacheKey, rateJSON, 0)
		}
	}

	return result, nil
}
