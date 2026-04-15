package svc

import (
	"context"
	"fmt"

	"github.com/openimsdk/chat/freechat/apps/exchangeRate/dto"
	"github.com/openimsdk/chat/freechat/apps/exchangeRate/model"
	"github.com/openimsdk/chat/freechat/plugin"
)

// ExchangeRateSvc 汇率服务
type ExchangeRateSvc struct{}

// NewExchangeRateService 创建汇率服务实例
func NewExchangeRateService() *ExchangeRateSvc {
	return &ExchangeRateSvc{}
}

// GetLatestRates 获取最新汇率数据
func (e *ExchangeRateSvc) GetLatestRates(ctx context.Context, base string) (*dto.ExchangeRateResp, error) {
	db := plugin.MongoCli().GetDB()
	exchangeRateDao := model.NewExchangeRateDao(db)

	// 从数据库获取最新汇率，传入base参数
	exchangeRate, err := exchangeRateDao.GetLatestRates(ctx, base)
	if err != nil {
		return nil, fmt.Errorf("get latest rates failed: %w", err)
	}

	// 转换为响应格式
	return &dto.ExchangeRateResp{
		Base:      exchangeRate.Base,
		Timestamp: exchangeRate.Timestamp,
		Rates:     exchangeRate.Rates,
	}, nil
}
