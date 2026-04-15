package exchangeRate

import (
	"context"
	"fmt"
	"github.com/openimsdk/chat/freechat/apps/exchangeRate/dto"
	"github.com/openimsdk/chat/freechat/apps/exchangeRate/model"
	"time"

	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/tools/log"
	"github.com/robfig/cron/v3"
)

// ExchangeRateCronJob 汇率获取定时任务
type ExchangeRateCronJob struct {
	cron *cron.Cron
}

// NewExchangeRateCronJob 创建汇率获取定时任务
func NewExchangeRateCronJob() *ExchangeRateCronJob {
	c := cron.New(cron.WithSeconds())
	return &ExchangeRateCronJob{
		cron: c,
	}
}

// Start 开始定时任务
func (e *ExchangeRateCronJob) Start() {
	// 立即执行一次
	ctx := context.Background()
	go e.CNYAndUSD(ctx)

	// 每小时执行一次（0分0秒）
	_, err := e.cron.AddFunc("0 0 1 * * *", func() {
		e.CNYAndUSD(ctx)
	})
	if err != nil {
		log.ZError(ctx, "添加汇率获取定时任务失败", err)
		return
	}

	e.cron.Start()
	log.ZInfo(ctx, "汇率获取定时任务已启动")
}

// Stop 停止定时任务
func (e *ExchangeRateCronJob) Stop() {
	if e.cron != nil {
		e.cron.Stop()
	}
}

func (e *ExchangeRateCronJob) CNYAndUSD(ctx context.Context) {
	e.fetchRates(ctx, "CNY")
	e.fetchRates(ctx, "USD")
}

// fetchRates 获取并保存汇率数据
func (e *ExchangeRateCronJob) fetchRates(ctx context.Context, base string) {
	// 获取配置
	cfg := plugin.ChatCfg().ApiConfig.Exchange
	url := fmt.Sprintf("%s/%s/latest/%s",
		cfg.BaseURL,
		cfg.AppID,
		base)

	httpClient := utils.NewHTTPClient()

	// 发起HTTP请求获取汇率数据
	data, err := httpClient.Get(ctx, url)
	if err != nil {
		log.ZError(ctx, "获取汇率数据失败", err, "url", url)
		return
	}

	// 解析响应数据
	var externalResp dto.ExternalExchangeRateResp
	if err := utils.UnmarshalJSON(data, &externalResp); err != nil {
		log.ZError(ctx, "解析汇率数据失败", err)
		return
	}

	// 保存到数据库
	db := plugin.MongoCli().GetDB()
	exchangeRateDao := model.NewExchangeRateDao(db)

	// 创建汇率实体
	exchangeRate := &model.ExchangeRate{
		Base:      externalResp.Base,
		Timestamp: externalResp.Timestamp,
		Rates:     externalResp.Rates,
		UpdatedAt: time.Now().UTC(),
	}

	// 更新或插入数据库
	if err := exchangeRateDao.UpsertLatestRates(ctx, exchangeRate); err != nil {
		log.ZError(ctx, "保存汇率数据失败", err, "base", externalResp.Base)
		return
	}

	log.ZInfo(ctx, "汇率数据已更新", "base", externalResp.Base, "timestamp", externalResp.Timestamp)
}
