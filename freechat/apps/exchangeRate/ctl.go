package exchangeRate

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/exchangeRate/svc"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/tools/apiresp"
)

// ExchangeRateCtl 汇率控制器
type ExchangeRateCtl struct{}

// NewExchangeRateCtl 创建汇率控制器实例
func NewExchangeRateCtl() *ExchangeRateCtl {
	return &ExchangeRateCtl{}
}

// GetLatestRates 获取最新汇率
func (e *ExchangeRateCtl) GetLatestRates(c *gin.Context) {
	// 获取查询参数base，支持USD和CNY，默认为USD
	base := c.Query("base")
	if base == "" {
		base = "CNY" // 默认基准货币
	}

	// 验证base参数是否为支持的币种
	if base != "USD" && base != "CNY" {
		apiresp.GinError(c, freeErrors.SystemErr(fmt.Errorf("not supported base: %s", base)))
		return
	}

	// 调用服务获取汇率数据
	exchangeRateSvc := svc.NewExchangeRateService()
	resp, err := exchangeRateSvc.GetLatestRates(c, base)
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}

	apiresp.GinSuccess(c, resp)
}
