package dto

// ExchangeRateResp 汇率响应
type ExchangeRateResp struct {
	Base      string             `json:"base"`      // 基准货币
	Timestamp int64              `json:"timestamp"` // 时间戳
	Rates     map[string]float64 `json:"rates"`     // 汇率映射
}

// ExternalExchangeRateResp 外部汇率API的响应结构
type ExternalExchangeRateResp struct {
	Disclaimer string             `json:"terms_of_use"`
	License    string             `json:"documentation"`
	Timestamp  int64              `json:"time_last_update_unix"`
	Base       string             `json:"base_code"`
	Rates      map[string]float64 `json:"conversion_rates"`
}
