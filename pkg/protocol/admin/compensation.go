// 补偿金相关的请求和响应结构体定义
package admin

// 补偿金系统设置响应
type CompensationSettingsResp struct {
	Enabled       bool   `json:"enabled"`        // 是否启用
	InitialAmount string `json:"initial_amount"` // 初始补偿金金额
	NoticeText    string `json:"notice_text"`    // 钱包开通时显示的说明文本
}

// 更新补偿金系统设置请求
type UpdateCompensationSettingsReq struct {
	Enabled       bool   `json:"enabled"`        // 是否启用
	InitialAmount string `json:"initial_amount"` // 初始补偿金金额
	NoticeText    string `json:"notice_text"`    // 钱包开通时显示的说明文本
}

// 获取用户补偿金余额请求
type GetUserCompensationBalanceReq struct {
	UserID     string `json:"user_id"`     // 用户ID
	CurrencyID string `json:"currency_id"` // 币种ID
}

// 获取用户补偿金余额响应
type GetUserCompensationBalanceResp struct {
	UserID              string `json:"user_id"`              // 用户ID
	Username            string `json:"username"`             // 用户名
	WalletID            string `json:"wallet_id"`            // 钱包ID
	CurrencyID          string `json:"currency_id"`          // 币种ID
	CurrencyName        string `json:"currency_name"`        // 币种名称
	CompensationBalance string `json:"compensation_balance"` // 补偿金余额
}

// 调整用户补偿金余额请求
type AdjustUserCompensationBalanceReq struct {
	UserID     string `json:"user_id"`     // 用户ID
	CurrencyID string `json:"currency_id"` // 币种ID
	Amount     string `json:"amount"`      // 调整金额
	Reason     string `json:"reason"`      // 调整原因
}
