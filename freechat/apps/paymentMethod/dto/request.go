package dto

// CreatePaymentMethodReq 创建支付方式请求
type CreatePaymentMethodReq struct {
	Type        *int32 `json:"type" binding:"required,min=0,max=2"` // 0=银行卡, 1=微信, 2=支付宝
	CardNumber  string `json:"cardNumber"`                          // 银行卡号
	BankName    string `json:"bankName"`                            // 银行名称
	BranchName  string `json:"branchName"`                          // 支行名称
	AccountName string `json:"accountName"`                         // 账户名
	QRCodeURL   string `json:"qrCodeUrl"`                           // 二维码URL
	IsDefault   bool   `json:"isDefault"`                           // 是否设为默认
}

// SetDefaultPaymentMethodReq 设置默认支付方式请求
type SetDefaultPaymentMethodReq struct {
	ID string `uri:"id" binding:"required"` // 支付方式ID
}

// DeletePaymentMethodReq 删除支付方式请求
type DeletePaymentMethodReq struct {
	ID string `uri:"id" binding:"required"` // 支付方式ID
}
