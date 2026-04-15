package dto

import "time"

// PaymentMethodResp 支付方式响应
type PaymentMethodResp struct {
	ID          string    `json:"id"`                    // 支付方式ID
	Type        int32     `json:"type"`                  // 0=银行卡, 1=微信, 2=支付宝
	CardNumber  string    `json:"cardNumber,omitempty"`  // 银行卡号
	BankName    string    `json:"bankName,omitempty"`    // 银行名称
	BranchName  string    `json:"branchName,omitempty"`  // 支行名称
	AccountName string    `json:"accountName,omitempty"` // 账户名
	QRCodeURL   string    `json:"qrCodeUrl,omitempty"`   // 二维码URL
	IsDefault   bool      `json:"isDefault"`             // 是否默认
	CreatedAt   time.Time `json:"createdAt"`             // 创建时间
	UpdatedAt   time.Time `json:"updatedAt"`             // 更新时间
}

// GetPaymentMethodsResp 获取支付方式列表响应
type GetPaymentMethodsResp struct {
	PaymentMethods []*PaymentMethodResp `json:"data"` // 支付方式列表
}
