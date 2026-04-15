package dto

import "time"

// GetWithdrawalRuleResp 获取提现规则响应
type GetWithdrawalRuleResp struct {
	IsEnabled       bool    `json:"isEnabled"`       // 是否启用提现功能
	MinAmount       float64 `json:"minAmount"`       // 最小提现金额
	MaxAmount       float64 `json:"maxAmount"`       // 最大提现金额
	FeeFixed        float64 `json:"feeFixed"`        // 固定手续费
	FeeRate         float64 `json:"feeRate"`         // 手续费率(百分比)
	NeedRealName    bool    `json:"needRealName"`    // 是否需要实名认证
	NeedBindAccount bool    `json:"needBindAccount"` // 是否需要绑定收款账户
}

// SubmitWithdrawalReq 提交提现申请请求
type SubmitWithdrawalReq struct {
	Amount          float64 `json:"amount" binding:"required,gt=0"`       // 提现金额
	PaymentMethodID string  `json:"paymentMethodId" binding:"required"`   // 收款方式ID
	PayPassword     string  `json:"payPassword" binding:"required,len=6"` // 支付密码
	CurrencyID      string  `json:"currencyId"`                           // 币种ID(可选,如果不提供则自动选择)
}

// SubmitWithdrawalResp 提交提现申请响应
type SubmitWithdrawalResp struct {
	OrderNo      string    `json:"orderNo"`      // 提现订单号
	Amount       float64   `json:"amount"`       // 提现金额
	Fee          float64   `json:"fee"`          // 手续费
	ActualAmount float64   `json:"actualAmount"` // 实际到账金额
	Status       int32     `json:"status"`       // 状态
	CreatedAt    time.Time `json:"createdAt"`    // 创建时间
}

// GetWithdrawalRecordListReq 获取提现记录列表请求
type GetWithdrawalRecordListReq struct {
	Page     int `form:"page" binding:"required,min=1"`
	PageSize int `form:"pageSize" binding:"required,min=1,max=100"`
}

// WithdrawalRecordResp 提现记录响应
type WithdrawalRecordResp struct {
	ID                string     `json:"id"`                     // 记录ID
	OrderNo           string     `json:"orderNo"`                // 提现订单号
	UserID            string     `json:"userId"`                 // 用户ID
	UserAccount       string     `json:"userAccount"`            // 用户账号
	Nickname          string     `json:"nickname"`               // 用户昵称（与 attribute 表 nickname 一致）
	CurrencyID        string     `json:"currencyId"`             // 币种ID
	CurrencyName      string     `json:"currencyName"`           // 币种名称（USD, USDT等）
	CurrencySymbol    string     `json:"currencySymbol"`         // 币种符号（$, ¥等）
	ExchangeRate      float64    `json:"exchangeRate"`           // 兑人民币汇率
	Amount            float64    `json:"amount"`                 // 提现金额（原币种）
	AmountInCNY       float64    `json:"amountInCny"`            // 人民币金额
	Fee               float64    `json:"fee"`                    // 手续费（人民币）
	ActualAmount      float64    `json:"actualAmount"`           // 实际到账（原币种）
	ActualAmountInCNY float64    `json:"actualAmountInCny"`      // 实际到账人民币金额
	Status            int32      `json:"status"`                 // 状态
	PaymentType       int32      `json:"paymentType"`            // 收款方式类型
	PaymentInfo       string     `json:"paymentInfo"`            // 收款账户信息
	RejectReason      string     `json:"rejectReason,omitempty"` // 拒绝原因
	ApproveTime       *time.Time `json:"approveTime,omitempty"`  // 审批时间
	TransferTime      *time.Time `json:"transferTime,omitempty"` // 打款时间
	CompleteTime      *time.Time `json:"completeTime,omitempty"` // 完成时间
	CreatedAt         time.Time  `json:"createdAt"`              // 创建时间
}

// GetWithdrawalRecordListResp 获取提现记录列表响应
type GetWithdrawalRecordListResp struct {
	Total   int64                   `json:"total"`   // 总数
	Records []*WithdrawalRecordResp `json:"records"` // 记录列表
}

// CancelWithdrawalReq 取消提现请求
type CancelWithdrawalReq struct {
	OrderNo string `json:"orderNo" binding:"required"` // 提现订单号
}

// AdminGetWithdrawalListReq 管理员获取提现列表请求
type AdminGetWithdrawalListReq struct {
	Page     int    `form:"page" binding:"required,min=1"`
	PageSize int    `form:"pageSize" binding:"required,min=1,max=100"`
	Status   *int32 `form:"status"`  // 状态筛选(可选)
	Keyword  string `form:"keyword"` // 关键词搜索(订单号/用户账号/用户ID)
}

// AdminGetWithdrawalListResp 管理员获取提现列表响应
type AdminGetWithdrawalListResp struct {
	Total   int64                   `json:"total"`   // 总数
	Records []*WithdrawalRecordResp `json:"records"` // 记录列表
}

// AdminApproveWithdrawalReq 管理员审批提现请求
type AdminApproveWithdrawalReq struct {
	ID string `json:"id" binding:"required"` // 提现记录ID
}

// AdminRejectWithdrawalReq 管理员拒绝提现请求
type AdminRejectWithdrawalReq struct {
	ID     string `json:"id" binding:"required"`     // 提现记录ID
	Reason string `json:"reason" binding:"required"` // 拒绝原因
}

// AdminTransferWithdrawalReq 管理员确认打款请求
type AdminTransferWithdrawalReq struct {
	ID string `json:"id" binding:"required"` // 提现记录ID
}

// AdminCompleteWithdrawalReq 管理员确认完成请求
type AdminCompleteWithdrawalReq struct {
	ID string `json:"id" binding:"required"` // 提现记录ID
}

// AdminBatchApproveReq 管理员批量审批请求
type AdminBatchApproveReq struct {
	IDs []string `json:"ids" binding:"required,min=1"` // 提现记录ID列表
}

// SaveWithdrawalRuleReq 保存提现规则请求(管理员使用)
type SaveWithdrawalRuleReq struct {
	IsEnabled       bool    `json:"isEnabled"`                          // 是否启用提现功能
	MinAmount       float64 `json:"minAmount" binding:"required,gte=0"` // 最小提现金额
	MaxAmount       float64 `json:"maxAmount" binding:"required,gt=0"`  // 最大提现金额
	FeeFixed        float64 `json:"feeFixed" binding:"gte=0"`           // 固定手续费
	FeeRate         float64 `json:"feeRate" binding:"gte=0,lte=100"`    // 手续费率(百分比)
	NeedRealName    bool    `json:"needRealName"`                       // 是否需要实名认证
	NeedBindAccount bool    `json:"needBindAccount"`                    // 是否需要绑定收款账户
}

// CheckPendingWithdrawalResp 检查未处理提现响应
type CheckPendingWithdrawalResp struct {
	HasPending        bool                   `json:"hasPending"`        // 是否有未处理提现
	PendingWithdrawal *PendingWithdrawalInfo `json:"pendingWithdrawal"` // 未处理提现信息
}

// PendingWithdrawalInfo 未处理提现信息
type PendingWithdrawalInfo struct {
	OrderNo        string  `json:"orderNo"`        // 订单号
	CurrencyName   string  `json:"currencyName"`   // 币种名称
	CurrencySymbol string  `json:"currencySymbol"` // 币种符号
	Amount         float64 `json:"amount"`         // 提现金额
	Status         int32   `json:"status"`         // 状态
	StatusText     string  `json:"statusText"`     // 状态文本
	CreatedAt      int64   `json:"createdAt"`      // 创建时间（时间戳，毫秒）
}
