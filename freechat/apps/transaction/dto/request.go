package dto

import (
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// CreateTransactionReq 创建交易请求
type CreateTransactionReq struct {
	SenderID            string             `json:"sender_id"`
	OrgID               string             `json:"org_id"`
	TargetID            string             `json:"target_id" binding:"required"`    // 目标ID (群组ID或用户ID)
	ExclusiveReceiverID string             `json:"exclusive_receiver_id,omitempty"` // 专属接收者ID（群组专属红包）
	TransactionType     int                `json:"transaction_type"`
	TotalAmount         string             `json:"total_amount" binding:"required"`
	TotalCount          int                `json:"total_count" binding:"required,min=1"`
	Greeting            string             `json:"greeting,omitempty"` // 红包祝福语
	Password            string             `json:"password"`
	PayPassword         string             `json:"pay_password"` // 支付密码
	CurrencyId          primitive.ObjectID `json:"currency_id" binding:"required"`
	WalletInfoOwnerType string             `json:"wallet_info_owner_type"`
}

// ReceiveTransactionReq 领取交易请求
type ReceiveTransactionReq struct {
	TransactionID string `json:"transaction_id" binding:"required"`
	ReceiverID    string `json:"receiver_id"`
	OrgID         string `json:"org_id"`
	Password      string `json:"password,omitempty"` // 口令（仅口令红包使用）
}

// QueryTransactionReq 查询交易请求
type QueryTransactionReq struct {
	TransactionID string `json:"transaction_id" binding:"required"`
}

// CheckUserReceivedReq 查询用户是否接收了交易请求
type CheckUserReceivedReq struct {
	TransactionID string `json:"transaction_id" binding:"required"`
	UserID        string `json:"user_id"`
}

// QueryTransactionReceiveDetailsReq 查询交易接收详情请求
type QueryTransactionReceiveDetailsReq struct {
	TransactionID string `form:"transaction_id" binding:"required"`
	// OpUserImID 可选，由 ctl 从请求头 X-User-IM-ID 注入；当 token 中无操作人 ID 时用此值查当前用户领取记录，避免重装/多端后显示 0.00
	OpUserImID string `form:"-" json:"-"`

	// 可选分页参数：不传则返回全部记录，传入则只返回指定页
	PageNum  int `form:"page_num"  binding:"omitempty,min=1"`
	PageSize int `form:"page_size" binding:"omitempty,min=1,max=200"`
}

// ListTransactionsReq 查询交易列表请求
type ListTransactionsReq struct {
	UserID    string `json:"user_id"`
	StartTime *int64 `json:"start_time,omitempty"`
	EndTime   *int64 `json:"end_time,omitempty"`
	PageNum   int    `json:"page_num" binding:"required,min=1"`
	PageSize  int    `json:"page_size" binding:"required,min=1,max=100"`
}

// QueryUserReceiveHistoryReq 查询用户24小时内接收交易记录请求
type QueryUserReceiveHistoryReq struct {
	UserID string `form:"user_id"` // 用户ID
	OrgID  string `json:"org_id"`
}

// CheckTransactionCompletedReq 检查交易是否已完成请求
type CheckTransactionCompletedReq struct {
	TransactionID string `json:"transaction_id" binding:"required"` // 交易ID
	UserID        string `json:"user_id"`                           // 用户ID，用于检查是否领取过
}

type ListOrgTransactions struct {
	OrgID  string `json:"org_id"` // 组织ID
	Status *int   `json:"status"` // 状态
	ListTransactionsReq
}

// TransactionRecordQueryReq 交易记录查询请求
type TransactionRecordQueryReq struct {
	Keyword         string `json:"keyword,omitempty"`          // 发送人搜索关键词（account，昵称，用户imserverID）
	StartTime       string `json:"start_time,omitempty"`       // 发送开始时间
	EndTime         string `json:"end_time,omitempty"`         // 发送结束时间
	Status          *int   `json:"status,omitempty"`           // 交易状态
	TransactionType *int   `json:"transaction_type,omitempty"` // 交易类型过滤（可选）
	paginationUtils.DepPagination
}

// ReceiveRecordQueryReq 用户领取详情查询请求
type ReceiveRecordQueryReq struct {
	SenderKeyword   string `json:"sender_keyword,omitempty"`   // 发送人搜索关键词（account，昵称，用户imserverID）
	ReceiverKeyword string `json:"receiver_keyword,omitempty"` // 领取人搜索关键词（account，昵称，用户imserverID）
	StartTime       string `json:"start_time,omitempty"`       // 领取开始时间
	EndTime         string `json:"end_time,omitempty"`         // 领取结束时间
	TransactionID   string `json:"transaction_id,omitempty"`   // 交易ID精准匹配查询（可选）
	TransactionType *int   `json:"transaction_type,omitempty"` // 交易类型过滤（可选）
	paginationUtils.DepPagination
}
