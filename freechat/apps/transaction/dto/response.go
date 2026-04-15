package dto

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ReceiveRecordResp 领取记录响应
type ReceiveRecordResp struct {
	TransactionID   string    `json:"transaction_id"`
	ReceiverID      string    `json:"receiver_id"`
	ReceiverIMID    string    `json:"receiver_im_id"`
	Amount          string    `json:"amount"`
	ReceivedAt      time.Time `json:"received_at"`
	TransactionType string    `json:"transaction_type"` // 交易类型
}

// CheckUserReceivedResp 查询用户是否接收了交易响应
type CheckUserReceivedResp struct {
	Received bool   `json:"received"`
	Amount   string `json:"amount,omitempty"` // 如果已接收，则返回接收金额
}

// TransactionReceiveDetailsResp 交易接收详情响应
type TransactionReceiveDetailsResp struct {
	Records        []*ReceiveRecordResp `json:"records"`
	TotalAmount    string               `json:"total_amount"`    // 整个红包的总金额
	ReceivedAmount string               `json:"received_amount"` // 整个红包已领取的总金额
	TotalCount     int                  `json:"total_count"`     // 红包总个数
	ReceivedCount  int                  `json:"received_count"`  // 已领取个数
	Status         string               `json:"status"`          // 红包状态：pending-进行中, completed-已领完, expired-已过期
	TotalRecords   int                  `json:"total_records"`   // 领取记录总条数（用于前端分页）

	// 以下是当前查看该红包的用户自己的领取情况，便于前端更友好展示
	SelfReceived bool   `json:"self_received"` // 当前用户是否已经领取
	SelfAmount   string `json:"self_amount"`   // 当前用户已领取金额（未领取时为 "0"）
}

// QueryUserReceiveHistoryResp 用户24小时内接收交易记录响应
type QueryUserReceiveHistoryResp struct {
	Total   int                  `json:"total"`   // 总记录数
	Records []*ReceiveRecordResp `json:"records"` // 接收记录列表
}

type TransactionOrgListResp struct {
	TransactionID   string               `json:"transaction_id"`   // 交易ID
	Status          int                  `json:"status"`           // 交易状态
	Currency        string               `json:"currency"`         // 币种
	Greeting        string               `json:"greeting"`         // 交易信息
	TotalAmount     primitive.Decimal128 `json:"total_amount"`     // 交易金额
	TotalCount      int                  `json:"total_count"`      // 交易个数
	RemainingAmount primitive.Decimal128 `json:"remaining_amount"` // 剩余金额
	RemainingCount  int                  `json:"remaining_count"`  // 剩余个数
	Sender          string               `json:"sender"`           // 发送者
	Receiver        string               `json:"receiver"`         // 接收者
	CreatedAt       time.Time            `json:"created_at"`       // 创建时间
}

// TransactionRecordWithUserInfo 包含用户信息的交易记录
type TransactionRecordWithUserInfo struct {
	TransactionID   string               `json:"transaction_id"`   // 交易ID
	Status          int                  `json:"status"`           // 交易状态
	TransactionType int                  `json:"transaction_type"` // 交易类型
	TotalAmount     string               `json:"total_amount"`     // 总金额
	RemainingAmount string               `json:"remaining_amount"` // 剩余金额
	TotalCount      int                  `json:"total_count"`      // 总个数
	RemainingCount  int                  `json:"remaining_count"`  // 剩余个数
	Greeting        string               `json:"greeting"`         // 交易备注/祝福语
	CreatedAt       time.Time            `json:"created_at"`       // 创建时间
	UpdatedAt       time.Time            `json:"updated_at"`       // 更新时间
	ExpireAt        time.Time            `json:"expire_at"`        // 到期时间
	Currency        string               `json:"currency"`         // 币种名称
	Sender          *TransactionUserInfo `json:"sender"`           // 发送者信息
	Receiver        *TransactionUserInfo `json:"receiver"`         // 接收者信息
}

// TransactionUserInfo 交易用户信息
type TransactionUserInfo struct {
	UserID     string `json:"user_id"`      // 用户ID
	ImServerID string `json:"im_server_id"` // IM服务器用户ID
	Nickname   string `json:"nickname"`     // 昵称
	Account    string `json:"account"`      // 账户
	FaceURL    string `json:"face_url"`     // 头像URL
}

// TransactionRecordQueryResp 交易记录查询响应
type TransactionRecordQueryResp struct {
	Total int                              `json:"total"`
	List  []*TransactionRecordWithUserInfo `json:"list"`
}

// ReceiveRecordWithUserInfo 包含用户信息的领取记录
type ReceiveRecordWithUserInfo struct {
	TransactionID   string               `json:"transaction_id"`   // 交易ID
	TransactionType int                  `json:"transaction_type"` // 交易类型
	Amount          string               `json:"amount"`           // 领取金额
	ReceivedAt      time.Time            `json:"received_at"`      // 领取时间
	Sender          *TransactionUserInfo `json:"sender"`           // 发送者信息
	Receiver        *TransactionUserInfo `json:"receiver"`         // 领取者信息
	Currency        string               `json:"currency"`         // 币种名称
}

// ReceiveRecordQueryResp 用户领取详情查询响应
type ReceiveRecordQueryResp struct {
	Total int                          `json:"total"`
	List  []*ReceiveRecordWithUserInfo `json:"list"`
}
