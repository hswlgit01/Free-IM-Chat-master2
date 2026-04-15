package chat

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// 提现状态常量
const (
	WithdrawalStatusPending      = 0 // 待审核
	WithdrawalStatusApproved     = 1 // 已通过
	WithdrawalStatusTransferring = 2 // 打款中
	WithdrawalStatusCompleted    = 3 // 已完成
	WithdrawalStatusRejected     = 4 // 已拒绝
	WithdrawalStatusCancelled    = 5 // 已取消
)

type WithdrawalRecord struct {
	ID              primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	OrderNo         string             `bson:"order_no" json:"orderNo"`                               // 提现订单号
	UserID          string             `bson:"user_id" json:"userId"`                                 // 用户ID
	OrganizationID  string             `bson:"organization_id" json:"organizationId"`                 // 组织ID
	CurrencyID      primitive.ObjectID `bson:"currency_id" json:"currencyId"`                         // 货币ID
	Amount          float64            `bson:"amount" json:"amount"`                                  // 提现金额
	Fee             float64            `bson:"fee" json:"fee"`                                        // 手续费
	ActualAmount    float64            `bson:"actual_amount" json:"actualAmount"`                     // 实际到账金额
	Status          int32              `bson:"status" json:"status"`                                  // 状态: 0-待审核,1-已通过,2-打款中,3-已完成,4-已拒绝,5-已取消
	PaymentMethodID string             `bson:"payment_method_id" json:"paymentMethodId"`              // 收款方式ID
	PaymentType     int32              `bson:"payment_type" json:"paymentType"`                       // 收款方式类型: 0-银行卡,1-微信,2-支付宝
	PaymentInfo     string             `bson:"payment_info" json:"paymentInfo"`                       // 收款账户信息(JSON字符串)
	RejectReason    string             `bson:"reject_reason,omitempty" json:"rejectReason,omitempty"` // 拒绝原因
	ApproverID      string             `bson:"approver_id,omitempty" json:"approverId,omitempty"`     // 审批人ID
	ApproveTime     *time.Time         `bson:"approve_time,omitempty" json:"approveTime,omitempty"`   // 审批时间
	TransferTime    *time.Time         `bson:"transfer_time,omitempty" json:"transferTime,omitempty"` // 打款时间
	CompleteTime    *time.Time         `bson:"complete_time,omitempty" json:"completeTime,omitempty"` // 完成时间
	CreatedAt       time.Time          `bson:"created_at" json:"createdAt"`
	UpdatedAt       time.Time          `bson:"updated_at" json:"updatedAt"`
}

func (WithdrawalRecord) TableName() string {
	return "withdrawal_records"
}

type WithdrawalRecordInterface interface {
	// Create 创建提现记录
	Create(ctx context.Context, record *WithdrawalRecord) error
	// FindByID 根据ID查找提现记录
	FindByID(ctx context.Context, id primitive.ObjectID) (*WithdrawalRecord, error)
	// FindByOrderNo 根据订单号查找提现记录
	FindByOrderNo(ctx context.Context, orderNo string) (*WithdrawalRecord, error)
	// FindByUserID 查找用户的提现记录列表
	FindByUserID(ctx context.Context, userID string, pagination Pagination) ([]*WithdrawalRecord, int64, error)
	// Update 更新提现记录
	Update(ctx context.Context, id primitive.ObjectID, update map[string]any) error
	// UpdateStatus 更新提现状态
	UpdateStatus(ctx context.Context, id primitive.ObjectID, status int32, extra map[string]any) error
	// FindByOrganization 查找组织的提现记录列表(管理员使用)
	FindByOrganization(ctx context.Context, organizationID string, status *int32, keyword string, pagination Pagination) ([]*WithdrawalRecord, int64, error)
	// CountByStatus 统计各状态的提现记录数量
	CountByStatus(ctx context.Context, organizationID string) (map[int32]int64, error)
}

type Pagination struct {
	Page     int
	PageSize int
}
