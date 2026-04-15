package chat

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type WithdrawalRule struct {
	ID              primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	OrganizationID  string             `bson:"organization_id" json:"organizationId"`
	IsEnabled       bool               `bson:"is_enabled" json:"isEnabled"`              // 是否启用提现功能
	MinAmount       float64            `bson:"min_amount" json:"minAmount"`              // 最小提现金额
	MaxAmount       float64            `bson:"max_amount" json:"maxAmount"`              // 最大提现金额
	FeeFixed        float64            `bson:"fee_fixed" json:"feeFixed"`                // 固定手续费
	FeeRate         float64            `bson:"fee_rate" json:"feeRate"`                  // 手续费率(百分比)
	NeedRealName    bool               `bson:"need_real_name" json:"needRealName"`       // 是否需要实名认证
	NeedBindAccount bool               `bson:"need_bind_account" json:"needBindAccount"` // 是否需要绑定收款账户
	CreatedAt       time.Time          `bson:"created_at" json:"createdAt"`
	UpdatedAt       time.Time          `bson:"updated_at" json:"updatedAt"`
}

func (WithdrawalRule) TableName() string {
	return "withdrawal_rules"
}

type WithdrawalRuleInterface interface {
	// Create 创建提现规则
	Create(ctx context.Context, rule *WithdrawalRule) error
	// FindByOrganizationID 根据组织ID查找提现规则
	FindByOrganizationID(ctx context.Context, organizationID string) (*WithdrawalRule, error)
	// Update 更新提现规则
	Update(ctx context.Context, organizationID string, update map[string]any) error
	// Upsert 创建或更新提现规则
	Upsert(ctx context.Context, rule *WithdrawalRule) error
}
