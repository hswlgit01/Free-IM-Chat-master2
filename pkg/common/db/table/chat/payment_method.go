package chat

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	PaymentMethodTypeBankCard = 0
	PaymentMethodTypeWechat   = 1
	PaymentMethodTypeAlipay   = 2
)

type PaymentMethod struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID      string             `bson:"user_id" json:"user_id"`
	Type        int32              `bson:"type" json:"type"`
	CardNumber  string             `bson:"card_number,omitempty" json:"cardNumber,omitempty"`
	BankName    string             `bson:"bank_name,omitempty" json:"bankName,omitempty"`
	BranchName  string             `bson:"branch_name,omitempty" json:"branchName,omitempty"`
	AccountName string             `bson:"account_name,omitempty" json:"accountName,omitempty"`
	QRCodeURL   string             `bson:"qr_code_url,omitempty" json:"qrCodeUrl,omitempty"`
	IsDefault   bool               `bson:"is_default" json:"isDefault"`
	CreatedAt   time.Time          `bson:"created_at" json:"createdAt"`
	UpdatedAt   time.Time          `bson:"updated_at" json:"updatedAt"`
}

func (PaymentMethod) TableName() string {
	return "payment_methods"
}

type PaymentMethodInterface interface {
	// Create 创建支付方式
	Create(ctx context.Context, paymentMethod *PaymentMethod) error
	// FindByUserID 查找用户的所有支付方式
	FindByUserID(ctx context.Context, userID string) ([]*PaymentMethod, error)
	// FindByID 根据ID查找支付方式
	FindByID(ctx context.Context, id primitive.ObjectID) (*PaymentMethod, error)
	// Update 更新支付方式
	Update(ctx context.Context, id primitive.ObjectID, update map[string]any) error
	// Delete 删除支付方式
	Delete(ctx context.Context, id primitive.ObjectID) error
	// SetDefault 设置默认支付方式(同时取消其他默认)
	SetDefault(ctx context.Context, userID string, id primitive.ObjectID) error
	// FindDefaultByUserID 查找用户的默认支付方式
	FindDefaultByUserID(ctx context.Context, userID string) (*PaymentMethod, error)
}
