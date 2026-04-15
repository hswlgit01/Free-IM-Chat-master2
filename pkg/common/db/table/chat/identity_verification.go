package chat

import (
	"context"
	"time"

	"github.com/openimsdk/chat/tools/db/pagination"
)

// IdentityVerification 身份认证表
type IdentityVerification struct {
	UserID       string    `bson:"user_id" json:"userID"`              // 用户ID（主键）
	Status       int32     `bson:"status" json:"status"`               // 认证状态 0-待认证 1-审核中 2-已认证 3-已拒绝
	RealName     string    `bson:"real_name" json:"realName"`          // 真实姓名
	IDCardNumber string    `bson:"id_card_number" json:"idCardNumber"` // 身份证号
	IDCardFront  string    `bson:"id_card_front" json:"idCardFront"`   // 身份证正面URL
	IDCardBack   string    `bson:"id_card_back" json:"idCardBack"`     // 身份证反面URL
	RejectReason string    `bson:"reject_reason" json:"rejectReason"`  // 拒绝原因
	ApplyTime    time.Time `bson:"apply_time" json:"applyTime"`        // 申请时间
	VerifyTime   time.Time `bson:"verify_time" json:"verifyTime"`      // 审核时间
	VerifyAdmin  string    `bson:"verify_admin" json:"verifyAdmin"`    // 审核管理员ID
	CreateTime   time.Time `bson:"create_time" json:"createTime"`      // 创建时间
	UpdateTime   time.Time `bson:"update_time" json:"updateTime"`      // 更新时间
}

func (IdentityVerification) TableName() string {
	return "identity_verifications"
}

// IdentityVerificationInterface 身份认证数据库接口
type IdentityVerificationInterface interface {
	// 创建认证记录
	Create(ctx context.Context, identity *IdentityVerification) error
	// 更新认证记录
	Update(ctx context.Context, userID string, data map[string]any) error
	// 根据用户ID获取认证信息
	Take(ctx context.Context, userID string) (*IdentityVerification, error)
	// 根据状态获取认证列表（支持keyword搜索）
	FindByStatus(ctx context.Context, status *int32, keyword string, pagination pagination.Pagination) (int64, []*IdentityVerification, error)
	// 根据状态和组织获取认证列表（支持keyword搜索、时间范围过滤和排序）
	FindByStatusAndOrg(ctx context.Context, status *int32, keyword string, orgID string, pagination pagination.Pagination,
		orderKey string, orderDirection string, startTime, endTime, verifyStartTime, verifyEndTime int64) (int64, []*IdentityVerification, error)
	// 审核通过
	Approve(ctx context.Context, userID string, adminID string) error
	// 审核拒绝
	Reject(ctx context.Context, userID string, adminID string, reason string) error
}
