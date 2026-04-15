package admin

import (
	"context"
	"time"
)

// SuperAdminForbidden 超管封禁用户表
type SuperAdminForbidden struct {
	UserID         string    `bson:"user_id"`
	Reason         string    `bson:"reason"`
	OperatorUserID string    `bson:"operator_user_id"`
	CreateTime     time.Time `bson:"create_time"`
}

func (SuperAdminForbidden) TableName() string {
	return "super_admin_forbidden"
}

type SuperAdminForbiddenInterface interface {
	Take(ctx context.Context, userID string) (*SuperAdminForbidden, error)
}
