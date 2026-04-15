package dto

import "github.com/openimsdk/chat/freechat/utils/paginationUtils"

// AdminLoginRequest 管理员登录请求参数
type AdminLoginRequest struct {
	Account  string `json:"account" binding:"required"`
	Password string `json:"password"`
	Email    string `json:"email" binding:"required,email"` // 邮箱地址，必填，格式验证
	Platform int32  `json:"platform"`
}

// TwoFactorAuthLoginViaEmailReq 二次验证邮箱登录请求
type TwoFactorAuthLoginViaEmailReq struct {
	VerifyCode string `json:"verify_code" binding:"required"`
	Account    string `json:"account" binding:"required"`
	Password   string `json:"password"`
	Email      string `json:"email" binding:"required,email"` // 邮箱地址，必填，格式验证
	Platform   int32  `json:"platform"`
}

// SendEmailVerifyCodeReq 发送邮箱验证码请求
type SendEmailVerifyCodeReq struct {
	Platform int32 `json:"platform"`
}

// SetEmailWithVerifyReq 通过验证码设置邮箱请求
type SetEmailWithVerifyReq struct {
	Email      string `json:"email" binding:"required,email"`       // 邮箱地址，必填，格式验证
	VerifyCode string `json:"verify_code" binding:"required,len=6"` // 验证码，必填，6位数字
}

// SuperAdminForbidUserReq 超管封禁用户请求
type SuperAdminForbidUserReq struct {
	UserID string `json:"user_id" binding:"required"` // 主账户user_id
	Reason string `json:"reason"`                     // 封禁原因
}

// SuperAdminUnforbidUserReq 超管解封用户请求
type SuperAdminUnforbidUserReq struct {
	UserID string `json:"user_id" binding:"required"` // 主账户user_id
}

// SuperAdminSearchForbiddenUsersReq 超管搜索封禁用户请求
type SuperAdminSearchForbiddenUsersReq struct {
	Keyword   string `json:"keyword"`    // 搜索关键词
	StartTime *int64 `json:"start_time"` // 开始时间，秒级时间戳，可选
	EndTime   *int64 `json:"end_time"`   // 结束时间，秒级时间戳，可选
	paginationUtils.DepPagination
}
