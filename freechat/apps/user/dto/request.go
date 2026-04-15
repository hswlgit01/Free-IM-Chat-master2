package dto

// ChangeEmailReq 修改邮箱请求
type ChangeEmailReq struct {
	NewEmail   string `json:"new_email" binding:"required,email"` // 新邮箱地址
	VerifyCode string `json:"verify_code" binding:"required"`     // 验证码
}

// SuperAdminGetAllUsersReq 超级管理员查询系统所有用户请求
type SuperAdminGetAllUsersReq struct {
	Page     int32  `json:"page" form:"page" binding:"min=1"`                    // 页码
	PageSize int32  `json:"page_size" form:"page_size" binding:"min=1,max=1000"` // 每页显示数量
	Keyword  string `json:"keyword" form:"keyword"`                              // 搜索关键字（用户ID、账号）
}

// SuperAdminGetUserDetailReq 超级管理员查询用户详情请求
type SuperAdminGetUserDetailReq struct {
	UserID string `json:"user_id" form:"user_id" binding:"required"` // 用户ID
}

// SuperAdminResetUserPasswordReq 超级管理员重置用户密码请求
type SuperAdminResetUserPasswordReq struct {
	UserID      string `json:"user_id" binding:"required"`      // 用户ID
	NewPassword string `json:"new_password" binding:"required"` // 新密码
}

type GetLoginRecordReq struct {
	UserID string `json:"user_id" binding:"required"`
}
