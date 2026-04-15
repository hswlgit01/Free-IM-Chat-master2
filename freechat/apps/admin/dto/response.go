package dto

import (
	"time"

	"github.com/openimsdk/chat/freechat/apps/admin/model"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
)

// AdminLoginResponse 管理员登录响应
type AdminLoginResponse struct {
	AdminUserID  string `json:"admin_user_id"`
	AdminAccount string `json:"admin_account"`
	AdminToken   string `json:"admin_token"`
	Nickname     string `json:"nickname"`
	FaceURL      string `json:"face_url"`
	Level        int32  `json:"level"`
	ImToken      string `json:"im_token"`
}

// AdminInfoResponse 管理员信息响应
type AdminInfoResponse struct {
	UserID   string `json:"user_id"`  // 用户ID
	Account  string `json:"account"`  // 账号
	Nickname string `json:"nickname"` // 昵称
	FaceURL  string `json:"face_url"` // 头像URL
	Level    int32  `json:"level"`    // 管理员级别
	Email    string `json:"email"`    // 邮箱地址
}

// SuperAdminForbidUserResp 超管封禁用户响应
type SuperAdminForbidUserResp struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// SuperAdminUnforbidUserResp 超管解封用户响应
type SuperAdminUnforbidUserResp struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// SuperAdminForbiddenUserInfo 封禁用户信息
type SuperAdminForbiddenUserInfo struct {
	UserID         string    `json:"user_id"`          // 主账户user_id
	Reason         string    `json:"reason"`           // 封禁原因
	OperatorUserID string    `json:"operator_user_id"` // 操作员ID
	CreateTime     time.Time `json:"create_time"`      // 封禁时间

	// 用户基本信息
	Account     string `json:"account"`      // 账号
	PhoneNumber string `json:"phone_number"` // 手机号
	AreaCode    string `json:"area_code"`    // 区号
	Email       string `json:"email"`        // 邮箱
	Nickname    string `json:"nickname"`     // 昵称
	FaceURL     string `json:"face_url"`     // 头像
	Gender      int32  `json:"gender"`       // 性别
}

// NewSuperAdminForbiddenUserInfo 从model转换为响应DTO
func NewSuperAdminForbiddenUserInfo(forbidden *model.SuperAdminForbiddenWithAttr) *SuperAdminForbiddenUserInfo {
	return &SuperAdminForbiddenUserInfo{
		UserID:         forbidden.UserID,
		Reason:         forbidden.Reason,
		OperatorUserID: forbidden.OperatorUserID,
		CreateTime:     forbidden.CreateTime,
		Account:        forbidden.UserAttr.Account,
		PhoneNumber:    forbidden.UserAttr.PhoneNumber,
		AreaCode:       forbidden.UserAttr.AreaCode,
		Email:          forbidden.UserAttr.Email,
		Nickname:       forbidden.UserAttr.Nickname,
		FaceURL:        forbidden.UserAttr.FaceURL,
		Gender:         forbidden.UserAttr.Gender,
	}
}

// SuperAdminSearchForbiddenUsersResp 超管搜索封禁用户响应
type SuperAdminSearchForbiddenUsersResp = paginationUtils.ListResp[*SuperAdminForbiddenUserInfo]
