package dto

import (
	"strconv"
	"time"

	"github.com/openimsdk/chat/freechat/apps/organization/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// CreateUserTagReq 创建标签请求
type CreateUserTagReq struct {
	TagName     string `json:"tag_name" binding:"required"`
	Description string `json:"description"`
}

// UpdateUserTagReq 更新标签请求
type UpdateUserTagReq struct {
	TagId       primitive.ObjectID `json:"tag_id" binding:"required"`
	TagName     string             `json:"tag_name" binding:"required"`
	Description string             `json:"description"`
}

// UserTagResp 标签响应
type UserTagResp struct {
	ID             primitive.ObjectID `json:"id"`
	OrganizationId primitive.ObjectID `json:"organization_id"`
	TagName        string             `json:"tag_name"`
	Description    string             `json:"description"`
	CreatedAt      time.Time          `json:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

// NewUserTagResp 创建标签响应
func NewUserTagResp(tag *model.UserTag) *UserTagResp {
	return &UserTagResp{
		ID:             tag.ID,
		OrganizationId: tag.OrganizationId,
		TagName:        tag.TagName,
		Description:    tag.Description,
		CreatedAt:      tag.CreatedAt,
		UpdatedAt:      tag.UpdatedAt,
	}
}

// AssignUserTagsReq 给用户打标签请求
type AssignUserTagsReq struct {
	ImUserSeverID string               `json:"im_user_server_id" binding:"required"`
	TagIds        []primitive.ObjectID `json:"tag_ids" binding:"required"`
}

// RemoveUserTagsReq 删除用户标签请求
type RemoveUserTagsReq struct {
	ImUserSeverID string               `json:"im_user_server_id" binding:"required"`
	TagIds        []primitive.ObjectID `json:"tag_ids" binding:"required"`
}

// GetOrgUserReq 查询组织用户请求（新版POST接口）
type GetOrgUserReq struct {
	Keyword        string                         `json:"keyword"`
	Account        string                         `json:"account"` // 精准账号查询（与 keyword 二选一；空或仅空白则忽略，走 keyword 等条件）
	UserIds        []string                       `json:"user_ids"`
	Roles          []model.OrganizationUserRole   `json:"roles"`
	Status         []model.OrganizationUserStatus `json:"status"`
	TagIds         []primitive.ObjectID           `json:"tag_ids"`           // 标签筛选
	CanSendFreeMsg *int32                         `json:"can_send_free_msg"` // 新增：0=普通用户需好友验证，1=可跳过消息验证，nil=不过滤
	StartTime      string                         `json:"start_time"`        // 起始时间，支持空字符串
	EndTime        string                         `json:"end_time"`          // 结束时间，支持空字符串
	LoginIP        string                         `json:"login_ip"`          // 按最近登录 IP 子串筛选（与列表展示列一致）
	OmitWallet     bool                           `json:"omit_wallet"`       // 为 true 时不查钱包/补偿金，列表走轻量；前端可再调 wallet_snapshot 按需合并
	OrderKey       string                         `json:"order_key"`         // 排序字段
	OrderDirection string                         `json:"order_direction"`   // 排序方向: asc/desc
	Page           int                            `json:"page"`
	PageSize       int                            `json:"page_size"`
}

// ParseStartTime 解析起始时间，空字符串返回nil，支持秒级时间戳和RFC3339格式
func (req *GetOrgUserReq) ParseStartTime() *time.Time {
	if req.StartTime == "" {
		return nil
	}

	// 尝试解析为秒级时间戳
	if timestamp, err := strconv.ParseInt(req.StartTime, 10, 64); err == nil {
		t := time.Unix(timestamp, 0).UTC()
		return &t
	}

	// 尝试解析为RFC3339格式
	if t, err := time.Parse(time.RFC3339, req.StartTime); err == nil {
		return &t
	}

	return nil
}

// ParseEndTime 解析结束时间，空字符串返回nil，支持秒级时间戳和RFC3339格式
func (req *GetOrgUserReq) ParseEndTime() *time.Time {
	if req.EndTime == "" {
		return nil
	}

	// 尝试解析为秒级时间戳
	if timestamp, err := strconv.ParseInt(req.EndTime, 10, 64); err == nil {
		t := time.Unix(timestamp, 0).UTC()
		return &t
	}

	// 尝试解析为RFC3339格式
	if t, err := time.Parse(time.RFC3339, req.EndTime); err == nil {
		return &t
	}

	return nil
}

// OrgUserWalletSnapshotReq 批量拉取当前页用户钱包展示数据（须同属本组织，单次最多 200 个 user_id）
type OrgUserWalletSnapshotReq struct {
	UserIDs []string `json:"user_ids" binding:"required"`
}

// OrgUserWalletSnapshotItem 单用户钱包快照
type OrgUserWalletSnapshotItem struct {
	UserID              string               `json:"user_id"`
	WalletBalances      []*WalletBalanceInfo `json:"wallet_balances"`
	CompensationBalance string               `json:"compensation_balance,omitempty"`
}

// OrgUserWalletSnapshotResp wallet_snapshot 响应
type OrgUserWalletSnapshotResp struct {
	List []*OrgUserWalletSnapshotItem `json:"list"`
}
