// Copyright © 2023 OpenIM open source community. All rights reserved.

package dto

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// HierarchyTreeNode represents a node in the hierarchy tree
type HierarchyTreeNode struct {
	UserID              string               `json:"user_id"`
	Account             string               `json:"account"` // 用户账号
	Nickname            string               `json:"nickname"`
	FaceURL             string               `json:"face_url"`
	Level               int                  `json:"level"`
	TeamSize            int                  `json:"team_size"`
	DirectDownlineCount int                  `json:"direct_downline_count"`
	Children            []*HierarchyTreeNode `json:"children,omitempty"`
	HasMoreChildren     bool                 `json:"has_more_children"`
	UserType            string               `json:"user_type,omitempty"`
}

// HierarchyTreeResp represents the response for the hierarchy tree API
type HierarchyTreeResp struct {
	Root *HierarchyTreeNode `json:"root"`
}

// UserHierarchyInfo represents basic hierarchy information for a user
type UserHierarchyInfo struct {
	UserID              string    `json:"user_id"`
	Nickname            string    `json:"nickname"`
	Account             string    `json:"account"` // 用户账号
	FaceURL             string    `json:"face_url"`
	Level               int       `json:"level"`
	InvitationCode      string    `json:"invitation_code"`
	TeamSize            int       `json:"team_size"`
	DirectDownlineCount int       `json:"direct_downline_count"`
	AncestorPath        []string  `json:"ancestor_path"`
	CreatedAt           time.Time `json:"created_at"`
	UserType            string    `json:"user_type,omitempty"`
	// 祖先信息，用于前端展示
	AncestorInfoList []AncestorInfo `json:"ancestor_info_list,omitempty"`
}

// AncestorInfo 表示祖先节点的账号和昵称信息
type AncestorInfo struct {
	UserID   string `json:"user_id"`
	Account  string `json:"account"`
	Nickname string `json:"nickname"`
	Level    int    `json:"level"`
}

// HierarchyDetailResp represents the response for the hierarchy detail API
type HierarchyDetailResp struct {
	User           UserHierarchyInfo   `json:"user"`
	Level1Parent   *UserHierarchyInfo  `json:"level1_parent,omitempty"`
	Level2Parent   *UserHierarchyInfo  `json:"level2_parent,omitempty"`
	Level3Parent   *UserHierarchyInfo  `json:"level3_parent,omitempty"`
	DirectChildren []UserHierarchyInfo `json:"direct_children"`
	TotalChildren  int                 `json:"total_children"`
}

// HierarchyChildrenResp represents the response for the hierarchy children API
type HierarchyChildrenResp struct {
	Children []UserHierarchyInfo `json:"children"`
	Total    int64               `json:"total"`
}

// SearchHierarchyReq represents the request for searching users in the hierarchy
type SearchHierarchyReq struct {
	Keyword         string `json:"keyword"`
	Level           int    `json:"level"`
	AncestorID      string `json:"ancestor_id"`
	Page            int    `json:"page"`
	PageSize        int    `json:"page_size"`
	SortByField     string `json:"sort_by_field"`
	SortOrder       string `json:"sort_order"`
	IncludeOrgNodes bool   `json:"include_org_nodes"` // 是否包含组织虚拟节点
}

// SearchHierarchyResp represents the response for searching users in the hierarchy
type SearchHierarchyResp struct {
	Users []UserHierarchyInfo `json:"users"`
	Total int64               `json:"total"`
}

// HierarchyPanelSearchUser 管理后台层级搜索列表项（仅返回面板展示所需字段）
type HierarchyPanelSearchUser struct {
	UserID              string `json:"user_id"`
	Account             string `json:"account"`
	Nickname            string `json:"nickname"`
	FaceURL             string `json:"face_url"`
	InvitationCode      string `json:"invitation_code"`
	Level               int    `json:"level"`
	TeamSize            int    `json:"team_size"`
	DirectDownlineCount int    `json:"direct_downline_count"`
}

// SearchHierarchyPanelResp POST /hierarchy/search_panel 响应
type SearchHierarchyPanelResp struct {
	Users []HierarchyPanelSearchUser `json:"users"`
	Total int64                      `json:"total"`
}

// HierarchyRepairReport represents the report for hierarchy repair operation
type HierarchyRepairReport struct {
	TotalUsers        int                `json:"total_users"`
	ProcessedUsers    int                `json:"processed_users"`
	FixedUsers        int                `json:"fixed_users"`
	ErrorCount        int                `json:"error_count"`
	StartTime         string             `json:"start_time"`
	EndTime           string             `json:"end_time"`
	DurationInSeconds int                `json:"duration_in_seconds"`
	OrganizationID    primitive.ObjectID `json:"organization_id"`
	OperationID       string             `json:"operation_id"`
	Message           string             `json:"message,omitempty"` // 可选消息字段，用于提供额外信息
}
