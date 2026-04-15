// Copyright © 2023 OpenIM open source community. All rights reserved.

package dto

// TeamInfoResp 表示用户的团队信息响应
type TeamInfoResp struct {
	UserID              string `json:"user_id"`               // 用户ID
	TeamSize            int    `json:"team_size"`             // 团队总人数
	DirectDownlineCount int    `json:"direct_downline_count"` // 直接下线数量
	InvitationCode      string `json:"invitation_code"`       // 邀请码
}
