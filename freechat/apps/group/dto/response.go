package dto

import (
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"time"
)

// GetGroupsByOrgIDResp 获取群组列表响应
type GetGroupsByOrgIDResp struct {
	Total  int64                `json:"total"`
	Groups []*openImModel.Group `json:"groups"`
}

type GroupResp struct {
	GroupID                string    `json:"group_id"`
	GroupName              string    `json:"group_name"`
	Notification           string    `json:"notification"`
	Introduction           string    `json:"introduction"`
	FaceURL                string    `json:"face_url"`
	CreateTime             time.Time `json:"create_time"`
	Ex                     string    `json:"ex"`
	Status                 int32     `json:"status"`
	CreatorUserID          string    `json:"creator_user_id"`
	GroupType              int32     `json:"group_type"`
	NeedVerification       int32     `json:"need_verification"`
	LookMemberInfo         int32     `json:"look_member_info"`
	ApplyMemberFriend      int32     `json:"apply_member_friend"`
	NotificationUpdateTime time.Time `json:"notification_update_time"`
	NotificationUserID     string    `json:"notification_user_id"`
	OrgID                  string    `json:"org_id"`
	OwnerId                string    `json:"owner_id"`
}

func NewGroupResp(obj *openImModel.Group) *GroupResp {
	resp := &GroupResp{
		GroupID:                obj.GroupID,
		GroupName:              obj.GroupName,
		Notification:           obj.Notification,
		Introduction:           obj.Introduction,
		FaceURL:                obj.FaceURL,
		CreateTime:             obj.CreateTime,
		Ex:                     obj.Ex,
		Status:                 obj.Status,
		CreatorUserID:          obj.CreatorUserID,
		GroupType:              obj.GroupType,
		NeedVerification:       obj.NeedVerification,
		LookMemberInfo:         obj.LookMemberInfo,
		ApplyMemberFriend:      obj.ApplyMemberFriend,
		NotificationUpdateTime: obj.NotificationUpdateTime,
		NotificationUserID:     obj.NotificationUserID,
		OrgID:                  obj.OrgID,
	}
	return resp
}
