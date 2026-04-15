package dto

import (
	"github.com/openimsdk/chat/freechat/apps/defaultGroup/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type RegisterAddGroupJoinAllResp struct {
	GroupID   string             `json:"group_id"`
	OrgId     primitive.ObjectID `json:"org_id"`
	CreatedAt int64              `json:"created_at"`
}

func NewRegisterAddGroupJoinAllResp(group *model.DefaultGroup) *RegisterAddGroupJoinAllResp {
	return &RegisterAddGroupJoinAllResp{
		GroupID:   group.GroupID,
		OrgId:     group.OrgId,
		CreatedAt: group.CreatedAt.UnixMilli(),
	}
}
