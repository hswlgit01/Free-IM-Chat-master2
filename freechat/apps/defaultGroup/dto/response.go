package dto

import (
	"github.com/openimsdk/chat/freechat/apps/defaultGroup/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type RegisterAddGroupJoinAllResp struct {
	ID        string             `json:"id"`
	GroupID   string             `json:"group_id"`
	GroupName string             `json:"group_name"`
	OrgId     primitive.ObjectID `json:"org_id"`
	// dawn 2026-06-14 默认群后台展示所属二级业务员信息。
	SalespersonUserID         string `json:"salesperson_user_id"`
	SalespersonNickname       string `json:"salesperson_nickname"`
	SalespersonAccount        string `json:"salesperson_account"`
	SalespersonImServerUserID string `json:"salesperson_im_server_user_id"`
	CreatedAt                 int64  `json:"created_at"`
}

func NewRegisterAddGroupJoinAllResp(group *model.DefaultGroup) *RegisterAddGroupJoinAllResp {
	return &RegisterAddGroupJoinAllResp{
		ID:                group.ID.Hex(),
		GroupID:           group.GroupID,
		OrgId:             group.OrgId,
		SalespersonUserID: group.SalespersonUserID,
		CreatedAt:         group.CreatedAt.UnixMilli(),
	}
}
