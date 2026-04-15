package dto

import (
	chatModel "github.com/openimsdk/chat/freechat/apps/defaultFriend/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type RegisterAddFriendJoinAllResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	ImServerUserId string             `bson:"im_server_user_id" json:"im_server_user_id"`
	OrgId          primitive.ObjectID `bson:"org_id" json:"org_id"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`

	OrganizationUser map[string]interface{} `json:"organization_user"`
	Attribute        interface{}            `json:"attribute"`
	User             interface{}            `json:"user"`
}

func NewRegisterAddFriendJoinAllResp(obj *chatModel.RegisterAddFriendJoinAll) *RegisterAddFriendJoinAllResp {

	res := &RegisterAddFriendJoinAllResp{
		ID:             obj.ID,
		ImServerUserId: obj.ImServerUserId,
		OrgId:          obj.OrgId,
		CreatedAt:      obj.CreatedAt,

		OrganizationUser: obj.OrganizationUser,
		Attribute:        obj.Attribute,
		User:             obj.User,
	}

	return res
}
