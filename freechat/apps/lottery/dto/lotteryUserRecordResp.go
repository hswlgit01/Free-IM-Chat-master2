package dto

import (
	"time"

	"github.com/openimsdk/chat/freechat/apps/lottery/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// 用户端抽奖记录响应
type LotteryUserRecordResp struct {
	ID                  primitive.ObjectID `json:"id,omitempty"`
	ImServerUserId      string             `json:"im_server_user_id"`
	LotteryId           primitive.ObjectID `json:"lottery_id"`
	LotteryUserTicketId primitive.ObjectID `json:"lottery_user_ticket_id"`
	WinTime             time.Time          `json:"win_time"`
	IsWin               bool               `json:"is_win"`
	RewardId            primitive.ObjectID `json:"reward_id,omitempty"`
	Status              int                `json:"status"`
	DistributeTime      *time.Time         `json:"distribute_time,omitempty"`
	CreatedAt           time.Time          `json:"created_at"`
	UpdatedAt           time.Time          `json:"updated_at"`

	// 关联的奖品信息
	RewardInfo interface{} `json:"reward_info,omitempty"`
}

// 管理端抽奖记录响应
type AdminLotteryUserRecordResp struct {
	ID                  primitive.ObjectID `json:"id,omitempty"`
	ImServerUserId      string             `json:"im_server_user_id"`
	LotteryId           primitive.ObjectID `json:"lottery_id"`
	LotteryUserTicketId primitive.ObjectID `json:"lottery_user_ticket_id"`
	WinTime             time.Time          `json:"win_time"`
	IsWin               bool               `json:"is_win"`
	RewardId            primitive.ObjectID `json:"reward_id,omitempty"`
	Status              int                `json:"status"`
	DistributeTime      *time.Time         `json:"distribute_time,omitempty"`
	CreatedAt           time.Time          `json:"created_at"`
	UpdatedAt           time.Time          `json:"updated_at"`

	// 关联的用户信息
	UserAttribute interface{} `json:"user_attribute,omitempty"`
	UserInfo      interface{} `json:"user_info,omitempty"`
	RewardInfo    interface{} `json:"reward_info,omitempty"`
}

func NewLotteryUserRecordResp(obj *model.LotteryUserRecord) *LotteryUserRecordResp {
	return &LotteryUserRecordResp{
		ID:                  obj.ID,
		ImServerUserId:      obj.ImServerUserId,
		LotteryId:           obj.LotteryId,
		LotteryUserTicketId: obj.LotteryUserTicketId,
		WinTime:             obj.WinTime,
		IsWin:               obj.IsWin,
		RewardId:            obj.RewardId,
		Status:              obj.Status,
		DistributeTime:      obj.DistributeTime,
		CreatedAt:           obj.CreatedAt,
		UpdatedAt:           obj.UpdatedAt,
	}
}

func NewLotteryUserRecordWithRewardResp(obj *model.LotteryUserRecordWithReward) *LotteryUserRecordResp {
	resp := NewLotteryUserRecordResp(obj.LotteryUserRecord)

	// 提取奖品信息
	if len(obj.Reward) > 0 {
		resp.RewardInfo = obj.Reward[0]
	}

	return resp
}

func NewAdminLotteryUserRecordResp(obj *model.LotteryUserRecordWithUserInfo) *AdminLotteryUserRecordResp {
	resp := &AdminLotteryUserRecordResp{
		ID:                  obj.ID,
		ImServerUserId:      obj.ImServerUserId,
		LotteryId:           obj.LotteryId,
		LotteryUserTicketId: obj.LotteryUserTicketId,
		WinTime:             obj.WinTime,
		IsWin:               obj.IsWin,
		RewardId:            obj.RewardId,
		Status:              obj.Status,
		DistributeTime:      obj.DistributeTime,
		CreatedAt:           obj.CreatedAt,
		UpdatedAt:           obj.UpdatedAt,
	}

	// 提取用户属性信息
	if len(obj.Attribute) > 0 {
		resp.UserAttribute = obj.Attribute[0]
	}

	// 提取IM用户信息
	if len(obj.User) > 0 {
		resp.UserInfo = obj.User[0]
	}

	// 提取奖品信息
	if len(obj.Reward) > 0 {
		resp.RewardInfo = obj.Reward[0]
	}

	return resp
}
