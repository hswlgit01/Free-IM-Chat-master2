package dto

import (
	"github.com/openimsdk/chat/freechat/apps/lottery/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type LotteryRewardResp struct {
	ID       primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"` // 奖品id
	Name     string             `bson:"name" json:"name"`                  // 奖品名称
	Entity   *bool              `bson:"entity" bson:"entity"`              // 实体奖品
	Img      string             `bson:"img" json:"img"`                    // 奖品图片
	Type     string             `bson:"type" json:"type"`                  // 奖品类型
	Remark   string             `bson:"remark" json:"remark"`              // 奖品描述
	OrgId    primitive.ObjectID `bson:"org_id" json:"org_id"`              // 组织id
	CreateAt time.Time          `bson:"create_at" json:"create_at"`        // 创建时间
	Status   int                `bson:"status" json:"status"`              // 奖品状态 0 禁用 1 启用
}

func NewLotteryRewardResp(obj *model.LotteryReward) *LotteryRewardResp {
	return &LotteryRewardResp{
		ID:       obj.ID,
		Name:     obj.Name,
		Entity:   obj.Entity,
		Img:      obj.Img,
		Type:     obj.Type,
		Remark:   obj.Remark,
		OrgId:    obj.OrgId,
		CreateAt: obj.CreateAt,
		Status:   obj.Status,
	}
}
