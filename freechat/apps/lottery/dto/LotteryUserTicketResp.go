package dto

import (
	"context"
	"github.com/openimsdk/chat/freechat/apps/lottery/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type LotteryUserTicketResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	ImServerUserId string             `bson:"im_server_user_id" json:"im_server_user_id"`
	LotteryId      primitive.ObjectID `bson:"lottery_id" json:"lottery_id"`

	Use    bool      `bson:"use" json:"use"`         // 是否已使用
	UsedAt time.Time `bson:"used_at" json:"used_at"` // 使用时间

	ExpiredAt time.Time `bson:"expired_at" json:"expired_at"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`

	LotteryInfo *LotterySimpleResp `json:"lottery_info"`
}

func NewLotteryUserTicketResp(db *mongo.Database, obj *model.LotteryUserTicket) (*LotteryUserTicketResp, error) {
	lotteryDao := model.NewLotteryDao(db)

	lottery, err := lotteryDao.GetById(context.Background(), obj.LotteryId)
	if err != nil {
		return nil, err
	}

	lotteryInfo := NewLotterySimpleResp(lottery)
	return &LotteryUserTicketResp{
		ID:             obj.ID,
		LotteryId:      obj.LotteryId,
		ImServerUserId: obj.ImServerUserId,
		Use:            obj.Use,
		CreatedAt:      obj.CreatedAt,
		UpdatedAt:      obj.UpdatedAt,
		UsedAt:         obj.UsedAt,
		ExpiredAt:      obj.CreatedAt.AddDate(0, 0, lottery.ValidDays),

		LotteryInfo: lotteryInfo,
	}, nil
}
