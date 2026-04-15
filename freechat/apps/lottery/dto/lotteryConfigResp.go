package dto

import (
	"context"
	"github.com/openimsdk/chat/freechat/apps/lottery/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type LotteryConfigJoinLotteryRewardResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	LotteryId       primitive.ObjectID `bson:"lottery_id" json:"lottery_id"`
	LotteryRewardId primitive.ObjectID `bson:"lottery_reward_id" json:"lottery_reward_id"`

	Left  primitive.Decimal128 `bson:"left" json:"left"`
	Right primitive.Decimal128 `bson:"right" json:"right"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func NewLotteryConfigJoinLotteryRewardResp(obj *model.LotteryConfigJoinLotteryReward) *LotteryConfigJoinLotteryRewardResp {
	return &LotteryConfigJoinLotteryRewardResp{
		ID:              obj.ID,
		LotteryId:       obj.LotteryId,
		LotteryRewardId: obj.LotteryRewardId,
		Left:            obj.Left,
		Right:           obj.Right,
	}
}

type LotteryConfigResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	LotteryId       primitive.ObjectID `bson:"lottery_id" json:"lottery_id"`
	LotteryRewardId primitive.ObjectID `bson:"lottery_reward_id" json:"lottery_reward_id"`

	Left  primitive.Decimal128 `bson:"left" json:"left"`
	Right primitive.Decimal128 `bson:"right" json:"right"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`

	LotteryRewardInfo *LotteryRewardResp `json:"lottery_reward_info"`
}

func NewLotteryConfigResp(db *mongo.Database, obj *model.LotteryConfig) (*LotteryConfigResp, error) {
	lotteryRewardDao := model.NewLotteryRewardDao(db)

	lotteryReward, err := lotteryRewardDao.GetById(context.Background(), obj.LotteryRewardId)
	if err != nil {
		return nil, err
	}

	return &LotteryConfigResp{
		ID:                obj.ID,
		LotteryId:         obj.LotteryId,
		LotteryRewardId:   obj.LotteryRewardId,
		Left:              obj.Left,
		Right:             obj.Right,
		LotteryRewardInfo: NewLotteryRewardResp(lotteryReward),
	}, nil
}
