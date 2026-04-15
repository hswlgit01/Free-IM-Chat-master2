package dto

import (
	"context"
	"github.com/openimsdk/chat/freechat/apps/lottery/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type LotterySimpleResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrgId primitive.ObjectID `bson:"org_id" json:"org_id"`
	Name  string             `bson:"name" json:"name"`
	Desc  string             `bson:"desc" json:"desc"`

	ValidDays int `bson:"valid_days" json:"valid_days"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func NewLotterySimpleResp(obj *model.Lottery) *LotterySimpleResp {
	return &LotterySimpleResp{
		ID:        obj.ID,
		OrgId:     obj.OrgId,
		Name:      obj.Name,
		Desc:      obj.Desc,
		ValidDays: obj.ValidDays,
		CreatedAt: obj.CreatedAt,
		UpdatedAt: obj.UpdatedAt,
	}
}

type DetailLotteryResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrgId primitive.ObjectID `bson:"org_id" json:"org_id"`
	Name  string             `bson:"name" json:"name"`
	Desc  string             `bson:"desc" json:"desc"`

	ValidDays int `bson:"valid_days" json:"valid_days"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`

	LotteryConfig []*LotteryConfigResp `json:"lottery_config"`
}

func NewDetailLotteryResp(db *mongo.Database, obj *model.Lottery) (*DetailLotteryResp, error) {
	lotteryConfigDao := model.NewLotteryConfigDao(db)
	_, items, err := lotteryConfigDao.Select(context.Background(), obj.ID, nil)
	if err != nil {
		return nil, err
	}

	lotteryConfig := make([]*LotteryConfigResp, 0)
	for _, item := range items {
		resp, err := NewLotteryConfigResp(db, item)
		if err != nil {
			return nil, err
		}
		lotteryConfig = append(lotteryConfig, resp)
	}

	return &DetailLotteryResp{
		ID:            obj.ID,
		OrgId:         obj.OrgId,
		Name:          obj.Name,
		Desc:          obj.Desc,
		ValidDays:     obj.ValidDays,
		CreatedAt:     obj.CreatedAt,
		UpdatedAt:     obj.UpdatedAt,
		LotteryConfig: lotteryConfig,
	}, nil
}
