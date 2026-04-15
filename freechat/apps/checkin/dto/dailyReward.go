package dto

import (
	"context"
	"errors"
	"github.com/openimsdk/chat/freechat/apps/checkin/model"
	walletDto "github.com/openimsdk/chat/freechat/apps/wallet/dto"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type DailyCheckinRewardConfigResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrgId        primitive.ObjectID      `bson:"org_id" json:"org_id"`
	RewardType   model.CheckinRewardType `bson:"type" json:"type"`
	RewardId     string                  `bson:"reward_id" json:"reward_id"`
	RewardAmount primitive.Decimal128    `bson:"reward_amount" json:"reward_amount"`

	RewardCurrencyInfo *walletDto.WalletCurrencyResp `bson:"reward_currency_info" json:"reward_currency_info"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func NewDailyCheckinRewardConfigResp(db *mongo.Database, obj *model.DailyCheckinRewardConfig) (*DailyCheckinRewardConfigResp, error) {
	res := &DailyCheckinRewardConfigResp{
		ID:           obj.ID,
		OrgId:        obj.OrgId,
		RewardType:   obj.RewardType,
		RewardId:     obj.RewardId,
		RewardAmount: obj.RewardAmount,
		CreatedAt:    obj.CreatedAt,
		UpdatedAt:    obj.UpdatedAt,
	}

	// 查询货币信息
	walletCurrencyDao := walletModel.NewWalletCurrencyDao(db)
	rewardId, err := primitive.ObjectIDFromHex(obj.RewardId)
	if err != nil {
		return nil, errors.New("invalid reward_id")
	}

	walletCurrency, err := walletCurrencyDao.GetById(context.TODO(), rewardId)
	if err != nil {
		return nil, err
	}
	res.RewardCurrencyInfo = walletDto.NewWalletCurrencyResp(walletCurrency)

	return res, nil
}
