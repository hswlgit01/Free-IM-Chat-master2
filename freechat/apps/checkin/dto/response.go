package dto

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/openimsdk/chat/freechat/apps/checkin/model"
	lotteryDto "github.com/openimsdk/chat/freechat/apps/lottery/dto"
	lotteryModel "github.com/openimsdk/chat/freechat/apps/lottery/model"
	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	userDto "github.com/openimsdk/chat/freechat/apps/user/dto"
	walletDto "github.com/openimsdk/chat/freechat/apps/wallet/dto"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	"github.com/openimsdk/chat/freechat/plugin"
	chatCache "github.com/openimsdk/chat/freechat/third/chat/cache"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type CheckinResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	ImServerUserId string             `bson:"im_server_user_id" json:"im_server_user_id"`
	OrgId          primitive.ObjectID `bson:"org_id" json:"org_id"`
	Date           time.Time          `bson:"date" json:"date"`
	Streak         int                `bson:"streak" json:"streak"` // 当前连续签到天数

	ImServerUserInfo *userDto.UserResp `bson:"im_server_user_info" json:"im_server_user_info"`

	Attribute *userDto.AttributeResp `json:"attribute"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}

func NewCheckinResp(db *mongo.Database, obj *model.Checkin) (*CheckinResp, error) {
	userDao := openImModel.NewUserDao(db)

	orgUserDao := orgModel.NewOrganizationUserDao(db)
	attributeCache := chatCache.NewAttributeCacheRedis(plugin.RedisCli(), db)

	user, err := userDao.Take(context.TODO(), obj.ImServerUserId)
	if err != nil {
		return nil, err
	}

	orgUser, err := orgUserDao.GetByUserIMServerUserId(context.Background(), user.UserID)
	if err != nil {
		return nil, err
	}
	attribute, err := attributeCache.Take(context.Background(), orgUser.UserId)
	if err != nil {
		return nil, err
	}

	res := &CheckinResp{
		ID:               obj.ID,
		OrgId:            obj.OrgId,
		ImServerUserId:   obj.ImServerUserId,
		Date:             obj.Date,
		Streak:           obj.Streak,
		CreatedAt:        obj.CreatedAt,
		ImServerUserInfo: userDto.NewUserResp(user),
		Attribute:        userDto.NewAttributeResp(attribute),
	}

	return res, nil
}

type CheckinRewardConfigResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrgId      primitive.ObjectID      `bson:"org_id" json:"org_id"`
	RewardType model.CheckinRewardType `bson:"type" json:"type"`
	Streak     int                     `bson:"streak" json:"streak"`

	RewardId     string               `bson:"reward_id" json:"reward_id"`
	RewardAmount primitive.Decimal128 `bson:"reward_amount" json:"reward_amount"`

	RewardCurrencyInfo *walletDto.WalletCurrencyResp `bson:"reward_currency_info" json:"reward_currency_info"`
	RewardLotteryInfo  *lotteryDto.LotterySimpleResp `bson:"reward_lottery_info" json:"reward_lottery_info"`

	Auto bool `json:"auto"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func NewCheckinRewardConfigResp(db *mongo.Database, obj *model.CheckinRewardConfig) (*CheckinRewardConfigResp, error) {
	var err error

	res := &CheckinRewardConfigResp{
		ID:           obj.ID,
		OrgId:        obj.OrgId,
		RewardType:   obj.RewardType,
		Streak:       obj.Streak,
		RewardId:     obj.RewardId,
		RewardAmount: obj.RewardAmount,
		CreatedAt:    obj.CreatedAt,
		UpdatedAt:    obj.UpdatedAt,
		Auto:         obj.Auto,
	}
	walletCurrencyDao := walletModel.NewWalletCurrencyDao(db)
	//lotteryUserTicketDao := lotteryModel.NewLotteryUserTicketDao(db)
	lotteryDao := lotteryModel.NewLotteryDao(db)

	rewardId := primitive.NilObjectID
	if obj.RewardId != "" {
		rewardId, err = primitive.ObjectIDFromHex(obj.RewardId)
		if err != nil {
			return nil, errors.New("invalid reward_id")
		}
	}
	if res.RewardType == model.CheckinRewardTypeLottery {
		lottery, err := lotteryDao.GetById(context.TODO(), rewardId)
		if err != nil {
			return nil, err
		}

		res.RewardLotteryInfo = lotteryDto.NewLotterySimpleResp(lottery)
	} else if res.RewardType == model.CheckinRewardTypeCash {
		walletCurrency, err := walletCurrencyDao.GetById(context.TODO(), rewardId)
		if err != nil {
			return nil, err
		}
		res.RewardCurrencyInfo = walletDto.NewWalletCurrencyResp(walletCurrency)
	}

	return res, nil
}

type CheckinRewardResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	ImServerUserId        string             `bson:"im_server_user_id" json:"im_server_user_id"`
	CheckinRewardConfigId primitive.ObjectID `bson:"checkin_reward_config_id" json:"checkin_reward_config_id"` // 签到奖励的id
	CheckinId             primitive.ObjectID `bson:"checkin_id" json:"checkin_id"`                             // 签到触发记录id

	RewardType   model.CheckinRewardType `bson:"type" json:"type"` // 奖励类型
	RewardId     string                  `bson:"reward_id" json:"reward_id"`
	RewardAmount primitive.Decimal128    `json:"amount"`

	Status model.CheckinRewardStatus `json:"status"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`

	RewardCurrencyInfo *walletDto.WalletCurrencyResp `bson:"reward_currency_info" json:"reward_currency_info"`
	RewardLotteryInfo  *lotteryDto.LotterySimpleResp `bson:"reward_lottery_info" json:"reward_lottery_info"`
}

func NewCheckinRewardResp(db *mongo.Database, obj *model.CheckinReward) (*CheckinRewardResp, error) {
	var err error

	res := &CheckinRewardResp{
		ID:                    obj.ID,
		ImServerUserId:        obj.ImServerUserId,
		CheckinRewardConfigId: obj.CheckinRewardConfigId,
		RewardType:            obj.RewardType,
		RewardId:              obj.RewardId,
		RewardAmount:          obj.RewardAmount,
		CheckinId:             obj.CheckinId,

		Status:    obj.Status,
		CreatedAt: obj.CreatedAt,
		UpdatedAt: obj.UpdatedAt,
	}
	walletCurrencyDao := walletModel.NewWalletCurrencyDao(db)
	lotteryDao := lotteryModel.NewLotteryDao(db)

	rewardId := primitive.NilObjectID
	if obj.RewardId != "" {
		rewardId, err = primitive.ObjectIDFromHex(obj.RewardId)
		if err != nil {
			return nil, errors.New("invalid reward_id")
		}
	}
	if res.RewardType == model.CheckinRewardTypeLottery {
		lottery, err := lotteryDao.GetById(context.TODO(), rewardId)
		if err != nil {
			return nil, err
		}
		res.RewardLotteryInfo = lotteryDto.NewLotterySimpleResp(lottery)
	} else if res.RewardType == model.CheckinRewardTypeCash {
		walletCurrency, err := walletCurrencyDao.GetById(context.TODO(), rewardId)
		if err != nil {
			return nil, err
		}
		res.RewardCurrencyInfo = walletDto.NewWalletCurrencyResp(walletCurrency)
	}

	return res, nil
}

type CheckinRewardJoinAllResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	ImServerUserId        string             `bson:"im_server_user_id" json:"im_server_user_id"`
	CheckinRewardConfigId primitive.ObjectID `bson:"checkin_reward_config_id" json:"checkin_reward_config_id"` // 签到奖励的id
	CheckinId             primitive.ObjectID `bson:"checkin_id" json:"checkin_id"`                             // 签到触发记录id

	RewardType   model.CheckinRewardType `bson:"type" json:"type"` // 奖励类型
	RewardId     string                  `bson:"reward_id" json:"reward_id"`
	RewardAmount primitive.Decimal128    `json:"amount"`

	// 奖励来源：daily（日常）或 continuous（连续签到阶段）
	Source model.CheckinRewardSource `bson:"source" json:"source,omitempty"`
	// 阶段签到天数：对于连续签到奖励，记录触发该奖励时的连续天数（例如 7、30、90）
	Description string `bson:"description,omitempty" json:"description,omitempty"`
	// 奖励对应的签到日期；旧数据可能没有该字段，因此使用 omitempty 避免序列化为 0001-01-01
	CheckinDate time.Time `bson:"checkin_date" json:"checkin_date,omitempty"`

	Status model.CheckinRewardStatus `json:"status"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`

	RewardCurrencyInfo *walletDto.WalletCurrencyResp `bson:"reward_currency_info" json:"reward_currency_info"`
	RewardLotteryInfo  *lotteryDto.LotterySimpleResp `bson:"reward_lottery_info" json:"reward_lottery_info"`

	Checkin   interface{} `json:"checkin"`
	Attribute interface{} `json:"attribute"`
	User      interface{} `json:"user"`
}

// 缓存奖励关联的货币和抽奖配置，避免列表接口中重复访问数据库
var (
	walletCurrencyRespCache sync.Map // map[string]*walletDto.WalletCurrencyResp，key 为 RewardId(hex)
	lotteryRespCache        sync.Map // map[string]*lotteryDto.LotterySimpleResp，key 为 RewardId(hex)
)

func NewCheckinRewardJoinAllResp(db *mongo.Database, obj *model.CheckinRewardJoinAll) (*CheckinRewardJoinAllResp, error) {
	var err error

	// 优先使用奖励表自身的 checkin_date；如果为空，则尝试从连表的 checkin.date 或 CreatedAt 推导
	checkinDate := obj.CheckinDate
	if checkinDate.IsZero() {
		if obj.Checkin != nil {
			if raw, ok := obj.Checkin["date"]; ok {
				switch v := raw.(type) {
				case time.Time:
					checkinDate = v
				case primitive.DateTime:
					checkinDate = v.Time()
				}
			}
		}
		if checkinDate.IsZero() {
			checkinDate = obj.CreatedAt
		}
	}

	res := &CheckinRewardJoinAllResp{
		ID:                    obj.ID,
		ImServerUserId:        obj.ImServerUserId,
		CheckinRewardConfigId: obj.CheckinRewardConfigId,
		RewardType:            obj.RewardType,
		RewardId:              obj.RewardId,
		RewardAmount:          obj.RewardAmount,
		CheckinId:             obj.CheckinId,

		Source:      obj.Source,
		Description: obj.Description,
		CheckinDate: checkinDate,

		Status:    obj.Status,
		CreatedAt: obj.CreatedAt,
		UpdatedAt: obj.UpdatedAt,

		Checkin:   obj.Checkin,
		Attribute: obj.Attribute,
		User:      obj.User,
	}
	rewardId := primitive.NilObjectID
	if obj.RewardId != "" {
		rewardId, err = primitive.ObjectIDFromHex(obj.RewardId)
		if err != nil {
			return nil, errors.New("invalid reward_id")
		}
	}
	switch res.RewardType {
	case model.CheckinRewardTypeLottery:
		if obj.RewardId != "" {
			if cached, ok := lotteryRespCache.Load(obj.RewardId); ok {
				res.RewardLotteryInfo = cached.(*lotteryDto.LotterySimpleResp)
			} else {
				lotteryDao := lotteryModel.NewLotteryDao(db)
				lottery, err := lotteryDao.GetById(context.TODO(), rewardId)
				if err != nil {
					return nil, err
				}
				lotteryResp := lotteryDto.NewLotterySimpleResp(lottery)
				lotteryRespCache.Store(obj.RewardId, lotteryResp)
				res.RewardLotteryInfo = lotteryResp
			}
		}
	case model.CheckinRewardTypeCash:
		if obj.RewardId != "" {
			if cached, ok := walletCurrencyRespCache.Load(obj.RewardId); ok {
				res.RewardCurrencyInfo = cached.(*walletDto.WalletCurrencyResp)
			} else {
				walletCurrencyDao := walletModel.NewWalletCurrencyDao(db)
				walletCurrency, err := walletCurrencyDao.GetById(context.TODO(), rewardId)
				if err != nil {
					return nil, err
				}
				walletResp := walletDto.NewWalletCurrencyResp(walletCurrency)
				walletCurrencyRespCache.Store(obj.RewardId, walletResp)
				res.RewardCurrencyInfo = walletResp
			}
		}
	}

	return res, nil
}
