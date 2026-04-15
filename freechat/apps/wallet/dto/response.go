package dto

import (
	"context"
	"fmt"
	"time"

	orgCache "github.com/openimsdk/chat/freechat/apps/organization/cache"
	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/apps/wallet/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// DetailWalletInfoResp 钱包信息响应
type DetailWalletInfoResp struct {
	ID        primitive.ObjectID        `bson:"_id,omitempty" json:"id,omitempty"`
	OwnerId   string                    `bson:"owner_id" json:"owner_id"`     // 所有人id
	OwnerType model.WalletInfoOwnerType `bson:"owner_type" json:"owner_type"` // 所有人类型

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`

	TotalBalanceUsd     decimal.Decimal `json:"total_balance_usd"`    // 合计总金额
	TotalFrozenBalance  decimal.Decimal `json:"total_frozen_balance"` // 合计总冻结余额
	CompensationBalance string          `json:"compensation_balance"` // 补偿金总余额

	WalletBalance []*DetailWalletBalanceResp `json:"wallet_balance"`
}

func NewDetailWalletInfoResp(ctx context.Context, db *mongo.Database, obj *model.WalletInfo) (*DetailWalletInfoResp, error) {
	dao := model.NewWalletBalanceDao(db)
	count, balances, err := dao.Select(ctx, obj.ID, primitive.NilObjectID, nil)
	if err != nil {
		return nil, err
	}

	totalBalanceUsd := decimal.NewFromInt(0)
	totalFrozenBalance := decimal.NewFromInt(0)

	// 直接从钱包信息获取补偿金余额
	compensationBalance, err := decimal.NewFromString(obj.CompensationBalance.String())
	if err != nil {
		compensationBalance = decimal.NewFromInt(0)
	}

	walletBalance := make([]*DetailWalletBalanceResp, 0, count)
	for _, balance := range balances {
		resp, err := NewDetailWalletBalanceResp(ctx, db, balance)
		if err != nil {
			return nil, err
		}
		walletBalance = append(walletBalance, resp)

		totalBalanceUsd = totalBalanceUsd.Add(resp.BalanceToUsd)
		totalFrozenBalance = totalFrozenBalance.Add(resp.FrozenBalanceToUsd)
	}

	return &DetailWalletInfoResp{
		ID:                  obj.ID,
		OwnerId:             obj.OwnerId,
		OwnerType:           obj.OwnerType,
		CreatedAt:           obj.CreatedAt,
		UpdatedAt:           obj.UpdatedAt,
		WalletBalance:       walletBalance,
		TotalBalanceUsd:     totalBalanceUsd,
		TotalFrozenBalance:  totalFrozenBalance,
		CompensationBalance: compensationBalance.String(),
	}, nil
}

// DetailWalletBalanceResp 表示一个用户的余额信息, 余额表
type DetailWalletBalanceResp struct {
	ID         primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	WalletId   primitive.ObjectID `bson:"wallet_id" json:"wallet_id"`     // 钱包ID
	CurrencyId primitive.ObjectID `bson:"currency_id" json:"currency_id"` // 币种id

	AvailableBalance       primitive.Decimal128 `bson:"available_balance" json:"available_balance"`                 // 可用余额
	RedPacketFrozenBalance primitive.Decimal128 `bson:"red_packet_frozen_balance" json:"red_packet_frozen_balance"` // 红包冻结余额
	TransferFrozenBalance  primitive.Decimal128 `bson:"transfer_frozen_balance" json:"transfer_frozen_balance"`     // 转账冻结余额
	// CompensationBalance 字段已移至钱包级别，不再在币种级别返回
	BalanceToUsd       decimal.Decimal `json:"balance_to_usd"`
	FrozenBalanceToUsd decimal.Decimal `json:"frozen_balance_to_usd"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`

	WalletCurrency *WalletCurrencyResp `json:"wallet_currency"`
}

func NewDetailWalletBalanceResp(ctx context.Context, db *mongo.Database, obj *model.WalletBalance) (*DetailWalletBalanceResp, error) {
	dao := model.NewWalletCurrencyDao(db)
	walletCurrencyModel, err := dao.GetById(ctx, obj.CurrencyId)
	if err != nil {
		return nil, err
	}

	walletCurrency := NewWalletCurrencyResp(walletCurrencyModel)
	availableBalance, err := decimal.NewFromString(obj.AvailableBalance.String())
	if err != nil {
		return nil, err
	}

	exchangeRateDecimal, err := decimal.NewFromString(walletCurrency.ExchangeRate)
	if err != nil {
		return nil, err
	}
	balanceToUsd := exchangeRateDecimal.Mul(availableBalance)

	redPacketFrozenBalance, err := decimal.NewFromString(obj.RedPacketFrozenBalance.String())
	if err != nil {
		return nil, fmt.Errorf("redPacketFrozenBalance: %s", err)
	}
	transferFrozenBalance, err := decimal.NewFromString(obj.TransferFrozenBalance.String())
	if err != nil {
		return nil, fmt.Errorf("transferFrozenBalance: %s", err)
	}
	frozenBalanceToUsd := exchangeRateDecimal.Mul(redPacketFrozenBalance.Add(transferFrozenBalance))

	return &DetailWalletBalanceResp{
		ID:                     obj.ID,
		WalletId:               obj.WalletId,
		CurrencyId:             obj.CurrencyId,
		AvailableBalance:       obj.AvailableBalance,
		RedPacketFrozenBalance: obj.RedPacketFrozenBalance,
		TransferFrozenBalance:  obj.TransferFrozenBalance,
		// 不再返回币种级别的补偿金余额
		CreatedAt:          obj.CreatedAt,
		UpdatedAt:          obj.UpdatedAt,
		WalletCurrency:     walletCurrency,
		BalanceToUsd:       balanceToUsd,
		FrozenBalanceToUsd: frozenBalanceToUsd,
	}, nil
}

// WalletBalanceResp 查询账户某个币种余额
type WalletBalanceResp struct {
	ID         primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	WalletId   primitive.ObjectID `bson:"wallet_id" json:"wallet_id"`     // 钱包ID
	CurrencyId primitive.ObjectID `bson:"currency_id" json:"currency_id"` // 币种id

	AvailableBalance       primitive.Decimal128 `bson:"available_balance" json:"available_balance"`                 // 可用余额
	RedPacketFrozenBalance primitive.Decimal128 `bson:"red_packet_frozen_balance" json:"red_packet_frozen_balance"` // 红包冻结余额
	TransferFrozenBalance  primitive.Decimal128 `bson:"transfer_frozen_balance" json:"transfer_frozen_balance"`     // 转账冻结余额
	// CompensationBalance 字段已移至钱包级别，不再在币种级别返回
	BalanceToUsd       decimal.Decimal `json:"balance_to_usd"`
	FrozenBalanceToUsd decimal.Decimal `json:"frozen_balance_to_usd"`
}

func NewWalletBalanceResp(balance *model.WalletBalance, currency *model.WalletCurrency) (*WalletBalanceResp, error) {
	availableBalance, err := decimal.NewFromString(balance.AvailableBalance.String())
	if err != nil {
		return nil, err
	}

	exchangeRateDecimal, err := decimal.NewFromString(currency.ExchangeRate.String())
	if err != nil {
		return nil, err
	}
	balanceToUsd := exchangeRateDecimal.Mul(availableBalance)

	redPacketFrozenBalance, err := decimal.NewFromString(balance.RedPacketFrozenBalance.String())
	if err != nil {
		return nil, fmt.Errorf("redPacketFrozenBalance: %s", err)
	}
	transferFrozenBalance, err := decimal.NewFromString(balance.TransferFrozenBalance.String())
	if err != nil {
		return nil, fmt.Errorf("transferFrozenBalance: %s", err)
	}
	frozenBalanceToUsd := exchangeRateDecimal.Mul(redPacketFrozenBalance.Add(transferFrozenBalance))

	return &WalletBalanceResp{
		ID:                     balance.ID,
		WalletId:               balance.WalletId,
		CurrencyId:             balance.CurrencyId,
		AvailableBalance:       balance.AvailableBalance,
		RedPacketFrozenBalance: balance.RedPacketFrozenBalance,
		TransferFrozenBalance:  balance.TransferFrozenBalance,
		// 不再返回币种级别的补偿金余额
		BalanceToUsd:       balanceToUsd,
		FrozenBalanceToUsd: frozenBalanceToUsd,
	}, nil
}

type WalletCurrencyResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	Name               string               `bson:"name" json:"name"`
	Icon               string               `bson:"icon" json:"icon"`
	Order              int                  `bson:"order" json:"order"`
	ExchangeRate       string               `bson:"exchange_rate" json:"exchange_rate"`
	MinAvailableAmount string               `bson:"min_available_amount" json:"min_available_amount"`
	MaxTotalSupply     int64                `bson:"max_total_supply" json:"max_total_supply"`
	MaxRedPacketAmount primitive.Decimal128 `bson:"max_red_packet_amount" json:"max_red_packet_amount"`
	CreatorId          primitive.ObjectID   `bson:"creator_id" json:"creator_id"`
	Decimals           int                  `json:"decimals"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func NewWalletCurrencyResp(obj *model.WalletCurrency) *WalletCurrencyResp {
	return &WalletCurrencyResp{
		ID:                 obj.ID,
		Icon:               obj.Icon,
		Name:               obj.Name,
		Order:              obj.Order,
		ExchangeRate:       obj.ExchangeRate.String(),
		MinAvailableAmount: obj.MinAvailableAmount.String(),
		CreatorId:          obj.CreatorId,
		CreatedAt:          obj.CreatedAt,
		UpdatedAt:          obj.UpdatedAt,
		MaxRedPacketAmount: obj.MaxRedPacketAmount,
		Decimals:           obj.Decimals,
		MaxTotalSupply:     obj.MaxTotalSupply,
	}
}

type WalletBalanceByOrgUserResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	Name   string `bson:"name" json:"name"`
	Type   string `bson:"type" json:"type"`
	Status string `bson:"status" json:"status"`
	Logo   string `bson:"logo" json:"logo"`

	TotalBalanceUsd     decimal.Decimal `json:"total_balance_usd"`    // 合计总金额
	TotalFrozenBalance  decimal.Decimal `json:"total_frozen_balance"` // 合计总冻结余额
	CompensationBalance string          `json:"compensation_balance"` // 补偿金总余额，与币种无关

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`

	Currency []*WalletBalanceByCurrencyResp `json:"currency"`
}

func NewWalletBalanceByOrgUserResp(db *mongo.Database, orgUser *orgModel.OrganizationUser) (*WalletBalanceByOrgUserResp, error) {
	org, err := orgCache.NewOrgCacheRedis(plugin.RedisCli(), db).GetById(context.TODO(), orgUser.OrganizationId)
	if err != nil {
		return nil, err
	}
	resp := &WalletBalanceByOrgUserResp{
		ID:        org.ID,
		Name:      org.Name,
		Type:      string(org.Type),
		Status:    string(org.Status),
		CreatedAt: org.CreatedAt,
		UpdatedAt: org.UpdatedAt,
		Logo:      org.Logo,
	}

	walletCurrency := model.NewWalletCurrencyDao(db)
	walletDao := model.NewWalletInfoDao(db)
	walletBalanceDao := model.NewWalletBalanceDao(db)

	wallet, err := walletDao.GetByOwnerIdAndOwnerType(context.TODO(), orgUser.UserId, model.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.WalletNotOpenErr
		}
		return nil, err
	}

	_, currencies, err := walletCurrency.Select(context.TODO(), []primitive.ObjectID{orgUser.OrganizationId}, nil)
	if err != nil {
		return nil, err
	}

	totalBalanceUsd := decimal.NewFromInt(0)
	totalFrozenBalance := decimal.NewFromInt(0)

	// 直接从钱包信息获取补偿金余额
	compensationBalance, err := decimal.NewFromString(wallet.CompensationBalance.String())
	if err != nil {
		compensationBalance = decimal.NewFromInt(0)
	}

	resp.Currency = make([]*WalletBalanceByCurrencyResp, 0, len(currencies))
	for _, currency := range currencies {
		balance, err := walletBalanceDao.GetByWalletIdAndCurrencyId(context.TODO(), wallet.ID, currency.ID)
		if err != nil {
			if !dbutil.IsDBNotFound(err) {
				return nil, err
			}
			// 原子确保余额记录存在（upsert 零余额），避免并发下重复插入
			if err = walletBalanceDao.EnsureWalletBalanceExists(context.TODO(), wallet.ID, currency.ID); err != nil {
				return nil, err
			}
			balance, err = walletBalanceDao.GetByWalletIdAndCurrencyId(context.TODO(), wallet.ID, currency.ID)
			if err != nil {
				return nil, err
			}
		}

		currencyResp, err := NewWalletBalanceByCurrencyResp(currency, balance)
		if err != nil {
			return nil, err
		}
		resp.Currency = append(resp.Currency, currencyResp)
		totalBalanceUsd = totalBalanceUsd.Add(currencyResp.BalanceInfo.BalanceToUsd)
		totalFrozenBalance = totalFrozenBalance.Add(currencyResp.BalanceInfo.FrozenBalanceToUsd)
	}

	resp.TotalBalanceUsd = totalBalanceUsd
	resp.TotalFrozenBalance = totalFrozenBalance
	resp.CompensationBalance = compensationBalance.String()

	return resp, nil
}

type WalletBalanceByCurrencyResp struct {
	CurrencyInfo *WalletCurrencyResp `json:"currency_info"`
	BalanceInfo  *WalletBalanceResp  `json:"balance_info"`
}

func NewWalletBalanceByCurrencyResp(currency *model.WalletCurrency, balance *model.WalletBalance) (*WalletBalanceByCurrencyResp, error) {
	balanceInfo, err := NewWalletBalanceResp(balance, currency)
	if err != nil {
		return nil, err
	}
	result := &WalletBalanceByCurrencyResp{
		CurrencyInfo: NewWalletCurrencyResp(currency),
		BalanceInfo:  balanceInfo,
	}
	return result, err
}

// CreateWalletResp 创建钱包响应
type CreateWalletResp struct {
	WalletInfo *model.WalletInfo `json:"wallet_info"` // 钱包信息
	NoticeText string            `json:"notice_text"` // 钱包开通提示文本
}
