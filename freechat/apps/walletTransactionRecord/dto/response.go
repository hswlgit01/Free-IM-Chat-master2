package dto

import (
	"context"
	"github.com/openimsdk/chat/freechat/apps/wallet/dto"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	"github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type SimpleWalletTsRecordResp struct {
	WalletId     primitive.ObjectID      `bson:"wallet_id"`                      // 交易事件的钱包id
	CurrencyId   primitive.ObjectID      `bson:"currency_id" json:"currency_id"` // 币种id
	CurrencyInfo *dto.WalletCurrencyResp `json:"currency_info"`                  // 币种信息

	Amount          string             `json:"amount"`
	ID              primitive.ObjectID `json:"id,omitempty"`
	Remark          string             `json:"remark"`
	Source          string             `json:"source"`
	TransactionTime time.Time          `json:"transaction_time"`
	Type            model.TsRecordType `json:"type"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func NewSimpleWalletTsRecord(ctx context.Context, db *mongo.Database, obj *model.WalletTransactionRecord) (*SimpleWalletTsRecordResp, error) {
	// 判断交易类型是否属于补偿金相关交易
	isCompensationRecord := obj.Type == model.TsRecordTypeCompensationInitial ||
		obj.Type == model.TsRecordTypeCompensationDeduction ||
		obj.Type == model.TsRecordTypeCompensationAdjust

	// 对于补偿金记录，不关联任何币种信息
	if isCompensationRecord {
		return &SimpleWalletTsRecordResp{
			WalletId:        obj.WalletId,
			CurrencyId:      primitive.NilObjectID, // 使用空ID表示不关联币种
			ID:              obj.ID,
			Amount:          obj.Amount.String(),
			Remark:          obj.Remark,
			Source:          obj.Source,
			Type:            obj.Type,
			TransactionTime: obj.TransactionTime,
			CreatedAt:       obj.CreatedAt,
			UpdatedAt:       obj.UpdatedAt,
			CurrencyInfo:    nil, // 不关联币种信息
		}, nil
	}

	// 对于非补偿金记录，正常处理币种信息
	dao := walletModel.NewWalletCurrencyDao(db)
	currencyInfoModel, err := dao.GetById(ctx, obj.CurrencyId)
	if err != nil {
		return nil, err
	}
	currencyInfo := dto.NewWalletCurrencyResp(currencyInfoModel)

	return &SimpleWalletTsRecordResp{
		WalletId:        obj.WalletId,
		CurrencyId:      obj.CurrencyId,
		ID:              obj.ID,
		Amount:          obj.Amount.String(),
		Remark:          obj.Remark,
		Source:          obj.Source,
		Type:            obj.Type,
		TransactionTime: obj.TransactionTime,
		CreatedAt:       obj.CreatedAt,
		UpdatedAt:       obj.UpdatedAt,
		CurrencyInfo:    currencyInfo,
	}, nil
}

type DetailWalletTsRecordResp struct {
	WalletId     primitive.ObjectID      `bson:"wallet_id"`                      // 交易事件的钱包id
	CurrencyId   primitive.ObjectID      `bson:"currency_id" json:"currency_id"` // 币种id
	CurrencyInfo *dto.WalletCurrencyResp `json:"currency_info"`                  // 币种信息

	Amount          string             `json:"amount"`
	ID              primitive.ObjectID `json:"id,omitempty"`
	Remark          string             `json:"remark"`
	Source          string             `json:"source"`
	TransactionTime time.Time          `json:"transaction_time"`
	Type            model.TsRecordType `json:"type"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func NewDetailWalletTsRecord(ctx context.Context, db *mongo.Database, obj *model.WalletTransactionRecord) (*DetailWalletTsRecordResp, error) {
	// 判断交易类型是否属于补偿金相关交易
	isCompensationRecord := obj.Type == model.TsRecordTypeCompensationInitial ||
		obj.Type == model.TsRecordTypeCompensationDeduction ||
		obj.Type == model.TsRecordTypeCompensationAdjust

	// 对于补偿金记录，不关联任何币种信息
	if isCompensationRecord {
		return &DetailWalletTsRecordResp{
			ID:              obj.ID,
			WalletId:        obj.WalletId,
			CurrencyId:      primitive.NilObjectID, // 使用空ID表示不关联币种
			CurrencyInfo:    nil,                   // 不关联币种信息
			Amount:          obj.Amount.String(),
			Remark:          obj.Remark,
			Source:          obj.Source,
			Type:            obj.Type,
			TransactionTime: obj.TransactionTime,
			CreatedAt:       obj.CreatedAt,
			UpdatedAt:       obj.UpdatedAt,
		}, nil
	}

	// 对于非补偿金记录，正常处理币种信息
	dao := walletModel.NewWalletCurrencyDao(db)
	currencyInfoModel, err := dao.GetById(ctx, obj.CurrencyId)
	if err != nil {
		return nil, err
	}
	currencyInfo := dto.NewWalletCurrencyResp(currencyInfoModel)

	return &DetailWalletTsRecordResp{
		ID:              obj.ID,
		WalletId:        obj.WalletId,
		CurrencyId:      obj.CurrencyId,
		CurrencyInfo:    currencyInfo,
		Amount:          obj.Amount.String(),
		Remark:          obj.Remark,
		Source:          obj.Source,
		Type:            obj.Type,
		TransactionTime: obj.TransactionTime,
		CreatedAt:       obj.CreatedAt,
		UpdatedAt:       obj.UpdatedAt,
	}, nil
}
