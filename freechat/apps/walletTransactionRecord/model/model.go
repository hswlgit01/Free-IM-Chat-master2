package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// TsRecordType 表示交易类型
type TsRecordType int

const (
	TsRecordTypeTransferExpense TsRecordType = 1 // "转账支出"
	TsRecordTypeTransferRefund  TsRecordType = 2 // "转账退款"
	TsRecordTypeTransferReceive TsRecordType = 3 // "转账领取"

	TsRecordTypeRedPacketRefund  TsRecordType = 11 // "红包退款"
	TsRecordTypeRedPacketExpense TsRecordType = 12 // "红包支出"
	TsRecordTypeRedPacketReceive TsRecordType = 13 // "红包领取"

	TsRecordTypeDeposit    TsRecordType = 21 // "充值"
	TsRecordTypeWithdrawal TsRecordType = 22 // "提现"
	TsRecordTypePayment    TsRecordType = 23 // "消费"

	TsRecordTypeCreateCurrency TsRecordType = 31 // 代币创建

	TsRecordTypeSignInRewardExpense      TsRecordType = 41 // "签到奖励支出"
	TsRecordTypeSignInRewardReceive      TsRecordType = 42 // "签到奖励领取"
	TsRecordTypeSignInRewardRefund       TsRecordType = 43 // "签到奖励冲回（用户扣减）"
	TsRecordTypeSignInRewardRefundIncome TsRecordType = 44 // "签到奖励冲回（组织加回）"

	TsRecordTypeCompensationInitial   TsRecordType = 51 // "初始补偿金"
	TsRecordTypeCompensationDeduction TsRecordType = 52 // "补偿金扣减"
	TsRecordTypeCompensationAdjust    TsRecordType = 53 // "补偿金调整"
)

// WalletTransactionRecord 表示一个用户的余额明细信息, 交易记录表
type WalletTransactionRecord struct {
	ID         primitive.ObjectID `bson:"_id,omitempty"`
	WalletId   primitive.ObjectID `bson:"wallet_id"`                      // 交易事件的钱包id
	CurrencyId primitive.ObjectID `bson:"currency_id" json:"currency_id"` // 币种id

	Source          string               `bson:"source"`           // 交易来源
	TransactionTime time.Time            `bson:"transaction_time"` // 交易时间
	Type            TsRecordType         `bson:"type"`             // 交易类型
	Amount          primitive.Decimal128 `bson:"amount"`           // 交易金额
	Remark          string               `bson:"remark"`           // 交易备注

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (WalletTransactionRecord) TableName() string {
	return constant.CollectionWalletTsRecord
}

func CreateTransactionRecordIndex(db *mongo.Database) error {
	transactionRecord := &WalletTransactionRecord{}

	coll := db.Collection(transactionRecord.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "wallet_id", Value: 1},
				{Key: "currency_id", Value: 1},
				{Key: "type", Value: 1},
			},
		},
	})
	return err
}

type WalletTransactionRecordDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewWalletTsRecordDao(db *mongo.Database) *WalletTransactionRecordDao {
	return &WalletTransactionRecordDao{
		DB:         db,
		Collection: db.Collection(WalletTransactionRecord{}.TableName()),
	}
}

func (o *WalletTransactionRecordDao) GetById(ctx context.Context, id primitive.ObjectID) (*WalletTransactionRecord, error) {
	return mongoutil.FindOne[*WalletTransactionRecord](ctx, o.Collection, bson.M{"_id": id})

}

func (o *WalletTransactionRecordDao) Create(ctx context.Context, obj *WalletTransactionRecord) error {
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*WalletTransactionRecord{obj})
}

func (o *WalletTransactionRecordDao) Select(ctx context.Context, walletId, currencyId primitive.ObjectID, tsRecordType TsRecordType,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (int64, []*WalletTransactionRecord, error) {
	filter := bson.M{}
	if tsRecordType != 0 {
		filter["type"] = tsRecordType
		//filter = append(filter, bson.E{Key: "type", Value: *tsRecordType})
	}
	if !walletId.IsZero() {
		filter["wallet_id"] = walletId
	}

	if !currencyId.IsZero() {
		filter["currency_id"] = currencyId
	}

	tsTime := bson.M{}
	if !startTime.IsZero() {
		tsTime["$gte"] = startTime
	}
	if !endTime.IsZero() {
		tsTime["$lte"] = endTime
	}
	if len(tsTime) > 0 {
		filter["transaction_time"] = tsTime
	}

	opts := page.ToOptions()

	if len(filter) == 0 {
		filter = nil
	}
	log.ZInfo(ctx, "Select", "filter", filter, "page", page, "tsRecordType", tsRecordType)

	data, err := mongoutil.Find[*WalletTransactionRecord](ctx, o.Collection, filter, opts)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}

// SelectByTypes 根据多个交易类型查询交易记录
func (o *WalletTransactionRecordDao) SelectByTypes(ctx context.Context, walletId primitive.ObjectID, currencyId primitive.ObjectID,
	tsRecordTypes []TsRecordType, startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (int64, []*WalletTransactionRecord, error) {
	filter := bson.M{}

	// 添加钱包ID过滤
	if !walletId.IsZero() {
		filter["wallet_id"] = walletId
	}

	// 添加币种ID过滤（如果提供）
	// 对于补偿金记录，我们可能不需要按币种过滤
	if !currencyId.IsZero() {
		filter["currency_id"] = currencyId
	}

	// 添加多类型过滤
	if len(tsRecordTypes) > 0 {
		filter["type"] = bson.M{"$in": tsRecordTypes}
	}

	// 添加时间范围过滤
	tsTime := bson.M{}
	if !startTime.IsZero() {
		tsTime["$gte"] = startTime
	}
	if !endTime.IsZero() {
		tsTime["$lte"] = endTime
	}
	if len(tsTime) > 0 {
		filter["transaction_time"] = tsTime
	}

	opts := page.ToOptions()

	if len(filter) == 0 {
		filter = nil
	}

	log.ZInfo(ctx, "SelectByTypes", "filter", filter, "page", page, "tsRecordTypes", tsRecordTypes)

	data, err := mongoutil.Find[*WalletTransactionRecord](ctx, o.Collection, filter, opts)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}
