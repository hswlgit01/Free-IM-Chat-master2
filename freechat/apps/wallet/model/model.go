package model

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/model"
	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type WalletInfoOwnerType string

const (
	WalletInfoOwnerTypeOrdinary     WalletInfoOwnerType = "ordinary"
	WalletInfoOwnerTypeOrganization WalletInfoOwnerType = "organization"
)

// WalletInfo 表示一个用户的余额信息, 余额表
// 存储用户的支付密码和余额
type WalletInfo struct {
	ID        primitive.ObjectID  `bson:"_id,omitempty" json:"id,omitempty"`
	PayPwd    string              `bson:"pay_pwd" json:"-"`             // 支付密码
	OwnerId   string              `bson:"owner_id" json:"owner_id"`     // 所有人id
	OwnerType WalletInfoOwnerType `bson:"owner_type" json:"owner_type"` // 所有人类型

	// 补偿金余额 - 直接存储在钱包级别，与币种无关
	CompensationBalance primitive.Decimal128 `bson:"compensation_balance" json:"compensation_balance"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (WalletInfo) TableName() string {
	return constant.CollectionWalletInfo
}

func CreateWalletInfoIndex(db *mongo.Database) error {
	m := &WalletInfo{}

	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "owner_id", Value: 1},
				{Key: "owner_type", Value: 1},
			},
		},
	})
	if err != nil {
		return errs.Wrap(err)
	}
	return nil
}

type WalletInfoDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewWalletInfoDao(db *mongo.Database) *WalletInfoDao {
	return &WalletInfoDao{
		DB:         db,
		Collection: db.Collection(WalletInfo{}.TableName()),
	}
}

//func (o *WalletInfoDao) GetByOwnerId(ctx context.Context, ownerId string) (*WalletInfo, error) {
//	return mongoutil.FindOne[*WalletInfo](ctx, o.Collection, bson.M{"owner_id": ownerId})
//}

func (o *WalletInfoDao) GetByOwnerIdAndOwnerType(ctx context.Context, ownerId string, ownerType WalletInfoOwnerType) (*WalletInfo, error) {
	return mongoutil.FindOne[*WalletInfo](ctx, o.Collection, bson.M{"owner_id": ownerId, "owner_type": ownerType})
}

func (o *WalletInfoDao) Create(ctx context.Context, obj *WalletInfo) error {
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*WalletInfo{obj})
}

func (o *WalletInfoDao) ExistByOwnerIdAndOwnerType(ctx context.Context, ownerId string, ownerType WalletInfoOwnerType) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"owner_id": ownerId, "owner_type": ownerType})
}

func (o *WalletInfoDao) UpdatePayPwd(ctx context.Context, ownerId string, payPwd string, ownerType WalletInfoOwnerType) error {
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"owner_id": ownerId, "owner_type": ownerType}, bson.M{"$set": bson.M{"pay_pwd": payPwd}}, false)
}

// GetById 根据ID获取钱包信息
func (o *WalletInfoDao) GetById(ctx context.Context, id primitive.ObjectID) (*WalletInfo, error) {
	return mongoutil.FindOne[*WalletInfo](ctx, o.Collection, bson.M{"_id": id})
}

// UpdateCompensationBalance 更新钱包的补偿金余额
// Parameters:
//   - ctx - 上下文
//   - walletId - 钱包ID
//   - amount - 要修改的补偿金金额，负数表示减少，正数表示增加
func (o *WalletInfoDao) UpdateCompensationBalance(ctx context.Context, walletId primitive.ObjectID, amount decimal.Decimal) error {
	wallet, err := o.GetById(ctx, walletId)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return freeErrors.WalletNotOpenErr
		}
		return err
	}

	// 获取当前补偿金余额
	currentBalance, err := decimal.NewFromString(wallet.CompensationBalance.String())
	if err != nil {
		// 如果解析失败，假设当前余额为0
		currentBalance = decimal.NewFromInt(0)
	}

	// 计算新余额
	newBalance := currentBalance.Add(amount)
	if newBalance.Cmp(decimal.NewFromInt(0)) < 0 {
		return freeErrors.WalletInsufficientBalanceErr
	}

	// 转换为Decimal128类型
	newBalance128, err := primitive.ParseDecimal128(newBalance.String())
	if err != nil {
		return err
	}

	// 更新数据库
	data := map[string]any{
		"compensation_balance": newBalance128,
		"updated_at":           time.Now().UTC(),
	}
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"_id": walletId}, bson.M{"$set": data}, false)
}

// GetCompensationBalance 获取钱包的补偿金余额
// Parameters:
//   - ctx - 上下文
//   - walletId - 钱包ID
//
// Returns:
//   - decimal.Decimal - 补偿金余额
//   - error - 错误信息
func (o *WalletInfoDao) GetCompensationBalance(ctx context.Context, walletId primitive.ObjectID) (decimal.Decimal, error) {
	wallet, err := o.GetById(ctx, walletId)
	if err != nil {
		return decimal.Zero, err
	}

	result, err := decimal.NewFromString(wallet.CompensationBalance.String())
	if err != nil {
		return decimal.Zero, err
	}

	return result, nil
}

type WalletCurrency struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	Name               string               `bson:"name" json:"name"`
	Icon               string               `bson:"icon" json:"icon"`
	Order              int                  `bson:"order" json:"order"`
	ExchangeRate       primitive.Decimal128 `bson:"exchange_rate" json:"exchange_rate"`
	MinAvailableAmount primitive.Decimal128 `bson:"min_available_amount" json:"min_available_amount"`
	Decimals           int                  `bson:"decimals" json:"decimals"`
	MaxTotalSupply     int64                `bson:"max_total_supply" json:"max_total_supply"`
	MaxRedPacketAmount primitive.Decimal128 `bson:"max_red_packet_amount" json:"max_red_packet_amount"`

	CreatorId primitive.ObjectID `bson:"creator_id" json:"creator_id"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (WalletCurrency) TableName() string {
	return constant.CollectionWalletCurrency
}

func CreateWalletCurrencyIndex(db *mongo.Database) error {
	m := &WalletCurrency{}

	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "order", Value: 1},
			},
		},
	})
	return err
}

type WalletCurrencyDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewWalletCurrencyDao(db *mongo.Database) *WalletCurrencyDao {
	return &WalletCurrencyDao{
		DB:         db,
		Collection: db.Collection(WalletCurrency{}.TableName()),
	}
}

func (o *WalletCurrencyDao) GetUsdCurrency() *WalletCurrency {
	/*
		use openim_v3;
		db.wallet_currency.insertOne({
		    "_id": ObjectId("000000000000000000000001"),
		    "name": "Usd",
		    "order": 0,
		    "exchange_rate": Decimal128("1"),
		    "min_available_amount": Decimal128("0"),
		    "created_at": ISODate("2025-04-24T00:00:00Z"),
		    "updated_at": ISODate("2025-04-24T00:00:00Z"),
			"creator_id": ObjectId("000000000000000000000000"),
			"icon": "https://d00.paixin.com/thumbs/1842549/40140151/staff_1024.jpg",
			"decimals": 6,
			"max_total_supply": 2200 * 10000
		});
	*/
	idHex, err := primitive.ObjectIDFromHex("000000000000000000000001")
	if err != nil {
		log.ZError(context.Background(), "GetUsdCurrency", err)
	}

	minAvailableAmountDecimal := decimal.NewFromInt(0)
	minAvailableAmount, err := primitive.ParseDecimal128(minAvailableAmountDecimal.String())
	if err != nil {
		log.ZError(context.Background(), "ParseDecimal128", err)
	}

	exchangeRateDecimal := decimal.NewFromInt(1)
	exchangeRate, err := primitive.ParseDecimal128(exchangeRateDecimal.String())
	if err != nil {
		log.ZError(context.Background(), "exchangeRateDecimal ParseDecimal128", err)
	}
	return &WalletCurrency{
		ID:                 idHex,
		Name:               "Usd",
		Order:              0,
		ExchangeRate:       exchangeRate,
		MinAvailableAmount: minAvailableAmount,
		CreatedAt:          time.Date(2025, 4, 24, 10, 0, 0, 0, time.UTC),
		UpdatedAt:          time.Date(2025, 4, 24, 10, 0, 0, 0, time.UTC),
		CreatorId:          constant.GetSystemCreatorId(),
		Decimals:           6,
		MaxTotalSupply:     math.MaxInt,
	}
}

// GetBtcCurrency todo 测试多币种使用
func (o *WalletCurrencyDao) GetBtcCurrency() *WalletCurrency {
	/*
		use openim_v3;
		db.wallet_currency.insertOne({
		    "_id": ObjectId("000000000000000000000002"),
		    "name": "Btc",
		    "order": 0,
		    "exchange_rate": Decimal128("90000"),
		    "min_available_amount": Decimal128("0"),
		    "created_at": ISODate("2025-04-24T00:00:00Z"),
		    "updated_at": ISODate("2025-04-24T00:00:00Z"),
			"creator_id": ObjectId("000000000000000000000000"),
			"icon": "https://abc.forcast.money/api/object/6381206320/msg_picture_ed7caa6cdcdbf684a148437e49844025.png",
		    "decimals": 6,
			"max_total_supply": 2200 * 10000
		});
	*/
	idHex, err := primitive.ObjectIDFromHex("000000000000000000000002")
	if err != nil {
		log.ZError(context.Background(), "GetBtcCurrency", err)
	}
	minAvailableAmountDecimal := decimal.NewFromInt(0)
	minAvailableAmount, err := primitive.ParseDecimal128(minAvailableAmountDecimal.String())
	if err != nil {
		log.ZError(context.Background(), "ParseDecimal128", err)
	}

	exchangeRateDecimal := decimal.NewFromInt(90000)
	exchangeRate, err := primitive.ParseDecimal128(exchangeRateDecimal.String())
	if err != nil {
		log.ZError(context.Background(), "exchangeRateDecimal ParseDecimal128", err)
	}
	return &WalletCurrency{
		ID:                 idHex,
		Name:               "Btc",
		Order:              0,
		ExchangeRate:       exchangeRate,
		MinAvailableAmount: minAvailableAmount,
		CreatedAt:          time.Date(2025, 4, 24, 10, 0, 0, 0, time.UTC),
		UpdatedAt:          time.Date(2025, 4, 24, 10, 0, 0, 0, time.UTC),
		CreatorId:          constant.GetSystemCreatorId(),
		Decimals:           6,
		MaxTotalSupply:     2200 * 10000,
	}
}

func (o *WalletCurrencyDao) GetById(ctx context.Context, id primitive.ObjectID) (*WalletCurrency, error) {
	return mongoutil.FindOne[*WalletCurrency](ctx, o.Collection, bson.M{"_id": id})
}

// GetByIds 批量根据 ID 获取币种，用于领取列表等场景避免 N+1
func (o *WalletCurrencyDao) GetByIds(ctx context.Context, ids []primitive.ObjectID) ([]*WalletCurrency, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	return mongoutil.Find[*WalletCurrency](ctx, o.Collection, bson.M{"_id": bson.M{"$in": ids}}, nil)
}

func (o *WalletCurrencyDao) GetByNameAndOrgId(ctx context.Context, name string, orgId primitive.ObjectID) (*WalletCurrency, error) {
	return mongoutil.FindOne[*WalletCurrency](ctx, o.Collection, bson.M{"name": name, "creator_id": orgId})
}

func (o *WalletCurrencyDao) ExistByIdAndOrgID(ctx context.Context, id primitive.ObjectID, orgId primitive.ObjectID) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"_id": id, "creator_id": orgId})
}

func (o *WalletCurrencyDao) ExistById(ctx context.Context, id primitive.ObjectID) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"_id": id})
}
func (o *WalletCurrencyDao) Create(ctx context.Context, obj *WalletCurrency) error {
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*WalletCurrency{obj})
}

type WalletCurrencyUpdateInfoField struct {
	MaxTotalSupply     int64                `bson:"max_total_supply" json:"max_total_supply"`
	Name               string               `bson:"name" json:"name"`
	Icon               string               `bson:"icon" json:"icon"`
	ExchangeRate       primitive.Decimal128 `bson:"exchange_rate" json:"exchange_rate"`
	MinAvailableAmount primitive.Decimal128 `bson:"min_available_amount" json:"min_available_amount"`
	MaxRedPacketAmount primitive.Decimal128 `bson:"max_red_packet_amount" json:"max_red_packet_amount"`
	Decimals           int                  `bson:"decimals" json:"decimals"`
}

func (o *WalletCurrencyDao) UpdateInfoById(ctx context.Context, id primitive.ObjectID, update WalletCurrencyUpdateInfoField) error {
	/*
		Icon               string               `bson:"icon" json:"icon"`
			ExchangeRate       int                  `bson:"exchange_rate" json:"exchange_rate"`
			MinAvailableAmount primitive.Decimal128 `bson:"min_available_amount" json:"min_available_amount"`
			Decimals           int                  `bson:"decimals" json:"decimals"`
	*/
	data := map[string]any{
		"name":                  update.Name,
		"icon":                  update.Icon,
		"exchange_rate":         update.ExchangeRate,
		"min_available_amount":  update.MinAvailableAmount,
		"decimals":              update.Decimals,
		"updated_at":            time.Now().UTC(),
		"max_red_packet_amount": update.MaxRedPacketAmount,
		"max_total_supply":      update.MaxTotalSupply,
	}
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"_id": id}, bson.M{"$set": data}, false)
}

func (o *WalletCurrencyDao) Select(ctx context.Context, creatorIds []primitive.ObjectID,
	page *paginationUtils.DepPagination) (int64, []*WalletCurrency, error) {
	filter := bson.M{}

	if len(creatorIds) > 0 {
		filter["creator_id"] = bson.M{"$in": creatorIds}
	}

	opts := make([]*options.FindOptions, 0)
	// 默认用order排序
	opts = append(opts, options.Find().SetSort(bson.M{"order": 1}))

	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	if len(filter) == 0 {
		filter = nil
	}

	data, err := mongoutil.Find[*WalletCurrency](ctx, o.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}

// WalletBalance 表示一个用户的余额信息, 余额表
type WalletBalance struct {
	ID         primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	WalletId   primitive.ObjectID `bson:"wallet_id" json:"wallet_id"`     // 钱包ID
	CurrencyId primitive.ObjectID `bson:"currency_id" json:"currency_id"` // 币种id

	AvailableBalance       primitive.Decimal128 `bson:"available_balance" json:"available_balance"`                 // 可用余额
	RedPacketFrozenBalance primitive.Decimal128 `bson:"red_packet_frozen_balance" json:"red_packet_frozen_balance"` // 红包冻结余额
	TransferFrozenBalance  primitive.Decimal128 `bson:"transfer_frozen_balance" json:"transfer_frozen_balance"`     // 转账冻结余额
	CompensationBalance    primitive.Decimal128 `bson:"compensation_balance" json:"compensation_balance"`           // 补偿金余额

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (WalletBalance) TableName() string {
	return constant.CollectionWalletBalance
}

func (WalletBalance) ZeroBalance(walletId primitive.ObjectID, currencyId primitive.ObjectID) *WalletBalance {
	zeroBalance, _ := primitive.ParseDecimal128(decimal.NewFromInt(0).String())

	return &WalletBalance{
		WalletId:               walletId,
		CurrencyId:             currencyId,
		AvailableBalance:       zeroBalance,
		RedPacketFrozenBalance: zeroBalance,
		TransferFrozenBalance:  zeroBalance,
		CompensationBalance:    zeroBalance,
	}
}

func CreateWalletBalanceIndex(db *mongo.Database) error {
	balance := &WalletBalance{}
	coll := db.Collection(balance.TableName())
	ctx := context.Background()

	// 若已存在同名非唯一索引，需先删除再创建唯一索引，否则会报 IndexKeySpecsConflict
	_, _ = coll.Indexes().DropOne(ctx, "wallet_id_1_currency_id_1") // 忽略错误（索引可能不存在）

	_, err := coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "wallet_id", Value: 1},
				{Key: "currency_id", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
	})
	return err
}

type WalletBalanceDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewWalletBalanceDao(db *mongo.Database) *WalletBalanceDao {
	return &WalletBalanceDao{
		DB:         db,
		Collection: db.Collection(WalletBalance{}.TableName()),
	}
}

func (o *WalletBalanceDao) GetByWalletIdAndCurrencyId(ctx context.Context, walletId primitive.ObjectID, currencyId primitive.ObjectID) (*WalletBalance, error) {
	return mongoutil.FindOne[*WalletBalance](ctx, o.Collection, bson.M{"wallet_id": walletId, "currency_id": currencyId})
}

func (o *WalletBalanceDao) FindByWalletId(ctx context.Context, walletId primitive.ObjectID) ([]*WalletBalance, error) {
	return mongoutil.Find[*WalletBalance](ctx, o.Collection, bson.M{"wallet_id": walletId})
}

func (o *WalletBalanceDao) Create(ctx context.Context, obj *WalletBalance) error {
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*WalletBalance{obj})
}

func (o *WalletBalanceDao) ExistByWalletIdAndCurrencyId(ctx context.Context, walletId primitive.ObjectID, currencyId primitive.ObjectID) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"wallet_id": walletId, "currency_id": currencyId})
}

// EnsureWalletBalanceExists 原子地确保 (wallet_id, currency_id) 对应余额记录存在；不存在则插入零余额记录。
// 用于避免高并发下「先查再创建」导致的重复插入，需配合 (wallet_id, currency_id) 唯一索引使用。
func (o *WalletBalanceDao) EnsureWalletBalanceExists(ctx context.Context, walletId primitive.ObjectID, currencyId primitive.ObjectID) error {
	zero128, _ := primitive.ParseDecimal128(decimal.NewFromInt(0).String())
	now := time.Now().UTC()
	filter := bson.M{"wallet_id": walletId, "currency_id": currencyId}
	update := bson.M{
		"$setOnInsert": bson.M{
			"available_balance":         zero128,
			"red_packet_frozen_balance": zero128,
			"transfer_frozen_balance":   zero128,
			"compensation_balance":      zero128,
			"created_at":                now,
		},
		"$set": bson.M{"updated_at": now},
	}
	_, err := o.Collection.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	return err
}

var zeroDecimal = decimal.NewFromInt(0)

// UpdateAvailableBalance 更新可用余额
// Parameters:
//
//	walletId - 用户的钱包id
//	currencyId - 用户钱包币种的id
//	amount - 要修改的余额,减少即为负数,增加为正数
//
// 【优化】使用MongoDB原子操作，彻底避免高并发下的读-检查-写竞态条件
func (o *WalletBalanceDao) UpdateAvailableBalance(ctx context.Context, walletId primitive.ObjectID, currencyId primitive.ObjectID, amount decimal.Decimal) error {
	if amount.IsZero() {
		return nil
	}

	baseFilter := bson.M{"wallet_id": walletId, "currency_id": currencyId}

	// 原子确保余额记录存在（不存在则 upsert 零余额），避免并发下重复插入
	if err := o.EnsureWalletBalanceExists(ctx, walletId, currencyId); err != nil {
		return err
	}

	if amount.IsNegative() {
		// 【关键优化】减少余额时，使用原子条件更新
		absAmount := amount.Abs()
		absAmount128, err := primitive.ParseDecimal128(absAmount.String())
		if err != nil {
			return err
		}

		// 原子条件：余额必须 >= 要扣减的金额
		atomicFilter := bson.M{
			"wallet_id":         walletId,
			"currency_id":       currencyId,
			"available_balance": bson.M{"$gte": absAmount128},
		}

		amount128, err := primitive.ParseDecimal128(amount.String())
		if err != nil {
			return err
		}

		update := bson.M{
			"$inc": bson.M{
				"available_balance": amount128,
			},
			"$set": bson.M{
				"updated_at": time.Now().UTC(),
			},
		}

		result, err := o.Collection.UpdateOne(ctx, atomicFilter, update)
		if err != nil {
			return errs.WrapMsg(err, "更新可用余额失败")
		}

		if result.MatchedCount == 0 {
			return freeErrors.WalletInsufficientBalanceErr
		}

		return nil
	}

	// 增加余额 - 使用 $inc 原子递增
	amount128, err := primitive.ParseDecimal128(amount.String())
	if err != nil {
		return err
	}

	update := bson.M{
		"$inc": bson.M{
			"available_balance": amount128,
		},
		"$set": bson.M{
			"updated_at": time.Now().UTC(),
		},
	}

	_, err = o.Collection.UpdateOne(ctx, baseFilter, update)
	if err != nil {
		return errs.WrapMsg(err, "更新可用余额失败")
	}

	return nil
}

// UpdateAvailableBalanceAndAddTsRecord 更新可用余额并且添加交易记录
// Parameters:
//
//		walletId - 用户的钱包id
//		currencyId - 用户钱包币种的id
//		amount - 要修改的余额,减少即为负数,增加为正数
//		recordType - 交易记录类型
//	 source - 交易源信息
//		remark - 交易记录备注
func (o *WalletBalanceDao) UpdateAvailableBalanceAndAddTsRecord(ctx context.Context,
	walletId primitive.ObjectID, currencyId primitive.ObjectID, amount decimal.Decimal,
	recordType model.TsRecordType, source, remark string) error {
	if err := o.UpdateAvailableBalance(ctx, walletId, currencyId, amount); err != nil {
		return err
	}

	amount128, err := primitive.ParseDecimal128(amount.String())
	if err != nil {
		return err
	}

	return model.NewWalletTsRecordDao(o.DB).Create(ctx, &model.WalletTransactionRecord{
		WalletId:        walletId,
		CurrencyId:      currencyId,
		TransactionTime: time.Now().UTC(),
		Type:            recordType,
		Amount:          amount128,
		Remark:          remark,
		Source:          source,
	})
}

// UpdateTransferFrozenBalance 更新转账冻结余额
// Parameters:
//
//		walletId - 用户的钱包id
//	 currencyId - 用户钱包币种的id
//		amount - 要冻结的转账金额,减少即为负数,增加为正数
//
// 【优化】使用MongoDB原子操作，彻底避免高并发下的读-检查-写竞态条件
func (o *WalletBalanceDao) UpdateTransferFrozenBalance(ctx context.Context, walletId primitive.ObjectID, currencyId primitive.ObjectID, amount decimal.Decimal) error {
	if amount.IsZero() {
		return nil
	}

	baseFilter := bson.M{"wallet_id": walletId, "currency_id": currencyId}

	if amount.IsNegative() {
		// 【关键优化】减少余额时，使用原子条件更新
		absAmount := amount.Abs()
		absAmount128, err := primitive.ParseDecimal128(absAmount.String())
		if err != nil {
			return err
		}

		// 原子条件：余额必须 >= 要扣减的金额
		atomicFilter := bson.M{
			"wallet_id":               walletId,
			"currency_id":             currencyId,
			"transfer_frozen_balance": bson.M{"$gte": absAmount128},
		}

		amount128, err := primitive.ParseDecimal128(amount.String())
		if err != nil {
			return err
		}

		update := bson.M{
			"$inc": bson.M{
				"transfer_frozen_balance": amount128,
			},
			"$set": bson.M{
				"updated_at": time.Now().UTC(),
			},
		}

		result, err := o.Collection.UpdateOne(ctx, atomicFilter, update)
		if err != nil {
			return errs.WrapMsg(err, "更新转账冻结余额失败")
		}

		if result.MatchedCount == 0 {
			// 区分是钱包不存在还是余额不足
			var exists bson.M
			err := o.Collection.FindOne(ctx, baseFilter, options.FindOne().SetProjection(bson.M{"_id": 1})).Decode(&exists)
			if err != nil {
				if errors.Is(err, mongo.ErrNoDocuments) {
					return freeErrors.WalletNotOpenErr
				}
				return err
			}
			return freeErrors.WalletInsufficientBalanceErr
		}

		return nil
	}

	// 增加余额 - 先确认钱包存在
	var exists bson.M
	err := o.Collection.FindOne(ctx, baseFilter, options.FindOne().SetProjection(bson.M{"_id": 1})).Decode(&exists)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return freeErrors.WalletNotOpenErr
		}
		return err
	}

	amount128, err := primitive.ParseDecimal128(amount.String())
	if err != nil {
		return err
	}

	update := bson.M{
		"$inc": bson.M{
			"transfer_frozen_balance": amount128,
		},
		"$set": bson.M{
			"updated_at": time.Now().UTC(),
		},
	}

	_, err = o.Collection.UpdateOne(ctx, baseFilter, update)
	if err != nil {
		return errs.WrapMsg(err, "更新转账冻结余额失败")
	}

	return nil
}

// UpdateRedPacketFrozenBalance 红包转账冻结
// Parameters:
//
//		walletId - 用户的钱包id
//	 currencyId - 用户钱包币种的id
//		amount - 要冻结的红包金额,减少即为负数,增加为正数
//
// 【优化】使用MongoDB原子操作，彻底避免高并发下的读-检查-写竞态条件
func (o *WalletBalanceDao) UpdateRedPacketFrozenBalance(ctx context.Context, walletId primitive.ObjectID, currencyId primitive.ObjectID, amount decimal.Decimal) error {
	if amount.IsZero() {
		return nil
	}

	baseFilter := bson.M{"wallet_id": walletId, "currency_id": currencyId}

	if amount.IsNegative() {
		// 【关键优化】减少余额时，使用原子条件更新
		// 将余额检查放入filter条件中，确保检查和更新是原子的
		absAmount := amount.Abs()
		absAmount128, err := primitive.ParseDecimal128(absAmount.String())
		if err != nil {
			return err
		}

		// 原子条件：余额必须 >= 要扣减的金额
		atomicFilter := bson.M{
			"wallet_id":                 walletId,
			"currency_id":               currencyId,
			"red_packet_frozen_balance": bson.M{"$gte": absAmount128},
		}

		// 使用 $inc 原子递减（amount 是负数）
		amount128, err := primitive.ParseDecimal128(amount.String())
		if err != nil {
			return err
		}

		update := bson.M{
			"$inc": bson.M{
				"red_packet_frozen_balance": amount128,
			},
			"$set": bson.M{
				"updated_at": time.Now().UTC(),
			},
		}

		result, err := o.Collection.UpdateOne(ctx, atomicFilter, update)
		if err != nil {
			return errs.WrapMsg(err, "更新红包冻结余额失败")
		}

		// 如果没有匹配到文档，说明余额不足或钱包不存在
		if result.MatchedCount == 0 {
			// 区分是钱包不存在还是余额不足
			var exists bson.M
			err := o.Collection.FindOne(ctx, baseFilter, options.FindOne().SetProjection(bson.M{"_id": 1})).Decode(&exists)
			if err != nil {
				if errors.Is(err, mongo.ErrNoDocuments) {
					return freeErrors.WalletNotOpenErr
				}
				return err
			}
			// 钱包存在但余额不足
			return freeErrors.WalletInsufficientBalanceErr
		}

		return nil
	}

	// 增加余额 - 使用 $inc 原子递增，无需检查余额
	// 先确认钱包存在
	var exists bson.M
	err := o.Collection.FindOne(ctx, baseFilter, options.FindOne().SetProjection(bson.M{"_id": 1})).Decode(&exists)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return freeErrors.WalletNotOpenErr
		}
		return err
	}

	amount128, err := primitive.ParseDecimal128(amount.String())
	if err != nil {
		return err
	}

	update := bson.M{
		"$inc": bson.M{
			"red_packet_frozen_balance": amount128,
		},
		"$set": bson.M{
			"updated_at": time.Now().UTC(),
		},
	}

	_, err = o.Collection.UpdateOne(ctx, baseFilter, update)
	if err != nil {
		return errs.WrapMsg(err, "更新红包冻结余额失败")
	}

	return nil
}

// UpdateCompensationBalance 更新补偿金余额
// Parameters:
//
//		walletId - 用户的钱包id
//	 currencyId - 用户钱包币种的id
//		amount - 要修改的补偿金金额,减少即为负数,增加为正数
//
// 【优化】使用MongoDB原子操作，彻底避免高并发下的读-检查-写竞态条件
func (o *WalletBalanceDao) UpdateCompensationBalance(ctx context.Context, walletId primitive.ObjectID, currencyId primitive.ObjectID, amount decimal.Decimal) error {
	if amount.IsZero() {
		return nil
	}

	baseFilter := bson.M{"wallet_id": walletId, "currency_id": currencyId}

	if amount.IsNegative() {
		// 【关键优化】减少余额时，使用原子条件更新
		absAmount := amount.Abs()
		absAmount128, err := primitive.ParseDecimal128(absAmount.String())
		if err != nil {
			return err
		}

		atomicFilter := bson.M{
			"wallet_id":            walletId,
			"currency_id":          currencyId,
			"compensation_balance": bson.M{"$gte": absAmount128},
		}

		amount128, err := primitive.ParseDecimal128(amount.String())
		if err != nil {
			return err
		}

		update := bson.M{
			"$inc": bson.M{
				"compensation_balance": amount128,
			},
			"$set": bson.M{
				"updated_at": time.Now().UTC(),
			},
		}

		result, err := o.Collection.UpdateOne(ctx, atomicFilter, update)
		if err != nil {
			return errs.WrapMsg(err, "更新补偿金余额失败")
		}

		if result.MatchedCount == 0 {
			var exists bson.M
			err := o.Collection.FindOne(ctx, baseFilter, options.FindOne().SetProjection(bson.M{"_id": 1})).Decode(&exists)
			if err != nil {
				if errors.Is(err, mongo.ErrNoDocuments) {
					return freeErrors.WalletNotOpenErr
				}
				return err
			}
			return freeErrors.WalletInsufficientBalanceErr
		}

		return nil
	}

	// 增加余额 - 先确认钱包存在
	var exists bson.M
	err := o.Collection.FindOne(ctx, baseFilter, options.FindOne().SetProjection(bson.M{"_id": 1})).Decode(&exists)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return freeErrors.WalletNotOpenErr
		}
		return err
	}

	amount128, err := primitive.ParseDecimal128(amount.String())
	if err != nil {
		return err
	}

	update := bson.M{
		"$inc": bson.M{
			"compensation_balance": amount128,
		},
		"$set": bson.M{
			"updated_at": time.Now().UTC(),
		},
	}

	_, err = o.Collection.UpdateOne(ctx, baseFilter, update)
	if err != nil {
		return errs.WrapMsg(err, "更新补偿金余额失败")
	}

	return nil
}

// UpdateCompensationBalanceAndAddTsRecord 更新补偿金余额并添加交易记录
// Parameters:
//
//	walletId - 用户的钱包id
//	currencyId - 用户钱包币种的id
//	amount - 要修改的补偿金金额,减少即为负数,增加为正数
//	recordType - 交易记录类型
//	source - 交易源信息
//	remark - 交易记录备注
func (o *WalletBalanceDao) UpdateCompensationBalanceAndAddTsRecord(ctx context.Context,
	walletId primitive.ObjectID, currencyId primitive.ObjectID, amount decimal.Decimal,
	recordType model.TsRecordType, source, remark string) error {
	if err := o.UpdateCompensationBalance(ctx, walletId, currencyId, amount); err != nil {
		return err
	}

	amount128, err := primitive.ParseDecimal128(amount.String())
	if err != nil {
		return err
	}

	return model.NewWalletTsRecordDao(o.DB).Create(ctx, &model.WalletTransactionRecord{
		WalletId:        walletId,
		CurrencyId:      currencyId,
		TransactionTime: time.Now().UTC(),
		Type:            recordType,
		Amount:          amount128,
		Remark:          remark,
		Source:          source,
	})
}

func (o *WalletBalanceDao) Select(ctx context.Context, walletId, currencyId primitive.ObjectID,
	page *paginationUtils.DepPagination) (int64, []*WalletBalance, error) {
	filter := bson.M{}

	if !walletId.IsZero() {
		filter["wallet_id"] = walletId
	}

	if !currencyId.IsZero() {
		filter["currency_id"] = currencyId
	}

	opts := make([]*options.FindOptions, 0)
	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	if len(filter) == 0 {
		filter = nil
	}

	data, err := mongoutil.Find[*WalletBalance](ctx, o.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}
