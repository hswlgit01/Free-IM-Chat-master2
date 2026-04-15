package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/tools/log"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// WalletSettings 钱包系统设置
type WalletSettings struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	// 补偿金系统设置
	CompensationEnabled bool                 `bson:"compensation_enabled" json:"compensation_enabled"` // 是否启用补偿金系统
	InitialCompensation primitive.Decimal128 `bson:"initial_compensation" json:"initial_compensation"` // 初始补偿金金额
	NoticeText          string               `bson:"notice_text" json:"notice_text"`                   // 钱包开通时显示的说明文本

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (WalletSettings) TableName() string {
	return constant.CollectionWalletSettings
}

func CreateWalletSettingsIndex(db *mongo.Database) error {
	m := &WalletSettings{}
	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{})
	return err
}

type WalletSettingsDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewWalletSettingsDao(db *mongo.Database) *WalletSettingsDao {
	return &WalletSettingsDao{
		DB:         db,
		Collection: db.Collection(WalletSettings{}.TableName()),
	}
}

// GetDefaultSettings 获取默认设置
func (o *WalletSettingsDao) GetDefaultSettings(ctx context.Context) (*WalletSettings, error) {
	settings, err := mongoutil.FindOne[*WalletSettings](ctx, o.Collection, bson.M{})
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			// 如果没有设置记录，创建默认设置
			initialCompensation, err := primitive.ParseDecimal128(decimal.NewFromInt(1000).String())
			if err != nil {
				log.ZError(ctx, "ParseDecimal128 error", err)
				return nil, err
			}

			defaultSettings := &WalletSettings{
				CompensationEnabled: true, // 默认启用补偿金系统
				InitialCompensation: initialCompensation,
				NoticeText:          "",
				CreatedAt:           time.Now().UTC(),
				UpdatedAt:           time.Now().UTC(),
			}

			err = mongoutil.InsertMany(ctx, o.Collection, []*WalletSettings{defaultSettings})
			if err != nil {
				log.ZError(ctx, "Insert default settings error", err)
				return nil, err
			}

			return defaultSettings, nil
		}
		log.ZError(ctx, "Get wallet settings error", err)
		return nil, err
	}

	return settings, nil
}

// UpdateSettings 更新设置
func (o *WalletSettingsDao) UpdateSettings(ctx context.Context, settings *WalletSettings) error {
	currentSettings, err := o.GetDefaultSettings(ctx)
	if err != nil {
		return err
	}

	settings.ID = currentSettings.ID
	settings.UpdatedAt = time.Now().UTC()

	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"_id": settings.ID}, bson.M{"$set": settings}, false)
}

// UpdateCompensationEnabled 更新补偿金系统启用状态
func (o *WalletSettingsDao) UpdateCompensationEnabled(ctx context.Context, enabled bool) error {
	settings, err := o.GetDefaultSettings(ctx)
	if err != nil {
		return err
	}

	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"_id": settings.ID}, bson.M{"$set": bson.M{
		"compensation_enabled": enabled,
		"updated_at":           time.Now().UTC(),
	}}, false)
}

// UpdateInitialCompensation 更新初始补偿金金额
func (o *WalletSettingsDao) UpdateInitialCompensation(ctx context.Context, amount decimal.Decimal) error {
	settings, err := o.GetDefaultSettings(ctx)
	if err != nil {
		return err
	}

	amountDecimal128, err := primitive.ParseDecimal128(amount.String())
	if err != nil {
		return err
	}

	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"_id": settings.ID}, bson.M{"$set": bson.M{
		"initial_compensation": amountDecimal128,
		"updated_at":           time.Now().UTC(),
	}}, false)
}
