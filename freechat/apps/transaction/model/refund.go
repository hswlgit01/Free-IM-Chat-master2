package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// RefundReason 退款原因类型
type RefundReason int

const (
	RefundReasonExpired RefundReason = 1 // 交易过期退款
	RefundReasonCancel  RefundReason = 2 // 手动取消退款
	RefundReasonSystem  RefundReason = 3 // 系统异常退款
)

// RefundRecord 退款记录表
type RefundRecord struct {
	ID              primitive.ObjectID   `bson:"_id,omitempty" json:"id,omitempty"`
	RefundID        string               `bson:"refund_id" json:"refund_id"`               // 退款唯一ID
	TransactionID   string               `bson:"transaction_id" json:"transaction_id"`     // 原交易ID
	UserID          string               `bson:"user_id" json:"user_id"`                   // 被退款用户ID
	UserImID        string               `bson:"user_im_id" json:"user_im_id"`             // 被退款用户IM ID
	TransactionType int                  `bson:"transaction_type" json:"transaction_type"` // 原交易类型
	RefundAmount    primitive.Decimal128 `bson:"refund_amount" json:"refund_amount"`       // 退款金额
	RefundCount     int                  `bson:"refund_count" json:"refund_count"`         // 退款数量（红包剩余个数）
	RefundReason    RefundReason         `bson:"refund_reason" json:"refund_reason"`       // 退款原因
	WalletID        primitive.ObjectID   `bson:"wallet_id" json:"wallet_id"`               // 钱包ID
	CurrencyID      primitive.ObjectID   `bson:"currency_id" json:"currency_id"`           // 币种ID
	OrgID           string               `bson:"org_id" json:"org_id"`                     // 组织ID
	RefundTime      time.Time            `bson:"refund_time" json:"refund_time"`           // 退款时间
	Remark          string               `bson:"remark" json:"remark,omitempty"`           // 退款备注
	CreatedAt       time.Time            `bson:"created_at" json:"created_at"`
	UpdatedAt       time.Time            `bson:"updated_at" json:"updated_at"`
}

func (RefundRecord) TableName() string {
	return constant.CollectionRefund
}

// CreateRefundIndexes 创建退款记录索引
func CreateRefundIndexes(db *mongo.Database) error {
	refund := &RefundRecord{}

	// 退款记录表索引
	_, err := db.Collection(refund.TableName()).Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "refund_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "transaction_id", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "user_id", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "user_im_id", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "refund_time", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "refund_reason", Value: 1}},
		},
	})
	if err != nil {
		return errs.Wrap(err)
	}

	return nil
}

type RefundRecordDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewRefundRecordDao(db *mongo.Database) *RefundRecordDao {
	return &RefundRecordDao{
		DB:         db,
		Collection: db.Collection(RefundRecord{}.TableName()),
	}
}

// Create 创建退款记录
func (d *RefundRecordDao) Create(ctx context.Context, record *RefundRecord) error {
	record.CreatedAt = time.Now().UTC()
	record.UpdatedAt = time.Now().UTC()
	if err := mongoutil.InsertMany(ctx, d.Collection, []*RefundRecord{record}); err != nil {
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	return nil
}

// GetByRefundID 根据退款ID获取退款记录
func (d *RefundRecordDao) GetByRefundID(ctx context.Context, refundID string) (*RefundRecord, error) {
	record, err := mongoutil.FindOne[*RefundRecord](ctx, d.Collection, bson.M{"refund_id": refundID})
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, errs.NewCodeError(freeErrors.ErrTransactionNotFound, "退款记录不存在")
		}
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	return record, nil
}

// GetByTransactionID 根据交易ID获取退款记录
func (d *RefundRecordDao) GetByTransactionID(ctx context.Context, transactionID string) ([]*RefundRecord, error) {
	records, err := mongoutil.Find[*RefundRecord](ctx, d.Collection,
		bson.M{"transaction_id": transactionID},
		options.Find().SetSort(bson.M{"refund_time": -1})) // 按退款时间倒序
	if err != nil {
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	return records, nil
}

// GetUserRefundHistory 获取用户退款历史记录
func (d *RefundRecordDao) GetUserRefundHistory(ctx context.Context, userImID string, startTime, endTime time.Time, page, pageSize int) ([]*RefundRecord, int64, error) {
	filter := bson.M{"user_im_id": userImID}

	// 时间范围过滤
	if !startTime.IsZero() || !endTime.IsZero() {
		timeFilter := bson.M{}
		if !startTime.IsZero() {
			timeFilter["$gte"] = startTime
		}
		if !endTime.IsZero() {
			timeFilter["$lte"] = endTime
		}
		filter["refund_time"] = timeFilter
	}

	// 计算总数
	total, err := mongoutil.Count(ctx, d.Collection, filter)
	if err != nil {
		return nil, 0, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 分页查询
	skip := (page - 1) * pageSize
	opts := options.Find().
		SetSort(bson.M{"refund_time": -1}).
		SetSkip(int64(skip)).
		SetLimit(int64(pageSize))

	records, err := mongoutil.Find[*RefundRecord](ctx, d.Collection, filter, opts)
	if err != nil {
		return nil, 0, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	return records, total, nil
}
