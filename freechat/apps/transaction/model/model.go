package model

import (
	"context"
	"strings"
	"time"

	"github.com/openimsdk/chat/freechat/apps/transaction/dto"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TransactionType 交易类型

const (
	TransactionTypeTransfer                 = 0 // 转账
	TransactionTypeP2PRedPacket             = 1 // 一对一红包
	TransactionTypeNormalPacket             = 2 // 普通红包
	TransactionTypeLuckyPacket              = 3 // 拼手气红包
	TransactionTypeOrganization             = 4 // 组织账户转账
	TransactionTypeGroupExclusive           = 5 // 群组专属红包
	TransactionTypePasswordPacket           = 6 // 群组口令红包
	TransactionTypeOrganizationSignInReward = 7 // 组织签到奖励转账
)

// TransactionStatus 交易状态

const (
	TransactionStatusPending  = 0 // 进行中
	TransactionStatusComplete = 1 // 已完成
	TransactionStatusExpired  = 2 // 已过期
)

// Transaction 交易信息
// Transaction 交易记录模型，对应数据库表
type Transaction struct {
	ID                    primitive.ObjectID   `bson:"_id,omitempty" json:"id,omitempty"`
	TransactionID         string               `bson:"transaction_id" json:"transaction_id"`     // 交易唯一ID
	SenderID              string               `bson:"sender_id" json:"sender_id"`               // 发起者ID
	TargetImID            string               `bson:"target_im_id" json:"target_im_id"`         // 目标ID 子账户id或者是群组id
	SenderImID            string               `bson:"sender_im_id" json:"sender_im_id"`         // 发起者 子账户ID
	TransactionType       int                  `bson:"transaction_type" json:"transaction_type"` // 交易类型
	OrgID                 string               `bson:"org_id" json:"org_id"`                     // 组织ID
	TotalAmount           primitive.Decimal128 `bson:"total_amount" json:"total_amount"`         // 总金额
	TotalCount            int                  `bson:"total_count" json:"total_count"`           // 总个数（转账为1）
	RemainingAmount       primitive.Decimal128 `bson:"remaining_amount" json:"remaining_amount"` // 剩余金额
	RemainingCount        int                  `bson:"remaining_count" json:"remaining_count"`   // 剩余个数
	WalletID              primitive.ObjectID   `bson:"wallet_id" json:"wallet_id"`
	CurrencyId            primitive.ObjectID   `bson:"currency_id" json:"currency_id"`
	Greeting              string               `bson:"greeting" json:"greeting,omitempty"`                                           // 交易备注/祝福语
	Password              string               `bson:"password" json:"password,omitempty"`                                           // 口令红包的口令
	ExclusiveReceiverImID string               `bson:"exclusive_receiver_im_id,omitempty" json:"exclusive_receiver_im_id,omitempty"` // 专属接收者IM ID（群组专属红包）
	CreatedAt             time.Time            `bson:"created_at" json:"created_at"`                                                 // 创建时间
	Status                int                  `bson:"status" json:"status"`                                                         // 交易状态
	UpdatedAt             time.Time            `bson:"updated_at" json:"updated_at"`
}

type TransactionOrg struct {
	SenderName   string      `bson:"sender_name" json:"sender_name"`     // 发起者名称
	ReceiverName string      `bson:"receiver_name" json:"receiver_name"` // 接收者名称
	Currency     string      `bson:"currency" json:"currency"`           // 币种名称
	Base         Transaction `bson:",inline"`
}

// ReceiveRecord 接收记录（包含转账确认和红包领取）
type ReceiveRecord struct {
	ID              primitive.ObjectID   `bson:"_id,omitempty" json:"id,omitempty"`
	TransactionID   string               `bson:"transaction_id" json:"transaction_id"`     // 交易ID
	UserID          string               `bson:"user_id" json:"user_id"`                   // 接收者ID
	UserImID        string               `bson:"user_im_id" json:"user_im_id"`             // 接收者 子账户ID
	Amount          primitive.Decimal128 `bson:"amount" json:"amount"`                     // 接收金额
	ReceivedAt      time.Time            `bson:"received_at" json:"received_at"`           // 接收时间
	TransactionType int                  `bson:"transaction_type" json:"transaction_type"` // 交易类型
}

func (Transaction) TableName() string {
	return constant.CollectionTransaction
}

func (ReceiveRecord) TableName() string {
	return constant.CollectionReceive
}

// CreateTransactionIndexes 创建交易相关索引
func CreateTransactionIndexes(db *mongo.Database) error {
	transaction := &Transaction{}
	record := &ReceiveRecord{}

	// 交易表索引
	_, err := db.Collection(transaction.TableName()).Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "transaction_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "sender_id", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "target_im_id", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "status", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "created_at", Value: 1}},
		},
	})
	if err != nil {
		return errs.Wrap(err)
	}

	// 接收记录表索引
	_, err = db.Collection(record.TableName()).Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "transaction_id", Value: 1},
				{Key: "user_id", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "received_at", Value: 1}},
		},
	})
	if err != nil {
		return errs.Wrap(err)
	}

	return nil
}

type TransactionDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewTransactionDao(db *mongo.Database) *TransactionDao {
	return &TransactionDao{
		DB:         db,
		Collection: db.Collection(Transaction{}.TableName()),
	}
}

// Create 创建交易记录
func (d *TransactionDao) Create(ctx context.Context, transaction *Transaction) error {
	if err := mongoutil.InsertMany(ctx, d.Collection, []*Transaction{transaction}); err != nil {
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	return nil
}

// GetByTransactionID 根据交易ID获取交易信息
func (d *TransactionDao) GetByTransactionID(ctx context.Context, transactionID string) (*Transaction, error) {
	transaction, err := mongoutil.FindOne[*Transaction](ctx, d.Collection, bson.M{"transaction_id": transactionID})
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, errs.NewCodeError(freeErrors.ErrTransactionNotFound, freeErrors.ErrorMessages[freeErrors.ErrTransactionNotFound])
		}
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	return transaction, nil
}

// UpdateStatus 更新交易状态
func (d *TransactionDao) UpdateStatus(ctx context.Context, transactionID string, status int) error {
	if err := mongoutil.UpdateOne(ctx, d.Collection,
		bson.M{"transaction_id": transactionID},
		bson.M{"$set": bson.M{"status": status}},
		false); err != nil {
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	return nil
}

// UpdateRemainingAmountAndCount 更新交易的剩余金额和剩余个数
func (d *TransactionDao) UpdateRemainingAmountAndCount(ctx context.Context, transactionID string, remainingAmount primitive.Decimal128, remainingCount int) error {
	updateTime := time.Now().UTC()

	if err := mongoutil.UpdateOne(ctx, d.Collection,
		bson.M{"transaction_id": transactionID},
		bson.M{"$set": bson.M{
			"remaining_amount": remainingAmount,
			"remaining_count":  remainingCount,
			"updated_at":       updateTime,
		}},
		false); err != nil {
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	//log.ZInfo(ctx, "更新交易剩余金额和个数", "transaction_id", transactionID,
	//	"remaining_amount", remainingAmount.String(), "remaining_count", remainingCount,
	//	"update_time", updateTime.Format(time.RFC3339))

	return nil
}

// DecrementRemainingAmountAndCount 【高并发安全】使用原子操作递减剩余金额和个数
// 这个方法使用 $inc 操作，而不是 $set，避免高并发下的覆盖问题
// amountDecrement: 要减少的金额（正数）
// 返回值：是否更新成功，以及更新后的剩余数量
func (d *TransactionDao) DecrementRemainingAmountAndCount(ctx context.Context, transactionID string, amountDecrement primitive.Decimal128) (bool, int, error) {
	updateTime := time.Now().UTC()

	// 使用 $inc 原子递减，同时要求 remaining_count > 0 防止超卖
	filter := bson.M{
		"transaction_id":  transactionID,
		"remaining_count": bson.M{"$gt": 0},
	}

	// 注意：MongoDB 的 $inc 对 Decimal128 类型需要传入负数来递减
	// 我们需要将 amountDecrement 转为负数
	amountStr := amountDecrement.String()
	negAmountStr := "-" + amountStr
	negAmount, err := primitive.ParseDecimal128(negAmountStr)
	if err != nil {
		return false, 0, errs.NewCodeError(freeErrors.ErrSystem, "金额转换失败")
	}

	update := bson.M{
		"$inc": bson.M{
			"remaining_count":  -1,
			"remaining_amount": negAmount,
		},
		"$set": bson.M{
			"updated_at": updateTime,
		},
	}

	// 使用 FindOneAndUpdate 获取更新后的文档
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var updatedDoc Transaction
	err = d.Collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&updatedDoc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// 没有匹配的文档（可能 remaining_count 已经是 0）
			return false, 0, nil
		}
		return false, 0, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	return true, updatedDoc.RemainingCount, nil
}

// UpdateTransactionComplete 更新交易为完成状态，同时更新剩余金额和剩余数量
func (d *TransactionDao) UpdateTransactionComplete(ctx context.Context, transactionID string) error {
	zeroAmount, err := primitive.ParseDecimal128("0")
	if err != nil {
		return errs.NewCodeError(freeErrors.ErrSystem, "无法解析零金额")
	}

	updateTime := time.Now().UTC()

	if err := mongoutil.UpdateOne(ctx, d.Collection,
		bson.M{"transaction_id": transactionID},
		bson.M{"$set": bson.M{
			"status":           TransactionStatusComplete,
			"remaining_amount": zeroAmount,
			"remaining_count":  0,
			"updated_at":       updateTime,
		}},
		false); err != nil {
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	log.ZInfo(ctx, "更新交易为已完成状态", "transaction_id", transactionID,
		"update_time", updateTime.Format(time.RFC3339))

	return nil
}

// UpdateTransactionCompleteOnlyIfRemainingOne 仅当当前 remaining_count 为 1 时才更新为完成状态（防止 Redis 与 DB 不同步时误置为已完成）
// 返回 matched 表示是否命中并更新了文档；若未命中则不做任何更新，调用方不应视为错误。
func (d *TransactionDao) UpdateTransactionCompleteOnlyIfRemainingOne(ctx context.Context, transactionID string) (matched bool, err error) {
	zeroAmount, parseErr := primitive.ParseDecimal128("0")
	if parseErr != nil {
		return false, errs.NewCodeError(freeErrors.ErrSystem, "无法解析零金额")
	}
	updateTime := time.Now().UTC()
	filter := bson.M{
		"transaction_id":  transactionID,
		"remaining_count": 1, // 仅当确实是「最后一份」时才更新为完成
	}
	update := bson.M{"$set": bson.M{
		"status":           TransactionStatusComplete,
		"remaining_amount": zeroAmount,
		"remaining_count":  0,
		"updated_at":       updateTime,
	}}
	res, err := mongoutil.UpdateOneResult(ctx, d.Collection, filter, update)
	if err != nil {
		return false, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	matched = res.ModifiedCount > 0
	if matched {
		log.ZInfo(ctx, "更新交易为已完成状态（仅当剩余1个时）", "transaction_id", transactionID,
			"update_time", updateTime.Format(time.RFC3339))
	}
	return matched, nil
}

// UpdateTransactionExpired 更新交易为过期状态，同时更新剩余金额和数量（用于退款）
func (d *TransactionDao) UpdateTransactionExpired(ctx context.Context, transactionID string, remainingAmount primitive.Decimal128, remainingCount int) error {
	updateTime := time.Now().UTC()

	if err := mongoutil.UpdateOne(ctx, d.Collection,
		bson.M{"transaction_id": transactionID},
		bson.M{"$set": bson.M{
			"status":           TransactionStatusExpired,
			"remaining_amount": remainingAmount,
			"remaining_count":  remainingCount,
			"updated_at":       updateTime,
		}},
		false); err != nil {
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	log.ZInfo(ctx, "更新交易为过期状态", "transaction_id", transactionID,
		"remaining_amount", remainingAmount.String(), "remaining_count", remainingCount,
		"update_time", updateTime.Format(time.RFC3339))

	return nil
}

// FindTransactions 根据条件查询交易列表
func (d *TransactionDao) FindTransactions(ctx context.Context, filter interface{}) ([]*Transaction, error) {
	transactions, err := mongoutil.Find[*Transaction](ctx, d.Collection, filter)
	if err != nil {
		return nil, err
	}
	return transactions, nil
}

type ReceiveRecordDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewReceiveRecordDao(db *mongo.Database) *ReceiveRecordDao {
	return &ReceiveRecordDao{
		DB:         db,
		Collection: db.Collection(ReceiveRecord{}.TableName()),
	}
}

// Create 创建接收记录
func (d *ReceiveRecordDao) Create(ctx context.Context, record *ReceiveRecord) error {
	if err := mongoutil.InsertMany(ctx, d.Collection, []*ReceiveRecord{record}); err != nil {
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	return nil
}

// GetByTransactionAndUser 获取用户的接收记录（仅按 user_id 匹配）
func (d *ReceiveRecordDao) GetByTransactionAndUser(ctx context.Context, transactionID, userID string) (*ReceiveRecord, error) {
	record, err := mongoutil.FindOne[*ReceiveRecord](ctx, d.Collection, bson.M{
		"transaction_id": transactionID,
		"user_id":        userID,
	})
	if err != nil {
		//log.ZError(ctx, "获取接收记录失败", err, "transaction_id", transactionID, "user_id", userID)
		return nil, errs.NewCodeError(freeErrors.ErrTransactionNotFound, freeErrors.ErrorMessages[freeErrors.ErrTransactionNotFound])
	}
	return record, nil
}

// GetByTransactionAndUserOrImID 按 user_id 或 user_im_id 查询接收记录（兼容 token 为 IM 用户 ID 时查不到的问题）
func (d *ReceiveRecordDao) GetByTransactionAndUserOrImID(ctx context.Context, transactionID, opUserID string) (*ReceiveRecord, error) {
	record, err := mongoutil.FindOne[*ReceiveRecord](ctx, d.Collection, bson.M{
		"transaction_id": transactionID,
		"$or": []bson.M{
			{"user_id": opUserID},
			{"user_im_id": opUserID},
		},
	})
	if err != nil {
		return nil, errs.NewCodeError(freeErrors.ErrTransactionNotFound, freeErrors.ErrorMessages[freeErrors.ErrTransactionNotFound])
	}
	return record, nil
}

// GetByTransactionID 根据交易ID获取所有接收记录
func (d *ReceiveRecordDao) GetByTransactionID(ctx context.Context, transactionID string) ([]*ReceiveRecord, error) {
	records, err := mongoutil.Find[*ReceiveRecord](ctx, d.Collection,
		bson.M{"transaction_id": transactionID},
		options.Find().SetSort(bson.M{"received_at": 1})) // 按接收时间升序
	if err != nil {
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	return records, nil
}

// GetByTransactionIDPaged 根据交易ID分页获取接收记录（按接收时间倒序，最新在前）
func (d *ReceiveRecordDao) GetByTransactionIDPaged(ctx context.Context, transactionID string, pageNum, pageSize int) ([]*ReceiveRecord, error) {
	if pageNum <= 0 || pageSize <= 0 {
		// 回退到完整列表，兼容旧调用
		return d.GetByTransactionID(ctx, transactionID)
	}
	skip := int64((pageNum - 1) * pageSize)
	limit := int64(pageSize)

	opts := options.Find().
		SetSort(bson.M{"received_at": -1}). // 最新记录在前
		SetSkip(skip).
		SetLimit(limit)

	records, err := mongoutil.Find[*ReceiveRecord](ctx, d.Collection,
		bson.M{"transaction_id": transactionID}, opts)
	if err != nil {
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	return records, nil
}

// CountByTransactionID 统计交易的接收记录数量（性能优化：只返回数量，不返回完整记录）
func (d *ReceiveRecordDao) CountByTransactionID(ctx context.Context, transactionID string) (int64, error) {
	count, err := d.Collection.CountDocuments(ctx, bson.M{"transaction_id": transactionID})
	if err != nil {
		return 0, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	return count, nil
}

// GetUserReceiveHistoryLast24Hours 获取用户24小时内的接收记录
func (d *ReceiveRecordDao) GetUserReceiveHistoryLast24Hours(ctx context.Context, userID string) ([]*ReceiveRecord, error) {
	// 计算24小时前的时间点
	twentyFourHoursAgo := time.Now().UTC().Add(-time.Duration(constant.TransactionExpireTime) * time.Second)

	// 查询条件：用户ID匹配且接收时间在过去24小时内
	filter := bson.M{
		"user_im_id": userID,
		"received_at": bson.M{
			"$gte": twentyFourHoursAgo,
		},
	}

	// 按接收时间倒序排列
	opts := options.Find().SetSort(bson.M{"received_at": -1})

	records, err := mongoutil.Find[*ReceiveRecord](ctx, d.Collection, filter, opts)
	if err != nil {
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	return records, nil
}

// GetUserSentTransactionsLast24Hours 获取用户24小时内发起的交易记录
func (d *TransactionDao) GetUserSentTransactionsLast24Hours(ctx context.Context, userID string) ([]*Transaction, error) {
	// 计算24小时前的时间点
	twentyFourHoursAgo := time.Now().UTC().Add(-time.Duration(constant.TransactionExpireTime) * time.Second)

	// 查询条件：发送者ID匹配且创建时间在过去24小时内，且交易状态为已完成
	filter := bson.M{
		"sender_im_id": userID,
		"created_at": bson.M{
			"$gte": twentyFourHoursAgo,
		},
		"status": TransactionStatusComplete,
	}

	// 按创建时间倒序排列
	opts := options.Find().SetSort(bson.M{"created_at": -1})

	transactions, err := mongoutil.Find[*Transaction](ctx, d.Collection, filter, opts)
	if err != nil {
		return nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	return transactions, nil
}

func (d *TransactionDao) ListTransactions(ctx context.Context, req *dto.ListOrgTransactions) (int64, []*TransactionOrg, error) {

	pageSize := req.PageSize
	pageNum := req.PageNum
	skip := (pageNum - 1) * pageSize
	match := make(bson.M)
	match["org_id"] = req.OrgID
	if req.UserID != "" {
		match["sender_im_id"] = req.UserID
	}
	if req.Status != nil {
		match["status"] = req.Status
	}
	if req.StartTime != nil && req.EndTime != nil {
		match["created_at"] = bson.M{"$gte": *req.StartTime, "$lte": *req.EndTime}
	}
	pipe := []bson.M{
		{"$match": match},
		{"$lookup": bson.M{"from": "user", "localField": "sender_im_id", "foreignField": "user_id", "as": "sender_info"}},
		{"$lookup": bson.M{"from": "user", "localField": "target_im_id", "foreignField": "user_id", "as": "receiver_info"}},
		{"$lookup": bson.M{"from": "wallet_currency", "localField": "currency_id", "foreignField": "_id", "as": "currency_info"}},
		{"$addFields": bson.M{
			"sender_name":   bson.M{"$ifNull": []interface{}{bson.M{"$arrayElemAt": []interface{}{"$sender_info.nickname", 0}}, ""}},
			"receiver_name": bson.M{"$ifNull": []interface{}{bson.M{"$arrayElemAt": []interface{}{"$receiver_info.nickname", 0}}, ""}},
			"currency":      bson.M{"$ifNull": []interface{}{bson.M{"$arrayElemAt": []interface{}{"$currency_info.name", 0}}, ""}},
		}},
		{"$limit": pageSize},
		{"$skip": skip},
	}
	transactions, err := mongoutil.Aggregate[*TransactionOrg](ctx, d.Collection, pipe)
	total, err := mongoutil.Count(ctx, d.Collection, match)
	if err != nil {
		return 0, nil, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	return total, transactions, nil
}

// TransactionWithUserInfo 包含用户信息的交易记录（用于聚合查询）
type TransactionWithUserInfo struct {
	Transaction  `bson:",inline"`
	SenderUser   map[string]interface{} `bson:"sender_user,omitempty"`
	SenderAttr   map[string]interface{} `bson:"sender_attr,omitempty"`
	ReceiverUser map[string]interface{} `bson:"receiver_user,omitempty"`
	ReceiverAttr map[string]interface{} `bson:"receiver_attr,omitempty"`
	Currency     map[string]interface{} `bson:"currency,omitempty"`
}

func (d *TransactionDao) QueryTransactionRecordsWithUserInfo(ctx context.Context, orgID string, keyword string, startTime, endTime *time.Time, status *int, transactionType *int, page *paginationUtils.DepPagination) (int64, []*TransactionWithUserInfo, error) {
	// 构建聚合管道
	pipeline := []bson.M{
		// 第一阶段：基础过滤，排除组织账户相关的交易类型
		{
			"$match": bson.M{
				"org_id": orgID,
				"transaction_type": bson.M{
					"$nin": []int{TransactionTypeOrganization, TransactionTypeOrganizationSignInReward},
				},
			},
		},
	}

	// 添加时间过滤
	if startTime != nil || endTime != nil {
		timeFilter := bson.M{}
		if startTime != nil {
			timeFilter["$gte"] = *startTime
		}
		if endTime != nil {
			timeFilter["$lte"] = *endTime
		}
		pipeline = append(pipeline, bson.M{
			"$match": bson.M{
				"created_at": timeFilter,
			},
		})
	}

	// 添加状态过滤
	if status != nil {
		pipeline = append(pipeline, bson.M{
			"$match": bson.M{
				"status": *status,
			},
		})
	}

	// 添加交易类型过滤
	if transactionType != nil {
		pipeline = append(pipeline, bson.M{
			"$match": bson.M{
				"transaction_type": *transactionType,
			},
		})
	}

	pipeline = append(pipeline,
		bson.M{
			"$lookup": bson.M{
				"from":         "user",
				"localField":   "sender_im_id",
				"foreignField": "user_id",
				"as":           "sender_user",
			},
		},
		bson.M{
			"$lookup": bson.M{
				"from":         "attribute",
				"localField":   "sender_id",
				"foreignField": "user_id",
				"as":           "sender_attr",
			},
		},
		bson.M{
			"$lookup": bson.M{
				"from":         "user",
				"localField":   "target_im_id",
				"foreignField": "user_id",
				"as":           "receiver_user",
			},
		},
		bson.M{
			"$lookup": bson.M{
				"from":         "attribute",
				"localField":   "target_id",
				"foreignField": "user_id",
				"as":           "receiver_attr",
			},
		},
		bson.M{
			"$lookup": bson.M{
				"from":         "wallet_currency",
				"localField":   "currency_id",
				"foreignField": "_id",
				"as":           "currency",
			},
		},
	)

	pipeline = append(pipeline, bson.M{
		"$unwind": bson.M{
			"path":                       "$sender_user",
			"preserveNullAndEmptyArrays": true,
		},
	})

	pipeline = append(pipeline, bson.M{
		"$unwind": bson.M{
			"path":                       "$sender_attr",
			"preserveNullAndEmptyArrays": true,
		},
	})

	pipeline = append(pipeline, bson.M{
		"$unwind": bson.M{
			"path":                       "$receiver_user",
			"preserveNullAndEmptyArrays": true,
		},
	})

	pipeline = append(pipeline, bson.M{
		"$unwind": bson.M{
			"path":                       "$receiver_attr",
			"preserveNullAndEmptyArrays": true,
		},
	})

	pipeline = append(pipeline, bson.M{
		"$unwind": bson.M{
			"path":                       "$currency",
			"preserveNullAndEmptyArrays": true,
		},
	})

	if keyword != "" {
		re := bson.M{"$regex": keyword, "$options": "i"}
		pipeline = append(pipeline, bson.M{
			"$match": bson.M{
				"$or": []bson.M{
					{"sender_user.nickname": re},
					{"sender_attr.account": re},
					{"sender_attr.user_id": re},
					{"sender_im_id": re},
				},
			},
		})
	}

	// 获取总数
	countPipeline := append(pipeline, bson.M{"$count": "total"})
	countResult, err := mongoutil.Aggregate[map[string]interface{}](ctx, d.Collection, countPipeline)
	if err != nil {
		return 0, nil, err
	}

	var total int64 = 0
	if len(countResult) > 0 {
		switch v := countResult[0]["total"].(type) {
		case int32:
			total = int64(v)
		case int64:
			total = v
		case float64:
			total = int64(v)
		default:
			total = 0
		}
	}

	// 排序和分页
	pipeline = append(pipeline, bson.M{"$sort": bson.M{"created_at": -1}})
	if page != nil {
		pipeline = append(pipeline, page.ToBsonMList()...)
	}

	// 执行查询
	data, err := mongoutil.Aggregate[*TransactionWithUserInfo](ctx, d.Collection, pipeline)
	if err != nil {
		return 0, nil, err
	}

	return total, data, nil
}

// ReceiveRecordWithUserInfo 包含用户信息的领取记录（用于聚合查询）
type ReceiveRecordWithUserInfo struct {
	ReceiveRecord `bson:",inline"`
	SenderUser    map[string]interface{} `bson:"sender_user,omitempty"`
	SenderAttr    map[string]interface{} `bson:"sender_attr,omitempty"`
	ReceiverUser  map[string]interface{} `bson:"receiver_user,omitempty"`
	ReceiverAttr  map[string]interface{} `bson:"receiver_attr,omitempty"`
	Transaction   map[string]interface{} `bson:"transaction,omitempty"`
}

func (d *ReceiveRecordDao) transactionCollection() *mongo.Collection {
	return d.DB.Collection(Transaction{}.TableName())
}

// transactionTableBaseMatch 交易表上的公共过滤（组织、单笔交易、类型）。
func transactionTableBaseMatch(orgID, transactionID string, transactionType *int) bson.M {
	txMatch := bson.M{}
	if orgID != "" {
		txMatch["org_id"] = orgID
	}
	if transactionID != "" {
		txMatch["transaction_id"] = transactionID
	}
	if transactionType != nil {
		txMatch["transaction_type"] = *transactionType
	} else {
		txMatch["transaction_type"] = bson.M{
			"$nin": []int{TransactionTypeOrganization, TransactionTypeOrganizationSignInReward},
		}
	}
	return txMatch
}

// distinctTransactionIDsForOrgScope 某组织下符合类型等条件的 transaction_id（用于领取表 $in 收窄）。
func (d *ReceiveRecordDao) distinctTransactionIDsForOrgScope(ctx context.Context, orgID, transactionID string, transactionType *int) ([]string, error) {
	filter := transactionTableBaseMatch(orgID, transactionID, transactionType)
	vals, err := d.transactionCollection().Distinct(ctx, "transaction_id", filter)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}

// transactionIDsMatchingSenderKeyword 在 transaction_record 上按发送人昵称/账号/sender_im_id 匹配，得到 transaction_id 列表。
func (d *ReceiveRecordDao) transactionIDsMatchingSenderKeyword(ctx context.Context, orgID, senderKeyword, transactionID string, transactionType *int) ([]string, error) {
	kw := strings.TrimSpace(senderKeyword)
	if kw == "" {
		return nil, nil
	}
	re := bson.M{"$regex": kw, "$options": "i"}
	txMatch := transactionTableBaseMatch(orgID, transactionID, transactionType)
	pipeline := []bson.M{
		{"$match": txMatch},
		{"$lookup": bson.M{"from": "user", "localField": "sender_im_id", "foreignField": "user_id", "as": "sender_user"}},
		{"$lookup": bson.M{"from": "attribute", "localField": "sender_id", "foreignField": "user_id", "as": "sender_attr"}},
		{"$unwind": bson.M{"path": "$sender_user", "preserveNullAndEmptyArrays": true}},
		{"$unwind": bson.M{"path": "$sender_attr", "preserveNullAndEmptyArrays": true}},
		{"$match": bson.M{"$or": []bson.M{
			{"sender_user.nickname": re},
			{"sender_attr.account": re},
			{"sender_attr.user_id": re},
			{"sender_im_id": re},
		}}},
		{"$group": bson.M{"_id": "$transaction_id"}},
	}
	type idRow struct {
		ID string `bson:"_id"`
	}
	rows, err := mongoutil.Aggregate[idRow](ctx, d.transactionCollection(), pipeline)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if r.ID != "" {
			out = append(out, r.ID)
		}
	}
	return out, nil
}

// buildReceiveRecordFilter 领取表 $match：时间、类型、transaction_id 范围（组织用 $in 列表或发送人预筛选列表）。
func buildReceiveRecordFilter(startTime, endTime *time.Time, transactionID string, transactionType *int, scopeTxIDs []string) bson.M {
	m := bson.M{}
	if transactionType != nil {
		m["transaction_type"] = *transactionType
	} else {
		m["transaction_type"] = bson.M{
			"$nin": []int{TransactionTypeOrganization, TransactionTypeOrganizationSignInReward},
		}
	}
	if startTime != nil || endTime != nil {
		tf := bson.M{}
		if startTime != nil {
			tf["$gte"] = *startTime
		}
		if endTime != nil {
			tf["$lte"] = *endTime
		}
		m["received_at"] = tf
	}
	if transactionID != "" {
		if len(scopeTxIDs) > 0 {
			m["$and"] = []bson.M{
				{"transaction_id": transactionID},
				{"transaction_id": bson.M{"$in": scopeTxIDs}},
			}
		} else {
			m["transaction_id"] = transactionID
		}
	} else if len(scopeTxIDs) > 0 {
		m["transaction_id"] = bson.M{"$in": scopeTxIDs}
	}
	return m
}

// lookupTransactionThenUnwindOptional 分页后再关联交易；无交易文档时保留领取行，发送方 lookup 为空。
func lookupTransactionThenUnwindOptional() []bson.M {
	return []bson.M{
		{"$lookup": bson.M{
			"from":         Transaction{}.TableName(),
			"localField":   "transaction_id",
			"foreignField": "transaction_id",
			"as":           "transaction",
		}},
		{"$unwind": bson.M{"path": "$transaction", "preserveNullAndEmptyArrays": true}},
	}
}

// appendReceiverUserLookupsForKeyword 仅接收方 user/attribute，用于接收人关键词过滤（在领取表上执行）。
func appendReceiverUserLookupsForKeyword(pipeline []bson.M) []bson.M {
	pipeline = append(pipeline,
		bson.M{"$lookup": bson.M{"from": "user", "localField": "user_im_id", "foreignField": "user_id", "as": "receiver_user"}},
		bson.M{"$lookup": bson.M{"from": "attribute", "localField": "user_id", "foreignField": "user_id", "as": "receiver_attr"}},
		bson.M{"$unwind": bson.M{"path": "$receiver_user", "preserveNullAndEmptyArrays": true}},
		bson.M{"$unwind": bson.M{"path": "$receiver_attr", "preserveNullAndEmptyArrays": true}},
	)
	return pipeline
}

// 在已有管道后追加 4 个 user/attr lookup 与 unwind（用于当前页数据）
func appendReceiveRecordUserLookups(pipeline []bson.M) []bson.M {
	pipeline = append(pipeline,
		bson.M{"$lookup": bson.M{"from": "user", "localField": "transaction.sender_im_id", "foreignField": "user_id", "as": "sender_user"}},
		bson.M{"$lookup": bson.M{"from": "attribute", "localField": "transaction.sender_id", "foreignField": "user_id", "as": "sender_attr"}},
		bson.M{"$lookup": bson.M{"from": "user", "localField": "user_im_id", "foreignField": "user_id", "as": "receiver_user"}},
		bson.M{"$lookup": bson.M{"from": "attribute", "localField": "user_id", "foreignField": "user_id", "as": "receiver_attr"}},
	)
	pipeline = append(pipeline,
		bson.M{"$unwind": bson.M{"path": "$sender_user", "preserveNullAndEmptyArrays": true}},
		bson.M{"$unwind": bson.M{"path": "$sender_attr", "preserveNullAndEmptyArrays": true}},
		bson.M{"$unwind": bson.M{"path": "$receiver_user", "preserveNullAndEmptyArrays": true}},
		bson.M{"$unwind": bson.M{"path": "$receiver_attr", "preserveNullAndEmptyArrays": true}},
	)
	return pipeline
}

// appendReceiveRecordSenderLookupsOnly 根上已有 transaction 与 receiver_user/receiver_attr 时，仅补发送方 lookup（避免关键词路径重复查接收方）。
func appendReceiveRecordSenderLookupsOnly(pipeline []bson.M) []bson.M {
	pipeline = append(pipeline,
		bson.M{"$lookup": bson.M{"from": "user", "localField": "transaction.sender_im_id", "foreignField": "user_id", "as": "sender_user"}},
		bson.M{"$lookup": bson.M{"from": "attribute", "localField": "transaction.sender_id", "foreignField": "user_id", "as": "sender_attr"}},
	)
	pipeline = append(pipeline,
		bson.M{"$unwind": bson.M{"path": "$sender_user", "preserveNullAndEmptyArrays": true}},
		bson.M{"$unwind": bson.M{"path": "$sender_attr", "preserveNullAndEmptyArrays": true}},
	)
	return pipeline
}

//	查询带用户信息的领取记录（根据组织过滤）
//
// 无发送/接收关键词：仅在 transaction_receive_record 上 $match（组织通过 transaction_id $in 预筛）→ 排序分页 → lookup 交易 → 发送/接收方信息；交易行缺失时发送方为空。
// 有发送人关键词：先在 transaction_record 上解析出 transaction_id，再查领取表。
// 有接收人关键词：在领取表上 lookup 接收方 user/attr 后 $match，$facet 内再分页并关联交易。
func (d *ReceiveRecordDao) QueryReceiveRecordsWithUserInfo(ctx context.Context, orgID string, senderKeyword, receiverKeyword string, startTime, endTime *time.Time, transactionID string, transactionType *int, page *paginationUtils.DepPagination) (int64, []*ReceiveRecordWithUserInfo, error) {
	recvColl := d.Collection
	hasSenderKW := strings.TrimSpace(senderKeyword) != ""
	hasRecvKW := strings.TrimSpace(receiverKeyword) != ""

	var scopeTxIDs []string
	var err error
	if hasSenderKW {
		scopeTxIDs, err = d.transactionIDsMatchingSenderKeyword(ctx, orgID, senderKeyword, transactionID, transactionType)
		if err != nil {
			return 0, nil, err
		}
		if len(scopeTxIDs) == 0 {
			return 0, []*ReceiveRecordWithUserInfo{}, nil
		}
	}
	//else if orgID != "" {
	//	scopeTxIDs, err = d.distinctTransactionIDsForOrgScope(ctx, orgID, transactionID, transactionType)
	//	if err != nil {
	//		return 0, nil, err
	//	}
	//	if len(scopeTxIDs) == 0 {
	//		return 0, []*ReceiveRecordWithUserInfo{}, nil
	//	}
	//}

	recvFilter := buildReceiveRecordFilter(startTime, endTime, transactionID, transactionType, scopeTxIDs)

	parseTotal := func(countResult []map[string]interface{}) int64 {
		if len(countResult) == 0 {
			return 0
		}
		switch v := countResult[0]["total"].(type) {
		case int32:
			return int64(v)
		case int64:
			return v
		case float64:
			return int64(v)
		default:
			return 0
		}
	}

	facetListStages := []bson.M{{"$sort": bson.M{"received_at": -1}}}
	if page != nil {
		facetListStages = append(facetListStages, page.ToBsonMList()...)
	}
	facetListStages = append(facetListStages, lookupTransactionThenUnwindOptional()...)
	if hasRecvKW {
		facetListStages = appendReceiveRecordSenderLookupsOnly(facetListStages)
	} else {
		facetListStages = appendReceiveRecordUserLookups(facetListStages)
	}

	if !hasSenderKW && !hasRecvKW {
		countPipeline := []bson.M{
			{"$match": recvFilter},
			{"$count": "total"},
		}
		countResult, err := mongoutil.Aggregate[map[string]interface{}](ctx, recvColl, countPipeline)
		if err != nil {
			return 0, nil, errs.Wrap(err)
		}
		total := parseTotal(countResult)

		listPipeline := []bson.M{{"$match": recvFilter}, {"$sort": bson.M{"received_at": -1}}}
		if page != nil {
			listPipeline = append(listPipeline, page.ToBsonMList()...)
		}
		listPipeline = append(listPipeline, lookupTransactionThenUnwindOptional()...)
		listPipeline = appendReceiveRecordUserLookups(listPipeline)
		data, err := mongoutil.Aggregate[*ReceiveRecordWithUserInfo](ctx, recvColl, listPipeline)
		if err != nil {
			return 0, nil, errs.Wrap(err)
		}
		return total, data, nil
	}

	re := bson.M{"$regex": receiverKeyword, "$options": "i"}
	recvKeywordMatch := bson.M{"$or": []bson.M{
		{"receiver_user.nickname": re},
		{"receiver_attr.account": re},
		{"receiver_attr.user_id": re},
		{"user_im_id": re},
	}}

	pipeBeforeFacet := []bson.M{{"$match": recvFilter}}
	if hasRecvKW {
		pipeBeforeFacet = appendReceiverUserLookupsForKeyword(pipeBeforeFacet)
		pipeBeforeFacet = append(pipeBeforeFacet, bson.M{"$match": recvKeywordMatch})
	}

	pipeBeforeFacet = append(pipeBeforeFacet, bson.M{
		"$facet": bson.M{
			"total": []bson.M{{"$count": "total"}},
			"list":  facetListStages,
		},
	})

	type facetResult struct {
		Total []map[string]interface{}     `bson:"total"`
		List  []*ReceiveRecordWithUserInfo `bson:"list"`
	}
	facetOut, err := mongoutil.Aggregate[*facetResult](ctx, recvColl, pipeBeforeFacet)
	if err != nil {
		return 0, nil, errs.Wrap(err)
	}
	if len(facetOut) == 0 {
		return 0, []*ReceiveRecordWithUserInfo{}, nil
	}
	doc := facetOut[0]
	total := parseTotal(doc.Total)
	list := doc.List
	if list == nil {
		list = []*ReceiveRecordWithUserInfo{}
	}
	return total, list, nil
}
