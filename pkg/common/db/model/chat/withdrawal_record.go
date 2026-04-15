package chat

import (
	"context"
	"time"

	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func NewWithdrawalRecord(db *mongo.Database) (chat.WithdrawalRecordInterface, error) {
	coll := db.Collection("withdrawal_records")
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "order_no", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{
				{Key: "user_id", Value: 1},
				{Key: "created_at", Value: -1},
			},
		},
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "status", Value: 1},
				{Key: "created_at", Value: -1},
			},
		},
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	return &WithdrawalRecord{coll: coll}, nil
}

type WithdrawalRecord struct {
	coll *mongo.Collection
}

func (w *WithdrawalRecord) Create(ctx context.Context, record *chat.WithdrawalRecord) error {
	if record.ID.IsZero() {
		record.ID = primitive.NewObjectID()
	}
	now := time.Now()
	record.CreatedAt = now
	record.UpdatedAt = now
	return mongoutil.InsertMany(ctx, w.coll, []*chat.WithdrawalRecord{record})
}

func (w *WithdrawalRecord) FindByID(ctx context.Context, id primitive.ObjectID) (*chat.WithdrawalRecord, error) {
	return mongoutil.FindOne[*chat.WithdrawalRecord](ctx, w.coll, bson.M{"_id": id})
}

func (w *WithdrawalRecord) FindByOrderNo(ctx context.Context, orderNo string) (*chat.WithdrawalRecord, error) {
	return mongoutil.FindOne[*chat.WithdrawalRecord](ctx, w.coll, bson.M{"order_no": orderNo})
}

func (w *WithdrawalRecord) FindByUserID(ctx context.Context, userID string, pagination chat.Pagination) ([]*chat.WithdrawalRecord, int64, error) {
	filter := bson.M{"user_id": userID}
	return w.findWithPagination(ctx, filter, pagination)
}

func (w *WithdrawalRecord) Update(ctx context.Context, id primitive.ObjectID, update map[string]any) error {
	if len(update) == 0 {
		return nil
	}
	update["updated_at"] = time.Now()
	return mongoutil.UpdateOne(ctx, w.coll, bson.M{"_id": id}, bson.M{"$set": update}, true)
}

func (w *WithdrawalRecord) UpdateStatus(ctx context.Context, id primitive.ObjectID, status int32, extra map[string]any) error {
	update := make(map[string]any)
	update["status"] = status
	update["updated_at"] = time.Now()

	// 根据状态设置对应的时间字段
	now := time.Now()
	switch status {
	case chat.WithdrawalStatusApproved:
		update["approve_time"] = now
		if approverID, ok := extra["approver_id"]; ok {
			update["approver_id"] = approverID
		}
	case chat.WithdrawalStatusTransferring:
		update["transfer_time"] = now
	case chat.WithdrawalStatusCompleted:
		update["complete_time"] = now
	case chat.WithdrawalStatusRejected:
		if reason, ok := extra["reject_reason"]; ok {
			update["reject_reason"] = reason
		}
		if approverID, ok := extra["approver_id"]; ok {
			update["approver_id"] = approverID
		}
		update["approve_time"] = now
	}

	// 添加额外字段
	for k, v := range extra {
		if _, exists := update[k]; !exists {
			update[k] = v
		}
	}

	return mongoutil.UpdateOne(ctx, w.coll, bson.M{"_id": id}, bson.M{"$set": update}, true)
}

func (w *WithdrawalRecord) FindByOrganization(ctx context.Context, organizationID string, status *int32, keyword string, pagination chat.Pagination) ([]*chat.WithdrawalRecord, int64, error) {
	filter := bson.M{"organization_id": organizationID}
	if status != nil {
		filter["status"] = *status
	}

	// 如果有关键词,添加搜索条件(支持订单号、用户ID、用户账号)
	if keyword != "" {
		// 使用$or条件支持多字段搜索
		// 注意: user_account字段需要从Attribute表join,这里只搜索order_no和user_id
		filter["$or"] = []bson.M{
			{"order_no": bson.M{"$regex": keyword, "$options": "i"}}, // 订单号模糊匹配,不区分大小写
			{"user_id": keyword}, // 用户ID精确匹配
		}
	}

	return w.findWithPagination(ctx, filter, pagination)
}

func (w *WithdrawalRecord) CountByStatus(ctx context.Context, organizationID string) (map[int32]int64, error) {
	pipeline := []bson.M{
		{"$match": bson.M{"organization_id": organizationID}},
		{
			"$group": bson.M{
				"_id":   "$status",
				"count": bson.M{"$sum": 1},
			},
		},
	}

	cursor, err := w.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	defer cursor.Close(ctx)

	result := make(map[int32]int64)
	for cursor.Next(ctx) {
		var doc struct {
			ID    int32 `bson:"_id"`
			Count int64 `bson:"count"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, errs.Wrap(err)
		}
		result[doc.ID] = doc.Count
	}

	return result, nil
}

func (w *WithdrawalRecord) findWithPagination(ctx context.Context, filter bson.M, pagination chat.Pagination) ([]*chat.WithdrawalRecord, int64, error) {
	// 计算总数
	total, err := w.coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, errs.Wrap(err)
	}

	// 查询数据
	opts := options.Find().
		SetSort(bson.D{{Key: "created_at", Value: -1}}).
		SetSkip(int64((pagination.Page - 1) * pagination.PageSize)).
		SetLimit(int64(pagination.PageSize))

	records, err := mongoutil.Find[*chat.WithdrawalRecord](ctx, w.coll, filter, opts)
	if err != nil {
		return nil, 0, errs.Wrap(err)
	}

	return records, total, nil
}
