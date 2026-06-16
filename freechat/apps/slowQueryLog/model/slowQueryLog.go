package model

import (
	"context"
	"sync"
	"time"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const SlowQueryLogTTLSeconds int32 = 30 * 24 * 60 * 60

var (
	ensureSlowQueryIndexMu    sync.Mutex
	ensureSlowQueryIndexReady bool
)

// dawn 2026-06-16 新增慢查询日志：保存超过阈值的 Mongo 查询，供后台排查登录/列表卡顿。
type SlowQueryLog struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	CreatedAt     time.Time          `bson:"created_at" json:"created_at"`
	Timestamp     string             `bson:"timestamp" json:"timestamp"`
	Collection    string             `bson:"collection" json:"collection"`
	Operation     string             `bson:"operation" json:"operation"`
	CompleteQuery string             `bson:"complete_query" json:"complete_query"`
	Duration      string             `bson:"duration" json:"duration"`
	DurationMS    int64              `bson:"duration_ms" json:"duration_ms"`
	Error         string             `bson:"error,omitempty" json:"error,omitempty"`
}

func (SlowQueryLog) TableName() string {
	return constant.CollectionSlowQueryLog
}

type SlowQueryLogSearchFilter struct {
	Keyword       string
	Collection    string
	Operation     string
	MinDurationMS int64
	StartTime     time.Time
	EndTime       time.Time
}

type SlowQueryLogDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewSlowQueryLogDao(db *mongo.Database) *SlowQueryLogDao {
	return &SlowQueryLogDao{
		DB:         db,
		Collection: db.Collection(SlowQueryLog{}.TableName()),
	}
}

func EnsureSlowQueryLogIndexes(db *mongo.Database) error {
	ensureSlowQueryIndexMu.Lock()
	defer ensureSlowQueryIndexMu.Unlock()
	if ensureSlowQueryIndexReady {
		return nil
	}
	coll := db.Collection(SlowQueryLog{}.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "created_at", Value: 1}},
			Options: options.Index().SetName("slow_query_created_at_ttl").SetExpireAfterSeconds(SlowQueryLogTTLSeconds),
		},
		{
			Keys:    bson.D{{Key: "created_at", Value: -1}},
			Options: options.Index().SetName("slow_query_created_at_desc"),
		},
		{
			Keys:    bson.D{{Key: "duration_ms", Value: -1}, {Key: "created_at", Value: -1}},
			Options: options.Index().SetName("slow_query_duration_time"),
		},
		{
			Keys:    bson.D{{Key: "collection", Value: 1}, {Key: "operation", Value: 1}, {Key: "created_at", Value: -1}},
			Options: options.Index().SetName("slow_query_coll_op_time"),
		},
	})
	if err != nil {
		return err
	}
	ensureSlowQueryIndexReady = true
	return nil
}

func (d *SlowQueryLogDao) Search(ctx context.Context, filter SlowQueryLogSearchFilter, page *paginationUtils.DepPagination) (int64, []*SlowQueryLog, error) {
	if err := EnsureSlowQueryLogIndexes(d.DB); err != nil {
		return 0, nil, err
	}

	query := bson.M{}
	if filter.Collection != "" {
		query["collection"] = filter.Collection
	}
	if filter.Operation != "" {
		query["operation"] = filter.Operation
	}
	if filter.MinDurationMS > 0 {
		query["duration_ms"] = bson.M{"$gte": filter.MinDurationMS}
	}
	timeMatch := bson.M{}
	if !filter.StartTime.IsZero() {
		timeMatch["$gte"] = filter.StartTime
	}
	if !filter.EndTime.IsZero() {
		timeMatch["$lte"] = filter.EndTime
	}
	if len(timeMatch) > 0 {
		query["created_at"] = timeMatch
	}
	if filter.Keyword != "" {
		regex := bson.M{"$regex": filter.Keyword, "$options": "i"}
		query["$or"] = []bson.M{
			{"collection": regex},
			{"operation": regex},
			{"complete_query": regex},
			{"error": regex},
		}
	}

	total, err := mongoutil.Count(ctx, d.Collection, query)
	if err != nil {
		return 0, nil, err
	}

	opts := options.Find().SetSort(bson.M{"created_at": -1})
	if page != nil {
		offset := (page.Page - 1) * page.PageSize
		if offset > 0 {
			opts.SetSkip(int64(offset))
		}
		if page.PageSize > 0 {
			opts.SetLimit(int64(page.PageSize))
		}
	}

	rows, err := mongoutil.Find[*SlowQueryLog](ctx, d.Collection, query, opts)
	if err != nil {
		return 0, nil, err
	}
	return total, rows, nil
}
