package model

import (
	"context"
	"sync"
	"time"

	organizationModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/constant"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const AppLogTTLSeconds int32 = 7 * 24 * 60 * 60

var (
	ensureIndexMu    sync.Mutex
	ensureIndexReady bool
)

type AppLog struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	OrgID          primitive.ObjectID `bson:"org_id" json:"org_id"`
	UserID         string             `bson:"user_id" json:"user_id"`
	ImServerUserID string             `bson:"im_server_user_id" json:"im_server_user_id"`
	BatchID        string             `bson:"batch_id" json:"batch_id"`
	SessionID      string             `bson:"session_id" json:"session_id"`
	DeviceID       string             `bson:"device_id" json:"device_id"`
	Platform       int32              `bson:"platform" json:"platform"`
	SystemType     string             `bson:"system_type" json:"system_type"`
	AppVersion     string             `bson:"app_version" json:"app_version"`
	Level          string             `bson:"level" json:"level"`
	Tag            string             `bson:"tag" json:"tag"`
	Message        string             `bson:"message" json:"message"`
	Stack          string             `bson:"stack,omitempty" json:"stack,omitempty"`
	Extra          bson.M             `bson:"extra,omitempty" json:"extra,omitempty"`
	Reason         string             `bson:"reason" json:"reason"`
	SourceIP       string             `bson:"source_ip" json:"source_ip"`
	ClientTime     time.Time          `bson:"client_time" json:"client_time"`
	ServerTime     time.Time          `bson:"server_time" json:"server_time"`
}

func (AppLog) TableName() string {
	return constant.CollectionAppLog
}

func EnsureAppLogIndexes(db *mongo.Database) error {
	ensureIndexMu.Lock()
	defer ensureIndexMu.Unlock()
	if ensureIndexReady {
		return nil
	}
	if err := CreateAppLogIndexes(db); err != nil {
		return err
	}
	ensureIndexReady = true
	return nil
}

func CreateAppLogIndexes(db *mongo.Database) error {
	coll := db.Collection(AppLog{}.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "server_time", Value: 1}},
			Options: options.Index().SetName("app_log_server_time_ttl").SetExpireAfterSeconds(AppLogTTLSeconds),
		},
		{
			Keys:    bson.D{{Key: "org_id", Value: 1}, {Key: "server_time", Value: -1}},
			Options: options.Index().SetName("app_log_org_time"),
		},
		{
			Keys:    bson.D{{Key: "org_id", Value: 1}, {Key: "im_server_user_id", Value: 1}, {Key: "server_time", Value: -1}},
			Options: options.Index().SetName("app_log_org_im_user_time"),
		},
		{
			Keys:    bson.D{{Key: "org_id", Value: 1}, {Key: "level", Value: 1}, {Key: "server_time", Value: -1}},
			Options: options.Index().SetName("app_log_org_level_time"),
		},
		{
			Keys:    bson.D{{Key: "org_id", Value: 1}, {Key: "device_id", Value: 1}, {Key: "server_time", Value: -1}},
			Options: options.Index().SetName("app_log_org_device_time"),
		},
	})
	return err
}

type AppLogDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewAppLogDao(db *mongo.Database) *AppLogDao {
	return &AppLogDao{
		DB:         db,
		Collection: db.Collection(AppLog{}.TableName()),
	}
}

func (d *AppLogDao) InsertMany(ctx context.Context, rows []*AppLog) error {
	if len(rows) == 0 {
		return nil
	}
	if err := EnsureAppLogIndexes(d.DB); err != nil {
		return err
	}
	return mongoutil.InsertMany(ctx, d.Collection, rows)
}

type AppLogJoinAll struct {
	*AppLog `bson:",inline"`

	User      map[string]interface{} `bson:"user"`
	Attribute map[string]interface{} `bson:"attribute"`
	OrgUser   map[string]interface{} `bson:"org_user"`
}

type AppLogSearchFilter struct {
	OrgID          primitive.ObjectID
	Keyword        string
	UserID         string
	ImServerUserID string
	Level          string
	DeviceID       string
	SessionID      string
	AppVersion     string
	Reason         string
	Platform       int32
	StartTime      time.Time
	EndTime        time.Time
}

func (d *AppLogDao) Search(ctx context.Context, filter AppLogSearchFilter, page *paginationUtils.DepPagination) (int64, []*AppLogJoinAll, error) {
	if err := EnsureAppLogIndexes(d.DB); err != nil {
		return 0, nil, err
	}

	baseMatch := bson.M{}
	if !filter.OrgID.IsZero() {
		baseMatch["org_id"] = filter.OrgID
	}
	if filter.UserID != "" {
		baseMatch["user_id"] = filter.UserID
	}
	if filter.ImServerUserID != "" {
		baseMatch["im_server_user_id"] = filter.ImServerUserID
	}
	if filter.Level != "" {
		baseMatch["level"] = filter.Level
	}
	if filter.DeviceID != "" {
		baseMatch["device_id"] = filter.DeviceID
	}
	if filter.SessionID != "" {
		baseMatch["session_id"] = filter.SessionID
	}
	if filter.AppVersion != "" {
		baseMatch["app_version"] = filter.AppVersion
	}
	if filter.Reason != "" {
		baseMatch["reason"] = filter.Reason
	}
	if filter.Platform > 0 {
		baseMatch["platform"] = filter.Platform
	}
	timeMatch := bson.M{}
	if !filter.StartTime.IsZero() {
		timeMatch["$gte"] = filter.StartTime
	}
	if !filter.EndTime.IsZero() {
		timeMatch["$lte"] = filter.EndTime
	}
	if len(timeMatch) > 0 {
		baseMatch["server_time"] = timeMatch
	}

	pipeline := make([]bson.M, 0, 10)
	if len(baseMatch) > 0 {
		pipeline = append(pipeline, bson.M{"$match": baseMatch})
	}
	pipeline = append(pipeline,
		bson.M{"$lookup": bson.M{
			"from":         constant.CollectionUser,
			"localField":   "im_server_user_id",
			"foreignField": "user_id",
			"as":           "user",
		}},
		bson.M{"$unwind": bson.M{"path": "$user", "preserveNullAndEmptyArrays": true}},
		bson.M{"$lookup": bson.M{
			"from":         chatModel.Attribute{}.TableName(),
			"localField":   "user_id",
			"foreignField": "user_id",
			"as":           "attribute",
		}},
		bson.M{"$unwind": bson.M{"path": "$attribute", "preserveNullAndEmptyArrays": true}},
		bson.M{"$lookup": bson.M{
			"from":         organizationModel.OrganizationUser{}.TableName(),
			"localField":   "user_id",
			"foreignField": "user_id",
			"as":           "org_user",
		}},
		bson.M{"$unwind": bson.M{"path": "$org_user", "preserveNullAndEmptyArrays": true}},
	)

	if filter.Keyword != "" {
		regex := bson.M{"$regex": filter.Keyword, "$options": "i"}
		pipeline = append(pipeline, bson.M{"$match": bson.M{"$or": []bson.M{
			{"message": regex},
			{"tag": regex},
			{"user_id": regex},
			{"im_server_user_id": regex},
			{"device_id": regex},
			{"user.nickname": regex},
			{"attribute.account": regex},
			{"attribute.nickname": regex},
		}}})
	}

	countPipeline := append([]bson.M{}, pipeline...)
	countPipeline = append(countPipeline, bson.M{"$count": "total"})

	var countResult []bson.M
	cursor, err := d.Collection.Aggregate(ctx, countPipeline)
	if err != nil {
		return 0, nil, err
	}
	defer cursor.Close(ctx)
	if err = cursor.All(ctx, &countResult); err != nil {
		return 0, nil, err
	}

	pipeline = append(pipeline, bson.M{"$sort": bson.M{"server_time": -1}})
	if page != nil {
		pipeline = append(pipeline, page.ToBsonMList()...)
	}

	rows, err := mongoutil.Aggregate[*AppLogJoinAll](ctx, d.Collection, pipeline)
	if err != nil {
		return 0, nil, err
	}

	return extractTotal(countResult), rows, nil
}

func extractTotal(countResult []bson.M) int64 {
	if len(countResult) == 0 {
		return 0
	}
	switch v := countResult[0]["total"].(type) {
	case int32:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return 0
	}
}
