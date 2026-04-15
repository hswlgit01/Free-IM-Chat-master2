package model

import (
	"context"
	"github.com/openimsdk/chat/freechat/constant"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type LivestreamStatisticsStatus string

const (
	LivestreamStatisticsStatusStop  LivestreamStatisticsStatus = "stop"
	LivestreamStatisticsStatusStart LivestreamStatisticsStatus = "start"
)

// LivestreamStatistics 直播数据统计
type LivestreamStatistics struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	RoomName  string             `bson:"room_name" json:"room_name"`   // 房间名称
	Nickname  string             `bson:"nickname" json:"nickname"`     // 房间昵称
	CreatorId string             `bson:"creator_id" json:"creator_id"` // 创建人id
	OrgId     primitive.ObjectID `bson:"org_id" json:"org_id"`
	Cover     string             `bson:"cover" json:"cover"`
	Detail    string             `bson:"detail" json:"detail"`

	TotalRaiseHands int `bson:"total_raise_hands" json:"total_raise_hands"` // 举手总数
	TotalUsers      int `bson:"total_users" json:"total_users"`             // 进入直播的总人数
	MaxOnlineUsers  int `bson:"max_online_users" json:"max_online_users"`   // 最多在线人数
	TotalOnStage    int `bson:"total_on_stage" json:"total_on_stage"`       // 上台总数

	StopTime  time.Time                  `bson:"stop_time" json:"stop_time"`   // 直播结束时间
	StartTime time.Time                  `bson:"start_time" json:"start_time"` // 直播结束时间
	Status    LivestreamStatisticsStatus `bson:"status" json:"status"`         // 直播状态

	RecordFile []string `bson:"record_file" json:"record_file"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"` // 创建时间
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"` // 更新时间
}

func (LivestreamStatistics) TableName() string {
	return constant.CollectionLivestreamStatistics
}

func CreateLivestreamStatisticsIndex(db *mongo.Database) error {
	m := &LivestreamStatistics{}

	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "room_name", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "creator_id", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "stop_time", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "start_time", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "created_at", Value: 1},
			},
		},
	})
	return err
}

type LivestreamStatisticsDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewLivestreamStatisticsDao(db *mongo.Database) *LivestreamStatisticsDao {
	return &LivestreamStatisticsDao{
		DB:         db,
		Collection: db.Collection(LivestreamStatistics{}.TableName()),
	}
}

func (o *LivestreamStatisticsDao) GetById(ctx context.Context, id primitive.ObjectID) (*LivestreamStatistics, error) {
	return mongoutil.FindOne[*LivestreamStatistics](ctx, o.Collection, bson.M{"_id": id})
}

func (o *LivestreamStatisticsDao) GetByRoomName(ctx context.Context, roomName string) (*LivestreamStatistics, error) {
	return mongoutil.FindOne[*LivestreamStatistics](ctx, o.Collection, bson.M{"room_name": roomName})
}

func (o *LivestreamStatisticsDao) Create(ctx context.Context, obj *LivestreamStatistics) error {
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*LivestreamStatistics{obj})
}

func (o *LivestreamStatisticsDao) Select(ctx context.Context, creatorId string,
	startCreatedTime time.Time, endCreatedTime time.Time, page *paginationUtils.DepPagination) (int64, []*LivestreamStatistics, error) {
	filter := bson.M{}
	if creatorId != "" {
		filter["creator_id"] = creatorId
	}

	tsTime := bson.M{}
	if !startCreatedTime.IsZero() {
		tsTime["$gte"] = startCreatedTime
	}
	if !endCreatedTime.IsZero() {
		tsTime["$lte"] = endCreatedTime
	}
	if len(tsTime) > 0 {
		filter["created_at"] = tsTime
	}

	opts := page.ToOptions()

	if len(filter) == 0 {
		filter = nil
	}

	data, err := mongoutil.Find[*LivestreamStatistics](ctx, o.Collection, filter, opts)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}

func (o *LivestreamStatisticsDao) UpdateByRoomName(ctx context.Context, roomName string, param *LivestreamStatistics) error {
	updateField := bson.M{"$set": bson.M{
		"total_raise_hands": param.TotalRaiseHands,
		"total_users":       param.TotalUsers,
		"max_online_users":  param.MaxOnlineUsers,
		"total_on_stage":    param.TotalOnStage,
		"stop_time":         param.StopTime,
		"start_time":        param.StartTime,
		"status":            param.Status,

		"updated_at": time.Now().UTC(),
	}}
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"room_name": roomName}, updateField, false)
}

func (o *LivestreamStatisticsDao) UpdateStatusByRoomName(ctx context.Context, roomName string, status LivestreamStatisticsStatus) error {
	updateField := bson.M{"$set": bson.M{
		"status":     status,
		"updated_at": time.Now().UTC(),
	}}
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"room_name": roomName}, updateField, false)
}

func (o *LivestreamStatisticsDao) UpdateStatusByNotInRoomName(ctx context.Context, notInRoomName []string, status LivestreamStatisticsStatus) (*mongo.UpdateResult, error) {
	updateField := bson.M{"$set": bson.M{
		"status":     status,
		"updated_at": time.Now().UTC(),
	}}

	return mongoutil.UpdateMany(ctx, o.Collection, bson.M{"room_name": bson.M{"$nin": notInRoomName}, "status": LivestreamStatisticsStatusStart}, updateField)
}

func (o *LivestreamStatisticsDao) UpdateRecordFileByRoomName(ctx context.Context, roomName string, recordFile []string) error {
	updateField := bson.M{"$set": bson.M{
		"record_file": recordFile,
		"updated_at":  time.Now().UTC(),
	}}

	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"room_name": roomName}, updateField, false)
}

func (o *LivestreamStatisticsDao) IncTotalUsersByRoomName(ctx context.Context, roomName string, incUsers int64) error {
	filter := bson.M{
		"room_name": roomName,
	}

	update := bson.M{
		"$inc": bson.M{"total_users": incUsers},
		"$set": bson.M{"updated_at": time.Now().UTC()},
	}

	return mongoutil.UpdateOne(ctx, o.Collection, filter, update, false)
}

type LivestreamStatisticsJoinUser struct {
	*LivestreamStatistics `bson:",inline"`
	User                  map[string]interface{} `bson:"user"`
}

func (o *LivestreamStatisticsDao) SelectJoinUser(ctx context.Context, creatorId, keyword string, orgId primitive.ObjectID, status LivestreamStatisticsStatus,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (int64, []*LivestreamStatisticsJoinUser, error) {
	// 聚合查询
	pipeline := []bson.M{
		//{
		//	"$lookup": bson.M{
		//		"from":         orgModel.OrganizationUser{}.TableName(),
		//		"localField":   "creator_id",
		//		"foreignField": "im_server_user_id",
		//		"as":           "organization_user",
		//	},
		//},
		//{
		//	"$unwind": bson.M{
		//		"path":                       "$organization_user",
		//		"preserveNullAndEmptyArrays": false,
		//	},
		//},

		{
			"$lookup": bson.M{
				"from":         openImModel.User{}.TableName(),
				"localField":   "creator_id",
				"foreignField": "user_id",
				"as":           "user",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$user",
				"preserveNullAndEmptyArrays": false,
			},
		},
	}

	// 构建过滤条件
	filter := bson.M{}

	if creatorId != "" {
		filter["creator_id"] = creatorId
	}

	if !orgId.IsZero() {
		filter["org_id"] = orgId
	}

	if status != "" {
		filter["status"] = status
	}

	tsTime := bson.M{}
	if !startTime.IsZero() {
		tsTime["$gte"] = startTime
	}
	if !endTime.IsZero() {
		tsTime["$lte"] = endTime
	}
	if len(tsTime) > 0 {
		filter["created_at"] = tsTime
	}

	if keyword != "" {
		filter["$or"] = []bson.M{
			{"user.nickname": bson.M{"$regex": keyword, "$options": "i"}},
			{"user.user_id": bson.M{"$regex": keyword, "$options": "i"}},
			{"nickname": bson.M{"$regex": keyword, "$options": "i"}},
		}
	}

	findPipeline := make([]bson.M, 0)
	countPipeline := make([]bson.M, 0)

	if len(filter) > 0 {
		findPipeline = append(pipeline, bson.M{"$match": filter})
		countPipeline = append(pipeline, bson.M{"$match": filter})
	}

	// 按时间倒序排列：从新到旧
	findPipeline = append(findPipeline, bson.M{"$sort": bson.M{"created_at": -1}})

	// 添加排序和分页
	if page != nil {
		findPipeline = append(findPipeline, page.ToBsonMList()...)
	}

	// 执行聚合查询获取数据
	data, err := mongoutil.Aggregate[*LivestreamStatisticsJoinUser](ctx, o.Collection, findPipeline)
	if err != nil {
		return 0, nil, err
	}

	countPipeline = append(countPipeline, bson.M{"$count": "total"})

	var countResult []bson.M
	cursor, err := o.Collection.Aggregate(ctx, countPipeline)
	if err != nil {
		return 0, nil, err
	}
	if err = cursor.All(ctx, &countResult); err != nil {
		return 0, nil, err
	}

	total := int32(0)
	if len(countResult) > 0 {
		total = countResult[0]["total"].(int32)
	}

	return int64(total), data, nil
}
