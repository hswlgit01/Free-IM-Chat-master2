package model

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type UserLoginRecord struct {
	UserID    string    `bson:"user_id"`
	LoginTime time.Time `bson:"login_time"`
	IP        string    `bson:"ip"`
	DeviceID  string    `bson:"device_id"`
	Platform  string    `bson:"platform"`
}

func (UserLoginRecord) TableName() string {
	return "user_login_record"
}

type UserLoginRecordDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewUserLoginRecordDao(db *mongo.Database) *UserLoginRecordDao {
	return &UserLoginRecordDao{
		DB:         db,
		Collection: db.Collection(UserLoginRecord{}.TableName()),
	}
}

// CreateUserLoginRecordIndexes 列表按 user_id 取最新登录、IP 子串筛选等聚合依赖 user_id + login_time
func CreateUserLoginRecordIndexes(db *mongo.Database) error {
	coll := db.Collection(UserLoginRecord{}.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "login_time", Value: -1}}},
	})
	return err
}

func (o *UserLoginRecordDao) Create(ctx context.Context, records ...*UserLoginRecord) error {
	return mongoutil.InsertMany(ctx, o.Collection, records)
}

func (o *UserLoginRecordDao) GetByUserId(ctx context.Context, userId string) (*UserLoginRecord, error) {
	// 添加排序选项，按登录时间倒序排列，获取最新的一条记录
	opts := options.FindOne().SetSort(bson.M{"login_time": -1})
	return mongoutil.FindOne[*UserLoginRecord](ctx, o.Collection, bson.M{"user_id": userId}, opts)
}

func (o *UserLoginRecordDao) CountTotal(ctx context.Context, before *time.Time) (count int64, err error) {
	filter := bson.M{}
	if before != nil {
		filter["create_time"] = bson.M{"$lt": before}
	}
	return mongoutil.Count(ctx, o.Collection, filter)
}

// FindLatestByUserIDs 根据用户ID列表批量获取最新登录记录
// DistinctIPsByUserID 返回该主账号历史上出现过的登录 IP（去重）。
func (o *UserLoginRecordDao) DistinctIPsByUserID(ctx context.Context, userID string) ([]string, error) {
	if userID == "" {
		return nil, nil
	}
	vals, err := o.Collection.Distinct(ctx, "ip", bson.M{"user_id": userID})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		s, _ := v.(string)
		s = trimIP(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out, nil
}

func (o *UserLoginRecordDao) FindLatestByUserIDs(ctx context.Context, userIDs []string) ([]*UserLoginRecord, error) {
	if len(userIDs) == 0 {
		return []*UserLoginRecord{}, nil
	}

	// 过滤空字符串
	validUserIDs := make([]string, 0, len(userIDs))
	for _, userID := range userIDs {
		if userID != "" {
			validUserIDs = append(validUserIDs, userID)
		}
	}

	if len(validUserIDs) == 0 {
		return []*UserLoginRecord{}, nil
	}

	// 简化的聚合查询：数据库层面筛选每个用户的最新记录
	pipeline := []bson.M{
		// 匹配指定用户
		{"$match": bson.M{"user_id": bson.M{"$in": validUserIDs}}},
		// 排序：按用户ID和登录时间
		{"$sort": bson.M{"user_id": 1, "login_time": -1}},
		// 分组：每个用户取最新的一条记录
		{"$group": bson.M{
			"_id":    "$user_id",
			"record": bson.M{"$first": "$$ROOT"},
		}},
		// 返回原始记录结构
		{"$replaceWith": "$record"},
	}

	return mongoutil.Aggregate[*UserLoginRecord](ctx, o.Collection, pipeline)
}

// FindUserIDsByLatestLoginIPContains 返回「最近一条登录记录」的 IP 包含 ipPart（子串、忽略大小写）的用户 user_id 列表。
// user_id 与 organization_user.user_id 一致，供管理端用户列表筛选。
func (o *UserLoginRecordDao) FindUserIDsByLatestLoginIPContains(ctx context.Context, ipPart string) ([]string, error) {
	ipPart = strings.TrimSpace(ipPart)
	if ipPart == "" {
		return nil, nil
	}
	pattern := regexp.QuoteMeta(ipPart)
	pipeline := []bson.M{
		{"$sort": bson.M{"user_id": 1, "login_time": -1}},
		{"$group": bson.M{
			"_id": "$user_id",
			"ip":  bson.M{"$first": "$ip"},
		}},
		{"$match": bson.M{"ip": bson.M{"$regex": pattern, "$options": "i"}}},
	}
	type row struct {
		UserID string `bson:"_id"`
	}
	rows, err := mongoutil.Aggregate[row](ctx, o.Collection, pipeline)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if r.UserID != "" {
			out = append(out, r.UserID)
		}
	}
	return out, nil
}

func (o *UserLoginRecordDao) CountRangeEverydayTotal(ctx context.Context, start *time.Time, end *time.Time) (map[string]int64, int64, error) {
	pipeline := make([]bson.M, 0, 4)
	if start != nil || end != nil {
		filter := bson.M{}
		if start != nil {
			filter["$gte"] = start
		}
		if end != nil {
			filter["$lt"] = end
		}
		pipeline = append(pipeline, bson.M{"$match": bson.M{"login_time": filter}})
	}
	pipeline = append(pipeline,
		bson.M{
			"$project": bson.M{
				"_id":     0,
				"user_id": 1,
				"login_time": bson.M{
					"$dateToString": bson.M{
						"format": "%Y-%m-%d",
						"date":   "$login_time",
					},
				},
			},
		},

		bson.M{
			"$group": bson.M{
				"_id": bson.M{
					"user_id":    "$user_id",
					"login_time": "$login_time",
				},
			},
		},

		bson.M{
			"$group": bson.M{
				"_id": "$_id.login_time",
				"count": bson.M{
					"$sum": 1,
				},
			},
		},
	)

	type Temp struct {
		ID    string `bson:"_id"`
		Count int64  `bson:"count"`
	}
	res, err := mongoutil.Aggregate[Temp](ctx, o.Collection, pipeline)
	if err != nil {
		return nil, 0, err
	}
	var loginCount int64
	countMap := make(map[string]int64, len(res))
	for _, r := range res {
		loginCount += r.Count
		countMap[r.ID] = r.Count
	}
	return countMap, loginCount, nil
}
