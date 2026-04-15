package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// PointsType 积分类型
type PointsType int8

const (
	PointsTypeCheckin PointsType = 1 // 签到
)

type Points struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`                  // ID
	Points         int64              `bson:"points" json:"points"`                               // 积分数量
	ImServerUserId string             `bson:"im_server_user_id" json:"im_server_user_id"`         // 用户IM服务器ID（子账户ID）
	OrganizationId primitive.ObjectID `bson:"organization_id" json:"organization_id"`             // 组织ID
	PointsType     PointsType         `bson:"points_type" json:"points_type"`                     // 积分类型
	Source         string             `bson:"source,omitempty" json:"source,omitempty"`           // 来源
	Description    string             `bson:"description,omitempty" json:"description,omitempty"` // 积分发放描述
	CreatedAt      time.Time          `bson:"created_at" json:"created_at"`                       // 创建时间
	UpdatedAt      time.Time          `bson:"updated_at" json:"updated_at"`                       // 更新时间
}

func (Points) TableName() string {
	return constant.CollectionPoints
}

type PointsDao struct {
	Collection *mongo.Collection
	DB         *mongo.Database
}

func NewPointsDao(db *mongo.Database) *PointsDao {
	return &PointsDao{
		Collection: db.Collection(Points{}.TableName()),
		DB:         db,
	}
}

// Create 创建积分记录
func (p *PointsDao) Create(ctx context.Context, obj *Points) error {
	obj.CreatedAt = time.Now().UTC()
	obj.UpdatedAt = time.Now().UTC()
	_, err := p.Collection.InsertOne(ctx, obj)
	return err
}

// PointsWithUserInfo 包含用户信息的积分记录（用于聚合查询）
type PointsWithUserInfo struct {
	Points    `bson:",inline"`
	User      map[string]interface{} `bson:"user,omitempty"`
	Attribute map[string]interface{} `bson:"attribute,omitempty"`
}

// QueryPointsRecordsWithUserInfo 查询带用户信息的积分记录（根据组织过滤）
func (p *PointsDao) QueryPointsRecordsWithUserInfo(ctx context.Context, orgID primitive.ObjectID, keyword string, minPoints, maxPoints *int64, pointsType *PointsType, startTime, endTime *time.Time, page *paginationUtils.DepPagination) (int64, []*PointsWithUserInfo, error) {
	// 构建聚合管道
	pipeline := []bson.M{
		// 第一阶段：基础过滤
		{
			"$match": bson.M{
				"organization_id": orgID,
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

	// 添加积分类型过滤
	if pointsType != nil {
		pipeline = append(pipeline, bson.M{
			"$match": bson.M{
				"points_type": *pointsType,
			},
		})
	}

	// 添加积分数量范围过滤
	if minPoints != nil || maxPoints != nil {
		pointsFilter := bson.M{}
		if minPoints != nil {
			pointsFilter["$gte"] = *minPoints
		}
		if maxPoints != nil {
			pointsFilter["$lte"] = *maxPoints
		}
		pipeline = append(pipeline, bson.M{
			"$match": bson.M{
				"points": pointsFilter,
			},
		})
	}

	// 连接用户信息
	pipeline = append(pipeline, bson.M{
		"$lookup": bson.M{
			"from":         "user",
			"localField":   "im_server_user_id",
			"foreignField": "user_id",
			"as":           "user",
		},
	})

	// 连接用户属性信息
	pipeline = append(pipeline, bson.M{
		"$lookup": bson.M{
			"from":         "attribute",
			"localField":   "im_server_user_id",
			"foreignField": "user_id",
			"as":           "attribute",
		},
	})

	// 展开数组字段
	pipeline = append(pipeline, bson.M{
		"$unwind": bson.M{
			"path":                       "$user",
			"preserveNullAndEmptyArrays": true,
		},
	})

	pipeline = append(pipeline, bson.M{
		"$unwind": bson.M{
			"path":                       "$attribute",
			"preserveNullAndEmptyArrays": true,
		},
	})

	// 关键词搜索过滤
	if keyword != "" {
		pipeline = append(pipeline, bson.M{
			"$match": bson.M{
				"$or": []bson.M{
					{"user.nickname": bson.M{"$regex": keyword, "$options": "i"}},
					{"attribute.account": bson.M{"$regex": keyword, "$options": "i"}},
					{"im_server_user_id": bson.M{"$regex": keyword, "$options": "i"}},
				},
			},
		})
	}

	// 获取总数
	countPipeline := append(pipeline, bson.M{"$count": "total"})
	countResult, err := mongoutil.Aggregate[map[string]interface{}](ctx, p.Collection, countPipeline)
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
	data, err := mongoutil.Aggregate[*PointsWithUserInfo](ctx, p.Collection, pipeline)
	if err != nil {
		return 0, nil, err
	}

	return total, data, nil
}

// QueryUserPointsRecords 查询用户积分记录列表（用户端）
func (p *PointsDao) QueryUserPointsRecords(ctx context.Context, imServerUserId string, orgID primitive.ObjectID, page *paginationUtils.DepPagination) (int64, []*Points, error) {
	filter := bson.M{
		"im_server_user_id": imServerUserId,
		"organization_id":   orgID,
	}

	// 获取总数
	total, err := mongoutil.Count(ctx, p.Collection, filter)
	if err != nil {
		return 0, nil, err
	}

	// 构建查询选项
	var opts []*options.FindOptions
	if page != nil {
		// 添加分页选项
		offset := (page.GetPageNumber() - 1) * page.GetShowNumber()
		opts = append(opts, options.Find().SetSkip(int64(offset)))
		opts = append(opts, options.Find().SetLimit(int64(page.GetShowNumber())))
	}
	// 按创建时间倒序排列
	opts = append(opts, options.Find().SetSort(bson.M{"created_at": -1}))

	// 查询数据
	data, err := mongoutil.Find[*Points](ctx, p.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	return total, data, nil
}
