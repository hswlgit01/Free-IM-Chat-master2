package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// SystemStatistics 系统统计数据结构
type SystemStatistics struct {
	Date  string `json:"date" bson:"date"`   // 日期（格式：20060102）
	Login int32  `json:"login" bson:"login"` // 登录用户数（按邀请人统计暂不使用，保留兼容）

	// 按邀请人维度的统计字段
	InviterImUserId string `json:"inviter_im_user_id" bson:"inviter_im_user_id"` // 邀请人 IM 用户ID（user.user_id）
	Register        int32  `json:"register" bson:"register"`                     // 当日邀请注册人数
	Sign            int32  `json:"sign" bson:"sign"`                             // 当日邀请用户签到人数
	AllCheckin      int32  `json:"all_checkin" bson:"all_checkin"`               // 下级所有签到人数（当日所有被邀请用户签到总数）

	// 新增字段：实名认证相关统计
	Verified   int32 `json:"verified" bson:"verified"`     // 已实名用户数
	Unverified int32 `json:"unverified" bson:"unverified"` // 未实名用户数

	SignByCreatedAt int32 `json:"sign_by_created_at" bson:"sign_by_created_at"` // 按创建时间统计的签到人数
}

// StatisticsCount 统计计数结构
type StatisticsCount struct {
	Date  string `json:"date" bson:"_id"`
	Count int32  `json:"count"`
}

// InviteStatisticsCount 按邀请人+日期的计数结果
type InviteStatisticsCount struct {
	Date            string `bson:"date" json:"date"`
	InviterImUserId string `bson:"inviter_im_user_id" json:"inviter_im_user_id"`
	Count           int32  `bson:"count" json:"count"`
}

// StatisticsResult 统计查询结果
type StatisticsResult struct {
	Collection string             `json:"collection"`
	Data       []*StatisticsCount `json:"data"`
	Err        error              `json:"err"`
}

// StatisticsDao 统计数据访问对象
type StatisticsDao struct {
	DB *mongo.Database
}

// NewStatisticsDao 创建统计DAO实例
func NewStatisticsDao(db *mongo.Database) *StatisticsDao {
	return &StatisticsDao{
		DB: db,
	}
}

// GetDailyUniqueUserCount 获取每日去重用户统计数据
func (o *StatisticsDao) GetDailyUniqueUserCount(ctx context.Context, orgId primitive.ObjectID,
	collection string, orgField string, startTime, endTime time.Time) ([]*StatisticsCount, error) {

	// MongoDB 聚合管道
	pipeline := []bson.M{
		// 第一步：过滤条件
		{
			"$match": bson.M{
				orgField: orgId,
				"created_at": bson.M{
					"$gte": startTime,
					"$lte": endTime,
				},
				// 过滤掉空的im_server_user_id
				"im_server_user_id": bson.M{
					"$exists": true,
					"$nin":    []interface{}{"", nil},
				},
			},
		},
		// 第二步：按日期和用户ID分组（去重）
		{
			"$group": bson.M{
				"_id": bson.M{
					"date": bson.M{
						"$dateToString": bson.M{
							"format":   "%Y%m%d",
							"date":     "$created_at",
							"timezone": shanghaiTimezone,
						},
					},
					"im_server_user_id": "$im_server_user_id",
				},
			},
		},
		// 第三步：按日期分组，统计每天的用户数
		{
			"$group": bson.M{
				"_id": "$_id.date",
				"count": bson.M{
					"$sum": 1,
				},
			},
		},
		// 第四步：按日期排序
		{
			"$sort": bson.M{
				"_id": 1,
			},
		},
	}

	// 执行聚合查询
	coll := plugin.MongoCli().GetDB().Collection(collection)
	return mongoutil.Aggregate[*StatisticsCount](ctx, coll, pipeline)
}

const shanghaiTimezone = "Asia/Shanghai"

// GetDailyUniqueCheckinCountByDate 获取每日签到去重用户数（按 date 字段过滤，分组时使用上海时区）
// 与 GetDailyUniqueUserCount 区别：查询条件使用 date 而非 created_at，分组时按上海时区取日期
func (o *StatisticsDao) GetDailyUniqueCheckinCountByDate(ctx context.Context, orgId primitive.ObjectID,
	collection string, startTime, endTime time.Time) ([]*StatisticsCount, error) {

	pipeline := []bson.M{
		{
			"$match": bson.M{
				"org_id": orgId,
				"date": bson.M{
					"$gte": startTime,
					"$lte": endTime,
				},
				"im_server_user_id": bson.M{
					"$exists": true,
					"$nin":    []interface{}{"", nil},
				},
			},
		},
		{
			"$group": bson.M{
				"_id": bson.M{
					"date": bson.M{
						"$dateToString": bson.M{
							"format":   "%Y%m%d",
							"date":     "$date",
							"timezone": shanghaiTimezone,
						},
					},
					"im_server_user_id": "$im_server_user_id",
				},
			},
		},
		{
			"$group": bson.M{
				"_id":   "$_id.date",
				"count": bson.M{"$sum": 1},
			},
		},
		{
			"$sort": bson.M{"_id": 1},
		},
	}

	coll := plugin.MongoCli().GetDB().Collection(collection)
	return mongoutil.Aggregate[*StatisticsCount](ctx, coll, pipeline)
}

// GetDailyUniqueUserCountExcludeRoles 获取每日去重用户统计数据（排除指定角色）
func (o *StatisticsDao) GetDailyUniqueUserCountExcludeRoles(ctx context.Context, orgId primitive.ObjectID,
	collection string, orgField string, startTime, endTime time.Time, excludeRoles []string) ([]*StatisticsCount, error) {

	// MongoDB 聚合管道
	matchStage := bson.M{
		orgField: orgId,
		"created_at": bson.M{
			"$gte": startTime,
			"$lte": endTime,
		},
		// 过滤掉空的im_server_user_id
		"im_server_user_id": bson.M{
			"$exists": true,
			"$nin":    []interface{}{"", nil},
		},
	}

	// 添加角色过滤条件
	if len(excludeRoles) > 0 {
		matchStage["role"] = bson.M{"$nin": excludeRoles}
	}

	pipeline := []bson.M{
		// 第一步：过滤条件
		{
			"$match": matchStage,
		},
		// 第二步：按日期和用户ID分组（去重）
		{
			"$group": bson.M{
				"_id": bson.M{
					"date": bson.M{
						"$dateToString": bson.M{
							"format":   "%Y%m%d",
							"date":     "$created_at",
							"timezone": "+08:00",
						},
					},
					"im_server_user_id": "$im_server_user_id",
				},
			},
		},
		// 第三步：按日期分组，统计每天的用户数
		{
			"$group": bson.M{
				"_id": "$_id.date",
				"count": bson.M{
					"$sum": 1,
				},
			},
		},
		// 第四步：按日期排序
		{
			"$sort": bson.M{
				"_id": 1,
			},
		},
	}

	// 执行聚合查询
	coll := plugin.MongoCli().GetDB().Collection(collection)
	return mongoutil.Aggregate[*StatisticsCount](ctx, coll, pipeline)
}

// GetDailyVerifiedUserCountByOrg 统计每个自然日完成实名认证的去重用户数（按组织维度）
// - 以 identity_verifications.verify_time 所在自然日为统计日期
// - 只统计 status = 2（已认证）
// - 通过 organization_user 关联过滤到指定组织
func (o *StatisticsDao) GetDailyVerifiedUserCountByOrg(ctx context.Context, orgId primitive.ObjectID,
	startTime, endTime time.Time) ([]*StatisticsCount, error) {

	// 在 identity_verifications 集合上做聚合，并通过 $lookup 关联 organization_user 过滤组织
	pipeline := []bson.M{
		// 1. 先按实名认证状态 + 时间范围过滤
		{
			"$match": bson.M{
				"status": int32(2), // 已认证
				"verify_time": bson.M{
					"$gte": startTime,
					"$lte": endTime,
				},
			},
		},
		// 2. 关联 organization_user，获取组织信息
		{
			"$lookup": bson.M{
				"from":         "organization_user",
				"localField":   "user_id",
				"foreignField": "user_id",
				"as":           "org_user",
			},
		},
		// 3. 展开关联结果
		{
			"$unwind": bson.M{
				"path":                       "$org_user",
				"preserveNullAndEmptyArrays": false,
			},
		},
		// 4. 过滤指定组织
		{
			"$match": bson.M{
				"org_user.organization_id": orgId,
			},
		},
		// 5. 按日期+用户ID去重
		{
			"$group": bson.M{
				"_id": bson.M{
					"date": bson.M{
						"$dateToString": bson.M{
							"format":   "%Y%m%d",
							"date":     "$verify_time",
							"timezone": "+08:00",
						},
					},
					"user_id": "$user_id",
				},
			},
		},
		// 6. 按日期汇总统计人数
		{
			"$group": bson.M{
				"_id": "$_id.date",
				"count": bson.M{
					"$sum": 1,
				},
			},
		},
		// 7. 按日期升序排列
		{
			"$sort": bson.M{
				"_id": 1,
			},
		},
	}

	coll := plugin.MongoCli().GetDB().Collection("identity_verifications")
	return mongoutil.Aggregate[*StatisticsCount](ctx, coll, pipeline)
}

// GetInviteDailyRegisterStats 统计：按邀请人+日期的当日注册人数（organization_user.created_at）
func (o *StatisticsDao) GetInviteDailyRegisterStats(ctx context.Context, orgId primitive.ObjectID,
	startTime, endTime time.Time) ([]*InviteStatisticsCount, error) {
	pipeline := []bson.M{
		{
			"$match": bson.M{
				"organization_id": orgId,
				"created_at": bson.M{
					"$gte": startTime,
					"$lte": endTime,
				},
				// 只统计有邀请人的记录
				"inviter_im_server_user_id": bson.M{
					"$exists": true,
					"$nin":    []interface{}{"", nil},
				},
				// 过滤掉被禁用和后台管理员/超管
				"status": bson.M{"$ne": "Disable"},
				"role": bson.M{
					"$nin": []interface{}{"SuperAdmin", "BackendAdmin"},
				},
			},
		},
		// 按日期+邀请人+被邀请用户去重
		{
			"$group": bson.M{
				"_id": bson.M{
					"date": bson.M{
						"$dateToString": bson.M{
							"format": "%Y%m%d",
							"date":   "$created_at",
						},
					},
					"inviter_im_user_id": "$inviter_im_server_user_id",
					"user_id":            "$user_id",
				},
			},
		},
		// 再按日期+邀请人汇总
		{
			"$group": bson.M{
				"_id": bson.M{
					"date":               "$_id.date",
					"inviter_im_user_id": "$_id.inviter_im_user_id",
				},
				"count": bson.M{
					"$sum": 1,
				},
			},
		},
		{
			"$project": bson.M{
				"date":               "$_id.date",
				"inviter_im_user_id": "$_id.inviter_im_user_id",
				"count":              1,
				"_id":                0,
			},
		},
	}

	coll := plugin.MongoCli().GetDB().Collection("organization_user")
	return mongoutil.Aggregate[*InviteStatisticsCount](ctx, coll, pipeline)
}

// GetInviteDailyVerifiedStats 统计：按邀请人+日期的当日完成实名认证人数（identity_verifications.verify_time）
func (o *StatisticsDao) GetInviteDailyVerifiedStats(ctx context.Context, orgId primitive.ObjectID,
	startTime, endTime time.Time) ([]*InviteStatisticsCount, error) {
	pipeline := []bson.M{
		// 从 organization_user 作为入口，限定组织与邀请人
		{
			"$match": bson.M{
				"organization_id": orgId,
				"status":          bson.M{"$ne": "Disable"},
				"role": bson.M{
					"$nin": []interface{}{"SuperAdmin", "BackendAdmin"},
				},
				"inviter_im_server_user_id": bson.M{
					"$exists": true,
					"$nin":    []interface{}{"", nil},
				},
			},
		},
		// 关联 identity_verifications
		{
			"$lookup": bson.M{
				"from":         "identity_verifications",
				"localField":   "user_id",
				"foreignField": "user_id",
				"as":           "iv",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$iv",
				"preserveNullAndEmptyArrays": false,
			},
		},
		// 过滤实名认证通过且在时间范围内
		{
			"$match": bson.M{
				"iv.status": int32(2),
				"iv.verify_time": bson.M{
					"$gte": startTime,
					"$lte": endTime,
				},
			},
		},
		// 按日期+邀请人+被邀请用户去重
		{
			"$group": bson.M{
				"_id": bson.M{
					"date": bson.M{
						"$dateToString": bson.M{
							"format":   "%Y%m%d",
							"date":     "$iv.verify_time",
							"timezone": "+08:00",
						},
					},
					"inviter_im_user_id": "$inviter_im_server_user_id",
					"user_id":            "$user_id",
				},
			},
		},
		// 再按日期+邀请人汇总
		{
			"$group": bson.M{
				"_id": bson.M{
					"date":               "$_id.date",
					"inviter_im_user_id": "$_id.inviter_im_user_id",
				},
				"count": bson.M{
					"$sum": 1,
				},
			},
		},
		{
			"$project": bson.M{
				"date":               "$_id.date",
				"inviter_im_user_id": "$_id.inviter_im_user_id",
				"count":              1,
				"_id":                0,
			},
		},
	}

	coll := plugin.MongoCli().GetDB().Collection("organization_user")
	return mongoutil.Aggregate[*InviteStatisticsCount](ctx, coll, pipeline)
}

// GetInviteDailyCheckinStats 统计：按邀请人+日期的当日签到人数（checkin.created_at）
func (o *StatisticsDao) GetInviteDailyCheckinStats(ctx context.Context, orgId primitive.ObjectID,
	startTime, endTime time.Time) ([]*InviteStatisticsCount, error) {
	pipeline := []bson.M{
		// 组织用户基础过滤
		{
			"$match": bson.M{
				"organization_id": orgId,
				"status":          bson.M{"$ne": "Disable"},
				"role": bson.M{
					"$nin": []interface{}{"SuperAdmin", "BackendAdmin"},
				},
				"inviter_im_server_user_id": bson.M{
					"$exists": true,
					"$nin":    []interface{}{"", nil},
				},
				"im_server_user_id": bson.M{
					"$exists": true,
					"$nin":    []interface{}{"", nil},
				},
			},
		},
		// 关联 checkin
		{
			"$lookup": bson.M{
				"from":         "checkin",
				"localField":   "im_server_user_id",
				"foreignField": "im_server_user_id",
				"as":           "checkin",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$checkin",
				"preserveNullAndEmptyArrays": false,
			},
		},
		// 时间范围过滤
		{
			"$match": bson.M{
				"checkin.created_at": bson.M{
					"$gte": startTime,
					"$lte": endTime,
				},
			},
		},
		// 按日期+邀请人+被邀请用户去重
		{
			"$group": bson.M{
				"_id": bson.M{
					"date": bson.M{
						"$dateToString": bson.M{
							"format":   "%Y%m%d",
							"date":     "$checkin.created_at",
							"timezone": "+08:00",
						},
					},
					"inviter_im_user_id": "$inviter_im_server_user_id",
					"user_id":            "$user_id",
				},
			},
		},
		// 再按日期+邀请人汇总
		{
			"$group": bson.M{
				"_id": bson.M{
					"date":               "$_id.date",
					"inviter_im_user_id": "$_id.inviter_im_user_id",
				},
				"count": bson.M{
					"$sum": 1,
				},
			},
		},
		{
			"$project": bson.M{
				"date":               "$_id.date",
				"inviter_im_user_id": "$_id.inviter_im_user_id",
				"count":              1,
				"_id":                0,
			},
		},
	}

	coll := plugin.MongoCli().GetDB().Collection("organization_user")
	return mongoutil.Aggregate[*InviteStatisticsCount](ctx, coll, pipeline)
}

// GetInviteDailyNewRegisterCheckinStats 统计：按邀请人+日期的当日【新增用户】签到人数
// 含义：仅统计当天被该业务员邀请注册的用户，并在同一天完成签到的去重人数。
func (o *StatisticsDao) GetInviteDailyNewRegisterCheckinStats(ctx context.Context, orgId primitive.ObjectID,
	startTime, endTime time.Time) ([]*InviteStatisticsCount, error) {
	pipeline := []bson.M{
		// 组织用户基础过滤 + 邀请时间范围（当天新增）
		{
			"$match": bson.M{
				"organization_id": orgId,
				"created_at": bson.M{
					"$gte": startTime,
					"$lte": endTime,
				},
				"status": bson.M{"$ne": "Disable"},
				"role": bson.M{
					"$nin": []interface{}{"SuperAdmin", "BackendAdmin"},
				},
				"inviter_im_server_user_id": bson.M{
					"$exists": true,
					"$nin":    []interface{}{"", nil},
				},
				"im_server_user_id": bson.M{
					"$exists": true,
					"$nin":    []interface{}{"", nil},
				},
			},
		},
		// 关联 checkin
		{
			"$lookup": bson.M{
				"from":         "checkin",
				"localField":   "im_server_user_id",
				"foreignField": "im_server_user_id",
				"as":           "checkin",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$checkin",
				"preserveNullAndEmptyArrays": false,
			},
		},
		// 时间范围过滤：签到发生在统计时间段内
		{
			"$match": bson.M{
				"checkin.created_at": bson.M{
					"$gte": startTime,
					"$lte": endTime,
				},
			},
		},
		// 计算邀请日期和签到日期的字符串形式
		{
			"$addFields": bson.M{
				"invite_date": bson.M{
					"$dateToString": bson.M{
						"format":   "%Y%m%d",
						"date":     "$created_at",
						"timezone": "+08:00",
					},
				},
				"checkin_date": bson.M{
					"$dateToString": bson.M{
						"format":   "%Y%m%d",
						"date":     "$checkin.created_at",
						"timezone": "+08:00",
					},
				},
			},
		},
		// 要求同一天邀请且同一天签到
		{
			"$match": bson.M{
				"$expr": bson.M{
					"$eq": []interface{}{"$invite_date", "$checkin_date"},
				},
			},
		},
		// 按日期+邀请人+被邀请用户去重
		{
			"$group": bson.M{
				"_id": bson.M{
					"date":               "$checkin_date",
					"inviter_im_user_id": "$inviter_im_server_user_id",
					"user_id":            "$user_id",
				},
			},
		},
		// 再按日期+邀请人汇总
		{
			"$group": bson.M{
				"_id": bson.M{
					"date":               "$_id.date",
					"inviter_im_user_id": "$_id.inviter_im_user_id",
				},
				"count": bson.M{
					"$sum": 1,
				},
			},
		},
		{
			"$project": bson.M{
				"date":               "$_id.date",
				"inviter_im_user_id": "$_id.inviter_im_user_id",
				"count":              1,
				"_id":                0,
			},
		},
	}

	coll := plugin.MongoCli().GetDB().Collection("organization_user")
	return mongoutil.Aggregate[*InviteStatisticsCount](ctx, coll, pipeline)
}
