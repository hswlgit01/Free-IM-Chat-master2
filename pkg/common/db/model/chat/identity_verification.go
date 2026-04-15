// Copyright © 2023 OpenIM open source community. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chat

import (
	"context"
	"time"

	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/chat/tools/db/pagination"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func NewIdentityVerification(db *mongo.Database) (chat.IdentityVerificationInterface, error) {
	coll := db.Collection("identity_verifications")
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "user_id", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "apply_time", Value: -1},
			},
		},
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	return &IdentityVerification{coll: coll, db: db}, nil
}

type IdentityVerification struct {
	coll *mongo.Collection
	db   *mongo.Database
}

func (o *IdentityVerification) Create(ctx context.Context, identity *chat.IdentityVerification) error {
	return mongoutil.InsertMany(ctx, o.coll, []*chat.IdentityVerification{identity})
}

func (o *IdentityVerification) Update(ctx context.Context, userID string, data map[string]any) error {
	if len(data) == 0 {
		return nil
	}
	data["update_time"] = time.Now()
	return mongoutil.UpdateOne(ctx, o.coll, bson.M{"user_id": userID}, bson.M{"$set": data}, false)
}

func (o *IdentityVerification) Take(ctx context.Context, userID string) (*chat.IdentityVerification, error) {
	return mongoutil.FindOne[*chat.IdentityVerification](ctx, o.coll, bson.M{"user_id": userID})
}

func (o *IdentityVerification) FindByStatus(ctx context.Context, status *int32, keyword string, pagination pagination.Pagination) (int64, []*chat.IdentityVerification, error) {
	filter := bson.M{}

	// 状态筛选（可选）
	if status != nil {
		filter["status"] = *status
	}

	// 关键词搜索（搜索userID）
	if keyword != "" {
		filter["user_id"] = bson.M{"$regex": keyword, "$options": "i"}
	}

	return mongoutil.FindPage[*chat.IdentityVerification](ctx, o.coll, filter, pagination)
}

func (o *IdentityVerification) Approve(ctx context.Context, userID string, adminID string) error {
	now := time.Now()
	updateData := bson.M{
		"$set": bson.M{
			"status":       int32(2), // 已认证
			"verify_time":  now,
			"verify_admin": adminID,
			"update_time":  now,
		},
	}
	return mongoutil.UpdateOne(ctx, o.coll, bson.M{"user_id": userID}, updateData, false)
}

func (o *IdentityVerification) Reject(ctx context.Context, userID string, adminID string, reason string) error {
	now := time.Now()
	updateData := bson.M{
		"$set": bson.M{
			"status":        int32(3), // 已拒绝
			"reject_reason": reason,
			"verify_time":   now,
			"verify_admin":  adminID,
			"update_time":   now,
		},
	}
	return mongoutil.UpdateOne(ctx, o.coll, bson.M{"user_id": userID}, updateData, false)
}

// FindByStatusAndOrg 根据状态和组织查询认证列表
func (o *IdentityVerification) FindByStatusAndOrg(ctx context.Context, status *int32, keyword string, orgID string,
	pagination pagination.Pagination, orderKey string, orderDirection string,
	startTime, endTime, verifyStartTime, verifyEndTime int64) (int64, []*chat.IdentityVerification, error) {

	// 1. 转换组织ID为ObjectID
	organizationObjectID, err := primitive.ObjectIDFromHex(orgID)
	if err != nil {
		return 0, nil, errs.Wrap(err)
	}

	// 2. 先从organization_user表查询该组织的所有用户ID
	orgUserColl := o.db.Collection("organization_user")
	orgUserFilter := bson.M{"organization_id": organizationObjectID}

	cursor, err := orgUserColl.Find(ctx, orgUserFilter)
	if err != nil {
		return 0, nil, errs.Wrap(err)
	}
	defer cursor.Close(ctx)

	// 收集该组织的所有用户ID
	var orgUserIDs []string
	for cursor.Next(ctx) {
		var orgUser struct {
			UserID string `bson:"user_id"`
		}
		if err := cursor.Decode(&orgUser); err != nil {
			continue
		}
		orgUserIDs = append(orgUserIDs, orgUser.UserID)
	}

	if len(orgUserIDs) == 0 {
		// 组织没有用户，返回空列表
		return 0, []*chat.IdentityVerification{}, nil
	}

	// 如果有关键词搜索，使用聚合管道实现跨集合搜索
	if keyword != "" {
		// 3. 使用聚合管道实现跨集合搜索(用户ID、账号、昵称)
		pipeline := []bson.M{
			// 第一阶段：基本过滤 - 只查找组织内用户
			{
				"$match": bson.M{
					"user_id": bson.M{"$in": orgUserIDs},
				},
			},
		}

		// 添加状态筛选（可选）
		if status != nil {
			pipeline = append(pipeline, bson.M{
				"$match": bson.M{"status": *status},
			})
		}

		// 添加提交时间范围筛选
		if startTime > 0 || endTime > 0 {
			applyTimeFilter := bson.M{}
			if startTime > 0 {
				applyTimeFilter["$gte"] = time.Unix(startTime, 0)
			}
			if endTime > 0 {
				applyTimeFilter["$lte"] = time.Unix(endTime, 0)
			}
			pipeline = append(pipeline, bson.M{
				"$match": bson.M{"apply_time": applyTimeFilter},
			})
		}

		// 添加审核时间范围筛选
		if verifyStartTime > 0 || verifyEndTime > 0 {
			verifyTimeFilter := bson.M{}
			if verifyStartTime > 0 {
				verifyTimeFilter["$gte"] = time.Unix(verifyStartTime, 0)
			}
			if verifyEndTime > 0 {
				verifyTimeFilter["$lte"] = time.Unix(verifyEndTime, 0)
			}
			pipeline = append(pipeline, bson.M{
				"$match": bson.M{"verify_time": verifyTimeFilter},
			})
		}

		// 第二阶段：关联attribute集合，获取账号和attribute中的昵称
		pipeline = append(pipeline, bson.M{
			"$lookup": bson.M{
				"from":         "attribute",
				"localField":   "user_id",
				"foreignField": "user_id",
				"as":           "user_attr",
			},
		})

		// 第三阶段：展开关联结果
		pipeline = append(pipeline, bson.M{
			"$unwind": bson.M{
				"path":                       "$user_attr",
				"preserveNullAndEmptyArrays": true,
			},
		})

		// 第四阶段：关联organization_user表获取im_server_user_id
		pipeline = append(pipeline, bson.M{
			"$lookup": bson.M{
				"from":         "organization_user",
				"localField":   "user_id",
				"foreignField": "user_id",
				"as":           "org_user",
			},
		})

		// 第五阶段：展开organization_user关联结果
		pipeline = append(pipeline, bson.M{
			"$unwind": bson.M{
				"path":                       "$org_user",
				"preserveNullAndEmptyArrays": true,
			},
		})

		// 第六阶段：关联IM用户表获取IM用户昵称
		pipeline = append(pipeline, bson.M{
			"$lookup": bson.M{
				"from":         "user",
				"localField":   "org_user.im_server_user_id",
				"foreignField": "user_id",
				"as":           "im_user",
			},
		})

		// 第七阶段：展开IM用户关联结果
		pipeline = append(pipeline, bson.M{
			"$unwind": bson.M{
				"path":                       "$im_user",
				"preserveNullAndEmptyArrays": true,
			},
		})

		// 第八阶段：关键词匹配 - 同时搜索user_id、account和两个来源的nickname
		pipeline = append(pipeline, bson.M{
			"$match": bson.M{
				"$or": []bson.M{
					{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
					{"user_attr.account": bson.M{"$regex": keyword, "$options": "i"}},
					{"user_attr.nickname": bson.M{"$regex": keyword, "$options": "i"}},
					{"im_user.nickname": bson.M{"$regex": keyword, "$options": "i"}},
				},
			},
		})

		// 计算总数（为了分页）
		countPipeline := append([]bson.M{}, pipeline...)
		countPipeline = append(countPipeline, bson.M{
			"$count": "total",
		})

		countCursor, err := o.coll.Aggregate(ctx, countPipeline)
		if err != nil {
			return 0, nil, errs.Wrap(err)
		}
		defer countCursor.Close(ctx)

		// 解析总数
		total := int64(0)
		if countCursor.Next(ctx) {
			var countResult struct {
				Total int64 `bson:"total"`
			}
			if err := countCursor.Decode(&countResult); err == nil {
				total = countResult.Total
			}
		}

		// 添加排序
		if orderKey != "" {
			// 转换字段名
			dbFieldName := ""
			switch orderKey {
			case "applyTime":
				dbFieldName = "apply_time"
			case "verifyTime":
				dbFieldName = "verify_time"
			default:
				dbFieldName = orderKey
			}

			// 排序方向
			direction := 1 // 默认升序
			if orderDirection == "desc" {
				direction = -1
			}

			pipeline = append(pipeline, bson.M{
				"$sort": bson.M{dbFieldName: direction},
			})
		} else {
			// 默认按申请时间倒序排序
			pipeline = append(pipeline, bson.M{
				"$sort": bson.M{"apply_time": -1},
			})
		}

		// 添加分页
		if pagination != nil {
			pipeline = append(pipeline, bson.M{
				"$skip": (pagination.GetPageNumber() - 1) * pagination.GetShowNumber(),
			})
			pipeline = append(pipeline, bson.M{
				"$limit": pagination.GetShowNumber(),
			})
		}

		// 最后阶段：去掉不需要的关联字段，保持原有返回格式
		pipeline = append(pipeline, bson.M{
			"$project": bson.M{
				"user_attr": 0, // 移除临时的关联字段
				"org_user":  0, // 移除临时的组织用户字段
				"im_user":   0, // 移除临时的IM用户字段
			},
		})

		// 执行聚合查询
		resultCursor, err := o.coll.Aggregate(ctx, pipeline)
		if err != nil {
			return 0, nil, errs.Wrap(err)
		}
		defer resultCursor.Close(ctx)

		// 解析结果
		var results []*chat.IdentityVerification
		if err := resultCursor.All(ctx, &results); err != nil {
			return 0, nil, errs.Wrap(err)
		}

		return total, results, nil
	} else {
		// 没有关键词搜索，使用原来的查询逻辑 - 保持原有性能
		// 3. 构建identity_verification的查询条件
		filter := bson.M{}

		// 过滤组织用户
		filter["user_id"] = bson.M{"$in": orgUserIDs}

		// 状态筛选（可选）
		if status != nil {
			filter["status"] = *status
		}

		// 添加时间范围筛选条件
		// 提交时间范围筛选
		if startTime > 0 || endTime > 0 {
			applyTimeFilter := bson.M{}
			if startTime > 0 {
				applyTimeFilter["$gte"] = time.Unix(startTime, 0)
			}
			if endTime > 0 {
				applyTimeFilter["$lte"] = time.Unix(endTime, 0)
			}
			filter["apply_time"] = applyTimeFilter
		}

		// 审核时间范围筛选
		if verifyStartTime > 0 || verifyEndTime > 0 {
			verifyTimeFilter := bson.M{}
			if verifyStartTime > 0 {
				verifyTimeFilter["$gte"] = time.Unix(verifyStartTime, 0)
			}
			if verifyEndTime > 0 {
				verifyTimeFilter["$lte"] = time.Unix(verifyEndTime, 0)
			}
			filter["verify_time"] = verifyTimeFilter
		}

		// 处理排序选项
		opts := []*options.FindOptions{}
		if orderKey != "" {
			// 根据数据库中的字段名转换前端传来的字段名
			dbFieldName := ""
			switch orderKey {
			case "applyTime":
				dbFieldName = "apply_time"
			case "verifyTime":
				dbFieldName = "verify_time"
			default:
				// 如果不是需要特别处理的字段，则默认转为snake_case
				dbFieldName = orderKey
			}

			// 设置排序方向
			direction := 1 // 默认升序
			if orderDirection == "desc" {
				direction = -1
			}

			sortOption := options.Find().SetSort(bson.D{{Key: dbFieldName, Value: direction}})
			opts = append(opts, sortOption)
		}

		return mongoutil.FindPage[*chat.IdentityVerification](ctx, o.coll, filter, pagination, opts...)
	}
}
