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

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/chat/tools/db/pagination"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func NewAttribute(db *mongo.Database) (chat.AttributeInterface, error) {
	coll := db.Collection("attribute")
	//_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
	//	{
	//		Keys: bson.D{
	//			{Key: "user_id", Value: 1},
	//		},
	//		Options: options.Index().SetUnique(true),
	//	},
	//	{
	//		Keys: bson.D{
	//			{Key: "account", Value: 1},
	//		},
	//	},
	//	{
	//		Keys: bson.D{
	//			{Key: "email", Value: 1},
	//		},
	//	},
	//	{
	//		Keys: bson.D{
	//			{Key: "area_code", Value: 1},
	//			{Key: "phone_number", Value: 1},
	//		},
	//	},
	//})
	//if err != nil {
	//	return nil, errs.Wrap(err)
	//}
	return &Attribute{coll: coll}, nil
}

type Attribute struct {
	coll *mongo.Collection
}

func (o *Attribute) Create(ctx context.Context, attribute ...*chat.Attribute) error {
	return mongoutil.InsertMany(ctx, o.coll, attribute)
}

func (o *Attribute) Update(ctx context.Context, userID string, data map[string]any) error {
	if len(data) == 0 {
		return nil
	}
	// 使用 upsert 选项:如果记录不存在则创建,存在则更新
	// 这样可以处理 attribute 记录缺失的情况
	opts := options.Update().SetUpsert(true)
	return mongoutil.UpdateOne(ctx, o.coll, bson.M{"user_id": userID}, bson.M{"$set": data}, false, opts)
}

func (o *Attribute) Find(ctx context.Context, userIds []string) ([]*chat.Attribute, error) {
	return mongoutil.Find[*chat.Attribute](ctx, o.coll, bson.M{"user_id": bson.M{"$in": userIds}})
}

func (o *Attribute) FindAccount(ctx context.Context, accounts []string) ([]*chat.Attribute, error) {
	return mongoutil.Find[*chat.Attribute](ctx, o.coll, bson.M{"account": bson.M{"$in": accounts}})
}

func (o *Attribute) FindPhone(ctx context.Context, phoneNumbers []string) ([]*chat.Attribute, error) {
	return mongoutil.Find[*chat.Attribute](ctx, o.coll, bson.M{"phone_number": bson.M{"$in": phoneNumbers}})
}

func (o *Attribute) Search(ctx context.Context, keyword string, genders []int32, pagination pagination.Pagination) (int64, []*chat.Attribute, error) {
	filter := bson.M{}
	if len(genders) > 0 {
		filter["gender"] = bson.M{
			"$in": genders,
		}
	}
	if keyword != "" {
		filter["$or"] = []bson.M{
			//{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
			//{"account": bson.M{"$regex": keyword, "$options": "i"}},
			//{"nickname": bson.M{"$regex": keyword, "$options": "i"}},
			//{"phone_number": bson.M{"$regex": keyword, "$options": "i"}},

			{"user_id": keyword},
			{"account": keyword},
			{"phone_number": keyword},
		}
	}
	return mongoutil.FindPage[*chat.Attribute](ctx, o.coll, filter, pagination)
}

func (o *Attribute) TakePhone(ctx context.Context, areaCode string, phoneNumber string) (*chat.Attribute, error) {
	return mongoutil.FindOne[*chat.Attribute](ctx, o.coll, bson.M{"area_code": areaCode, "phone_number": phoneNumber})
}

func (o *Attribute) TakeEmail(ctx context.Context, email string) (*chat.Attribute, error) {
	return mongoutil.FindOne[*chat.Attribute](ctx, o.coll, bson.M{"email": email})
}

func (o *Attribute) TakeAccount(ctx context.Context, account string) (*chat.Attribute, error) {
	return mongoutil.FindOne[*chat.Attribute](ctx, o.coll, bson.M{"account": account})
}

func (o *Attribute) Take(ctx context.Context, userID string) (*chat.Attribute, error) {
	return mongoutil.FindOne[*chat.Attribute](ctx, o.coll, bson.M{"user_id": userID})
}

func (o *Attribute) SearchNormalUser(ctx context.Context, keyword string, forbiddenIDs []string, gender int32, pagination pagination.Pagination, org_id string) (int64, []*chat.AttributeWithOrgUser, error) {
	// 验证org_id格式（如果提供）
	var orgObjectID primitive.ObjectID
	var err error
	if org_id != "" {
		orgObjectID, err = primitive.ObjectIDFromHex(org_id)
		if err != nil {
			return 0, nil, err
		}
	}

	// 构建基础匹配条件（不包含关键词搜索）
	baseMatchStage := bson.M{}

	if gender == 0 {
		baseMatchStage["gender"] = bson.M{
			"$in": []int32{0, 1, 2},
		}
	} else {
		baseMatchStage["gender"] = gender
	}
	if len(forbiddenIDs) > 0 {
		baseMatchStage["user_id"] = bson.M{
			"$nin": forbiddenIDs,
		}
	}

	// 构建基础聚合管道
	basePipeline := []bson.M{
		// 第一阶段：匹配基础条件
		{"$match": baseMatchStage},
	}

	// 如果提供了org_id，添加连表查询
	if org_id != "" {
		basePipeline = append(basePipeline,
			// 连接organization_user表
			bson.M{
				"$lookup": bson.M{
					"from":         "organization_user",
					"localField":   "user_id",
					"foreignField": "user_id",
					"as":           "org_user",
				},
			},
			// 展开org_user数组
			bson.M{
				"$unwind": bson.M{
					"path":                       "$org_user",
					"preserveNullAndEmptyArrays": false,
				},
			},
			// 匹配organization_id
			bson.M{
				"$match": bson.M{
					"org_user.organization_id": orgObjectID,
				},
			},
			// 添加im_server_user_id字段
			bson.M{
				"$addFields": bson.M{
					"im_server_user_id": "$org_user.im_server_user_id",
				},
			},
			// 移除org_user字段
			bson.M{
				"$project": bson.M{
					"org_user": 0,
				},
			},
		)

		// 如果有关键词搜索，添加关键词匹配阶段（包含 im_server_user_id）
		if keyword != "" {
			keywordMatchStage := bson.M{
				"$match": bson.M{
					"$or": []bson.M{
						{"user_id": keyword},
						{"account": keyword},
						{"phone_number": keyword},
						{"email": keyword},
						{"im_server_user_id": keyword}, // 只有在有 org_id 时才支持搜索 im_server_user_id
					},
				},
			}
			basePipeline = append(basePipeline, keywordMatchStage)
		}
	} else {
		// 如果没有org_id，添加空的im_server_user_id字段
		basePipeline = append(basePipeline,
			bson.M{
				"$addFields": bson.M{
					"im_server_user_id": "",
				},
			},
		)

		// 如果有关键词搜索，添加关键词匹配阶段（不包含 im_server_user_id）
		if keyword != "" {
			keywordMatchStage := bson.M{
				"$match": bson.M{
					"$or": []bson.M{
						{"user_id": keyword},
						{"account": keyword},
						{"phone_number": keyword},
						{"email": keyword},
						// 注意：没有 org_id 时不支持 im_server_user_id 搜索
					},
				},
			}
			basePipeline = append(basePipeline, keywordMatchStage)
		}
	}

	// 计算总数 - 使用独立的管道副本
	countPipeline := make([]bson.M, len(basePipeline))
	copy(countPipeline, basePipeline)
	countPipeline = append(countPipeline, bson.M{"$count": "total"})

	countCursor, err := o.coll.Aggregate(ctx, countPipeline)
	if err != nil {
		return 0, nil, err
	}
	defer countCursor.Close(ctx)

	var countResult []bson.M
	if err = countCursor.All(ctx, &countResult); err != nil {
		return 0, nil, err
	}

	var total int64 = 0
	if len(countResult) > 0 {
		// 更健壮的类型转换
		if totalVal, exists := countResult[0]["total"]; exists {
			switch v := totalVal.(type) {
			case int32:
				total = int64(v)
			case int64:
				total = v
			case int:
				total = int64(v)
			}
		}
	}

	// 构建查询管道 - 添加分页
	queryPipeline := make([]bson.M, len(basePipeline))
	copy(queryPipeline, basePipeline)

	if pagination.GetPageNumber() > 0 && pagination.GetShowNumber() > 0 {
		skip := (pagination.GetPageNumber() - 1) * pagination.GetShowNumber()
		queryPipeline = append(queryPipeline,
			bson.M{"$skip": skip},
			bson.M{"$limit": pagination.GetShowNumber()},
		)
	}

	// 执行查询
	cursor, err := o.coll.Aggregate(ctx, queryPipeline)
	if err != nil {
		return 0, nil, err
	}
	defer cursor.Close(ctx)

	var results []*chat.AttributeWithOrgUser
	if err = cursor.All(ctx, &results); err != nil {
		return 0, nil, err
	}

	return total, results, nil
}

func (o *Attribute) SearchUser(ctx context.Context, keyword string, userIDs []string, genders []int32, pagination pagination.Pagination) (int64, []*chat.Attribute, error) {
	filter := bson.M{}
	if len(genders) > 0 {
		filter["gender"] = bson.M{
			"$in": genders,
		}
	}
	if len(userIDs) > 0 {
		filter["user_id"] = bson.M{
			"$in": userIDs,
		}
	}
	if keyword != "" {
		filter["$or"] = []bson.M{
			//{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
			//{"account": bson.M{"$regex": keyword, "$options": "i"}},
			//{"nickname": bson.M{"$regex": keyword, "$options": "i"}},
			//{"phone_number": bson.M{"$regex": keyword, "$options": "i"}},
			//{"email": bson.M{"$regex": keyword, "$options": "i"}},

			{"user_id": keyword},
			{"account": keyword},
			{"phone_number": keyword},
			{"email": keyword},
		}
	}
	return mongoutil.FindPage[*chat.Attribute](ctx, o.coll, filter, pagination)
}

func (o *Attribute) Delete(ctx context.Context, userIDs []string) error {
	if len(userIDs) == 0 {
		return nil
	}
	return mongoutil.DeleteMany(ctx, o.coll, bson.M{"user_id": bson.M{"$in": userIDs}})
}
