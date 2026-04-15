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

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/errs"
)

type mongoVerifyCode struct {
	ID         primitive.ObjectID `bson:"_id"`
	Account    string             `bson:"account"`
	Platform   string             `bson:"platform"`
	Code       string             `bson:"code"`
	Duration   uint               `bson:"duration"`
	Count      int                `bson:"count"`
	Used       bool               `bson:"used"`
	CreateTime time.Time          `bson:"create_time"`
	UsedFor    int32              `bson:"used_for"` // 验证码用途
}

func NewVerifyCode(db *mongo.Database) (chat.VerifyCodeInterface, error) {
	coll := db.Collection("verify_codes")
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			// 按账号查询的索引
			Keys: bson.D{
				{Key: "account", Value: 1},
			},
		},
		{
			// TTL索引，使验证码在创建1天后自动删除，防止数据库中积累大量过期验证码
			Keys: bson.D{
				{Key: "create_time", Value: 1},
			},
			Options: options.Index().SetExpireAfterSeconds(86400), // 1天 = 86400秒
		},
		{
			// 添加复合索引，用于按账号和用途查询
			Keys: bson.D{
				{Key: "account", Value: 1},
				{Key: "used_for", Value: 1},
			},
		},
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	return &VerifyCode{
		coll: coll,
	}, nil
}

type VerifyCode struct {
	coll *mongo.Collection
}

func (o *VerifyCode) parseID(s string) (primitive.ObjectID, error) {
	objID, err := primitive.ObjectIDFromHex(s)
	if err != nil {
		var zero primitive.ObjectID
		return zero, errs.Wrap(err)
	}
	return objID, nil
}

func (o *VerifyCode) Add(ctx context.Context, ms []*chat.VerifyCode) error {
	tmp := make([]mongoVerifyCode, 0, len(ms))
	for i, m := range ms {
		var objID primitive.ObjectID
		if m.ID == "" {
			objID = primitive.NewObjectID()
			ms[i].ID = objID.Hex()
		} else {
			var err error
			objID, err = o.parseID(m.ID)
			if err != nil {
				return err
			}
		}
		tmp = append(tmp, mongoVerifyCode{
			ID:         objID,
			Account:    m.Account,
			Platform:   m.Platform,
			Code:       m.Code,
			Duration:   m.Duration,
			Count:      m.Count,
			Used:       m.Used,
			CreateTime: m.CreateTime,
			UsedFor:    m.UsedFor,
		})
	}
	return mongoutil.InsertMany(ctx, o.coll, tmp)
}

func (o *VerifyCode) RangeNum(ctx context.Context, account string, start time.Time, end time.Time) (int64, error) {
	filter := bson.M{
		"account": account,
		"create_time": bson.M{
			"$gte": start,
			"$lte": end,
		},
	}
	return mongoutil.Count(ctx, o.coll, filter)
}

func (o *VerifyCode) TakeLast(ctx context.Context, account string, usedFor int32) (*chat.VerifyCode, error) {
	filter := bson.M{
		"account":  account,
		"used_for": usedFor, // 添加用途过滤
	}
	opt := options.FindOne().SetSort(bson.M{"_id": -1})
	last, err := mongoutil.FindOne[*mongoVerifyCode](ctx, o.coll, filter, opt)
	if err != nil {
		return nil, err
	}
	return &chat.VerifyCode{
		ID:         last.ID.Hex(),
		Account:    last.Account,
		Platform:   last.Platform,
		Code:       last.Code,
		Duration:   last.Duration,
		Count:      last.Count,
		Used:       last.Used,
		CreateTime: last.CreateTime,
		UsedFor:    last.UsedFor,
	}, nil
}

func (o *VerifyCode) Incr(ctx context.Context, id string) error {
	objID, err := o.parseID(id)
	if err != nil {
		return err
	}
	return mongoutil.UpdateOne(ctx, o.coll, bson.M{"_id": objID}, bson.M{"$inc": bson.M{"count": 1}}, false)
}

func (o *VerifyCode) Delete(ctx context.Context, id string) error {
	objID, err := o.parseID(id)
	if err != nil {
		return err
	}
	return mongoutil.DeleteOne(ctx, o.coll, bson.M{"_id": objID})
}
