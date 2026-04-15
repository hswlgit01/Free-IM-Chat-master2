// Copyright © 2024 OpenIM open source community. All rights reserved.
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

package mongoutil

import (
	"context"
	"time"

	"github.com/openimsdk/chat/tools/db/pagination"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func basic[T any]() bool {
	var t T
	switch any(t).(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string, []byte:
		return true
	case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *float32, *float64, *string, *[]byte:
		return true
	default:
		return false
	}
}

func anes[T any](ts []T) []any {
	val := make([]any, len(ts))
	for i := range ts {
		val[i] = ts[i]
	}
	return val
}

func findOptionToCountOption(opts []*options.FindOptions) *options.CountOptions {
	return options.Count()
}

func InsertMany[T any](ctx context.Context, coll *mongo.Collection, val []T, opts ...*options.InsertManyOptions) error {
	start := time.Now()
	logger := GetMongoQueryLogger()

	_, err := coll.InsertMany(ctx, anes(val), opts...)
	duration := time.Since(start)

	// 记录查询日志
	logger.LogQuery(ctx, coll.Name(), "INSERT_MANY", nil, nil, val, opts, duration, err)

	if err != nil {
		return errs.WrapMsg(err, "mongo insert many")
	}
	return nil
}

func UpdateOne(ctx context.Context, coll *mongo.Collection, filter any, update any, notMatchedErr bool, opts ...*options.UpdateOptions) error {
	start := time.Now()
	logger := GetMongoQueryLogger()

	res, err := coll.UpdateOne(ctx, filter, update, opts...)
	duration := time.Since(start)

	// 记录查询日志
	logger.LogQuery(ctx, coll.Name(), "UPDATE_ONE", filter, nil, update, opts, duration, err)

	if err != nil {
		return errs.WrapMsg(err, "mongo update one")
	}
	if notMatchedErr && res.MatchedCount == 0 {
		matchErr := errs.WrapMsg(mongo.ErrNoDocuments, "mongo update not matched")
		// 记录额外的匹配错误日志
		logger.LogQuery(ctx, coll.Name(), "UPDATE_ONE_NO_MATCH", filter, nil, update, opts, duration, matchErr)
		return matchErr
	}
	return nil
}

func UpdateOneResult(ctx context.Context, coll *mongo.Collection, filter any, update any, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	start := time.Now()
	logger := GetMongoQueryLogger()

	res, err := coll.UpdateOne(ctx, filter, update, opts...)
	duration := time.Since(start)

	// 记录查询日志
	logger.LogQuery(ctx, coll.Name(), "UPDATE_ONE_RESULT", filter, nil, update, opts, duration, err)

	if err != nil {
		return nil, errs.WrapMsg(err, "mongo update one")
	}
	return res, nil
}

func UpdateMany(ctx context.Context, coll *mongo.Collection, filter any, update any, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	start := time.Now()
	logger := GetMongoQueryLogger()

	res, err := coll.UpdateMany(ctx, filter, update, opts...)
	duration := time.Since(start)

	// 记录查询日志
	logger.LogQuery(ctx, coll.Name(), "UPDATE_MANY", filter, nil, update, opts, duration, err)

	if err != nil {
		return nil, errs.WrapMsg(err, "mongo update many")
	}
	return res, nil
}

func Find[T any](ctx context.Context, coll *mongo.Collection, filter any, opts ...*options.FindOptions) ([]T, error) {
	start := time.Now()
	logger := GetMongoQueryLogger()

	cur, err := coll.Find(ctx, filter, opts...)
	if err != nil {
		duration := time.Since(start)
		logger.LogQuery(ctx, coll.Name(), "FIND", filter, nil, nil, opts, duration, err)
		return nil, errs.WrapMsg(err, "mongo find")
	}
	defer cur.Close(ctx)

	result, decodeErr := Decodes[T](ctx, cur)
	duration := time.Since(start)

	// 记录查询日志（包括解码错误）
	logger.LogQuery(ctx, coll.Name(), "FIND", filter, nil, nil, opts, duration, decodeErr)

	return result, decodeErr
}

func FindOne[T any](ctx context.Context, coll *mongo.Collection, filter any, opts ...*options.FindOneOptions) (res T, err error) {
	start := time.Now()
	logger := GetMongoQueryLogger()

	cur := coll.FindOne(ctx, filter, opts...)
	if err := cur.Err(); err != nil {
		duration := time.Since(start)
		logger.LogQuery(ctx, coll.Name(), "FIND_ONE", filter, nil, nil, opts, duration, err)
		return res, errs.WrapMsg(err, "mongo find one")
	}

	result, decodeErr := DecodeOne[T](cur.Decode)
	duration := time.Since(start)

	// 记录查询日志
	logger.LogQuery(ctx, coll.Name(), "FIND_ONE", filter, nil, nil, opts, duration, decodeErr)

	return result, decodeErr
}

func FindOneAndUpdate[T any](ctx context.Context, coll *mongo.Collection, filter any, update any, opts ...*options.FindOneAndUpdateOptions) (res T, err error) {
	start := time.Now()
	logger := GetMongoQueryLogger()

	result := coll.FindOneAndUpdate(ctx, filter, update, opts...)
	if err := result.Err(); err != nil {
		duration := time.Since(start)
		logger.LogQuery(ctx, coll.Name(), "FIND_ONE_AND_UPDATE", filter, nil, update, opts, duration, err)
		return res, errs.WrapMsg(err, "mongo find one and update")
	}

	finalResult, decodeErr := DecodeOne[T](result.Decode)
	duration := time.Since(start)

	// 记录查询日志
	logger.LogQuery(ctx, coll.Name(), "FIND_ONE_AND_UPDATE", filter, nil, update, opts, duration, decodeErr)

	return finalResult, decodeErr
}

func FindPage[T any](ctx context.Context, coll *mongo.Collection, filter any, pagination pagination.Pagination, opts ...*options.FindOptions) (int64, []T, error) {
	start := time.Now()
	logger := GetMongoQueryLogger()

	count, err := Count(ctx, coll, filter, findOptionToCountOption(opts))
	if err != nil {
		duration := time.Since(start)
		logger.LogQuery(ctx, coll.Name(), "FIND_PAGE_COUNT", filter, nil, nil, opts, duration, err)
		return 0, nil, errs.WrapMsg(err, "mongo failed to count documents in collection")
	}
	if count == 0 || pagination == nil {
		duration := time.Since(start)
		logger.LogQuery(ctx, coll.Name(), "FIND_PAGE_EMPTY", filter, nil, nil, opts, duration, nil)
		return count, nil, nil
	}
	skip := int64(pagination.GetPageNumber()-1) * int64(pagination.GetShowNumber())
	if skip < 0 || skip >= count || pagination.GetShowNumber() <= 0 {
		duration := time.Since(start)
		logger.LogQuery(ctx, coll.Name(), "FIND_PAGE_INVALID", filter, nil, nil, opts, duration, nil)
		return count, nil, nil
	}
	opt := options.Find().SetSkip(skip).SetLimit(int64(pagination.GetShowNumber()))
	res, err := Find[T](ctx, coll, filter, append(opts, opt)...)
	duration := time.Since(start)

	// 记录分页查询日志
	logger.LogQuery(ctx, coll.Name(), "FIND_PAGE", filter, nil, nil, append(opts, opt), duration, err)

	if err != nil {
		return 0, nil, err
	}
	return count, res, nil
}

func FindPageOnly[T any](ctx context.Context, coll *mongo.Collection, filter any, pagination pagination.Pagination, opts ...*options.FindOptions) ([]T, error) {
	start := time.Now()
	logger := GetMongoQueryLogger()

	skip := int64(pagination.GetPageNumber()-1) * int64(pagination.GetShowNumber())
	if skip < 0 || pagination.GetShowNumber() <= 0 {
		duration := time.Since(start)
		logger.LogQuery(ctx, coll.Name(), "FIND_PAGE_ONLY_INVALID", filter, nil, nil, opts, duration, nil)
		return nil, nil
	}
	opt := options.Find().SetSkip(skip).SetLimit(int64(pagination.GetShowNumber()))

	result, err := Find[T](ctx, coll, filter, append(opts, opt)...)
	duration := time.Since(start)

	// 记录查询日志
	logger.LogQuery(ctx, coll.Name(), "FIND_PAGE_ONLY", filter, nil, nil, append(opts, opt), duration, err)

	return result, err
}

func Count(ctx context.Context, coll *mongo.Collection, filter any, opts ...*options.CountOptions) (int64, error) {
	start := time.Now()
	logger := GetMongoQueryLogger()

	count, err := coll.CountDocuments(ctx, filter, opts...)
	duration := time.Since(start)

	// 记录查询日志
	logger.LogQuery(ctx, coll.Name(), "COUNT", filter, nil, nil, opts, duration, err)

	if err != nil {
		return 0, errs.WrapMsg(err, "mongo count")
	}
	return count, nil
}

func Exist(ctx context.Context, coll *mongo.Collection, filter any, opts ...*options.CountOptions) (bool, error) {
	opts = append(opts, options.Count().SetLimit(1))
	count, err := Count(ctx, coll, filter, opts...)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func DeleteOne(ctx context.Context, coll *mongo.Collection, filter any, opts ...*options.DeleteOptions) error {
	start := time.Now()
	logger := GetMongoQueryLogger()

	_, err := coll.DeleteOne(ctx, filter, opts...)
	duration := time.Since(start)

	// 记录查询日志
	logger.LogQuery(ctx, coll.Name(), "DELETE_ONE", filter, nil, nil, opts, duration, err)

	if err != nil {
		return errs.WrapMsg(err, "mongo delete one")
	}
	return nil
}

func DeleteOneResult(ctx context.Context, coll *mongo.Collection, filter any, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	start := time.Now()
	logger := GetMongoQueryLogger()

	res, err := coll.DeleteOne(ctx, filter, opts...)
	duration := time.Since(start)

	// 记录查询日志
	logger.LogQuery(ctx, coll.Name(), "DELETE_ONE_RESULT", filter, nil, nil, opts, duration, err)

	if err != nil {
		return nil, errs.WrapMsg(err, "mongo delete one")
	}
	return res, nil
}

func DeleteMany(ctx context.Context, coll *mongo.Collection, filter any, opts ...*options.DeleteOptions) error {
	start := time.Now()
	logger := GetMongoQueryLogger()

	_, err := coll.DeleteMany(ctx, filter, opts...)
	duration := time.Since(start)

	// 记录查询日志
	logger.LogQuery(ctx, coll.Name(), "DELETE_MANY", filter, nil, nil, opts, duration, err)

	if err != nil {
		return errs.WrapMsg(err, "mongo delete many")
	}
	return nil
}

func DeleteManyResult(ctx context.Context, coll *mongo.Collection, filter any, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	start := time.Now()
	logger := GetMongoQueryLogger()

	res, err := coll.DeleteMany(ctx, filter, opts...)
	duration := time.Since(start)

	// 记录查询日志
	logger.LogQuery(ctx, coll.Name(), "DELETE_MANY_RESULT", filter, nil, nil, opts, duration, err)

	if err != nil {
		return nil, errs.WrapMsg(err, "mongo delete many")
	}
	return res, nil
}

func Aggregate[T any](ctx context.Context, coll *mongo.Collection, pipeline any, opts ...*options.AggregateOptions) ([]T, error) {
	start := time.Now()
	logger := GetMongoQueryLogger()

	cur, err := coll.Aggregate(ctx, pipeline, opts...)
	if err != nil {
		duration := time.Since(start)
		logger.LogQuery(ctx, coll.Name(), "AGGREGATE", nil, pipeline, nil, opts, duration, err)
		return nil, errs.WrapMsg(err, "mongo aggregate")
	}
	defer cur.Close(ctx)

	result, decodeErr := Decodes[T](ctx, cur)
	duration := time.Since(start)

	// 记录聚合查询日志
	logger.LogQuery(ctx, coll.Name(), "AGGREGATE", nil, pipeline, nil, opts, duration, decodeErr)

	return result, decodeErr
}

func Decodes[T any](ctx context.Context, cur *mongo.Cursor) ([]T, error) {
	var res []T
	if basic[T]() {
		var temp []map[string]T
		if err := cur.All(ctx, &temp); err != nil {
			return nil, errs.WrapMsg(err, "mongo decodes")
		}
		res = make([]T, 0, len(temp))
		for _, m := range temp {
			if len(m) != 1 {
				return nil, errs.ErrInternalServer.WrapMsg("mongo find result len(m) != 1")
			}
			for _, t := range m {
				res = append(res, t)
			}
		}
	} else {
		if err := cur.All(ctx, &res); err != nil {
			return nil, errs.WrapMsg(err, "mongo all")
		}
	}
	return res, nil
}

func DecodeOne[T any](decoder func(v any) error) (res T, err error) {
	if basic[T]() {
		var temp map[string]T
		if err = decoder(&temp); err != nil {
			err = errs.WrapMsg(err, "mongo decodes one")
			return
		}
		if len(temp) != 1 {
			err = errs.ErrInternalServer.WrapMsg("mongo find result len(m) != 1")
			return
		}
		for k := range temp {
			res = temp[k]
		}
	} else {
		if err = decoder(&res); err != nil {
			err = errs.WrapMsg(err, "mongo decoder")
			return
		}
	}
	return
}

func Ignore[T any](_ T, err error) error {
	return err
}

func IgnoreWarp[T any](_ T, err error) error {
	if err != nil {
		return errs.Wrap(err)
	}
	return err
}

func IncrVersion(dbs ...func() error) error {
	for _, fn := range dbs {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
