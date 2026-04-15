package model

import (
	"context"
	"time"

	"github.com/openimsdk/tools/log"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// SuperAdminForbidden 超管封禁主表
type SuperAdminForbidden struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	UserID         string             `bson:"user_id" json:"user_id"`                   // 主账户user_id
	Reason         string             `bson:"reason" json:"reason"`                     // 封禁原因
	OperatorUserID string             `bson:"operator_user_id" json:"operator_user_id"` // 操作员ID
	CreateTime     time.Time          `bson:"create_time" json:"create_time"`           // 创建时间
}

func (SuperAdminForbidden) TableName() string {
	return constant.CollectionSuperAdminForbidden
}

// SuperAdminForbiddenDetail 超管封禁详情表（存储子账户信息）- 删除冗余字段
type SuperAdminForbiddenDetail struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	ForbiddenID    primitive.ObjectID `bson:"forbidden_id" json:"forbidden_id"`           // 关联主表ID
	ImServerUserID string             `bson:"im_server_user_id" json:"im_server_user_id"` // 子账户IM ID
	Nickname       string             `bson:"nickname" json:"nickname"`                   // IM昵称
	FaceURL        string             `bson:"face_url" json:"face_url"`                   // IM头像
	OrganizationID primitive.ObjectID `bson:"organization_id" json:"organization_id"`     // 组织ID
}

func (SuperAdminForbiddenDetail) TableName() string {
	return constant.CollectionSuperAdminForbiddenDetail
}

// SuperAdminForbiddenRecord 超管封禁解封记录表（操作历史记录）
type SuperAdminForbiddenRecord struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	UserID         string             `bson:"user_id" json:"user_id"`                   // 主账户user_id
	ActionType     string             `bson:"action_type" json:"action_type"`           // 操作类型：forbid(封禁)、unforbid(解封)
	Reason         string             `bson:"reason" json:"reason"`                     // 操作原因
	OperatorUserID string             `bson:"operator_user_id" json:"operator_user_id"` // 操作员ID
	CreateTime     time.Time          `bson:"create_time" json:"create_time"`           // 操作时间
}

func (SuperAdminForbiddenRecord) TableName() string {
	return constant.CollectionSuperAdminForbiddenRecord
}

// 操作类型常量
const (
	ActionTypeForbid   = "forbid"   // 封禁
	ActionTypeUnforbid = "unforbid" // 解封
)

// SuperAdminForbiddenDao 超管封禁数据访问对象
type SuperAdminForbiddenDao struct {
	DB               *mongo.Database
	Collection       *mongo.Collection
	DetailCollection *mongo.Collection
	RecordCollection *mongo.Collection
}

// NewSuperAdminForbiddenDao 创建新的SuperAdminForbiddenDao实例
func NewSuperAdminForbiddenDao(db *mongo.Database) *SuperAdminForbiddenDao {
	dao := &SuperAdminForbiddenDao{
		DB:               db,
		Collection:       db.Collection(SuperAdminForbidden{}.TableName()),
		DetailCollection: db.Collection(SuperAdminForbiddenDetail{}.TableName()),
		RecordCollection: db.Collection(SuperAdminForbiddenRecord{}.TableName()),
	}

	// 创建索引
	ctx := context.Background()

	// 主表索引
	_, err := dao.Collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "user_id", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		// 索引创建失败时记录日志但不中断程序
		log.ZWarn(ctx, "failed to create index for super_admin_forbidden", nil, "error", err)
	}

	// 详情表索引
	_, err = dao.DetailCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "forbidden_id", Value: 1},
		},
	})
	if err != nil {
		log.ZWarn(ctx, "failed to create index for super_admin_forbidden_detail forbidden_id", nil, "error", err)
	}

	_, err = dao.DetailCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "im_server_user_id", Value: 1},
		},
	})
	if err != nil {
		log.ZWarn(ctx, "failed to create index for super_admin_forbidden_detail im_server_user_id", nil, "error", err)
	}

	// 记录表索引
	_, err = dao.RecordCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "user_id", Value: 1},
		},
	})
	if err != nil {
		log.ZWarn(ctx, "failed to create index for super_admin_forbidden_record user_id", nil, "error", err)
	}

	_, err = dao.RecordCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "action_type", Value: 1},
		},
	})
	if err != nil {
		log.ZWarn(ctx, "failed to create index for super_admin_forbidden_record action_type", nil, "error", err)
	}

	_, err = dao.RecordCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "create_time", Value: -1},
		},
	})
	if err != nil {
		// log.ZWarn(ctx, "failed to create index for super_admin_forbidden_record create_time", nil, "error", err)
	}

	return dao
}

// CreateForbidden 创建封禁记录
func (s *SuperAdminForbiddenDao) CreateForbidden(ctx context.Context, forbidden *SuperAdminForbidden, details []*SuperAdminForbiddenDetail) error {
	// 设置创建时间
	now := time.Now()
	forbidden.CreateTime = now

	// 开启事务
	session, err := s.DB.Client().StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		// 插入主表记录
		result, err := s.Collection.InsertOne(sessCtx, forbidden)
		if err != nil {
			return nil, err
		}

		// 获取插入的ID
		forbiddenID := result.InsertedID.(primitive.ObjectID)

		// 更新详情记录的forbidden_id
		for _, detail := range details {
			detail.ForbiddenID = forbiddenID
		}

		// 插入详情记录
		if len(details) > 0 {
			var detailsInterface []interface{}
			for _, detail := range details {
				detailsInterface = append(detailsInterface, detail)
			}
			_, err = s.DetailCollection.InsertMany(sessCtx, detailsInterface)
			if err != nil {
				return nil, err
			}
		}

		// 插入操作记录
		record := &SuperAdminForbiddenRecord{
			UserID:         forbidden.UserID,
			ActionType:     ActionTypeForbid,
			Reason:         forbidden.Reason,
			OperatorUserID: forbidden.OperatorUserID,
			CreateTime:     now,
		}
		_, err = s.RecordCollection.InsertOne(sessCtx, record)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})

	return err
}

// GetForbiddenByUserID 根据UserID获取封禁记录
func (s *SuperAdminForbiddenDao) GetForbiddenByUserID(ctx context.Context, userID string) (*SuperAdminForbidden, error) {
	return mongoutil.FindOne[*SuperAdminForbidden](ctx, s.Collection, bson.M{"user_id": userID})
}

// GetForbiddenDetailsWithUserInfo 获取封禁详情
func (s *SuperAdminForbiddenDao) GetForbiddenDetailsWithUserInfo(ctx context.Context, userID string) ([]*SuperAdminForbiddenDetail, error) {
	// 先获取主表记录
	forbidden, err := s.GetForbiddenByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}

	// 通过forbidden_id查询详情
	return mongoutil.Find[*SuperAdminForbiddenDetail](ctx, s.DetailCollection, bson.M{"forbidden_id": forbidden.ID})
}

// DeleteForbidden 物理删除封禁记录
func (s *SuperAdminForbiddenDao) DeleteForbidden(ctx context.Context, userID string, operatorUserID string) error {
	// 开启事务
	session, err := s.DB.Client().StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		// 先获取主表记录以获取forbidden_id
		forbidden, err := s.GetForbiddenByUserID(ctx, userID)
		if err != nil {
			return nil, err
		}

		// 先插入解封操作记录（确保操作记录在删除数据前成功插入）
		record := &SuperAdminForbiddenRecord{
			UserID:         userID,
			ActionType:     ActionTypeUnforbid,
			Reason:         "解封用户",
			OperatorUserID: operatorUserID,
			CreateTime:     time.Now(),
		}
		_, err = s.RecordCollection.InsertOne(sessCtx, record)
		if err != nil {
			return nil, err
		}

		// 删除详情记录
		_, err = s.DetailCollection.DeleteMany(sessCtx, bson.M{"forbidden_id": forbidden.ID})
		if err != nil {
			return nil, err
		}

		// 删除主表记录
		_, err = s.Collection.DeleteOne(sessCtx, bson.M{"user_id": userID})
		if err != nil {
			return nil, err
		}

		return nil, nil
	})

	return err
}

// DistinctForbiddenImServerUserIDs 返回超管封禁详情表中出现的全部子账户 IM user_id（用于层级统计等排除封禁展示）。
func (s *SuperAdminForbiddenDao) DistinctForbiddenImServerUserIDs(ctx context.Context) ([]string, error) {
	vals, err := s.DetailCollection.Distinct(ctx, "im_server_user_id", bson.M{})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		id, ok := v.(string)
		if !ok || id == "" {
			continue
		}
		out = append(out, id)
	}
	return out, nil
}

// SearchForbiddenUsers 搜索封禁用户（连表查询attr表）
func (s *SuperAdminForbiddenDao) SearchForbiddenUsers(ctx context.Context, keyword string, startTime *int64, endTime *int64, pagination *paginationUtils.DepPagination) (int64, []*SuperAdminForbiddenWithAttr, error) {
	// 构建聚合管道
	pipeline := []bson.M{
		// 连接attribute表
		{
			"$lookup": bson.M{
				"from":         "attribute",
				"localField":   "user_id",
				"foreignField": "user_id",
				"as":           "user_attr",
			},
		},
		// 展开user_attr数组
		{
			"$unwind": bson.M{
				"path":                       "$user_attr",
				"preserveNullAndEmptyArrays": true,
			},
		},
	}

	// 构建过滤条件
	matchConditions := []bson.M{}

	// 时间筛选条件
	if startTime != nil || endTime != nil {
		timeFilter := bson.M{}
		if startTime != nil {
			timeFilter["$gte"] = time.Unix(*startTime, 0)
		}
		if endTime != nil {
			timeFilter["$lte"] = time.Unix(*endTime, 0)
		}
		matchConditions = append(matchConditions, bson.M{"create_time": timeFilter})
	}

	// 关键词搜索条件
	if keyword != "" {
		matchConditions = append(matchConditions, bson.M{
			"$or": []bson.M{
				{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
				{"reason": bson.M{"$regex": keyword, "$options": "i"}},
				{"user_attr.account": bson.M{"$regex": keyword, "$options": "i"}},
				{"user_attr.email": bson.M{"$regex": keyword, "$options": "i"}},
			},
		})
	}

	// 添加匹配条件到管道
	if len(matchConditions) > 0 {
		var matchStage bson.M
		if len(matchConditions) == 1 {
			matchStage = bson.M{"$match": matchConditions[0]}
		} else {
			matchStage = bson.M{"$match": bson.M{"$and": matchConditions}}
		}
		pipeline = append(pipeline, matchStage)
	}

	// 添加计数管道用于获取总数
	countPipeline := append(pipeline, bson.M{"$count": "total"})

	// 执行聚合查询获取总数
	var countResult []bson.M
	countCursor, err := s.Collection.Aggregate(ctx, countPipeline)
	if err != nil {
		return 0, nil, err
	}
	defer countCursor.Close(ctx)

	if err = countCursor.All(ctx, &countResult); err != nil {
		return 0, nil, err
	}

	var total int64 = 0
	if len(countResult) > 0 {
		if totalValue, ok := countResult[0]["total"]; ok {
			if totalInt, ok := totalValue.(int32); ok {
				total = int64(totalInt)
			} else if totalInt64, ok := totalValue.(int64); ok {
				total = totalInt64
			}
		}
	}

	// 添加排序和分页到数据查询管道
	pipeline = append(pipeline, bson.M{"$sort": bson.M{"create_time": -1}})

	// 使用DepPagination的分页功能
	if pagination != nil {
		pipeline = append(pipeline, pagination.ToBsonMList()...)
	}

	// 执行聚合查询获取数据
	cursor, err := s.Collection.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, nil, err
	}
	defer cursor.Close(ctx)

	var results []*SuperAdminForbiddenWithAttr
	if err = cursor.All(ctx, &results); err != nil {
		return 0, nil, err
	}

	return total, results, nil
}

// SuperAdminForbiddenWithAttr 包含用户属性信息的封禁记录
type SuperAdminForbiddenWithAttr struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	UserID         string             `bson:"user_id" json:"user_id"`
	Reason         string             `bson:"reason" json:"reason"`
	OperatorUserID string             `bson:"operator_user_id" json:"operator_user_id"`
	CreateTime     time.Time          `bson:"create_time" json:"create_time"`

	// 用户属性信息
	UserAttr struct {
		UserID      string    `bson:"user_id" json:"user_id"`
		Account     string    `bson:"account" json:"account"`
		PhoneNumber string    `bson:"phone_number" json:"phone_number"`
		AreaCode    string    `bson:"area_code" json:"area_code"`
		Email       string    `bson:"email" json:"email"`
		Nickname    string    `bson:"nickname" json:"nickname"`
		FaceURL     string    `bson:"face_url" json:"face_url"`
		Gender      int32     `bson:"gender" json:"gender"`
		CreateTime  time.Time `bson:"create_time" json:"create_time"`
		ChangeTime  time.Time `bson:"change_time" json:"change_time"`
	} `bson:"user_attr" json:"user_attr"`
}

// ExistByUserID 检查用户是否已被封禁
func (s *SuperAdminForbiddenDao) ExistByUserID(ctx context.Context, userID string) (bool, error) {
	return mongoutil.Exist(ctx, s.Collection, bson.M{"user_id": userID})
}

// GetAllForbiddenUserIDs 获取所有被封禁用户的ID列表
func (s *SuperAdminForbiddenDao) GetAllForbiddenUserIDs(ctx context.Context) ([]string, error) {
	cursor, err := s.Collection.Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{"user_id": 1, "_id": 0}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []struct {
		UserID string `bson:"user_id"`
	}
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	userIDs := make([]string, len(results))
	for i, result := range results {
		userIDs[i] = result.UserID
	}

	return userIDs, nil
}
