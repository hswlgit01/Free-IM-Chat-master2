package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ForbiddenAccount 被禁用账户表结构
type ForbiddenAccount struct {
	ID         primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	UserID     string             `bson:"user_id" json:"user_id"`
	Account    string             `bson:"account" json:"account"`
	Reason     string             `bson:"reason" json:"reason"`
	CreateTime time.Time          `bson:"create_time" json:"create_time"`
	OperatorID string             `bson:"operator_id" json:"operator_id"`
}

func (ForbiddenAccount) TableName() string {
	return "forbidden_account"
}

// ForbiddenAccountDao 被禁用账户数据访问对象
type ForbiddenAccountDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

// NewForbiddenAccountDao 创建新的ForbiddenAccountDao实例
func NewForbiddenAccountDao(db *mongo.Database) *ForbiddenAccountDao {
	return &ForbiddenAccountDao{
		DB:         db,
		Collection: db.Collection(ForbiddenAccount{}.TableName()),
	}
}

// FindAllIDs 获取所有被禁用的用户ID列表
func (f *ForbiddenAccountDao) FindAllIDs(ctx context.Context) ([]string, error) {
	cursor, err := f.Collection.Find(ctx, bson.M{}, nil)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var userIDs []string
	for cursor.Next(ctx) {
		var account ForbiddenAccount
		if err := cursor.Decode(&account); err != nil {
			continue // 跳过解码失败的记录
		}
		if account.UserID != "" {
			userIDs = append(userIDs, account.UserID)
		}
	}

	return userIDs, nil
}

// Take 根据用户ID获取禁用记录
func (f *ForbiddenAccountDao) Take(ctx context.Context, userID string) (*ForbiddenAccount, error) {
	return mongoutil.FindOne[*ForbiddenAccount](ctx, f.Collection, bson.M{"user_id": userID})
}

func (o *ForbiddenAccountDao) ExistByUserId(ctx context.Context, userId string) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"user_id": userId})
}

// Create 创建禁用记录
func (f *ForbiddenAccountDao) Create(ctx context.Context, accounts ...*ForbiddenAccount) error {
	return mongoutil.InsertMany(ctx, f.Collection, accounts)
}

// Delete 删除禁用记录
func (f *ForbiddenAccountDao) Delete(ctx context.Context, userIDs []string) error {
	if len(userIDs) == 0 {
		return nil
	}

	filter := bson.M{"user_id": bson.M{"$in": userIDs}}
	_, err := f.Collection.DeleteMany(ctx, filter)
	return err
}

// BlockUserWithOrgAndAttr 封禁用户结果结构体（4表联查结果）
type BlockUserWithOrgAndAttr struct {
	// 组织用户信息（根级别字段）
	OrganizationId primitive.ObjectID `bson:"organization_id" json:"organizationID"`
	UserId         string             `bson:"user_id" json:"userID"`
	ImServerUserId string             `bson:"im_server_user_id" json:"imServerUserID"`
	Role           string             `bson:"role" json:"role"`
	ThirdUserID    string             `bson:"third_user_id" json:"thirdUserID"`
	// 封禁信息
	ForbiddenAccount struct {
		UserID     string    `bson:"user_id" json:"userID"`
		Reason     string    `bson:"reason" json:"reason"`
		OperatorID string    `bson:"operator_id" json:"operatorID"`
		CreateTime time.Time `bson:"create_time" json:"createTime"`
	} `bson:"forbidden_account" json:"forbiddenAccount"`

	// IM用户信息（昵称、头像）
	User struct {
		UserID   string `bson:"user_id" json:"userID"`
		Nickname string `bson:"nickname" json:"nickname"`
		FaceURL  string `bson:"face_url" json:"faceUrl"`
	} `bson:"user" json:"user"`

	// 用户属性信息（account、email、性别等）
	UserAttr struct {
		UserID      string    `bson:"user_id" json:"userID"`
		Account     string    `bson:"account" json:"account"`
		PhoneNumber string    `bson:"phone_number" json:"phoneNumber"`
		AreaCode    string    `bson:"area_code" json:"areaCode"`
		Email       string    `bson:"email" json:"email"`
		Gender      int32     `bson:"gender" json:"gender"`
		CreateTime  time.Time `bson:"create_time" json:"createTime"`
		ChangeTime  time.Time `bson:"change_time" json:"changeTime"`
	} `bson:"user_attr" json:"userAttr"`
}

// SearchBlockUsersByOrg 查询指定组织的封禁用户
// 索引建议（与 pkg organization_user / forbidden_account 创建逻辑一致）：
// - organization_user: { organization_id: 1 } 或 { organization_id: 1, im_server_user_id: 1 } 用于首阶段 $match
// - forbidden_account: { user_id: 1 } 唯一索引，用于 $lookup 子管道
// - user: { user_id: 1 }；attribute: { user_id: 1 }
func (f *ForbiddenAccountDao) SearchBlockUsersByOrg(ctx context.Context, orgID primitive.ObjectID, keyword string, pageNumber, showNumber int32) (int64, []*BlockUserWithOrgAndAttr, error) {
	if pageNumber < 1 {
		pageNumber = 1
	}
	if showNumber < 1 {
		showNumber = 10
	}

	orgUserCollection := f.DB.Collection("organization_user")

	// 构建基础聚合管道（$lookup forbidden 使用 pipeline + $expr，便于走 forbidden_account.user_id 索引）
	pipeline := []bson.M{
		{
			"$match": bson.M{
				"organization_id": orgID,
			},
		},
		{
			"$lookup": bson.M{
				"from": "forbidden_account",
				"let":  bson.M{"imid": "$im_server_user_id"},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{"$eq": bson.A{"$user_id", "$$imid"}},
						},
					},
					{"$limit": 1},
				},
				"as": "forbidden_account",
			},
		},
		{
			"$unwind": bson.M{
				"path": "$forbidden_account",
			},
		},
		// 第四阶段：连接user表获取昵称和头像
		{
			"$lookup": bson.M{
				"from":         "user",
				"localField":   "im_server_user_id",
				"foreignField": "user_id",
				"as":           "user",
			},
		},
		// 第五阶段：展开user数组
		{
			"$unwind": bson.M{
				"path":                       "$user",
				"preserveNullAndEmptyArrays": true,
			},
		},
		// 第六阶段：连接attribute表获取用户详细属性
		{
			"$lookup": bson.M{
				"from":         "attribute",
				"localField":   "user_id",
				"foreignField": "user_id",
				"as":           "user_attr",
			},
		},
		// 第七阶段：展开user_attr数组
		{
			"$unwind": bson.M{
				"path":                       "$user_attr",
				"preserveNullAndEmptyArrays": true,
			},
		},
	}

	// 如果有关键词搜索，添加匹配条件
	if keyword != "" {
		searchStage := bson.M{
			"$match": bson.M{
				"$or": []bson.M{
					// organization_user表字段
					{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
					{"im_server_user_id": bson.M{"$regex": keyword, "$options": "i"}},
					// attribute表字段
					{"user_attr.account": bson.M{"$regex": keyword, "$options": "i"}},
					{"user_attr.email": bson.M{"$regex": keyword, "$options": "i"}},
					// user表字段
					{"user.nickname": bson.M{"$regex": keyword, "$options": "i"}},
					// forbidden_account表字段
					{"forbidden_account.reason": bson.M{"$regex": keyword, "$options": "i"}},
					{"forbidden_account.operator_id": bson.M{"$regex": keyword, "$options": "i"}},
				},
			},
		}
		pipeline = append(pipeline, searchStage)
	}

	skip := int64((pageNumber - 1) * showNumber)
	lim := int64(showNumber)

	// 单次聚合：$facet 同时产出总数与分页数据，避免对同一管道执行两次 Aggregate（原先会全表扫描两遍）
	facetStage := bson.M{
		"$facet": bson.M{
			"total": []bson.M{
				{"$count": "total"},
			},
			"list": []bson.M{
				{"$sort": bson.M{"forbidden_account.create_time": -1}},
				{"$skip": skip},
				{"$limit": lim},
			},
		},
	}
	pipeline = append(pipeline, facetStage)

	aggOpts := options.Aggregate().SetAllowDiskUse(true)
	cursor, err := orgUserCollection.Aggregate(ctx, pipeline, aggOpts)
	if err != nil {
		return 0, nil, err
	}
	defer cursor.Close(ctx)

	var facetRows []bson.M
	if err = cursor.All(ctx, &facetRows); err != nil {
		return 0, nil, err
	}

	var total int64
	var results []*BlockUserWithOrgAndAttr
	if len(facetRows) == 0 {
		return 0, results, nil
	}
	fr := facetRows[0]

	// total: [ { total: N } ] 或 []
	if ta, ok := fr["total"].(bson.A); ok && len(ta) > 0 {
		if tm, ok := ta[0].(bson.M); ok {
			total = countToInt64(tm["total"])
		}
	}

	if la, ok := fr["list"].(bson.A); ok {
		results = make([]*BlockUserWithOrgAndAttr, 0, len(la))
		for _, item := range la {
			var row BlockUserWithOrgAndAttr
			b, err := bson.Marshal(item)
			if err != nil {
				continue
			}
			if err := bson.Unmarshal(b, &row); err != nil {
				continue
			}
			results = append(results, &row)
		}
	}

	return total, results, nil
}

func countToInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int32:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	default:
		return 0
	}
}
