package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/pkg/common/db/table/chat"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/chat/tools/db/pagination"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Attribute 用户属性表结构
type Attribute struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	UserID           string    `bson:"user_id" json:"user_id"`
	Account          string    `bson:"account" json:"account"`
	PhoneNumber      string    `bson:"phone_number" json:"phone_number"`
	AreaCode         string    `bson:"area_code" json:"area_code"`
	Email            string    `bson:"email" json:"email"`
	Nickname         string    `bson:"nickname" json:"nickname"`
	FaceURL          string    `bson:"face_url" json:"face_url"`
	Gender           int32     `bson:"gender" json:"gender"`
	CreateTime       time.Time `bson:"create_time" json:"create_time"`
	ChangeTime       time.Time `bson:"change_time" json:"change_time"`
	BirthTime        time.Time `bson:"birth_time" json:"birth_time"`
	Level            int32     `bson:"level" json:"level"`
	AllowVibration   int32     `bson:"allow_vibration" json:"allow_vibration"`
	AllowBeep        int32     `bson:"allow_beep" json:"allow_beep"`
	AllowAddFriend   int32     `bson:"allow_add_friend" json:"allow_add_friend"`
	GlobalRecvMsgOpt int32     `bson:"global_recv_msg_opt" json:"global_recv_msg_opt"`
	RegisterType     int32     `bson:"register_type" json:"register_type"`
	// 实名认证字段
	IsRealNameVerified bool      `bson:"is_real_name_verified" json:"is_real_name_verified"` // 是否已实名认证
	RealName           string    `bson:"real_name" json:"real_name"`                         // 真实姓名
	VerifiedTime       time.Time `bson:"verified_time" json:"verified_time"`                 // 认证通过时间
}

// SuperAdminUserListItem 超管「全部用户」列表行：user_id；account 来自 attribute；create_time 对应 organization_user.created_at（最新一条）
type SuperAdminUserListItem struct {
	UserID     string    `bson:"user_id" json:"user_id"`
	Account    string    `bson:"account" json:"account"`
	CreateTime time.Time `bson:"create_time" json:"create_time"`
}

// AttributeWithImUserAndOrgUser 包含 IM user 表信息的连表查询结果结构体
type AttributeWithImUserAndOrgUser struct {
	UserID           string    `bson:"user_id" json:"user_chat_id"`
	Account          string    `bson:"account" json:"account"`
	PhoneNumber      string    `bson:"phone_number" json:"phone_number"`
	AreaCode         string    `bson:"area_code" json:"area_code"`
	Email            string    `bson:"email" json:"email"`
	Nickname         string    `bson:"nickname" json:"nickname"`
	FaceURL          string    `bson:"face_url" json:"face_url"`
	Gender           int32     `bson:"gender" json:"gender"`
	CreateTime       time.Time `bson:"create_time" json:"create_time"`
	ChangeTime       time.Time `bson:"change_time" json:"change_time"`
	BirthTime        time.Time `bson:"birth_time" json:"birth"`
	Level            int32     `bson:"level" json:"level"`
	AllowVibration   int32     `bson:"allow_vibration" json:"allow_vibration"`
	AllowBeep        int32     `bson:"allow_beep" json:"allow_beep"`
	AllowAddFriend   int32     `bson:"allow_add_friend" json:"allow_add_friend"`
	GlobalRecvMsgOpt int32     `bson:"global_recv_msg_opt" json:"global_recv_msg_opt"`
	RegisterType     int32     `bson:"register_type" json:"register_type"`
	ImServerUserId   string    `bson:"im_server_user_id" json:"user_id"`
	OrgRole          string    `bson:"org_role" json:"org_role"`
	ImNickname       string    `bson:"im_nickname" json:"im_nickname"`
	ImFaceURL        string    `bson:"im_face_url" json:"im_face_url"`
	InvitationCode   string    `bson:"invitation_code" json:"invitation_code"`
	CanSendFreeMsg   int32     `bson:"can_send_free_msg" json:"can_send_free_msg"`
}

func (Attribute) TableName() string {
	return "attribute"
}

// AttributeDao 用户属性数据访问对象
type AttributeDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

// NewAttributeDao 创建新的AttributeDao实例
func NewAttributeDao(db *mongo.Database) *AttributeDao {
	return &AttributeDao{
		DB:         db,
		Collection: db.Collection(Attribute{}.TableName()),
	}
}

// Take 根据用户ID获取用户信息
func (a *AttributeDao) Take(ctx context.Context, userID string) (*Attribute, error) {
	return mongoutil.FindOne[*Attribute](ctx, a.Collection, bson.M{"user_id": userID})
}

// TakePhone 根据手机号和区号获取用户信息
func (a *AttributeDao) TakePhone(ctx context.Context, areaCode string, phoneNumber string) (*Attribute, error) {
	return mongoutil.FindOne[*Attribute](ctx, a.Collection, bson.M{
		"area_code":    areaCode,
		"phone_number": phoneNumber,
	})
}

// TakeEmail 根据邮箱获取用户信息
func (a *AttributeDao) TakeEmail(ctx context.Context, email string) (*Attribute, error) {
	return mongoutil.FindOne[*Attribute](ctx, a.Collection, bson.M{"email": email})
}

// TakeAccount 根据账号获取用户信息
func (a *AttributeDao) TakeAccount(ctx context.Context, account string) (*Attribute, error) {
	return mongoutil.FindOne[*Attribute](ctx, a.Collection, bson.M{"account": account})
}

// TakeByKeyword 根据关键词搜索用户信息
func (a *AttributeDao) TakeByKeyword(ctx context.Context, keyword string, genders []int32, userIds []string) (*Attribute, error) {
	filter := bson.M{}

	// 添加关键词搜索
	if keyword != "" {
		filter["$or"] = []bson.M{
			{"account": keyword},
			{"email": keyword},
			{"user_id": keyword},
		}
	}

	// 添加性别过滤
	if len(genders) > 0 {
		filter["gender"] = bson.M{"$in": genders}
	}

	if len(userIds) > 0 {
		filter["user_id"] = bson.M{"$in": userIds}
	}

	findOptions := options.FindOne()
	// 默认按创建时间倒序排序
	findOptions.SetSort(bson.D{{Key: "create_time", Value: -1}})

	return mongoutil.FindOne[*Attribute](ctx, a.Collection, filter, findOptions)
}

// Find 根据用户ID列表批量获取用户信息
func (a *AttributeDao) Find(ctx context.Context, userIds []string) ([]*Attribute, error) {
	if len(userIds) == 0 {
		return []*Attribute{}, nil
	}

	// 过滤掉空字符串的用户ID
	validUserIds := make([]string, 0, len(userIds))
	for _, userId := range userIds {
		if userId != "" {
			validUserIds = append(validUserIds, userId)
		}
	}

	// 如果过滤后没有有效用户ID，直接返回空列表
	if len(validUserIds) == 0 {
		return []*Attribute{}, nil
	}

	return mongoutil.Find[*Attribute](ctx, a.Collection, bson.M{"user_id": bson.M{"$in": validUserIds}})
}

// FindAccount 根据账号列表批量获取用户信息
//func (a *AttributeDao) FindAccount(ctx context.Context, accounts []string) ([]*Attribute, error) {
//	if len(accounts) == 0 {
//		return []*Attribute{}, nil
//	}
//
//	// 过滤掉空字符串的账户
//	validAccounts := make([]string, 0, len(accounts))
//	for _, account := range accounts {
//		if account != "" {
//			validAccounts = append(validAccounts, account)
//		}
//	}
//
//	// 如果过滤后没有有效账户，直接返回空列表
//	if len(validAccounts) == 0 {
//		return []*Attribute{}, nil
//	}
//
//	return mongoutil.Find[*Attribute](ctx, a.Collection, bson.M{"account": bson.M{"$in": validAccounts}})
//}

// FindAccountCaseInsensitive 根据账号列表批量获取用户信息（不区分大小写）
func (a *AttributeDao) FindAccountCaseInsensitive(ctx context.Context, accounts []string) ([]*Attribute, error) {
	if len(accounts) == 0 {
		return []*Attribute{}, nil
	}

	// 过滤掉空字符串的账户
	validAccounts := make([]string, 0, len(accounts))
	for _, account := range accounts {
		if account != "" {
			validAccounts = append(validAccounts, account)
		}
	}

	// 如果过滤后没有有效账户，直接返回空列表
	if len(validAccounts) == 0 {
		return []*Attribute{}, nil
	}

	// 构建不区分大小写的正则表达式查询条件
	regexConditions := make([]bson.M, 0, len(validAccounts))
	for _, account := range validAccounts {
		// 转义特殊字符并创建不区分大小写的正则表达式
		// 使用 \Q...\E 来避免正则表达式特殊字符的影响
		escapedAccount := bson.M{"$regex": "^\\Q" + account + "\\E$", "$options": "i"}
		regexConditions = append(regexConditions, bson.M{"account": escapedAccount})
	}

	// 使用 $or 操作符组合所有条件
	filter := bson.M{"$or": regexConditions}

	return mongoutil.Find[*Attribute](ctx, a.Collection, filter)
}

// CheckAccountExists 检查账户是否已存在（不区分大小写）
func (a *AttributeDao) CheckAccountExists(ctx context.Context, account string) (bool, error) {
	if account == "" {
		return false, nil
	}

	// 构建不区分大小写的正则表达式查询条件
	filter := bson.M{
		"account": bson.M{
			"$regex":   "^\\Q" + account + "\\E$",
			"$options": "i",
		},
	}

	// 只需要检查是否存在，不需要返回具体数据
	count, err := a.Collection.CountDocuments(ctx, filter)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// FindEmail 根据邮箱列表批量获取用户信息
func (a *AttributeDao) FindEmail(ctx context.Context, emails []string) ([]*Attribute, error) {
	if len(emails) == 0 {
		return []*Attribute{}, nil
	}

	// 过滤掉空字符串的邮箱
	validEmails := make([]string, 0, len(emails))
	for _, email := range emails {
		if email != "" {
			validEmails = append(validEmails, email)
		}
	}

	// 如果过滤后没有有效邮箱，直接返回空列表
	if len(validEmails) == 0 {
		return []*Attribute{}, nil
	}

	return mongoutil.Find[*Attribute](ctx, a.Collection, bson.M{"email": bson.M{"$in": validEmails}})
}

func (a *AttributeDao) Create(ctx context.Context, attribute ...*Attribute) error {
	return mongoutil.InsertMany(ctx, a.Collection, attribute)
}

func (a *AttributeDao) Update(ctx context.Context, userID string, data map[string]any) error {
	if len(data) == 0 {
		return nil
	}
	filter := bson.M{"user_id": userID}
	update := bson.M{"$set": data}
	return mongoutil.UpdateOne(ctx, a.Collection, filter, update, false)
}

// Search 搜索用户信息
func (a *AttributeDao) Select(ctx context.Context, keyword string, genders []int32, userIds []string, p pagination.Pagination) (int64, []*Attribute, error) {
	filter := bson.M{}

	// 添加关键词搜索
	if keyword != "" {
		filter["$or"] = []bson.M{
			{"account": keyword},
			{"email": keyword},
			{"user_id": keyword},
			//{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
		}
	}

	// 添加性别过滤
	if len(genders) > 0 {
		filter["gender"] = bson.M{"$in": genders}
	}

	if len(userIds) > 0 {
		filter["user_id"] = bson.M{"$in": userIds}
	}

	// 设置查询选项
	findOptions := options.Find()
	if p != nil && p.GetPageNumber() > 0 && p.GetShowNumber() > 0 {
		findOptions.SetSkip(int64((p.GetPageNumber() - 1) * p.GetShowNumber()))
		findOptions.SetLimit(int64(p.GetShowNumber()))
	}

	// 默认按创建时间倒序排序
	findOptions.SetSort(bson.D{{Key: "create_time", Value: -1}})

	// 获取总数
	count, err := a.Collection.CountDocuments(ctx, filter)
	if err != nil {
		log.ZError(ctx, "count user attributes error", err)
		return 0, nil, err
	}

	// 执行查询
	cursor, err := a.Collection.Find(ctx, filter, findOptions)
	if err != nil {
		log.ZError(ctx, "find user attributes error", err)
		return 0, nil, err
	}
	defer cursor.Close(ctx)

	var attributes []*Attribute
	if err := cursor.All(ctx, &attributes); err != nil {
		log.ZError(ctx, "decode user attributes error", err)
		return 0, nil, err
	}

	return count, attributes, nil
}

// SearchUser 搜索用户信息（高级搜索，连表查询organization_user并过滤管理员角色）
func (a *AttributeDao) SearchUser(ctx context.Context, keyword string, userIDs []string, genders []int32, excludeUserIDs []string, p pagination.Pagination) (int64, []*Attribute, error) {
	// 构建基础匹配条件
	baseMatchStage := bson.M{}

	// 添加用户ID过滤
	if len(userIDs) > 0 {
		baseMatchStage["user_id"] = bson.M{"$in": userIDs}
	}

	// 排除特定用户ID（如被封禁用户）
	if len(excludeUserIDs) > 0 {
		baseMatchStage["user_id"] = bson.M{"$nin": excludeUserIDs}
	}

	// 如果同时有包含和排除条件，需要合并
	if len(userIDs) > 0 && len(excludeUserIDs) > 0 {
		baseMatchStage["user_id"] = bson.M{
			"$in":  userIDs,
			"$nin": excludeUserIDs,
		}
	}

	// 添加性别过滤
	if len(genders) > 0 {
		baseMatchStage["gender"] = bson.M{"$in": genders}
	}

	// 构建基础聚合管道
	basePipeline := []bson.M{
		// 第一阶段：匹配基础条件
		{"$match": baseMatchStage},
		// 连接organization_user表
		{
			"$lookup": bson.M{
				"from":         "organization_user",
				"localField":   "user_id",
				"foreignField": "user_id",
				"as":           "org_user",
			},
		},
		// 展开org_user数组以便过滤
		{
			"$unwind": bson.M{
				"path":                       "$org_user",
				"preserveNullAndEmptyArrays": false,
			},
		},
		// 过滤掉超级管理员和后端管理员
		{
			"$match": bson.M{
				"org_user.role": bson.M{"$nin": []string{"SuperAdmin", "BackendAdmin"}},
			},
		},
		// 按user_id分组去重，保留第一个匹配的attribute记录
		{
			"$group": bson.M{
				"_id": "$user_id",
				"doc": bson.M{"$first": "$$ROOT"},
			},
		},
		// 重新构造原始的Attribute结构，移除org_user字段
		{
			"$replaceRoot": bson.M{
				"newRoot": bson.M{
					"_id":                 "$doc._id",
					"user_id":             "$doc.user_id",
					"account":             "$doc.account",
					"phone_number":        "$doc.phone_number",
					"area_code":           "$doc.area_code",
					"email":               "$doc.email",
					"nickname":            "$doc.nickname",
					"face_url":            "$doc.face_url",
					"gender":              "$doc.gender",
					"create_time":         "$doc.create_time",
					"change_time":         "$doc.change_time",
					"birth_time":          "$doc.birth_time",
					"level":               "$doc.level",
					"allow_vibration":     "$doc.allow_vibration",
					"allow_beep":          "$doc.allow_beep",
					"allow_add_friend":    "$doc.allow_add_friend",
					"global_recv_msg_opt": "$doc.global_recv_msg_opt",
					"register_type":       "$doc.register_type",
				},
			},
		},
	}

	// 如果有关键词搜索，添加关键词匹配阶段
	if keyword != "" {
		keywordMatchStage := bson.M{
			"$match": bson.M{
				"$or": []bson.M{
					{"nickname": bson.M{"$regex": keyword, "$options": "i"}},
					{"account": bson.M{"$regex": keyword, "$options": "i"}},
					{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
					{"phone_number": bson.M{"$regex": keyword, "$options": "i"}},
					{"email": bson.M{"$regex": keyword, "$options": "i"}},
				},
			},
		}
		basePipeline = append(basePipeline, keywordMatchStage)
	}

	// 计算总数 - 使用独立的管道副本
	countPipeline := make([]bson.M, len(basePipeline))
	copy(countPipeline, basePipeline)
	countPipeline = append(countPipeline, bson.M{"$count": "total"})

	// 执行计数查询
	countCursor, err := a.Collection.Aggregate(ctx, countPipeline)
	if err != nil {
		log.ZError(ctx, "count user attributes error", err)
		return 0, nil, err
	}
	defer countCursor.Close(ctx)

	var countResult []bson.M
	if err = countCursor.All(ctx, &countResult); err != nil {
		log.ZError(ctx, "count result decode error", err)
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

	// 构建查询管道 - 添加排序和分页
	queryPipeline := make([]bson.M, len(basePipeline))
	copy(queryPipeline, basePipeline)

	// 添加排序
	queryPipeline = append(queryPipeline, bson.M{"$sort": bson.M{"create_time": -1}})

	// 添加分页
	if p != nil && p.GetPageNumber() > 0 && p.GetShowNumber() > 0 {
		skip := (p.GetPageNumber() - 1) * p.GetShowNumber()
		queryPipeline = append(queryPipeline,
			bson.M{"$skip": skip},
			bson.M{"$limit": p.GetShowNumber()},
		)
	}

	// 执行查询
	cursor, err := a.Collection.Aggregate(ctx, queryPipeline)
	if err != nil {
		log.ZError(ctx, "find user attributes error", err)
		return 0, nil, err
	}
	defer cursor.Close(ctx)

	var attributes []*Attribute
	if err = cursor.All(ctx, &attributes); err != nil {
		log.ZError(ctx, "decode user attributes error", err)
		return 0, nil, err
	}

	return total, attributes, nil
}

// CreateAttributeQueryIndexes 列表/搜索类查询辅助索引（启动时调用）
func CreateAttributeQueryIndexes(db *mongo.Database) error {
	coll := db.Collection(Attribute{}.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{Keys: bson.D{{Key: "create_time", Value: -1}}},
		// 注意：user_id 上通常已有 OpenIM 初始化创建的唯一索引 user_id_1，勿再 Create，否则会 IndexKeySpecsConflict
		// 层级搜索 $lookup 后按账号/昵称左前缀正则，单列索引便于锚定前缀扫描
		{Keys: bson.D{{Key: "account", Value: 1}}},
		//{Keys: bson.D{{Key: "nickname", Value: 1}}},
	})
	if err != nil {
		return errs.Wrap(err)
	}
	return nil
}

// SearchNormalUser 搜索普通用户信息
func (o *AttributeDao) SearchNormalUser(ctx context.Context, keyword string, genders []int32, pagination pagination.Pagination, org_id primitive.ObjectID) (int64, []*chat.AttributeWithOrgUser, error) {

	// 构建基础匹配条件（不包含关键词搜索）
	baseMatchStage := bson.M{}

	// 添加性别过滤
	if len(genders) > 0 {
		baseMatchStage["gender"] = bson.M{"$in": genders}
	}

	// 构建基础聚合管道
	basePipeline := []bson.M{
		// 第一阶段：匹配基础条件
		{"$match": baseMatchStage},
	}

	// 如果提供了org_id，添加连表查询
	if org_id != primitive.NilObjectID {
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
					"org_user.organization_id": org_id,
					// 排除超级管理员和后端管理员
					"org_user.role": bson.M{"$nin": []string{string(chat.OrganizationUserSuperAdminRole), string(chat.OrganizationUserBackendAdminRole)}},
				},
			},

			// 添加im_server_user_id字段
			bson.M{
				"$addFields": bson.M{
					"im_server_user_id": "$org_user.im_server_user_id",
					"third_user_id":     "$org_user.third_user_id",
					"invitation_code":   "$org_user.invitation_code",
					"inviter":           "$org_user.inviter",
					"inviter_type":      "$org_user.inviter_type",
					"role":              "$org_user.role",
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
						{"nickname": bson.M{"$regex": keyword, "$options": "i"}},
						{"account": bson.M{"$regex": keyword, "$options": "i"}},
						{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
						{"phone_number": bson.M{"$regex": keyword, "$options": "i"}},
						{"email": bson.M{"$regex": keyword, "$options": "i"}},
						{"im_server_user_id": bson.M{"$regex": keyword, "$options": "i"}}, // 只有在有 org_id 时才支持搜索 im_server_user_id
						{"third_user_id": bson.M{"$regex": keyword, "$options": "i"}},
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
						{"nickname": bson.M{"$regex": keyword, "$options": "i"}},
						{"account": bson.M{"$regex": keyword, "$options": "i"}},
						{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
						{"phone_number": bson.M{"$regex": keyword, "$options": "i"}},
						{"email": bson.M{"$regex": keyword, "$options": "i"}},
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

	countCursor, err := o.Collection.Aggregate(ctx, countPipeline)
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
	cursor, err := o.Collection.Aggregate(ctx, queryPipeline)
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

// Delete 删除用户信息
func (a *AttributeDao) Delete(ctx context.Context, userIDs []string) error {
	if len(userIDs) == 0 {
		return nil
	}
	filter := bson.M{"user_id": bson.M{"$in": userIDs}}
	_, err := a.Collection.DeleteMany(ctx, filter)
	return err
}

// GetUserDetailWithOrganizations 获取用户详情和组织信息（使用聚合查询）
func (a *AttributeDao) GetUserDetailWithOrganizations(ctx context.Context, userID string) ([]interface{}, error) {
	// 使用聚合查询连表获取用户信息和组织信息
	pipeline := []bson.M{
		// 匹配指定用户ID
		{
			"$match": bson.M{
				"user_id": userID,
			},
		},
		// 连接 organization_user 表
		{
			"$lookup": bson.M{
				"from":         "organization_user",
				"localField":   "user_id",
				"foreignField": "user_id",
				"as":           "org_users",
			},
		},
		// 展开 org_users 数组
		{
			"$unwind": bson.M{
				"path":                       "$org_users",
				"preserveNullAndEmptyArrays": true,
			},
		},
		// 连接 organization 表
		{
			"$lookup": bson.M{
				"from":         "organization",
				"localField":   "org_users.organization_id",
				"foreignField": "_id",
				"as":           "organization",
			},
		},
		// 展开 organization 数组
		{
			"$unwind": bson.M{
				"path":                       "$organization",
				"preserveNullAndEmptyArrays": true,
			},
		},
		// 连接 user 表获取IM用户信息
		{
			"$lookup": bson.M{
				"from":         "user",
				"localField":   "org_users.im_server_user_id",
				"foreignField": "user_id",
				"as":           "im_user",
			},
		},
		// 展开 im_user 数组
		{
			"$unwind": bson.M{
				"path":                       "$im_user",
				"preserveNullAndEmptyArrays": true,
			},
		},
		// 不进行分组，直接返回每个用户-组织的组合记录
		{
			"$project": bson.M{
				"user_info": "$im_user", // 直接使用user表的全部数据
				"organization_info": bson.M{
					"$cond": bson.M{
						"if": bson.M{"$ne": []interface{}{"$org_users", nil}},
						"then": bson.M{
							"organization_id":   "$organization._id",
							"organization_name": "$organization.name",
							"organization_logo": "$organization.logo",
							"role":              "$org_users.role",
							"im_server_user_id": "$org_users.im_server_user_id",
							"third_user_id":     "$org_users.third_user_id",
							"invitation_code":   "$org_users.invitation_code",
							"status":            "$org_users.status",
							"created_at":        "$org_users.created_at",
						},
						"else": nil,
					},
				},
			},
		},
	}

	// 执行聚合查询
	cursor, err := a.Collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, errs.New("用户不存在")
	}

	// 转换为数组格式，每个元素包含用户信息和对应的组织信息
	var userDetailArray []interface{}
	for _, result := range results {
		userDetail := map[string]interface{}{
			"user_info": result["user_info"],
		}

		// 如果有组织信息，添加到结果中
		if orgInfo := result["organization_info"]; orgInfo != nil {
			userDetail["organization_info"] = orgInfo
		} else {
			userDetail["organization_info"] = nil
		}

		userDetailArray = append(userDetailArray, userDetail)
	}

	// 如果用户没有任何组织，返回一个只包含用户信息的记录
	if len(userDetailArray) == 0 && len(results) > 0 {
		userDetailArray = append(userDetailArray, map[string]interface{}{
			"user_info":         results[0]["user_info"],
			"organization_info": nil,
		})
	}

	return userDetailArray, nil
}
