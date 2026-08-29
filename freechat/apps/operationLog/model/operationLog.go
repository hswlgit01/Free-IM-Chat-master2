package model

import (
	"context"
	"github.com/openimsdk/chat/freechat/constant"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// 群操作类型常量

type OperationLogType string

const (
	OpTypeCreateGroup   OperationLogType = "CreateGroup"   // 创建群组
	OpTypeAddBlockUser  OperationLogType = "AddBlockUser"  // 添加黑名单
	OpTypeUnBlockUser   OperationLogType = "UnBlockUser"   // 移除黑名单
	OpTypeUpdateOrgInfo OperationLogType = "UpdateOrgInfo" // 修改组织信息

	OpTypeCreateOrgCurrency  OperationLogType = "CreateOrganizationCurrency" // 创建组织货币
	OpTypeUpdateOrgCurrency  OperationLogType = "UpdateOrgCurrency"          // 修改组织货币
	OpTypeCreateBackendAdmin OperationLogType = "CreateBackendAdmin"         // 创建组织管理员

	OpTypeAdjustUserBalance    OperationLogType = "AdjustUserBalance"    // 后台手动调节用户余额
	OpTypeUpdateUserRole       OperationLogType = "UpdateUserRole"       // 修改用户角色
	OpTypeUpdateUserCanSendMsg OperationLogType = "UpdateUserCanSendMsg" // 修改用户是否可以发送消息

	OpTypeCreateUserTag       OperationLogType = "CreateUserTag"       // 创建用户标签
	OpTypeUpdateUserTag       OperationLogType = "UpdateUserTag"       // 修改用户标签
	OpTypeUpdateUserTagAssign OperationLogType = "UpdateUserTagAssign" // 给用户打标签

	OpTypeUpdateUserRolePermission OperationLogType = "UpdateUserRolePermission" // 修改用户角色权限

	OpTypeUpdateWalletPassword OperationLogType = "UpdateWalletPassword" // 修改组织钱包密码

	OpTypeCreateCheckinRewardCfg              OperationLogType = "CreateCheckinRewardCfg"              // 创建连续签到奖励配置
	OpTypeDeleteCheckinRewardCfg              OperationLogType = "DeleteCheckinRewardCfg"              // 删除连续签到奖励配置
	OpTypeCreateOrUpdateDailyCheckinRewardCfg OperationLogType = "CreateOrUpdateDailyCheckinRewardCfg" // 创建或更新日常签到奖励配置
	OpTypeDeleteDailyCheckinRewardCfg         OperationLogType = "DeleteDailyCheckinRewardCfg"         // 删除日常签到奖励配置
	OpTypeUpdateCheckinRuleDescription        OperationLogType = "UpdateCheckinRuleDescription"        // 更新签到规则说明
	OpTypeApproveUserCheckinReward            OperationLogType = "ApproveUserCheckinReward"            // 审批用户签到奖励
	OpTypeSupplementCheckin                   OperationLogType = "SupplementCheckin"                   // 管理员补签

	OpTypeCreateLottery      OperationLogType = "CreateLottery"      // 创建抽奖活动
	OpTypeUpdateLottery      OperationLogType = "UpdateLottery"      // 修改抽奖活动
	OpTypeAuditLotteryRecord OperationLogType = "AuditLotteryRecord" // 审批抽奖记录

	OpTypeCreateDefaultFriend OperationLogType = "CreateDefaultFriend" // 创建默认好友
	OpTypeDeleteDefaultFriend OperationLogType = "DeleteDefaultFriend" // 删除默认好友

	OpTypeCreateDefaultGroup OperationLogType = "CreateDefaultGroup" // 创建默认群
	OpTypeDeleteDefaultGroup OperationLogType = "DeleteDefaultGroup" // 删除默认群

	OpTypeRepairHierarchy   OperationLogType = "RepairHierarchy"   // 修复用户层级关系
	OpTypeFixCheckinRecords OperationLogType = "FixCheckinRecords" // 修复签到记录

	// dawn 2026-05-05 修复聊天记录审计：补充后台查看、撤回、删除的操作类型。
	OpTypeViewChatMessage   OperationLogType = "ViewChatMessage"   // 后台查看聊天记录
	OpTypeRevokeChatMessage OperationLogType = "RevokeChatMessage" // 后台撤回聊天消息
	OpTypeDeleteChatMessage OperationLogType = "DeleteChatMessage" // 后台删除聊天消息
)

// OperationLog 群操作日志表
type OperationLog struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	OrgId          primitive.ObjectID `bson:"org_id" json:"org_id"`
	UserId         string             `bson:"user_id" json:"user_id"` // 操作者用户ID
	ImServerUserId string             `bson:"im_server_user_id" json:"im_server_user_id"`

	OperationType OperationLogType `bson:"operation_type"`                       // 操作类型 (见常量定义)
	OperationTime time.Time        `bson:"operation_time" json:"operation_time"` // 操作时间
	Details       interface{}      `bson:"details" json:"details"`               // 操作详情,mongo对象格式
	DetailsRaw    string           `bson:"details_raw" json:"details_raw"`       // 操作详情,json文本数据

}

func (u OperationLog) TableName() string {
	return constant.CollectionOperationLog
}

func CreateOperationLogIndex(db *mongo.Database) error {
	m := &OperationLog{}

	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "user_id", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "im_server_user_id", Value: 1},
			},
		},
		// dawn 2026-06-17 优化后台操作日志慢查询：列表默认按组织和操作时间倒序查询。
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
				{Key: "operation_time", Value: -1},
			},
			Options: options.Index().SetName("idx_org_time"),
		},
		// dawn 2026-05-05 修复聊天记录审计查询：为操作类型、目标用户、会话和消息 ID 增加索引。
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
				{Key: "operation_type", Value: 1},
				{Key: "operation_time", Value: -1},
			},
			Options: options.Index().SetName("idx_org_operation_type_time"),
		},
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
				{Key: "details.target_user_id", Value: 1},
				{Key: "operation_time", Value: -1},
			},
			Options: options.Index().
				SetName("idx_org_chat_target_user_time").
				SetPartialFilterExpression(bson.M{"details.target_user_id": bson.M{"$exists": true}}),
		},
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
				{Key: "details.conversation_id", Value: 1},
				{Key: "operation_time", Value: -1},
			},
			Options: options.Index().
				SetName("idx_org_chat_conversation_time").
				SetPartialFilterExpression(bson.M{"details.conversation_id": bson.M{"$exists": true}}),
		},
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
				{Key: "details.server_msg_id", Value: 1},
			},
			Options: options.Index().
				SetName("idx_org_chat_server_msg_id").
				SetPartialFilterExpression(bson.M{"details.server_msg_id": bson.M{"$exists": true}}),
		},
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
				{Key: "details.client_msg_id", Value: 1},
			},
			Options: options.Index().
				SetName("idx_org_chat_client_msg_id").
				SetPartialFilterExpression(bson.M{"details.client_msg_id": bson.M{"$exists": true}}),
		},
	})
	return err
}

type OperationLogDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewOperationLogDao(db *mongo.Database) *OperationLogDao {
	m := OperationLog{}
	return &OperationLogDao{
		DB:         db,
		Collection: db.Collection(m.TableName()),
	}
}

func (o *OperationLogDao) Create(ctx context.Context, obj *OperationLog) error {
	obj.OperationTime = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*OperationLog{obj})
}

type OperationLogJoinAll struct {
	*OperationLog `bson:",inline"`

	User      map[string]interface{} `bson:"user"`
	Attribute map[string]interface{} `bson:"attribute"`
}

func (o *OperationLogDao) SelectJoinAll(ctx context.Context, keyword string, orgId primitive.ObjectID, operationType OperationLogType,
	page *paginationUtils.DepPagination) (int64, []*OperationLogJoinAll, error) {
	// 构建过滤条件
	baseFilter := bson.M{}

	if !orgId.IsZero() {
		baseFilter["org_id"] = orgId
	}

	if operationType != "" {
		baseFilter["operation_type"] = operationType
	}

	findPipeline := make([]bson.M, 0, 8)
	if len(baseFilter) > 0 {
		findPipeline = append(findPipeline, bson.M{"$match": baseFilter})
	}

	// dawn 2026-06-17 优化后台操作日志慢查询：无关键词时先走索引过滤、排序和分页，再关联用户信息。
	if keyword == "" {
		total, err := o.Collection.CountDocuments(ctx, baseFilter)
		if err != nil {
			return 0, nil, err
		}
		findPipeline = append(findPipeline, bson.M{"$sort": bson.M{"operation_time": -1}})
		if page != nil {
			findPipeline = append(findPipeline, page.ToBsonMList()...)
		}
		findPipeline = append(findPipeline, operationLogJoinStages()...)

		data, err := mongoutil.Aggregate[*OperationLogJoinAll](ctx, o.Collection, findPipeline)
		if err != nil {
			return 0, nil, err
		}
		return total, data, nil
	}

	findPipeline = append(findPipeline, operationLogJoinStages()...)
	regex := bson.M{"$regex": keyword, "$options": "i"}
	findPipeline = append(findPipeline, bson.M{"$match": bson.M{"$or": []bson.M{
		{"user.nickname": regex},
		{"user_id": regex},
		{"attribute.account": regex},
	}}})

	countPipeline := append([]bson.M{}, findPipeline...)
	countPipeline = append(countPipeline, bson.M{"$count": "total"})

	// 按时间倒序排列：从新到旧
	findPipeline = append(findPipeline, bson.M{"$sort": bson.M{"operation_time": -1}})

	// 添加排序和分页
	if page != nil {
		findPipeline = append(findPipeline, page.ToBsonMList()...)
	}

	// 执行聚合查询获取数据
	data, err := mongoutil.Aggregate[*OperationLogJoinAll](ctx, o.Collection, findPipeline)
	if err != nil {
		return 0, nil, err
	}

	var countResult []bson.M
	cursor, err := o.Collection.Aggregate(ctx, countPipeline)
	if err != nil {
		return 0, nil, err
	}
	defer cursor.Close(ctx)
	if err = cursor.All(ctx, &countResult); err != nil {
		return 0, nil, err
	}

	return extractOperationLogTotal(countResult), data, nil
}

func operationLogJoinStages() []bson.M {
	return []bson.M{
		{
			"$lookup": bson.M{
				"from":         constant.CollectionUser,
				"localField":   "im_server_user_id",
				"foreignField": "user_id",
				"as":           "user",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$user",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from":         chatModel.Attribute{}.TableName(),
				"localField":   "user_id",
				"foreignField": "user_id",
				"as":           "attribute",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$attribute",
				"preserveNullAndEmptyArrays": true,
			},
		},
	}
}

func extractOperationLogTotal(countResult []bson.M) int64 {
	if len(countResult) == 0 {
		return 0
	}
	switch v := countResult[0]["total"].(type) {
	case int32:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return 0
	}
}
