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
	// 聚合查询
	pipeline := []bson.M{
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
				"preserveNullAndEmptyArrays": false,
			},
		},
	}

	// 构建过滤条件
	filter := bson.M{}

	if !orgId.IsZero() {
		filter["org_id"] = orgId
	}

	if operationType != "" {
		filter["operation_type"] = operationType
	}

	if keyword != "" {
		filter["$or"] = []bson.M{
			{"user.nickname": bson.M{"$regex": keyword, "$options": "i"}},
			{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
			{"attribute.account": bson.M{"$regex": keyword, "$options": "i"}},
		}
	}

	findPipeline := make([]bson.M, 0)
	countPipeline := make([]bson.M, 0)

	if len(filter) > 0 {
		findPipeline = append(pipeline, bson.M{"$match": filter})
		countPipeline = append(pipeline, bson.M{"$match": filter})
	}

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

	countPipeline = append(countPipeline, bson.M{"$count": "total"})

	var countResult []bson.M
	cursor, err := o.Collection.Aggregate(ctx, countPipeline)
	if err != nil {
		return 0, nil, err
	}
	if err = cursor.All(ctx, &countResult); err != nil {
		return 0, nil, err
	}

	total := int32(0)
	if len(countResult) > 0 {
		total = countResult[0]["total"].(int32)
	}

	return int64(total), data, nil
}
