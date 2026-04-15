package model

import (
	"context"
	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// 群操作类型常量
const (
	GroupOpTypeCreateGroup      = 1001 // 创建群组
	GroupOpTypeKickMember       = 1002 // 踢出群成员
	GroupOpTypeDismissGroup     = 1003 // 解散群组
	GroupOpTypeTransferOwner    = 1004 // 转移群主
	GroupOpTypeMuteMember       = 1005 // 禁言群成员
	GroupOpTypeCancelMuteMember = 1006 // 取消禁言群成员
	GroupOpTypeMuteGroup        = 1007 // 禁言整个群
	GroupOpTypeCancelMuteGroup  = 1008 // 取消群禁言
)

// GroupOperationLog 群操作日志表
type GroupOperationLog struct {
	ID             string    `bson:"_id"`              // 日志ID
	GroupID        string    `bson:"group_id"`         // 群组ID
	OperatorUserID string    `bson:"operator_user_id"` // 操作者用户ID
	TargetUserID   string    `bson:"target_user_id"`   // 被操作者用户ID (可为空，如群禁言操作)
	OperationType  int32     `bson:"operation_type"`   // 操作类型 (见常量定义)
	OperationTime  time.Time `bson:"operation_time"`   // 操作时间
	Details        string    `bson:"details"`          // 操作详情 (JSON格式存储具体参数)
	Ex             string    `bson:"ex"`               // 扩展字段
}

func (u GroupOperationLog) TableName() string {
	return constant.CollectionGroupOperationLog
}

type GroupOperationLogDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewGroupOperationLogDao(db *mongo.Database) *GroupOperationLogDao {
	m := GroupOperationLog{}
	return &GroupOperationLogDao{
		DB:         db,
		Collection: db.Collection(m.TableName()),
	}
}

func (o *GroupOperationLogDao) Select(ctx context.Context, groupID string,
	page *paginationUtils.DepPagination) (int64, []*GroupOperationLog, error) {
	filter := bson.M{}

	if groupID != "" {
		filter["group_id"] = groupID
	}

	if len(filter) == 0 {
		filter = nil
	}

	opts := make([]*options.FindOptions, 0)
	opts = append(opts, options.Find().SetSort(bson.M{"operation_time": -1}))

	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	data, err := mongoutil.Find[*GroupOperationLog](ctx, o.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}
