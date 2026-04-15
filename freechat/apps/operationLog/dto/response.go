package dto

import (
	"context"
	"encoding/json"
	"github.com/openimsdk/chat/freechat/apps/operationLog/model"
	userDto "github.com/openimsdk/chat/freechat/apps/user/dto"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type GroupOperationLogResp struct {
	ID             string                 `bson:"_id" json:"id"`                            // 日志ID
	GroupID        string                 `bson:"group_id" json:"group_id"`                 // 群组ID
	OperatorUserID string                 `bson:"operator_user_id" json:"operator_user_id"` // 操作者用户ID
	TargetUserID   string                 `bson:"target_user_id" json:"target_user_id"`     // 被操作者用户ID (可为空，如群禁言操作)
	OperationType  int32                  `bson:"operation_type" json:"operation_type"`     // 操作类型 (见常量定义)
	OperationTime  time.Time              `bson:"operation_time" json:"operation_time"`     // 操作时间
	Details        map[string]interface{} `bson:"details" json:"details"`                   // 操作详情 (JSON格式存储具体参数)
	Ex             string                 `bson:"ex" json:"ex"`                             // 扩展字段

	OperatorUser *userDto.UserResp `json:"operator_user"`
	TargetUser   *userDto.UserResp `json:"target_user"`
}

func NewGroupOperationLogResp(ctx context.Context, db *mongo.Database, obj *openImModel.GroupOperationLog) (*GroupOperationLogResp, error) {
	userDao := openImModel.NewUserDao(db)

	opUser, err := userDao.Take(ctx, obj.OperatorUserID)
	if err != nil {
		return nil, err
	}
	operatorUserResp := userDto.NewUserResp(opUser)

	var targetUserResp *userDto.UserResp
	if obj.TargetUserID != "" {
		targetUser, err := userDao.Take(ctx, obj.OperatorUserID)
		if err != nil {
			return nil, err
		}
		targetUserResp = userDto.NewUserResp(targetUser)
	}

	details := make(map[string]interface{})
	if obj.Details != "" {
		_ = json.Unmarshal([]byte(obj.Details), &details)
	}

	return &GroupOperationLogResp{
		ID:             obj.ID,
		GroupID:        obj.GroupID,
		OperatorUserID: obj.OperatorUserID,
		TargetUserID:   obj.TargetUserID,
		OperationType:  obj.OperationType,
		OperationTime:  obj.OperationTime,
		Details:        details,
		Ex:             obj.Ex,
		OperatorUser:   operatorUserResp,
		TargetUser:     targetUserResp,
	}, nil
}

type OperationLogJoinAllResp struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	OrgId          primitive.ObjectID `json:"org_id"`
	UserId         string             `json:"user_id"` // 操作者用户ID
	ImServerUserId string             `json:"im_server_user_id"`

	OperationType model.OperationLogType `json:"operation_type"`                 // 操作类型 (见常量定义)
	OperationTime time.Time              `json:"operation_time"`                 // 操作时间
	Details       interface{}            `json:"details"`                        // 操作详情 (JSON格式存储具体参数)
	DetailsRaw    string                 `bson:"details_raw" json:"details_raw"` // 操作详情,json文本数据

	User      map[string]interface{} `json:"user"`
	Attribute map[string]interface{} `json:"attribute"`
}

func NewOperationLogJoinAllResp(db *mongo.Database, obj *model.OperationLogJoinAll) *OperationLogJoinAllResp {
	var details interface{}
	switch v := obj.Details.(type) {
	case primitive.D:
		dict := make(map[string]interface{})
		for _, item := range v {
			dict[item.Key] = item.Value
		}
		details = dict
	default:
		details = v
	}

	return &OperationLogJoinAllResp{
		ID:             obj.ID,
		OrgId:          obj.OrgId,
		UserId:         obj.UserId,
		ImServerUserId: obj.ImServerUserId,
		OperationType:  obj.OperationType,
		OperationTime:  obj.OperationTime,
		Details:        details,
		User:           obj.User,
		Attribute:      obj.Attribute,
		DetailsRaw:     obj.DetailsRaw,
	}
}
