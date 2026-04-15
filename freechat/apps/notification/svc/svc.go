package svc

import (
	"context"
	"fmt"

	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/pkg/common/mctx"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/log"
)

// NotificationService 系统通知服务
type NotificationService struct {
}

// NewNotificationService 创建系统通知服务实例
func NewNotificationService() *NotificationService {
	return &NotificationService{}
}

type SendMsg struct {
	SendID           string                 `json:"sendID"`
	RecvID           string                 `json:"recvID"`
	GroupID          string                 `json:"groupID"`
	SenderNickname   string                 `json:"senderNickname"`
	SenderFaceURL    string                 `json:"senderFaceURL"`
	SenderPlatformID int32                  `json:"senderPlatformID"`
	Content          map[string]any         `json:"content"`
	ContentType      int32                  `json:"contentType"`
	SessionType      int32                  `json:"sessionType"`
	IsOnlineOnly     bool                   `json:"isOnlineOnly"`
	NotOfflinePush   bool                   `json:"notOfflinePush"`
	SendTime         int64                  `json:"sendTime"`
	OfflinePushInfo  *sdkws.OfflinePushInfo `json:"offlinePushInfo"`
	Ex               string                 `json:"ex"`
}
type BatchSendMsg struct {
	IsSendAll bool `json:"isSendAll"` // 默认false
	// RecvIDs is a slice of receiver identifiers to whom the message will be sent, required field.
	RecvIDs          []string               `json:"recvIDs"`
	SendID           string                 `json:"sendID"`
	GroupID          string                 `json:"groupID"`
	SenderNickname   string                 `json:"senderNickname"`
	SenderFaceURL    string                 `json:"senderFaceURL"`
	SenderPlatformID int32                  `json:"senderPlatformID"`
	Content          map[string]any         `json:"content"`
	ContentType      int32                  `json:"contentType"`
	SessionType      int32                  `json:"sessionType"`
	IsOnlineOnly     bool                   `json:"isOnlineOnly"`
	NotOfflinePush   bool                   `json:"notOfflinePush"`
	SendTime         int64                  `json:"sendTime"`
	OfflinePushInfo  *sdkws.OfflinePushInfo `json:"offlinePushInfo"`
	Ex               string                 `json:"ex"`
}

// SendNotification 发送系统通知
func (s *NotificationService) SendNotification(ctx context.Context, msgData SendMsg, operationID string) error {
	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()
	if imApiCaller == nil {
		return fmt.Errorf("IM API调用器未初始化")
	}

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "receiver_id", msgData.RecvID)
		return err
	}
	_, err = imApiCaller.SendMsg(mctx.WithApiToken(ctxWithOpID, adminToken), msgData)
	if err != nil {
		log.ZError(ctx, "发送通知失败", err,
			"receiver_id", msgData.RecvID)
		return err
	}
	log.ZInfo(ctx, "发送通知成功", "receiver_id", msgData.RecvID, "operation_id", operationID)
	return nil
}

// BatchSendNotification 批量发送系统通知
func (s *NotificationService) BatchSendNotification(ctx context.Context, msgData BatchSendMsg, operationID string) error {
	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()
	if imApiCaller == nil {
		return fmt.Errorf("IM API调用器未初始化")
	}
	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return err
	}
	msgData.IsSendAll = false
	err = imApiCaller.BatchSendMsg(mctx.WithApiToken(ctxWithOpID, adminToken), msgData)
	if err != nil {
		log.ZError(ctx, "批量发送通知失败", err, "sendID", msgData.SendID)
		return err
	}
	log.ZInfo(ctx, "发送通知成功", "operation_id", operationID)
	return nil
}

// BuildBannerNotificationContent 构建图文通知内容，参考退款实现
func (s *NotificationService) BuildBannerNotificationContent(senderNickname, senderFaceURL string, bannerElem interface{}, ex string) map[string]any {
	return map[string]any{
		"notificationName":    senderNickname,
		"notificationFaceURL": senderFaceURL,
		"notificationType":    600, // 图文消息
		"text":                "[通知]",
		"externalUrl":         "",
		"mixType":             1,          // 图片+文字
		"bannerElem":          bannerElem, // 前端传递什么就发送什么
		"ex":                  ex,
	}
}
