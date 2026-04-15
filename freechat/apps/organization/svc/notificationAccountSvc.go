package svc

import (
	"context"
	"fmt"
	"time"

	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/log"

	notificationSvc "github.com/openimsdk/chat/freechat/apps/notification/svc"
	"github.com/openimsdk/chat/freechat/apps/organization/dto"
	"github.com/openimsdk/chat/freechat/plugin"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// NotificationAccountSvc 通知账户服务
type NotificationAccountSvc struct{}

func NewNotificationAccountService() *NotificationAccountSvc {
	return &NotificationAccountSvc{}
}

// CreateNotificationAccount 创建组织通知账户
func (w *NotificationAccountSvc) CreateNotificationAccount(ctx context.Context, operationID string, orgId primitive.ObjectID, req *dto.CreateNotificationAccountReq) (*dto.NotificationAccountResp, error) {
	// 生成通知账户的userID
	notificationUserID, err := utils.NewId()
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	// 调用ImApiCaller创建通知账户
	imApiCaller := plugin.ImApiCaller()
	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err)
	}
	// 构建通知账户信息
	err = imApiCaller.AddNotificationAccount(mctx.WithApiToken(ctxWithOpID, adminToken), notificationUserID, req.Nickname, req.FaceURL, orgId.Hex())
	if err != nil {
		return nil, freeErrors.SystemErr(fmt.Errorf("failed to create notification account: %v", err))
	}

	// 返回创建的通知账户信息
	resp := &dto.NotificationAccountResp{
		UserID:    notificationUserID,
		Nickname:  req.Nickname,
		FaceURL:   req.FaceURL,
		OrgId:     orgId.Hex(),
		CreatedAt: time.Now().Unix(),
	}

	return resp, nil
}

// UpdateNotificationAccount 更新通知账户
func (w *NotificationAccountSvc) UpdateNotificationAccount(ctx context.Context, operationID string, orgId primitive.ObjectID, req *dto.UpdateNotificationAccountReq) error {
	db := plugin.MongoCli().GetDB()
	// 验证要更新的通知账户是否属于该组织
	userDao := openImModel.NewUserDao(db)
	user, err := userDao.Take(ctx, req.UserID)
	if err != nil {
		return freeErrors.ApiErr("notification account not found")
	}

	// 检查账户是否为通知账户（app_manger_level == 3）且属于该组织
	if user.AppMangerLevel != 3 || user.OrgId != orgId.Hex() {
		return freeErrors.ApiErr("invalid notification account or not belong to this organization")
	}

	// 调用ImApiCaller更新通知账户
	imApiCaller := plugin.ImApiCaller()
	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err)
	}
	// 构建通知账户信息
	err = imApiCaller.UpdateNotificationAccount(mctx.WithApiToken(ctxWithOpID, adminToken), req.UserID, req.Nickname, req.FaceURL)
	if err != nil {
		return freeErrors.SystemErr(fmt.Errorf("failed to update notification account: %v", err))
	}
	return nil
}

// SearchNotificationAccount 搜索通知账户
func (w *NotificationAccountSvc) SearchNotificationAccount(ctx context.Context, orgId primitive.ObjectID, req *dto.SearchNotificationAccountReq) (*dto.SearchNotificationAccountResp, error) {
	db := plugin.MongoCli().GetDB()
	// 设置分页参数
	page := req.Pagination
	if page == nil {
		page = &paginationUtils.DepPagination{
			Page:     1,
			PageSize: 20,
		}
	}

	// 调用DAO层搜索通知账户
	userDao := openImModel.NewUserDao(db)
	total, users, err := userDao.SearchNotificationAccounts(ctx, orgId.Hex(), req.Keyword, page)
	if err != nil {
		return nil, freeErrors.SystemErr(fmt.Errorf("failed to search notification accounts: %v", err))
	}

	// 转换为响应格式
	list := make([]*dto.NotificationAccountResp, 0, len(users))
	for _, user := range users {
		list = append(list, &dto.NotificationAccountResp{
			UserID:    user.UserID,
			Nickname:  user.Nickname,
			FaceURL:   user.FaceURL,
			OrgId:     user.OrgId,
			CreatedAt: user.CreateTime.Unix(),
		})
	}

	return &dto.SearchNotificationAccountResp{
		Total: uint32(total),
		List:  list,
	}, nil
}

// SendBannerNotification 发送图文通知
func (w *NotificationAccountSvc) SendBannerNotification(ctx context.Context, orgId primitive.ObjectID, operationID string, req *dto.SendBannerNotificationReq) error {
	db := plugin.MongoCli().GetDB()
	userDao := openImModel.NewUserDao(db)

	// 1. 验证发送者通知账户是否属于该组织
	sender, err := userDao.Take(ctx, req.SenderID)
	if err != nil {
		return freeErrors.ApiErr("sender notification account not found")
	}

	// 检查账户是否为通知账户（app_manger_level == 3）且属于该组织
	if sender.AppMangerLevel != 3 || sender.OrgId != orgId.Hex() {
		return freeErrors.ApiErr("invalid sender notification account or not belong to this organization")
	}

	// 2. 获取接收用户ID列表
	var recvIDs []string
	if req.SendToAll {
		// 发送给组织全部用户
		allUserIDs, err := userDao.GetOrgUserIDs(ctx, orgId.Hex())
		if err != nil {
			return freeErrors.SystemErr(fmt.Errorf("failed to get organization users: %v", err))
		}
		if len(allUserIDs) == 0 {
			return freeErrors.ApiErr("no users found in this organization")
		}
		recvIDs = allUserIDs
	} else {
		// 发送给指定用户，需要验证
		if len(req.RecvIDs) == 0 {
			return freeErrors.ApiErr("receiver list cannot be empty when send_to_all is false")
		}

		// 批量验证接收用户是否属于该组织，强校验：只给有效用户发送
		validUserIDs, err := userDao.VerifyUsersInOrg(ctx, req.RecvIDs, orgId.Hex())
		if err != nil {
			return freeErrors.SystemErr(fmt.Errorf("failed to verify users in organization: %v", err))
		}

		// 检查是否有有效的接收用户
		if len(validUserIDs) == 0 {
			return freeErrors.ApiErr("no valid receivers found in this organization")
		}

		recvIDs = validUserIDs
	}

	// 3. 构建图文通知内容
	notificationService := notificationSvc.NewNotificationService()
	content := notificationService.BuildBannerNotificationContent(sender.Nickname, sender.FaceURL, req.Elem, req.Ex)

	// 4. 构建批量发送消息数据
	batchMsgData := notificationSvc.BatchSendMsg{
		IsSendAll:      false,
		RecvIDs:        recvIDs,
		SendID:         req.SenderID,
		SenderNickname: sender.Nickname,
		SenderFaceURL:  sender.FaceURL,
		Content:        content,
		ContentType:    constantpb.OANotification, // 通知消息类型
		SessionType:    constantpb.SingleChatType, // 单聊类型
		IsOnlineOnly:   false,
		NotOfflinePush: false,
		SendTime:       time.Now().UnixMilli(),
		Ex:             req.Ex,
	}

	// 5. 调用通知服务发送消息
	err = notificationService.BatchSendNotification(ctx, batchMsgData, operationID)
	if err != nil {
		return freeErrors.SystemErr(fmt.Errorf("failed to send banner notification: %v", err))
	}

	return nil
}
