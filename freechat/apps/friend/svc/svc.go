package svc

import (
	"context"
	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/pkg/common/mctx"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/relation"
	"github.com/openimsdk/tools/log"
)

type FriendSvc struct{}

func NewFriendSvc() *FriendSvc {
	return &FriendSvc{}
}

func (s *FriendSvc) WebApplyToAddFriend(ctx context.Context, org *orgModel.Organization, operationID string, req relation.ApplyToAddFriendReq) error {
	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return err
	}

	// 调用CreateGroup方法
	err = imApiCaller.AddFriend(mctx.WithApiToken(ctxWithOpID, adminToken), req)
	if err != nil {
		log.ZError(ctx, "添加好友失败", err, "operation_id", operationID)
		return err
	}

	log.ZInfo(ctx, "添加好友成功", "operation_id", operationID)
	return nil
}
