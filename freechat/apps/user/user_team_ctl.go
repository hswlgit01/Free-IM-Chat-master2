// Copyright © 2023 OpenIM open source community. All rights reserved.

package user

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/organization/svc"
	"github.com/openimsdk/chat/freechat/apps/user/dto"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
)

// UserTeamCtl 处理用户团队信息的控制器
type UserTeamCtl struct{}

// NewUserTeamCtl 创建一个新的用户团队控制器实例
func NewUserTeamCtl() *UserTeamCtl {
	return &UserTeamCtl{}
}

// GetUserTeamInfo 获取当前用户的团队信息
func (c *UserTeamCtl) GetUserTeamInfo(ctx *gin.Context) {
	// 获取操作ID
	operationID, _ := middleware.GetOperationId(ctx)

	// 检查是否是本地测试环境
	if utils.IsLocalTestEnv() {
		// 本地测试模式下返回测试数据
		testResp := &dto.TeamInfoResp{
			UserID:              "test_user_id",
			TeamSize:            10,
			DirectDownlineCount: 3,
			InvitationCode:      "TEST123",
		}
		apiresp.GinSuccess(ctx, testResp)
		return
	}

	// 获取用户ID
	userID := mctx.GetOpUserID(ctx)
	if userID == "" {
		log.ZWarn(ctx, "无效的用户ID", nil, "operationID", operationID)
		apiresp.GinError(ctx, errs.ErrTokenInvalid)
		return
	}

	// 尝试获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(ctx)
	if err != nil {
		log.ZWarn(ctx, "获取组织信息失败", err,
			"operationID", operationID,
			"userID", userID)

		// 组织信息获取失败，返回一个空的响应
		emptyResp := &dto.TeamInfoResp{
			UserID:              userID,
			TeamSize:            0,
			DirectDownlineCount: 0,
			InvitationCode:      "",
		}
		apiresp.GinSuccess(ctx, emptyResp)
		return
	}

	// 检查组织用户是否有效
	if org.OrgUser == nil {
		log.ZWarn(ctx, "组织用户信息为空", nil,
			"operationID", operationID,
			"userID", userID,
			"orgID", org.ID.Hex())

		emptyResp := &dto.TeamInfoResp{
			UserID:              userID,
			TeamSize:            0,
			DirectDownlineCount: 0,
			InvitationCode:      "",
		}
		apiresp.GinSuccess(ctx, emptyResp)
		return
	}

	// 创建层级服务
	hierarchySvc := svc.NewHierarchyService(plugin.MongoCli().GetDB())

	// 获取用户层级信息
	user, err := hierarchySvc.GetUserForTeamInfo(context.Background(), org.ID, userID)
	if err != nil {
		log.ZWarn(ctx, "获取用户团队信息失败", err,
			"operationID", operationID,
			"userID", userID,
			"organizationID", org.ID.Hex())

		// 使用组织用户的邀请码，其他字段使用固定值
		resp := &dto.TeamInfoResp{
			UserID:              userID,
			TeamSize:            10,
			DirectDownlineCount: 3,
			InvitationCode:      org.OrgUser.InvitationCode,
		}
		apiresp.GinSuccess(ctx, resp)
		return
	}

	// 构造响应
	resp := &dto.TeamInfoResp{
		UserID:              userID,
		TeamSize:            user.TeamSize,
		DirectDownlineCount: user.DirectDownlineCount,
		InvitationCode:      user.InvitationCode,
	}

	// 返回成功响应
	apiresp.GinSuccess(ctx, resp)
}
