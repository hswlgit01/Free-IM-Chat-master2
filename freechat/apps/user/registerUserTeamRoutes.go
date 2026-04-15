// Copyright © 2023 OpenIM open source community. All rights reserved.

package user

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils"
	chatmw "github.com/openimsdk/chat/internal/api/mw"
)

// RegisterUserTeamRoutes 注册用户团队相关路由
func RegisterUserTeamRoutes(router *gin.RouterGroup, userTeamCtl *UserTeamCtl) {
	// 检查是否是本地测试环境
	if utils.IsLocalTestEnv() {
		// 本地测试模式不需要验证
		router.GET("/user/team/info", userTeamCtl.GetUserTeamInfo)
	} else {
		// 生产环境添加token验证中间件和组织信息中间件
		router.GET("/user/team/info",
			chatmw.New(plugin.AdminClient()).CheckToken,
			middleware.OrgIdMiddleware(),
			userTeamCtl.GetUserTeamInfo)
	}
}
