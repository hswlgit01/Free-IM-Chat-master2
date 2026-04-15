// Copyright © 2023 OpenIM open source community. All rights reserved.

package organization

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	chatmw "github.com/openimsdk/chat/internal/api/mw"
)

// RegisterThirdAdminHierarchyRoutes registers all hierarchy-related routes in the third_admin path
func RegisterThirdAdminHierarchyRoutes(router *gin.RouterGroup, hierarchyCtl *HierarchyCtl) {
	// Admin hierarchy endpoints (require token and organization check)
	hierarchyRouter := router.Group("/hierarchy",
		chatmw.New(plugin.AdminClient()).CheckToken,
		middleware.CheckOrganization())

	// GET /third_admin/hierarchy/tree - Get hierarchy tree
	hierarchyRouter.GET("/tree", hierarchyCtl.GetHierarchyTree)

	// GET /third_admin/hierarchy/tree_root - 仅组织虚拟根摘要（无子树、无全量统计）
	hierarchyRouter.GET("/tree_root", hierarchyCtl.GetHierarchyTreeRootSummary)

	// GET /third_admin/hierarchy/children - Get direct children of a user
	hierarchyRouter.GET("/children", hierarchyCtl.GetHierarchyChildren)

	// GET /third_admin/hierarchy/detail - Get detailed information about a user
	hierarchyRouter.GET("/detail", hierarchyCtl.GetHierarchyDetail)

	// POST /third_admin/hierarchy/search - Search users in the hierarchy
	hierarchyRouter.POST("/search", hierarchyCtl.SearchHierarchy)

	// POST /third_admin/hierarchy/search_panel - 面板搜索：仅账号/昵称，排除封禁与已注销
	hierarchyRouter.POST("/search_panel", hierarchyCtl.SearchHierarchyPanel)

	// POST /third_admin/hierarchy/repair - Repair hierarchy data
	hierarchyRouter.POST("/repair", hierarchyCtl.RepairHierarchy)
}
