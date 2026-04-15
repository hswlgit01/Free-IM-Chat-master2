// Copyright © 2023 OpenIM open source community. All rights reserved.

package organization

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	chatmw "github.com/openimsdk/chat/internal/api/mw"
)

// RegisterHierarchyRoutes registers all hierarchy-related routes
func RegisterHierarchyRoutes(router *gin.RouterGroup, hierarchyCtl *HierarchyCtl) {
	// User hierarchy endpoints (require token)
	hierarchyRouter := router.Group("/hierarchy", chatmw.New(plugin.AdminClient()).CheckToken)

	// GET /third/organization/hierarchy/tree - Get hierarchy tree
	hierarchyRouter.GET("/tree", hierarchyCtl.GetHierarchyTree)

	// GET /third/organization/hierarchy/tree_root - 仅组织虚拟根摘要（无子树、无全量统计）
	hierarchyRouter.GET("/tree_root", hierarchyCtl.GetHierarchyTreeRootSummary)

	// GET /third/organization/hierarchy/children - Get direct children of a user
	hierarchyRouter.GET("/children", hierarchyCtl.GetHierarchyChildren)

	// GET /third/organization/hierarchy/detail - Get detailed information about a user
	hierarchyRouter.GET("/detail", hierarchyCtl.GetHierarchyDetail)

	// POST /third/organization/hierarchy/search - Search users in the hierarchy
	hierarchyRouter.POST("/search", hierarchyCtl.SearchHierarchy)

	// POST /third/organization/hierarchy/search_panel - 面板搜索（精简字段）
	hierarchyRouter.POST("/search_panel", hierarchyCtl.SearchHierarchyPanel)

	// Admin endpoints (require token and organization check)
	hierarchyAdminRouter := router.Group("/admin/repair/hierarchy",
		chatmw.New(plugin.AdminClient()).CheckToken, middleware.CheckOrganization())

	// POST /third/organization/admin/repair/hierarchy - Repair hierarchy data
	hierarchyAdminRouter.POST("", hierarchyCtl.RepairHierarchy)
}
