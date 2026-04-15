// Copyright © 2023 OpenIM open source community. All rights reserved.

package organization

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	opModel "github.com/openimsdk/chat/freechat/apps/operationLog/model"
	opSvc "github.com/openimsdk/chat/freechat/apps/operationLog/svc"
	"github.com/openimsdk/chat/freechat/apps/organization/dto"
	"github.com/openimsdk/chat/freechat/apps/organization/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/log"
)

// HierarchyCtl handles user hierarchy relationship APIs
type HierarchyCtl struct{}

// NewHierarchyCtl creates a new hierarchy controller
func NewHierarchyCtl() *HierarchyCtl {
	return &HierarchyCtl{}
}

// GetHierarchyTree returns a tree structure of the user hierarchy
func (h *HierarchyCtl) GetHierarchyTree(c *gin.Context) {
	// Get organization info from context
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// Get query parameters
	rootUserID := c.Query("root_user_id")
	maxDepth := 0
	if c.Query("max_depth") != "" {
		_, err := c.Request.URL.Query()["max_depth"]
		if err {
			maxDepth = 3
		}
	}

	// Create hierarchy service
	hierarchySvc := svc.NewHierarchyService(plugin.MongoCli().GetDB())

	// Get hierarchy tree
	resp, err := hierarchySvc.GetHierarchyTree(context.Background(), org.ID, rootUserID, maxDepth)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// Return success response
	apiresp.GinSuccess(c, resp)
}

// GetHierarchyTreeRootSummary 仅返回组织层级虚拟根节点信息（无子树、无全量有效人数重算）
func (h *HierarchyCtl) GetHierarchyTreeRootSummary(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	hierarchySvc := svc.NewHierarchyService(plugin.MongoCli().GetDB())
	resp, err := hierarchySvc.GetHierarchyTreeRootSummary(context.Background(), org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// GetHierarchyChildren returns direct children of a user in the hierarchy
func (h *HierarchyCtl) GetHierarchyChildren(c *gin.Context) {
	// Get organization info from context
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// Get pagination parameters
	pagination, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// Get parent user ID from query
	parentUserID := c.Query("parent_user_id")
	if parentUserID == "" {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// Create hierarchy service
	hierarchySvc := svc.NewHierarchyService(plugin.MongoCli().GetDB())

	// Get children
	resp, err := hierarchySvc.GetHierarchyChildren(context.Background(), org.ID, parentUserID, pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// Return success response
	apiresp.GinSuccess(c, resp)
}

// GetHierarchyDetail returns detailed information about a user in the hierarchy
func (h *HierarchyCtl) GetHierarchyDetail(c *gin.Context) {
	// Get organization info from context
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// Get user ID from query
	userID := c.Query("user_id")
	if userID == "" {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// Create hierarchy service
	hierarchySvc := svc.NewHierarchyService(plugin.MongoCli().GetDB())

	// Get user hierarchy detail
	resp, err := hierarchySvc.GetHierarchyDetail(context.Background(), org.ID, userID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// Return success response
	apiresp.GinSuccess(c, resp)
}

// SearchHierarchy searches for users in the hierarchy
func (h *HierarchyCtl) SearchHierarchy(c *gin.Context) {
	// Parse request parameters
	var req dto.SearchHierarchyReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取操作ID和调用详情用于日志
	operationID, _ := middleware.GetOperationId(c)
	// 记录查询参数，便于调试
	log.ZInfo(c, "搜索用户请求",
		"operationID", operationID,
		"keyword", req.Keyword,
		"level", req.Level,
		"page", req.Page,
		"pageSize", req.PageSize)

	// Get organization info from context
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// Create pagination object
	page := &paginationUtils.DepPagination{
		Page:     int32(req.Page),
		PageSize: int32(req.PageSize),
	}

	// Create hierarchy service
	hierarchySvc := svc.NewHierarchyService(plugin.MongoCli().GetDB())

	// 使用增强版搜索函数，支持搜索账号和昵称
	// 注意：这里使用SearchHierarchyEnhanced替代原来的SearchHierarchy
	resp, err := hierarchySvc.SearchHierarchyEnhanced(c, org.ID, &req, page)
	if err != nil {
		log.ZError(c, "搜索用户失败", err, "keyword", req.Keyword)
		apiresp.GinError(c, err)
		return
	}

	log.ZInfo(c, "搜索用户成功",
		"keyword", req.Keyword,
		"resultCount", len(resp.Users),
		"total", resp.Total)

	// Return success response
	apiresp.GinSuccess(c, resp)
}

// SearchHierarchyPanel 管理后台层级搜索（仅昵称/账号匹配，排除封禁与已注销，精简返回字段）
func (h *HierarchyCtl) SearchHierarchyPanel(c *gin.Context) {
	var req dto.SearchHierarchyReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	hierarchySvc := svc.NewHierarchyService(plugin.MongoCli().GetDB())
	resp, err := hierarchySvc.SearchHierarchyPanel(c, org.ID, &req)
	if err != nil {
		log.ZError(c, "search_panel 失败", err, "keyword", req.Keyword)
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// RepairHierarchy 已被废弃，前端不再调用此接口
// 保持接口兼容性，但不执行实际修复操作，仅返回成功消息
func (h *HierarchyCtl) RepairHierarchy(c *gin.Context) {
	// Get organization info from context
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// Get operation ID for logging
	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 创建一个简单的响应报告，但不执行实际修复
	startTime := time.Now()
	report := &dto.HierarchyRepairReport{
		StartTime:      startTime.Format(time.RFC3339),
		EndTime:        startTime.Format(time.RFC3339),
		OrganizationID: org.ID,
		OperationID:    operationID,
		Message:        "此API已被废弃，请使用getHierarchyTree获取最新数据",
	}

	// Log operation
	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        report,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeRepairHierarchy,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	// Return success response with repair report
	apiresp.GinSuccess(c, report)
}
