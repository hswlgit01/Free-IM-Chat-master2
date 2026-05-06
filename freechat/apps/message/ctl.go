package message

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	opModel "github.com/openimsdk/chat/freechat/apps/operationLog/model"
	opSvc "github.com/openimsdk/chat/freechat/apps/operationLog/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/log"
)

// dawn 2026-05-05 修复后台聊天记录管理：新增 CMS 查询、撤回、删除代理并写入操作审计。
type MessageCtl struct{}

func NewMessageCtl() *MessageCtl {
	return &MessageCtl{}
}

func (ctl *MessageCtl) CmsSearch(c *gin.Context) {
	org, req, ok := bindRequest(c)
	if !ok {
		return
	}

	forwardReq := normalizeSearchRequest(req)
	apiCtx, err := imAdminContext(c)
	if err != nil {
		ctl.writeOperationLog(c, org, opModel.OpTypeViewChatMessage, withResult(baseDetails(req), "failed", err, nil))
		apiresp.GinError(c, err)
		return
	}
	resp, err := plugin.ImApiCaller().SearchMsg(apiCtx, forwardReq)
	if err != nil {
		ctl.writeOperationLog(c, org, opModel.OpTypeViewChatMessage, withResult(baseDetails(req), "failed", err, nil))
		apiresp.GinError(c, err)
		return
	}

	ctl.writeOperationLog(c, org, opModel.OpTypeViewChatMessage, withResult(baseDetails(req), "success", nil, map[string]any{
		"result_count": searchResultCount(resp),
	}))
	apiresp.GinSuccess(c, deref(resp))
}

func (ctl *MessageCtl) CmsRevoke(c *gin.Context) {
	org, req, ok := bindRequest(c)
	if !ok {
		return
	}

	forwardReq := pick(req, "conversationID", "seq", "userID")
	apiCtx, err := imAdminContext(c)
	if err != nil {
		ctl.writeOperationLog(c, org, opModel.OpTypeRevokeChatMessage, withResult(messageDetails(req), "failed", err, nil))
		apiresp.GinError(c, err)
		return
	}
	resp, err := plugin.ImApiCaller().RevokeMsg(apiCtx, forwardReq)
	if err != nil {
		ctl.writeOperationLog(c, org, opModel.OpTypeRevokeChatMessage, withResult(messageDetails(req), "failed", err, nil))
		apiresp.GinError(c, err)
		return
	}

	ctl.writeOperationLog(c, org, opModel.OpTypeRevokeChatMessage, withResult(messageDetails(req), "success", nil, nil))
	apiresp.GinSuccess(c, deref(resp))
}

func (ctl *MessageCtl) CmsDelete(c *gin.Context) {
	org, req, ok := bindRequest(c)
	if !ok {
		return
	}

	forwardReq := pick(req, "conversationID", "seqs", "userID", "deleteSyncOpt")
	if _, ok := forwardReq["deleteSyncOpt"]; !ok {
		forwardReq["deleteSyncOpt"] = map[string]any{
			"IsSyncSelf":  true,
			"IsSyncOther": false,
		}
	}

	apiCtx, err := imAdminContext(c)
	if err != nil {
		ctl.writeOperationLog(c, org, opModel.OpTypeDeleteChatMessage, withResult(messageDetails(req), "failed", err, nil))
		apiresp.GinError(c, err)
		return
	}
	resp, err := plugin.ImApiCaller().DeleteMsgs(apiCtx, forwardReq)
	if err != nil {
		ctl.writeOperationLog(c, org, opModel.OpTypeDeleteChatMessage, withResult(messageDetails(req), "failed", err, nil))
		apiresp.GinError(c, err)
		return
	}

	ctl.writeOperationLog(c, org, opModel.OpTypeDeleteChatMessage, withResult(messageDetails(req), "success", nil, map[string]any{
		"delete_scope": deleteScope(forwardReq),
	}))
	apiresp.GinSuccess(c, deref(resp))
}

func bindRequest(c *gin.Context) (*middleware.OrgInfo, map[string]any, bool) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return nil, nil, false
	}

	req := map[string]any{}
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return nil, nil, false
	}
	return org, req, true
}

func (ctl *MessageCtl) writeOperationLog(c *gin.Context, org *middleware.OrgInfo, operationType opModel.OperationLogType, details map[string]any) {
	if err := opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        details,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  operationType,
	}); err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}
}

// dawn 2026-05-06 修复消息管理 ArgsError：后台代理 OpenIM 消息接口前使用默认 IM 管理员 token。
func imAdminContext(c *gin.Context) (context.Context, error) {
	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		return nil, err
	}
	ctxWithOpID := context.WithValue(c.Request.Context(), constantpb.OperationID, operationID)
	adminToken, err := plugin.ImApiCaller().ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return nil, err
	}
	return mctx.WithApiToken(ctxWithOpID, adminToken), nil
}

func pick(req map[string]any, keys ...string) map[string]any {
	dst := make(map[string]any, len(keys))
	for _, key := range keys {
		if value, ok := req[key]; ok {
			dst[key] = value
		}
	}
	return dst
}

// dawn 2026-05-06 修复消息查询 ArgsError：转发 OpenIM 前归一化表单数字和日期字段。
func normalizeSearchRequest(req map[string]any) map[string]any {
	dst := map[string]any{}
	if value := stringValue(req["sendID"]); value != "" {
		dst["sendID"] = value
	}
	if value := stringValue(req["recvID"]); value != "" {
		dst["recvID"] = value
	}
	if value, ok := intValue(req["contentType"]); ok {
		dst["contentType"] = value
	}
	if value, ok := intValue(req["sessionType"]); ok {
		dst["sessionType"] = value
	}
	if value := dateOnly(req["sendTime"]); value != "" {
		dst["sendTime"] = value
	}
	dst["pagination"] = normalizePagination(req["pagination"])
	return dst
}

func normalizePagination(value any) map[string]any {
	pageNumber, showNumber := int64(1), int64(10)
	if pagination, ok := value.(map[string]any); ok {
		if value, ok := intValue(pagination["pageNumber"]); ok {
			pageNumber = value
		}
		if value, ok := intValue(pagination["showNumber"]); ok {
			showNumber = value
		}
	}
	if pageNumber < 1 {
		pageNumber = 1
	}
	if showNumber < 1 {
		showNumber = 10
	}
	return map[string]any{
		"pageNumber": pageNumber,
		"showNumber": showNumber,
	}
}

func baseDetails(req map[string]any) map[string]any {
	return map[string]any{
		"request": req,
	}
}

func messageDetails(req map[string]any) map[string]any {
	details := baseDetails(req)
	for _, key := range []string{"target_user_id", "conversation_id", "server_msg_id", "client_msg_id", "reason"} {
		if value, ok := req[key]; ok {
			details[key] = value
		}
	}
	if value, ok := req["userID"]; ok {
		details["target_user_id"] = value
	}
	if value, ok := req["conversationID"]; ok {
		details["conversation_id"] = value
	}
	if value, ok := req["seq"]; ok {
		details["seq"] = value
	}
	if value, ok := req["seqs"]; ok {
		details["seqs"] = value
	}
	if value, ok := req["serverMsgID"]; ok {
		details["server_msg_id"] = value
	}
	if value, ok := req["clientMsgID"]; ok {
		details["client_msg_id"] = value
	}
	return details
}

func withResult(details map[string]any, result string, err error, extra map[string]any) map[string]any {
	details["result"] = result
	if err != nil {
		details["error"] = err.Error()
	}
	for key, value := range extra {
		details[key] = value
	}
	return details
}

func deref(resp *any) any {
	if resp == nil {
		return map[string]any{}
	}
	return *resp
}

func searchResultCount(resp *any) any {
	value := deref(resp)
	data, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	return data["chatLogsNum"]
}

func deleteScope(req map[string]any) string {
	opt, _ := req["deleteSyncOpt"].(map[string]any)
	isSyncSelf := boolValue(opt["IsSyncSelf"]) || boolValue(opt["isSyncSelf"])
	isSyncOther := boolValue(opt["IsSyncOther"]) || boolValue(opt["isSyncOther"])

	switch {
	case isSyncOther:
		return "conversation_all"
	case isSyncSelf:
		return "user_all_devices"
	default:
		return "user_server_only"
	}
}

func boolValue(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "1"
	case fmt.Stringer:
		return v.String() == "true" || v.String() == "1"
	default:
		return false
	}
}

func intValue(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	case string:
		if strings.TrimSpace(v) == "" {
			return 0, false
		}
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return ""
	}
}

func dateOnly(value any) string {
	date := stringValue(value)
	if len(date) >= len("2006-01-02") {
		return date[:len("2006-01-02")]
	}
	return date
}
