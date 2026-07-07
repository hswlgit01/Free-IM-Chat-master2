package message

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	opModel "github.com/openimsdk/chat/freechat/apps/operationLog/model"
	opSvc "github.com/openimsdk/chat/freechat/apps/operationLog/svc"
	organizationModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/group"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	pageNumber, showNumber := searchPage(forwardReq)
	senderNickname := stringValue(req["senderNickname"])
	recvNickname := stringValue(req["recvNickname"])
	senderFilter, recvFilter := senderNickname, recvNickname
	if stringValue(forwardReq["sendID"]) == "" && senderNickname != "" {
		ids, err := resolveSearchUserIDs(c, org, senderNickname)
		if err != nil {
			ctl.writeOperationLog(c, org, opModel.OpTypeViewChatMessage, withResult(baseDetails(req), "failed", err, nil))
			apiresp.GinError(c, err)
			return
		}
		if len(ids) == 0 {
			apiresp.GinSuccess(c, emptySearchResult())
			return
		}
		forwardReq["sendID"] = ids[0]
		senderFilter = ""
	}
	if stringValue(forwardReq["recvID"]) == "" && recvNickname != "" {
		ids, err := resolveSearchUserIDs(c, org, recvNickname)
		if err != nil {
			ctl.writeOperationLog(c, org, opModel.OpTypeViewChatMessage, withResult(baseDetails(req), "failed", err, nil))
			apiresp.GinError(c, err)
			return
		}
		if len(ids) == 0 {
			apiresp.GinSuccess(c, emptySearchResult())
			return
		}
		forwardReq["recvID"] = ids[0]
		recvFilter = ""
	}
	if senderNickname != "" || recvNickname != "" {
		forwardReq["pagination"] = map[string]any{
			"pageNumber": int64(1),
			"showNumber": expandedSearchLimit(pageNumber, showNumber),
		}
	}
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

	result := filterSearchResult(deref(resp), senderFilter, recvFilter, pageNumber, showNumber)
	result = sortSearchResult(result)
	ctl.writeOperationLog(c, org, opModel.OpTypeViewChatMessage, withResult(baseDetails(req), "success", nil, map[string]any{
		"result_count": searchResultCount(result),
	}))
	apiresp.GinSuccess(c, result)
}

func (ctl *MessageCtl) CmsRevoke(c *gin.Context) {
	org, req, ok := bindRequest(c)
	if !ok {
		return
	}

	forwardReq := pick(req, "conversationID", "seq", "userID")

	// dawn 2026-07-07 撤回按角色分权(对齐客户端原设计 fb8e55a：撤别人=本群群主/群管理员 或 组织团队长)：
	// 组织【超管/后台管理员/团队长】→ admin token 全局审计撤回(IM 核心视为管理员，绕过群角色)；
	// 其余角色(业务员 GroupManager/普通成员等)→ 在【本群】必须是【群主/群管理员】才放行，随后同样用
	// admin token 转发(仅撤回校验用群角色，转发用 admin，保留 #6 GetMsgForRevoke 撤旧消息能力)。
	//
	// 修复"群主/群管理员撤别人消息也 撤回失败"：旧实现改用【本人 IM token】(imUserContext→
	// GetUserToken platformID=AdminPlatformID(10))转发交 IM 核心判权，但 OpenIM /auth/get_user_token
	// 会拒绝 AdminPlatformID(1001 ArgsError)，导致每次 GetUserToken 失败 → 撤回失败。改为在 chat 侧
	// 直接查群角色(GetGroupMemberList filter=5 群主+群管理员)自行判权，避免那次会失败的取 token。
	// 路由已放开到所有组织角色(群主的组织角色可能是任意值)，真正判权在此。
	var apiCtx context.Context
	var err error
	switch org.OrgUser.Role {
	case organizationModel.OrganizationUserSuperAdminRole,
		organizationModel.OrganizationUserBackendAdminRole,
		organizationModel.OrganizationUserTermManagerRole:
		apiCtx, err = imAdminContext(c)
	default: // GroupManager / Normal / 其他：必须是本群群主/群管理员才能撤别人的消息
		// 撤回者强制为已登录本人的 IM 账号，避免伪造 userID 冒充他人身份撤回。
		imUserID := org.OrgUser.ImServerUserId
		forwardReq["userID"] = imUserID
		groupID := groupIDFromConversationID(stringValue(req["conversationID"]))
		if groupID == "" {
			err = freeErrors.ForbiddenErr("无法解析群聊会话，禁止撤回")
			break
		}
		var isGroupAdmin bool
		isGroupAdmin, err = ctl.isGroupOwnerOrAdmin(c, groupID, imUserID)
		if err != nil {
			break
		}
		if !isGroupAdmin {
			err = freeErrors.ForbiddenErr("只有群主/群管理员可以撤回他人消息")
			break
		}
		apiCtx, err = imAdminContext(c)
	}
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

// groupIDFromConversationID 从群会话 ID(格式 "sg_<groupID>")解析出 groupID；非群会话返回空串。
func groupIDFromConversationID(conversationID string) string {
	const prefix = "sg_"
	if strings.HasPrefix(conversationID, prefix) {
		return strings.TrimPrefix(conversationID, prefix)
	}
	return ""
}

// isGroupOwnerOrAdmin 用 admin token 查询【本群】群主+群管理员名单(filter=5)，判断 imUserID 是否在其中。
// 用于撤回"别人消息"的群角色判权：只有群主/群管理员放行(与客户端 canRevokeMessages / _queryOwnerAndAdmin 对齐)。
func (ctl *MessageCtl) isGroupOwnerOrAdmin(c *gin.Context, groupID, imUserID string) (bool, error) {
	if groupID == "" || imUserID == "" {
		return false, nil
	}
	adminCtx, err := imAdminContext(c)
	if err != nil {
		return false, err
	}
	// filter=5：群主 + 群管理员；群主/管理员数量很少，一页 500 足够覆盖(含 3 万人大群)。
	resp, err := plugin.ImApiCaller().GetGroupMemberList(adminCtx, group.GetGroupMemberListReq{
		GroupID: groupID,
		Filter:  5,
		Pagination: &sdkws.RequestPagination{
			PageNumber: 1,
			ShowNumber: 500,
		},
	})
	if err != nil {
		return false, err
	}
	for _, m := range resp.Members {
		if m.UserID == imUserID {
			return true, nil
		}
	}
	return false, nil
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

func searchPage(req map[string]any) (int64, int64) {
	pagination, _ := req["pagination"].(map[string]any)
	pageNumber, _ := pagination["pageNumber"].(int64)
	showNumber, _ := pagination["showNumber"].(int64)
	if pageNumber < 1 {
		pageNumber = 1
	}
	if showNumber < 1 {
		showNumber = 10
	}
	return pageNumber, showNumber
}

func expandedSearchLimit(pageNumber, showNumber int64) int64 {
	limit := pageNumber * showNumber
	if limit < showNumber {
		limit = showNumber
	}
	if limit < 100 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	return limit
}

func emptySearchResult() map[string]any {
	return map[string]any{
		"chatLogs":    []any{},
		"chatLogsNum": 0,
	}
}

// dawn 2026-05-08 修复后台按昵称查不到消息：OpenIM 搜索只支持用户 ID，代理层先把昵称/账号解析成 IM 用户 ID。
func resolveSearchUserIDs(ctx context.Context, org *middleware.OrgInfo, keyword string) ([]string, error) {
	keyword = strings.TrimSpace(keyword)
	if keyword == "" {
		return nil, nil
	}

	db := plugin.MongoCli().GetDB()
	ids := make([]string, 0, 8)
	seen := make(map[string]struct{}, 8)
	addID := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}

	regex := bson.M{"$regex": regexp.QuoteMeta(keyword), "$options": "i"}
	type orgUserRow struct {
		UserID         string `bson:"user_id"`
		ImServerUserID string `bson:"im_server_user_id"`
	}
	orgFilter := bson.M{
		"$or": bson.A{
			bson.M{"nickname": regex},
			bson.M{"account": regex},
			bson.M{"im_server_user_id": keyword},
			bson.M{"user_id": keyword},
		},
	}
	if org != nil && !org.ID.IsZero() {
		orgFilter["organization_id"] = org.ID
	}
	var orgRows []orgUserRow
	if cursor, err := db.Collection("organization_user").Find(ctx, orgFilter, options.Find().SetLimit(20)); err != nil {
		return nil, err
	} else if err := cursor.All(ctx, &orgRows); err != nil {
		return nil, err
	}
	for _, row := range orgRows {
		addID(row.ImServerUserID)
	}

	type userRow struct {
		UserID string `bson:"user_id"`
	}
	var userRows []userRow
	if cursor, err := db.Collection("user").Find(ctx, bson.M{
		"$or": bson.A{
			bson.M{"nickname": regex},
			bson.M{"user_id": keyword},
		},
	}, options.Find().SetLimit(20)); err != nil {
		return nil, err
	} else if err := cursor.All(ctx, &userRows); err != nil {
		return nil, err
	}
	for _, row := range userRows {
		addID(row.UserID)
	}

	type attrRow struct {
		UserID string `bson:"user_id"`
	}
	var attrRows []attrRow
	if cursor, err := db.Collection("attribute").Find(ctx, bson.M{
		"$or": bson.A{
			bson.M{"nickname": regex},
			bson.M{"account": regex},
			bson.M{"email": regex},
			bson.M{"phone_number": regex},
			bson.M{"user_id": keyword},
		},
	}, options.Find().SetLimit(20)); err != nil {
		return nil, err
	} else if err := cursor.All(ctx, &attrRows); err != nil {
		return nil, err
	}
	userIDs := make([]string, 0, len(attrRows))
	for _, row := range attrRows {
		if row.UserID != "" {
			userIDs = append(userIDs, row.UserID)
		}
	}
	if len(userIDs) > 0 {
		attrOrgFilter := bson.M{"user_id": bson.M{"$in": userIDs}}
		if org != nil && !org.ID.IsZero() {
			attrOrgFilter["organization_id"] = org.ID
		}
		var attrOrgRows []orgUserRow
		if cursor, err := db.Collection("organization_user").Find(ctx, attrOrgFilter, options.Find().SetLimit(20)); err != nil {
			return nil, err
		} else if err := cursor.All(ctx, &attrOrgRows); err != nil {
			return nil, err
		}
		for _, row := range attrOrgRows {
			addID(row.ImServerUserID)
		}
	}

	return ids, nil
}

// dawn 2026-05-06 修复聊天记录名称查询：OpenIM 仅支持 ID 查询，CMS 代理层补充昵称过滤。
func filterSearchResult(resp any, senderNickname, recvNickname string, pageNumber, showNumber int64) any {
	if senderNickname == "" && recvNickname == "" {
		return resp
	}
	data, ok := resp.(map[string]any)
	if !ok {
		return resp
	}
	logs, ok := data["chatLogs"].([]any)
	if !ok {
		return resp
	}
	filtered := make([]any, 0, len(logs))
	for _, item := range logs {
		if matchSearchChatLog(item, senderNickname, recvNickname) {
			filtered = append(filtered, item)
		}
	}
	total := len(filtered)
	start := int((pageNumber - 1) * showNumber)
	if start > total {
		start = total
	}
	end := start + int(showNumber)
	if end > total {
		end = total
	}
	data["chatLogs"] = filtered[start:end]
	data["chatLogsNum"] = total
	return data
}

// dawn 2026-05-06 修复消息管理排序：后台列表按发送者稳定排序，同一发送者按发送时间倒序。
func sortSearchResult(resp any) any {
	data, logs, ok := searchData(resp)
	if !ok {
		return resp
	}
	sort.SliceStable(logs, func(i, j int) bool {
		left := searchChatLog(logs[i])
		right := searchChatLog(logs[j])
		leftSender := sortSender(left)
		rightSender := sortSender(right)
		if leftSender != rightSender {
			return leftSender < rightSender
		}
		return searchChatTime(left) > searchChatTime(right)
	})
	data["chatLogs"] = logs
	return data
}

func sortSender(chatLog map[string]any) string {
	sender := strings.ToLower(stringValue(chatLog["senderNickname"]))
	if sender != "" {
		return sender
	}
	return strings.ToLower(stringValue(chatLog["sendID"]))
}

func searchChatTime(chatLog map[string]any) int64 {
	if value, ok := intValue(chatLog["createTime"]); ok {
		return value
	}
	if value, ok := intValue(chatLog["sendTime"]); ok {
		return value
	}
	return 0
}

func searchData(resp any) (map[string]any, []any, bool) {
	data, ok := resp.(map[string]any)
	if !ok {
		return nil, nil, false
	}
	logs, ok := data["chatLogs"].([]any)
	if !ok {
		return nil, nil, false
	}
	return data, logs, true
}

func searchChatLog(item any) map[string]any {
	row, ok := item.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	chatLog, ok := row["chatLog"].(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return chatLog
}

func matchSearchChatLog(item any, senderNickname, recvNickname string) bool {
	row, ok := item.(map[string]any)
	if !ok {
		return false
	}
	chatLog, ok := row["chatLog"].(map[string]any)
	if !ok {
		return false
	}
	if senderNickname != "" && !containsFold(stringValue(chatLog["senderNickname"]), senderNickname) {
		return false
	}
	if recvNickname != "" && !containsFold(stringValue(chatLog["recvNickname"]), recvNickname) {
		return false
	}
	return true
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

func searchResultCount(value any) any {
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

func containsFold(value, keyword string) bool {
	return strings.Contains(strings.ToLower(value), strings.ToLower(keyword))
}
