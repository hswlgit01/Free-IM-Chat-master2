package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openimsdk/chat/freechat/utils/paginationUtils"

	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/pkg/common/mctx"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/group"
	"github.com/openimsdk/tools/log"
)

type OrgGroupSvc struct{}

func NewOrgGroupSvc() *OrgGroupSvc {
	return &OrgGroupSvc{}
}

// validateUsersInOrganization 验证用户是否属于指定组织
func (s *OrgGroupSvc) validateUsersInOrganization(ctx context.Context, userIDs []string, orgID primitive.ObjectID) error {
	if len(userIDs) == 0 {
		return nil
	}

	// 获取组织用户DAO
	mongoCli := plugin.MongoCli()
	orgUserDao := orgModel.NewOrganizationUserDao(mongoCli.GetDB())

	// 使用$in批量查询指定的用户ID在组织中的记录
	foundOrgUsers, err := orgUserDao.GetByIMServerUserIdsAndOrgId(ctx, userIDs, orgID)
	if err != nil {
		log.ZError(ctx, "批量查询组织用户失败", err, "user_ids", userIDs, "org_id", orgID.Hex())
		return freeErrors.SystemErr(fmt.Errorf("batch query organization users failed: %v", err))
	}

	// 如果找到的用户数量与输入的用户ID数量不匹配，说明有用户不在组织中
	if len(foundOrgUsers) != len(userIDs) {
		// 构建已找到的用户ID集合
		foundUserIDSet := make(map[string]bool)
		for _, orgUser := range foundOrgUsers {
			foundUserIDSet[orgUser.ImServerUserId] = true
		}

		// 找出缺失的用户ID
		var missingUsers []string
		for _, userID := range userIDs {
			if !foundUserIDSet[userID] {
				missingUsers = append(missingUsers, userID)
			}
		}

		log.ZError(ctx, "部分用户不属于指定组织", nil, "missing_users", missingUsers, "org_id", orgID.Hex())
		return freeErrors.SystemErr(fmt.Errorf("some users are not in the organization: %v", missingUsers))
	}

	return nil
}

// CmsCreateGroupWithOrg 创建群组并与组织绑定
func (s *OrgGroupSvc) CmsCreateGroupWithOrg(ctx context.Context, req group.CreateGroupReq, userID string, operationID string,
	org *orgModel.Organization, user *orgModel.OrganizationUser) (*group.CreateGroupResp, error) {
	// 检查组织信息
	if org == nil {
		log.ZError(ctx, "Organization information is empty", nil, "user_id", userID)
		return nil, fmt.Errorf("Organization information is empty")
	}

	// 确保GroupInfo不为nil
	if req.GroupInfo == nil {
		req.GroupInfo = &sdkws.GroupInfo{}
	}

	// 收集所有需要验证的用户ID
	var allUserIDs []string

	// 添加群主ID
	if req.OwnerUserID != "" {
		allUserIDs = append(allUserIDs, req.OwnerUserID)
	}

	// 添加管理员IDs
	allUserIDs = append(allUserIDs, req.AdminUserIDs...)

	// 添加成员IDs
	allUserIDs = append(allUserIDs, req.MemberUserIDs...)

	// 验证所有用户是否属于指定组织
	if err := s.validateUsersInOrganization(ctx, allUserIDs, org.ID); err != nil {
		log.ZError(ctx, "用户验证失败", err, "user_id", userID, "org_id", org.ID.Hex())
		return nil, err
	}

	// 调用CreateGroup方法创建群组
	return s.createGroup(ctx, req, operationID, user)
}

type CreateGroupDetails struct {
	GroupName   string   `json:"group_name"`   // 群组名称
	GroupType   int32    `json:"group_type"`   // 群组类型
	MemberCount int32    `json:"member_count"` // 初始成员数量
	InitMembers []string `json:"init_members"` // 初始成员ID列表
}

// createGroup 创建群组
func (s *OrgGroupSvc) createGroup(ctx context.Context, req group.CreateGroupReq, operationID string, user *orgModel.OrganizationUser) (*group.CreateGroupResp, error) {
	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()
	if imApiCaller == nil {
		return nil, fmt.Errorf("IM API调用器未初始化")
	}

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}

	// 调用CreateGroup方法
	resp, err := imApiCaller.CreateGroup(mctx.WithApiToken(ctxWithOpID, adminToken), req)
	if err != nil {
		log.ZError(ctx, "创建群组失败", err, "operation_id", operationID)
		return nil, err
	}

	// 记录群创建操作日志
	// 记录创建群组操作日志
	userIDs := append(append(req.MemberUserIDs, req.AdminUserIDs...), req.OwnerUserID)
	createDetails := CreateGroupDetails{
		GroupName:   req.GroupInfo.GroupName,
		GroupType:   req.GroupInfo.GroupType,
		MemberCount: int32(len(userIDs)),
		InitMembers: userIDs,
	}
	detailsBytes, _ := json.Marshal(createDetails)
	s.logGroupOperation(mctx.WithApiToken(ctxWithOpID, adminToken), operationID, resp.GroupInfo.GroupID, user.ImServerUserId, "",
		string(detailsBytes), openImModel.GroupOpTypeCreateGroup)

	log.ZInfo(ctx, "创建群组成功", "operation_id", operationID)
	return resp, nil
}

// GetGroupsByOrgID 分页查询群组信息
func (s *OrgGroupSvc) GetGroupsByOrgID(ctx context.Context, operationID, userID string,
	page, pageSize int, groupName string, organization *orgModel.Organization) (*paginationUtils.ListResp[*sdkws.GroupInfo], error) {
	// 检查组织信息
	if organization == nil {
		log.ZError(ctx, "Organization information is empty", nil, "user_id", userID)
		return nil, errs.NewCodeError(freeErrors.ErrSystem, "Organization information is empty")
	}

	// 根据组织ID查询群组
	mongoCli := plugin.MongoCli()
	groupDao := openImModel.NewGroupDao(mongoCli.GetDB())
	groups, total, err := groupDao.GetGroupsByOrgID(ctx, organization.ID.Hex(), page, pageSize, groupName)
	if err != nil {
		log.ZError(ctx, "Failed to query groups", err, "user_id", userID, "org_id", organization.ID.Hex())
		return nil, errs.NewCodeError(freeErrors.ErrSystem, "Failed to query groups")
	}
	if len(groups) <= 0 {
		return &paginationUtils.ListResp[*sdkws.GroupInfo]{
			Total: total,
			List:  nil,
		}, nil
	}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()
	if imApiCaller == nil {
		return nil, fmt.Errorf("IM API调用器未初始化")
	}

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}

	groupIds := make([]string, 0)
	for _, g := range groups {
		groupIds = append(groupIds, g.GroupID)
	}

	groupsInfo, err := imApiCaller.FindGroupInfo(mctx.WithApiToken(ctxWithOpID, adminToken), groupIds)
	if err != nil {
		log.ZError(ctx, "查找群组失败", err, "operation_id", operationID)
		return nil, err
	}

	groupsInfoMap := make(map[string]*sdkws.GroupInfo)
	for _, info := range groupsInfo {
		groupsInfoMap[info.GroupID] = info
	}

	sortGroupsInfo := make([]*sdkws.GroupInfo, 0)
	for _, groupId := range groupIds {
		sortGroupsInfo = append(sortGroupsInfo, groupsInfoMap[groupId])
	}

	resp := &paginationUtils.ListResp[*sdkws.GroupInfo]{
		Total: total,
		List:  sortGroupsInfo,
	}

	return resp, nil
}

type ListGroupMembersReq struct {
	Pagination *sdkws.RequestPagination `protobuf:"bytes,1,opt,name=pagination,proto3" json:"pagination"`
	GroupID    string                   `protobuf:"bytes,2,opt,name=groupID,proto3" json:"groupID"`
	Filter     int32                    `protobuf:"varint,3,opt,name=filter,proto3" json:"filter"`
	Keyword    string                   `protobuf:"bytes,4,opt,name=keyword,proto3" json:"keyword"`
}

// ListGroupMembers 查看管理员自己组织内群组成员列表
func (s *OrgGroupSvc) ListGroupMembers(ctx context.Context, orgId primitive.ObjectID, operationID string, req *ListGroupMembersReq) (*group.GetGroupMemberListResp, error) {
	dao := openImModel.NewGroupDao(plugin.MongoCli().GetDB())
	if !orgId.IsZero() {
		_, err := dao.GetByGroupIDAndOrgID(context.TODO(), req.GroupID, orgId.Hex())
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return nil, freeErrors.NotFoundErrWithResource(orgId.String())
			}
			return nil, freeErrors.SystemErr(err)
		}
	}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}

	imApiReq := group.GetGroupMemberListReq{
		GroupID: req.GroupID,
		Pagination: &sdkws.RequestPagination{
			PageNumber: req.Pagination.PageNumber,
			ShowNumber: req.Pagination.ShowNumber,
		},
	}
	if req.Keyword != "" {
		imApiReq.Keyword = req.Keyword
	}

	resp, err := imApiCaller.GetGroupMemberList(mctx.WithApiToken(ctxWithOpID, adminToken), imApiReq)
	if err != nil {
		log.ZError(ctx, "查看群组成员失败", err, "operation_id", operationID)
		return nil, err
	}

	return resp, nil
}

type DismissGroupReq struct {
	GroupID string `json:"group_id" binding:"required"`
}

// DismissGroup 管理员解散群组
func (s *OrgGroupSvc) DismissGroup(ctx context.Context, req DismissGroupReq, orgId primitive.ObjectID, operationID string, user *orgModel.OrganizationUser) (*group.DismissGroupResp, error) {
	dao := openImModel.NewGroupDao(plugin.MongoCli().GetDB())
	_, err := dao.GetByGroupIDAndOrgID(context.TODO(), req.GroupID, orgId.Hex())
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource(orgId.String())
		}
		return nil, freeErrors.SystemErr(err)
	}
	//
	//groupDao := openImModel.NewGroupDao(plugin.MongoCli().GetDB())
	//
	//err = groupDao.UpdateStatus(ctx, req.GroupID, 2)
	//if err != nil {
	//	return nil, err
	//}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()

	//在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}

	imApiReq := group.DismissGroupReq{
		GroupID: req.GroupID,
	}

	resp, err := imApiCaller.DismissGroup(mctx.WithApiToken(ctxWithOpID, adminToken), imApiReq)
	if err != nil {
		log.ZError(ctx, "解散群组失败", err, "operation_id", operationID)
		return nil, err
	}

	// 记录群解散操作日志
	s.logGroupOperation(mctx.WithApiToken(ctxWithOpID, adminToken), operationID, req.GroupID, user.ImServerUserId, "", "", openImModel.GroupOpTypeDismissGroup)

	log.ZInfo(ctx, "解散群组成功", "operation_id", operationID)
	return resp, nil
}

type MuteGroupReq struct {
	GroupID string `json:"group_id" binding:"required"`
}
type MuteGroupDetails struct {
	MuteDuration int64     `json:"mute_duration"` // 群禁言时长(秒，0表示永久)
	MuteEndTime  time.Time `json:"mute_end_time"` // 群禁言结束时间
}

// MuteGroup 管理员群组全体禁言
func (s *OrgGroupSvc) MuteGroup(ctx context.Context, req MuteGroupReq, orgId primitive.ObjectID, operationID string, user *orgModel.OrganizationUser) (*group.MuteGroupResp, error) {
	dao := openImModel.NewGroupDao(plugin.MongoCli().GetDB())
	_, err := dao.GetByGroupIDAndOrgID(context.TODO(), req.GroupID, orgId.Hex())
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource(orgId.String())
		}
		return nil, freeErrors.SystemErr(err)
	}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}

	imApiReq := group.MuteGroupReq{
		GroupID: req.GroupID,
	}

	resp, err := imApiCaller.MuteGroup(mctx.WithApiToken(ctxWithOpID, adminToken), imApiReq)
	if err != nil {
		log.ZError(ctx, "禁言群组失败", err, "operation_id", operationID)
		return nil, err
	}

	// 记录群禁言操作日志
	var muteEndTime time.Time
	details := &MuteGroupDetails{
		MuteDuration: 0,
		MuteEndTime:  muteEndTime,
	}
	detailsBytes, _ := json.Marshal(details)
	s.logGroupOperation(mctx.WithApiToken(ctxWithOpID, adminToken), operationID, req.GroupID, user.ImServerUserId, "", string(detailsBytes), openImModel.GroupOpTypeMuteGroup)

	return resp, nil
}

type CancelMuteGroupReq struct {
	GroupID string `json:"group_id" binding:"required"`
}

// CancelMuteGroup 管理员群组取消全体禁言
func (s *OrgGroupSvc) CancelMuteGroup(ctx context.Context, req CancelMuteGroupReq, orgId primitive.ObjectID, operationID string, user *orgModel.OrganizationUser) (*group.CancelMuteGroupResp, error) {
	dao := openImModel.NewGroupDao(plugin.MongoCli().GetDB())
	_, err := dao.GetByGroupIDAndOrgID(context.TODO(), req.GroupID, orgId.Hex())
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource(orgId.String())
		}
		return nil, freeErrors.SystemErr(err)
	}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}

	imApiReq := group.CancelMuteGroupReq{
		GroupID: req.GroupID,
	}

	resp, err := imApiCaller.CancelMuteGroup(mctx.WithApiToken(ctxWithOpID, adminToken), imApiReq)
	if err != nil {
		log.ZError(ctx, "取消禁言群组失败", err, "operation_id", operationID)
		return nil, err
	}

	// 记录取消群禁言操作日志
	s.logGroupOperation(mctx.WithApiToken(ctxWithOpID, adminToken), operationID, req.GroupID, user.ImServerUserId, "", "", openImModel.GroupOpTypeCancelMuteGroup)

	return resp, nil
}

// UpdateGroupInfo 管理员更新群组信息
func (s *OrgGroupSvc) UpdateGroupInfo(ctx context.Context, req group.SetGroupInfoReq, orgId primitive.ObjectID, operationID string) (*group.SetGroupInfoResp, error) {
	dao := openImModel.NewGroupDao(plugin.MongoCli().GetDB())
	_, err := dao.GetByGroupIDAndOrgID(context.TODO(), req.GroupInfoForSet.GroupID, orgId.Hex())
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource(orgId.String())
		}
		return nil, freeErrors.SystemErr(err)
	}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}

	imApiReq := req
	resp, err := imApiCaller.SetGroupInfo(mctx.WithApiToken(ctxWithOpID, adminToken), imApiReq)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// UpdateGroupMemberInfo 管理员更新群组成员信息
func (s *OrgGroupSvc) UpdateGroupMemberInfo(ctx context.Context, req group.SetGroupMemberInfoReq, orgId primitive.ObjectID, operationID string) (*group.SetGroupMemberInfoResp, error) {
	dao := openImModel.NewGroupDao(plugin.MongoCli().GetDB())
	for _, member := range req.Members {
		_, err := dao.GetByGroupIDAndOrgID(context.TODO(), member.GroupID, orgId.Hex())
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return nil, freeErrors.NotFoundErrWithResource(orgId.String())
			}
			return nil, freeErrors.SystemErr(err)
		}
	}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}

	imApiReq := req
	resp, err := imApiCaller.SetGroupMemberInfo(mctx.WithApiToken(ctxWithOpID, adminToken), imApiReq)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

type TransferOwnerDetails struct {
	OldOwnerUserID string `json:"old_owner_user_id"` // 原群主ID
	NewOwnerUserID string `json:"new_owner_user_id"` // 新群主ID
}

// TransferGroupOwner 管理员更新群组管理员
func (s *OrgGroupSvc) TransferGroupOwner(ctx context.Context, req group.TransferGroupOwnerReq, orgId primitive.ObjectID, operationID string, user *orgModel.OrganizationUser) (*group.TransferGroupOwnerResp, error) {
	dao := openImModel.NewGroupDao(plugin.MongoCli().GetDB())
	_, err := dao.GetByGroupIDAndOrgID(context.TODO(), req.GroupID, orgId.Hex())
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource(orgId.String())
		}
		return nil, freeErrors.SystemErr(err)
	}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}

	imApiReq := req
	resp, err := imApiCaller.TransferGroup(mctx.WithApiToken(ctxWithOpID, adminToken), imApiReq)
	if err != nil {
		return nil, err
	}

	// 记录转移群主操作日志
	details := &TransferOwnerDetails{
		OldOwnerUserID: req.OldOwnerUserID,
		NewOwnerUserID: req.NewOwnerUserID,
	}
	detailsBytes, _ := json.Marshal(details)
	s.logGroupOperation(mctx.WithApiToken(ctxWithOpID, adminToken), operationID, req.GroupID, user.ImServerUserId, req.OldOwnerUserID,
		string(detailsBytes), openImModel.GroupOpTypeTransferOwner)

	return resp, nil
}

type MuteOperationDetails struct {
	MuteDuration int64     `json:"mute_duration"` // 禁言时长(秒)
	MuteEndTime  time.Time `json:"mute_end_time"` // 禁言结束时间
}

// MuteGroupMember 组织管理员设置群组某个成员禁言
func (s *OrgGroupSvc) MuteGroupMember(ctx context.Context, req group.MuteGroupMemberReq, orgId primitive.ObjectID, operationID string, user *orgModel.OrganizationUser) (*group.MuteGroupMemberResp, error) {
	dao := openImModel.NewGroupDao(plugin.MongoCli().GetDB())
	_, err := dao.GetByGroupIDAndOrgID(context.TODO(), req.GroupID, orgId.Hex())
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource(orgId.String())
		}
		return nil, freeErrors.SystemErr(err)
	}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}

	imApiReq := req

	resp, err := imApiCaller.MuteGroupMember(mctx.WithApiToken(ctxWithOpID, adminToken), imApiReq)
	if err != nil {
		log.ZError(ctx, "禁言群成员失败", err, "operation_id", operationID)
		return nil, err
	}

	// 记录禁言群成员操作日志
	muteEndTime := time.Now().Add(time.Second * time.Duration(req.MutedSeconds))
	details := &MuteOperationDetails{
		MuteDuration: int64(req.MutedSeconds),
		MuteEndTime:  muteEndTime,
	}
	detailsBytes, _ := json.Marshal(details)
	s.logGroupOperation(mctx.WithApiToken(ctxWithOpID, adminToken), operationID, req.GroupID, user.ImServerUserId, req.UserID,
		string(detailsBytes), openImModel.GroupOpTypeMuteMember)

	return resp, nil
}

// CancelMuteGroupMember 组织管理员取消群组某个成员禁言
func (s *OrgGroupSvc) CancelMuteGroupMember(ctx context.Context, req group.CancelMuteGroupMemberReq, orgId primitive.ObjectID, operationID string, user *orgModel.OrganizationUser) (*group.CancelMuteGroupMemberResp, error) {
	dao := openImModel.NewGroupDao(plugin.MongoCli().GetDB())
	_, err := dao.GetByGroupIDAndOrgID(context.TODO(), req.GroupID, orgId.Hex())
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource(orgId.String())
		}
		return nil, freeErrors.SystemErr(err)
	}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}

	imApiReq := req

	resp, err := imApiCaller.CancelMuteGroupMember(mctx.WithApiToken(ctxWithOpID, adminToken), imApiReq)
	if err != nil {
		log.ZError(ctx, "取消禁言群成员失败", err, "operation_id", operationID)
		return nil, err
	}

	// 记录取消禁言群成员操作日志
	s.logGroupOperation(mctx.WithApiToken(ctxWithOpID, adminToken), operationID, req.GroupID, user.ImServerUserId, req.UserID, "", openImModel.GroupOpTypeCancelMuteMember)

	return resp, nil
}

type KickMemberDetails struct {
	Reason string `json:"reason"` // 踢人原因
}

// KickGroupMember 组织管理员移除群组某个成员
func (s *OrgGroupSvc) KickGroupMember(ctx context.Context, req group.KickGroupMemberReq, orgId primitive.ObjectID, operationID string, user *orgModel.OrganizationUser) (*group.KickGroupMemberResp, error) {
	dao := openImModel.NewGroupDao(plugin.MongoCli().GetDB())
	_, err := dao.GetByGroupIDAndOrgID(context.TODO(), req.GroupID, orgId.Hex())
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource(orgId.String())
		}
		return nil, freeErrors.SystemErr(err)
	}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}

	imApiReq := req

	resp, err := imApiCaller.KickGroupMember(mctx.WithApiToken(ctxWithOpID, adminToken), imApiReq)
	if err != nil {
		log.ZError(ctx, "踢出群成员失败", err, "operation_id", operationID)
		return nil, err
	}

	// 记录踢出群成员操作日志
	details := &KickMemberDetails{
		Reason: req.Reason,
	}
	detailsBytes, _ := json.Marshal(details)
	for _, kickedUserID := range req.KickedUserIDs {
		s.logGroupOperation(mctx.WithApiToken(ctxWithOpID, adminToken), operationID, req.GroupID, user.ImServerUserId, kickedUserID,
			string(detailsBytes), openImModel.GroupOpTypeKickMember)
	}

	return resp, nil
}

// InviteUserToGroup 组织管理员邀请成员加入群组
func (s *OrgGroupSvc) InviteUserToGroup(ctx context.Context, req group.InviteUserToGroupReq, orgId primitive.ObjectID, operationID string) error {
	dao := openImModel.NewGroupDao(plugin.MongoCli().GetDB())
	_, err := dao.GetByGroupIDAndOrgID(context.TODO(), req.GroupID, orgId.Hex())
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return freeErrors.NotFoundErrWithResource(orgId.String())
		}
		return freeErrors.SystemErr(err)
	}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()

	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "operation_id", operationID)
		return err
	}

	for _, userId := range req.InvitedUserIDs {
		err = imApiCaller.InviteToGroup(mctx.WithApiToken(ctxWithOpID, adminToken), userId, []string{req.GroupID})
		if err != nil {
			log.ZError(ctx, "邀请成员加入群组失败", err, "operation_id", operationID)
			return err
		}
	}

	return nil
}

// logGroupOperation 记录群操作日志的通用方法
func (s *OrgGroupSvc) logGroupOperation(ctx context.Context, operationID, groupID, operatorUserID, targetUserID, details string, operationType int32) {
	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()
	if imApiCaller == nil {
		log.ZError(ctx, "IM API调用器未初始化", nil)
		return
	}

	// 构建请求参数
	imApiReq := map[string]interface{}{
		"groupID":        groupID,
		"operatorUserID": operatorUserID,
		"targetUserID":   targetUserID,
		"operationType":  operationType,
		"details":        details,
	}

	// 调用CreateGroupOperationLog方法
	_, err := imApiCaller.CreateGroupOperationLog(ctx, imApiReq)
	if err != nil {
		log.ZWarn(ctx, "创建群操作日志失败", err, "operation_id", operationID, "group_id", groupID, "operation_type", operationType)
		return
	}

	log.ZInfo(ctx, "创建群操作日志成功", "operation_id", operationID, "group_id", groupID, "operation_type", operationType)
}
