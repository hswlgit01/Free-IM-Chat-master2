package svc

import (
	"context"
	"time"

	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/plugin"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
)

type VerifyService struct {
}

func NewVerifyService() *VerifyService {
	return &VerifyService{}
}

// CheckFriendRelation 检查两个用户是否是好友关系
func (v *VerifyService) CheckFriendRelation(ctx context.Context, userID, friendID string) error {
	// 获取用户的好友列表
	imApiCaller := plugin.ImApiCaller()
	imToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctx)
	if err != nil {
		return errs.Wrap(err)
	}

	friendIDs, err := imApiCaller.FriendUserIDs(mctx.WithApiToken(ctx, imToken), userID)
	if err != nil {
		return errs.Wrap(err)
	}

	// 检查目标用户是否在好友列表中
	isFriend := false
	for _, id := range friendIDs {
		if id == friendID {
			isFriend = true
			break
		}
	}

	if !isFriend {
		return errs.NewCodeError(freeErrors.ErrNotFriend, freeErrors.ErrorMessages[freeErrors.ErrNotFriend])
	}

	return nil
}

// CheckGroupMembership 检查用户是否在群组中
func (v *VerifyService) CheckGroupMembership(ctx context.Context, userID string, groupID string) error {
	// 获取群成员列表，包含完整日志
	groupMemberIDs, err := v.getGroupMembers(ctx, userID, groupID)
	if err != nil {
		return err
	}

	// 检查用户是否在群组中
	for _, memberID := range groupMemberIDs {
		if memberID == userID {
			log.ZInfo(ctx, "用户在群组中验证通过", "user_id", userID, "group_id", groupID)
			return nil
		}
	}

	log.ZWarn(ctx, "用户不在群组中", nil, "user_id", userID, "group_id", groupID)
	return errs.NewCodeError(freeErrors.ErrNotInGroup, freeErrors.ErrorMessages[freeErrors.ErrNotInGroup])
}

// getGroupMembers 获取群组成员列表（内部方法，用于减少代码冗余）
func (v *VerifyService) getGroupMembers(ctx context.Context, userID string, groupID string) ([]string, error) {
	if groupID == "" {
		log.ZError(ctx, "群组ID为空", nil, "user_id", userID)
		return nil, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	// 获取IM API调用器
	imApiCaller := plugin.ImApiCaller()
	imToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctx)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err, "user_id", userID, "group_id", groupID)
		return nil, errs.Wrap(err)
	}

	// 添加超时控制，避免长时间等待
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// 获取群成员列表
	groupMemberIDs, err := imApiCaller.GetGroupMemberUserIDs(mctx.WithApiToken(ctxWithTimeout, imToken), groupID)
	if err != nil {
		log.ZError(ctx, "获取群成员列表失败", err, "user_id", userID, "group_id", groupID)
		return nil, errs.Wrap(err)
	}

	log.ZDebug(ctx, "获取群成员列表成功", "user_id", userID, "group_id", groupID, "member_count", len(groupMemberIDs))
	return groupMemberIDs, nil
}

// VerifyGroupRedPacket 同时验证用户是否在群中，以及红包数量是否合法
func (v *VerifyService) VerifyGroupRedPacket(ctx context.Context, userID string, groupID string, redPacketCount int) ([]string, error) {
	// 复用getGroupMembers方法获取群成员列表
	groupMemberIDs, err := v.getGroupMembers(ctx, userID, groupID)
	if err != nil {
		return nil, err
	}

	// 1. 检查用户是否在群中
	userInGroup := false
	for _, memberID := range groupMemberIDs {
		if memberID == userID {
			userInGroup = true
			break
		}
	}

	if !userInGroup {
		log.ZWarn(ctx, "用户不在群组中", nil, "user_id", userID, "group_id", groupID)
		return nil, errs.NewCodeError(freeErrors.ErrNotInGroup, freeErrors.ErrorMessages[freeErrors.ErrNotInGroup])
	}

	// 2. 如果指定了红包数量，检查红包数量是否合法
	if redPacketCount > 0 {
		memberCount := len(groupMemberIDs)
		if redPacketCount > memberCount {
			log.ZWarn(ctx, "红包数量超过群成员数", nil, "group_id", groupID, "red_packet_count", redPacketCount, "member_count", memberCount)
			return nil, errs.NewCodeError(freeErrors.ErrRedPacketCountExceedGroupMemberCount, freeErrors.ErrorMessages[freeErrors.ErrRedPacketCountExceedGroupMemberCount])
		}
		log.ZInfo(ctx, "红包数量验证通过", "user_id", userID, "group_id", groupID, "red_packet_count", redPacketCount, "member_count", memberCount)
	} else {
		log.ZInfo(ctx, "用户在群组中验证通过", "user_id", userID, "group_id", groupID)
	}

	return groupMemberIDs, nil
}

// CheckOrganizationRelation 检查两个用户是否在同一个组织中
func (v *VerifyService) CheckOrganizationRelation(ctx context.Context, senderImID, receiverImID string) error {
	mongoCli := plugin.MongoCli().GetDB()
	orgUserDao := orgModel.NewOrganizationUserDao(mongoCli)

	// 获取发送者的组织信息
	senderOrgUser, err := orgUserDao.GetByUserIMServerUserId(ctx, senderImID)
	if err != nil {
		log.ZError(ctx, "获取发送者组织信息失败", err, "sender_im_id", senderImID)
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 获取接收者的组织信息
	receiverOrgUser, err := orgUserDao.GetByUserIMServerUserId(ctx, receiverImID)
	if err != nil {
		log.ZError(ctx, "获取接收者组织信息失败", err, "receiver_im_id", receiverImID)
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 检查两个用户是否在同一个组织中
	if senderOrgUser.OrganizationId != receiverOrgUser.OrganizationId {
		log.ZWarn(ctx, "用户不在同一个组织中", nil,
			"sender_im_id", senderImID,
			"receiver_im_id", receiverImID,
			"sender_org_id", senderOrgUser.OrganizationId.Hex(),
			"receiver_org_id", receiverOrgUser.OrganizationId.Hex())
		return errs.NewCodeError(freeErrors.ErrNotInGroup, freeErrors.ErrorMessages[freeErrors.ErrNotInGroup])
	}

	log.ZInfo(ctx, "组织验证通过",
		"sender_im_id", senderImID,
		"receiver_im_id", receiverImID,
		"organization_id", senderOrgUser.OrganizationId.Hex())

	return nil
}

// CheckGroupOrganizationRelation 检查群组是否属于发送者的组织
func (v *VerifyService) CheckGroupOrganizationRelation(ctx context.Context, senderImID, groupID string) error {
	mongoCli := plugin.MongoCli().GetDB()
	orgUserDao := orgModel.NewOrganizationUserDao(mongoCli)

	// 获取发送者的组织信息
	senderOrgUser, err := orgUserDao.GetByUserIMServerUserId(ctx, senderImID)
	if err != nil {
		log.ZError(ctx, "获取发送者组织信息失败", err, "sender_im_id", senderImID)
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 查询群组信息，验证群组是否属于发送者的组织
	groupDao := openImModel.NewGroupDao(mongoCli)
	group, err := groupDao.GetByGroupIDAndOrgID(ctx, groupID, senderOrgUser.OrganizationId.Hex())
	if err != nil {
		log.ZError(ctx, "群组不属于发送者的组织", err,
			"group_id", groupID,
			"sender_im_id", senderImID,
			"organization_id", senderOrgUser.OrganizationId.Hex())
		return errs.NewCodeError(freeErrors.ErrNotInGroup, freeErrors.ErrorMessages[freeErrors.ErrNotInGroup])
	}

	log.ZInfo(ctx, "群组组织验证通过",
		"group_id", groupID,
		"organization_id", senderOrgUser.OrganizationId.Hex(),
		"group_name", group.GroupName)

	return nil
}
