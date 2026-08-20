package svc

import (
	"context"

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

// CheckGroupMembership 检查用户是否在群组中。
//
// 【性能】原来是调 OpenIM 的 /group/get_group_member_user_id 把**整个群的成员 ID 全拉回来**
// 再线性查找。抢红包时每个人都要跑一次：一个 5000 人的群被 5000 个人抢，
// 就是 5000 次 HTTP 调用、2500 万个 ID 在网络和 GC 里来回。
//
// chat 与 im-server 共用同一个 MongoDB，group_member 上又有 (group_id, user_id) 索引，
// 所以直接查一条即可，从「拉全量 + O(N) 扫描」变成一次索引命中的单文档读。
func (v *VerifyService) CheckGroupMembership(ctx context.Context, userID string, groupID string) error {
	if groupID == "" {
		log.ZError(ctx, "群组ID为空", nil, "user_id", userID)
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}

	exist, err := openImModel.NewGroupMemberDao(plugin.MongoCli().GetDB()).Exist(ctx, groupID, userID)
	if err != nil {
		log.ZError(ctx, "查询群成员关系失败", err, "user_id", userID, "group_id", groupID)
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	if !exist {
		log.ZWarn(ctx, "用户不在群组中", nil, "user_id", userID, "group_id", groupID)
		return errs.NewCodeError(freeErrors.ErrNotInGroup, freeErrors.ErrorMessages[freeErrors.ErrNotInGroup])
	}
	return nil
}

// VerifyGroupRedPacket 验证发红包的人是否在群中，以及红包个数是否超过群人数。
//
// 【性能】原来是把整个群的成员 ID 拉回来，再线性查找 + 取 len()。
// 现在拆成两次索引查询：一次判断成员关系，一次统计群人数（仅在需要时才统计）。
// 发红包本身是低频操作，但拉全量成员对大群同样是几万个 ID 的传输，没必要。
//
// 返回值去掉了原来的成员列表——唯一的调用方 validateUserRelations 本来就把它丢弃了，
// 留着只会让人误以为拿到的列表还有别的用途。
func (v *VerifyService) VerifyGroupRedPacket(ctx context.Context, userID string, groupID string, redPacketCount int) error {
	if groupID == "" {
		log.ZError(ctx, "群组ID为空", nil, "user_id", userID)
		return errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
	}
	dao := openImModel.NewGroupMemberDao(plugin.MongoCli().GetDB())

	// 1. 检查用户是否在群中
	exist, err := dao.Exist(ctx, groupID, userID)
	if err != nil {
		log.ZError(ctx, "查询群成员关系失败", err, "user_id", userID, "group_id", groupID)
		return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}
	if !exist {
		log.ZWarn(ctx, "用户不在群组中", nil, "user_id", userID, "group_id", groupID)
		return errs.NewCodeError(freeErrors.ErrNotInGroup, freeErrors.ErrorMessages[freeErrors.ErrNotInGroup])
	}

	// 2. 指定了红包个数才需要统计群人数
	if redPacketCount > 0 {
		memberCount, err := dao.CountByGroupID(ctx, groupID)
		if err != nil {
			log.ZError(ctx, "统计群成员数失败", err, "group_id", groupID)
			return errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
		}
		if int64(redPacketCount) > memberCount {
			log.ZWarn(ctx, "红包数量超过群成员数", nil, "group_id", groupID, "red_packet_count", redPacketCount, "member_count", memberCount)
			return errs.NewCodeError(freeErrors.ErrRedPacketCountExceedGroupMemberCount, freeErrors.ErrorMessages[freeErrors.ErrRedPacketCountExceedGroupMemberCount])
		}
		log.ZInfo(ctx, "红包数量验证通过", "user_id", userID, "group_id", groupID, "red_packet_count", redPacketCount, "member_count", memberCount)
	}
	return nil
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
