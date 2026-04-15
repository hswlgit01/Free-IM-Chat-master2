// Copyright © 2023 OpenIM open source community. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package imapi

import (
	"github.com/openimsdk/protocol/auth"
	"github.com/openimsdk/protocol/group"
	"github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/relation"
	"github.com/openimsdk/protocol/user"
)

// im caller.
var (
	importFriend   = NewApiCaller[relation.ImportFriendReq, relation.ImportFriendResp]("/friend/import_friend")
	getAdminToken  = NewApiCaller[any, auth.GetAdminTokenResp]("/auth/get_admin_token")
	getuserToken   = NewApiCaller[auth.GetUserTokenReq, auth.GetUserTokenResp]("/auth/get_user_token")
	inviteToGroup  = NewApiCaller[group.InviteUserToGroupReq, group.InviteUserToGroupResp]("/group/invite_user_to_group")
	updateUserInfo = NewApiCaller[user.UpdateUserInfoReq, user.UpdateUserInfoResp]("/user/update_user_info")

	registerUser          = NewApiCaller[user.UserRegisterReq, user.UserRegisterResp]("/user/user_register")
	forceOffLine          = NewApiCaller[auth.ForceLogoutReq, auth.ForceLogoutResp]("/auth/force_logout")
	forceOffLines         = NewApiCaller[any, any]("/auth/batch_force_logout")
	getGroupsInfo         = NewApiCaller[group.GetGroupsInfoReq, group.GetGroupsInfoResp]("/group/get_groups_info")
	registerUserCount     = NewApiCaller[user.UserRegisterCountReq, user.UserRegisterCountResp]("/statistics/user/register")
	friendUserIDs         = NewApiCaller[relation.GetFriendIDsReq, relation.GetFriendIDsResp]("/friend/get_friend_id")
	accountCheck          = NewApiCaller[user.AccountCheckReq, user.AccountCheckResp]("/user/account_check")
	getGroupMemberUserIDs = NewApiCaller[group.GetGroupMemberUserIDsReq, group.GetGroupMemberUserIDsResp]("/group/get_group_member_user_id")
	sendMsg               = NewApiCaller[any, msg.SendMsgResp]("/msg/send_msg")
	batchSendMsg          = NewApiCaller[any, any]("/msg/batch_send_msg")
	createGroup           = NewApiCaller[group.CreateGroupReq, group.CreateGroupResp]("/group/create_group")
	dismissGroup          = NewApiCaller[group.DismissGroupReq, group.DismissGroupResp]("/group/dismiss_group")
	muteGroup             = NewApiCaller[group.MuteGroupReq, group.MuteGroupResp]("/group/mute_group")
	cancelMuteGroup       = NewApiCaller[group.CancelMuteGroupReq, group.CancelMuteGroupResp]("/group/cancel_mute_group")
	getGroupMemberList    = NewApiCaller[group.GetGroupMemberListReq, group.GetGroupMemberListResp]("/group/get_group_member_list")
	setGroupInfo          = NewApiCaller[group.SetGroupInfoReq, group.SetGroupInfoResp]("/group/set_group_info")
	setGroupMemberInfo    = NewApiCaller[group.SetGroupMemberInfoReq, group.SetGroupMemberInfoResp]("/group/set_group_member_info")
	muteGroupMember       = NewApiCaller[group.MuteGroupMemberReq, group.MuteGroupMemberResp]("/group/mute_group_member")
	cancelMuteGroupMember = NewApiCaller[group.CancelMuteGroupMemberReq, group.CancelMuteGroupMemberResp]("/group/cancel_mute_group_member")
	kickGroupMember       = NewApiCaller[group.KickGroupMemberReq, group.KickGroupMemberResp]("/group/kick_group")
	transferGroup         = NewApiCaller[group.TransferGroupOwnerReq, group.TransferGroupOwnerResp]("/group/transfer_group")

	createGroupOperationLog = NewApiCaller[any, any]("/group/create_group_operation_log")
	registerOrgUser         = NewApiCaller[RegisterOrgUserReq, user.UserRegisterResp]("/user/user_register")
	updateOrgUserInfo       = NewApiCaller[UpdateOrgUserInfoReq, user.UpdateUserInfoResp]("/user/update_user_info")
	addFriend               = NewApiCaller[relation.ApplyToAddFriendReq, relation.ApplyToAddFriendResp]("/friend/add_friend")

	addNotificationAccount    = NewApiCaller[AddNotificationAccountReq, any]("/user/add_notification_account")
	updateNotificationAccount = NewApiCaller[user.UpdateNotificationAccountInfoReq, any]("/user/update_notification_account")
)
