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

package chat

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"strings"

	chatdb "github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/errs"

	"github.com/openimsdk/chat/pkg/common/constant"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/chat/pkg/protocol/chat"
)

func isMD5Hex(s string) bool {
	// MD5 is 32 lowercase/uppercase hex chars.
	if len(s) != 32 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

// normalizePasswordForStorage converts client password input to the storage format.
// Current clients are supposed to send MD5 hex, but after app/package changes some clients may send plaintext.
// This makes backend tolerant to both.
func normalizePasswordForStorage(p string) string {
	p = strings.TrimSpace(p)
	p = strings.ToLower(p)
	if isMD5Hex(p) {
		return p
	}
	sum := md5.Sum([]byte(p))
	return hex.EncodeToString(sum[:]) // lowercase hex
}

func (o *chatSvr) ResetPassword(ctx context.Context, req *chat.ResetPasswordReq) (*chat.ResetPasswordResp, error) {
	if req.Password == "" {
		return nil, errs.ErrArgs.WrapMsg("password must be set")
	}
	normalizedPassword := normalizePasswordForStorage(req.Password)
	if req.AreaCode == "" || req.PhoneNumber == "" {
		if !(req.AreaCode == "" && req.PhoneNumber == "") {
			return nil, errs.ErrArgs.WrapMsg("area code and phone number must set together")
		}
	}
	var verifyCodeID string
	var err error
	if req.Email == "" {
		verifyCodeID, err = o.verifyCode(ctx, o.verifyCodeJoin(req.AreaCode, req.PhoneNumber), req.VerifyCode, phone, constant.VerificationCodeForResetPassword)
	} else {
		verifyCodeID, err = o.verifyCode(ctx, req.Email, req.VerifyCode, mail, constant.VerificationCodeForResetPassword)
	}

	if err != nil {
		return nil, err
	}
	var account string
	if req.Email == "" {
		account = BuildCredentialPhone(req.AreaCode, req.PhoneNumber)
	} else {
		account = req.Email
	}
	cred, err := o.Database.TakeCredentialByAccount(ctx, account)
	if err != nil {
		return nil, err
	}
	err = o.Database.UpdatePasswordAndDeleteVerifyCode(ctx, cred.UserID, normalizedPassword, verifyCodeID)
	if err != nil {
		return nil, err
	}
	return &chat.ResetPasswordResp{}, nil
}

func (o *chatSvr) ChangePassword(ctx context.Context, req *chat.ChangePasswordReq) (*chat.ChangePasswordResp, error) {
	if req.NewPassword == "" {
		return nil, errs.ErrArgs.WrapMsg("new password must be set")
	}
	normalizedCurrentPassword := normalizePasswordForStorage(req.CurrentPassword)
	normalizedNewPassword := normalizePasswordForStorage(req.NewPassword)

	if normalizedNewPassword == normalizedCurrentPassword {
		return nil, errs.ErrArgs.WrapMsg("new password == current password")
	}
	opUserID, userType, err := mctx.Check(ctx)
	if err != nil {
		return nil, err
	}

	//根据opUserID ，orgid  查询 imserverid
	orgId, err := mctx.GetOrgId(ctx)
	orgUser, err := o.Database.GetImServerID(ctx, opUserID, orgId)
	if err != nil {
		return nil, err
	}
	if orgUser.ImServerUserId == "" {
		return nil, errs.ErrNoPermission.WrapMsg("no permission change other user password")
	}
	targetUserID := opUserID
	switch userType {
	case constant.NormalUser:
		if req.UserID != "" && req.UserID != opUserID {
			// 兼容：部分客户端会把 imServerUserId 当作 userID 传过来。
			// 如果传入的 req.UserID 恰好等于当前登录人的 imServerUserId，
			// 则按“改自己的密码”处理，避免误判成“改其他用户密码”。
			if req.UserID == orgUser.ImServerUserId {
				targetUserID = opUserID
			} else {
				// 允许组织管理角色（SuperAdmin/BackendAdmin）在本组织内为其他用户改密
				if orgUser.Role != chatdb.OrganizationUserSuperAdminRole && orgUser.Role != chatdb.OrganizationUserBackendAdminRole {
					return nil, errs.ErrNoPermission.WrapMsg("no permission change other user password")
				}
				targetUserID = req.UserID
			}
		}
	case constant.AdminUser:
		if req.UserID == "" {
			return nil, errs.ErrArgs.WrapMsg("user id must be set")
		}
		targetUserID = req.UserID
	default:
		return nil, errs.ErrInternalServer.WrapMsg("invalid user type")
	}

	targetOrgUser, err := o.Database.GetImServerID(ctx, targetUserID, orgId)
	if err != nil {
		return nil, err
	}
	user, err := o.Database.GetUser(ctx, targetUserID)
	if err != nil {
		return nil, err
	}
	if userType != constant.AdminUser {
		if strings.ToLower(user.Password) != normalizedCurrentPassword {
			return nil, errs.NewCodeError(11004, "current password is wrong")
		}
	}
	if strings.ToLower(user.Password) != normalizedNewPassword {
		if err := o.Database.UpdatePassword(ctx, targetUserID, normalizedNewPassword); err != nil {
			return nil, err
		}
	}

	//判断是不是H5用户  H5不踢出
	if orgUser.RegisterType != "h5" {
		if err := o.Admin.InvalidateToken(ctx, targetUserID); err != nil {
			return nil, err
		}
		return &chat.ChangePasswordResp{
			ShouldKickOut: true,
			ImServerID:    targetOrgUser.ImServerUserId,
		}, nil
	}
	return &chat.ChangePasswordResp{
		ShouldKickOut: false,
		ImServerID:    targetOrgUser.ImServerUserId,
	}, nil
}
