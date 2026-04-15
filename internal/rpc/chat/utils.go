package chat

import (
	"context"
	"strconv"
	"strings"

	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	table "github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/chat/pkg/eerrs"
	"github.com/openimsdk/chat/pkg/protocol/chat"
	"github.com/openimsdk/chat/pkg/protocol/common"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/utils/stringutil"
)

func DbToPbAttribute(attribute *table.Attribute, showSensitiveInfo bool) *common.UserPublicInfo {
	if attribute == nil {
		return nil
	}

	info := &common.UserPublicInfo{
		UserID:   attribute.UserID,
		Account:  attribute.Account,
		Nickname: attribute.Nickname,
		FaceURL:  attribute.FaceURL,
		Gender:   attribute.Gender,
		Level:    attribute.Level,
	}

	if showSensitiveInfo {
		info.Email = attribute.Email
	}

	return info
}

func DbToPbAttributes(attributes []*table.Attribute, isAdmin bool, currentUserID string) []*common.UserPublicInfo {
	result := make([]*common.UserPublicInfo, 0, len(attributes))
	for _, attr := range attributes {
		showSensitiveInfo := isAdmin || attr.UserID == currentUserID
		result = append(result, DbToPbAttribute(attr, showSensitiveInfo))
	}
	return result
}

func DbToPbUserFullInfo(attribute *table.Attribute, showSensitiveInfo bool) *common.UserFullInfo {
	if attribute == nil {
		return nil
	}

	info := &common.UserFullInfo{
		UserID:           attribute.UserID,
		UserChatID:       attribute.UserID,
		Password:         "",
		Account:          attribute.Account,
		Nickname:         attribute.Nickname,
		FaceURL:          attribute.FaceURL,
		Gender:           attribute.Gender,
		Level:            attribute.Level,
		AllowAddFriend:   attribute.AllowAddFriend,
		AllowBeep:        attribute.AllowBeep,
		AllowVibration:   attribute.AllowVibration,
		GlobalRecvMsgOpt: attribute.GlobalRecvMsgOpt,
		RegisterType:     attribute.RegisterType,
	}

	if showSensitiveInfo {
		info.Birth = attribute.BirthTime.UnixMilli()
		info.Email = attribute.Email
		info.PhoneNumber = attribute.PhoneNumber
		info.AreaCode = attribute.AreaCode
	}

	return info
}

func DbToPbUserFullInfoByOrg(attribute *table.AttributeWithOrgUser, showSensitiveInfo bool) *common.UserFullInfo {
	if attribute == nil {
		return nil
	}

	info := &common.UserFullInfo{
		UserID:           attribute.ImServerUserId,
		Password:         "",
		Account:          attribute.Account,
		Nickname:         attribute.Nickname,
		FaceURL:          attribute.FaceURL,
		Gender:           attribute.Gender,
		Level:            attribute.Level,
		AllowAddFriend:   attribute.AllowAddFriend,
		AllowBeep:        attribute.AllowBeep,
		AllowVibration:   attribute.AllowVibration,
		GlobalRecvMsgOpt: attribute.GlobalRecvMsgOpt,
		RegisterType:     attribute.RegisterType,
		UserChatID:       attribute.UserID,
	}

	if showSensitiveInfo {
		info.Birth = attribute.BirthTime.UnixMilli()
		info.Email = attribute.Email
		info.PhoneNumber = attribute.PhoneNumber
		info.AreaCode = attribute.AreaCode
	}

	return info
}

func DbToPbUserFullInfos(attributes []*table.Attribute, isAdmin bool, currentUserID string) []*common.UserFullInfo {
	result := make([]*common.UserFullInfo, 0, len(attributes))
	for _, attr := range attributes {
		// 如果是管理员或者是用户自己的数据，显示完整信息
		showSensitiveInfo := isAdmin || attr.UserID == currentUserID
		result = append(result, DbToPbUserFullInfo(attr, showSensitiveInfo))
	}
	return result
}

func DbToPbUserFullInfosByOrg(attributes []*table.AttributeWithOrgUser, isAdmin bool, currentUserID string) []*common.UserFullInfo {
	result := make([]*common.UserFullInfo, 0, len(attributes))
	for _, attr := range attributes {
		// 如果是管理员或者是用户自己的数据，显示完整信息
		showSensitiveInfo := isAdmin || attr.UserID == currentUserID
		result = append(result, DbToPbUserFullInfoByOrg(attr, showSensitiveInfo))
	}
	return result
}

func BuildCredentialPhone(areaCode, phone string) string {
	return areaCode + " " + phone
}

func (o *chatSvr) checkRegisterInfo(ctx context.Context, user *chat.RegisterUserInfo, isAdmin bool) error {
	if user == nil {
		return errs.ErrArgs.WrapMsg("user is nil")
	}
	if user.Email == "" && !(user.PhoneNumber != "" && user.AreaCode != "") && (!isAdmin || user.Account == "") {
		return errs.ErrArgs.WrapMsg("at least one valid account is required")
	}
	if user.PhoneNumber != "" {
		if !strings.HasPrefix(user.AreaCode, "+") {
			user.AreaCode = "+" + user.AreaCode
		}
		if _, err := strconv.ParseUint(user.AreaCode[1:], 10, 64); err != nil {
			return errs.ErrArgs.WrapMsg("area code must be number")
		}
		if _, err := strconv.ParseUint(user.PhoneNumber, 10, 64); err != nil {
			return errs.ErrArgs.WrapMsg("phone number must be number")
		}
		_, err := o.Database.TakeAttributeByPhone(ctx, user.AreaCode, user.PhoneNumber)
		if err == nil {
			return eerrs.ErrPhoneAlreadyRegister.Wrap()
		} else if !dbutil.IsDBNotFound(err) {
			return err
		}
	}
	if user.Account != "" {
		if !stringutil.IsAlphanumeric(user.Account) {
			return errs.ErrArgs.WrapMsg("account must be alphanumeric")
		}
		_, err := o.Database.TakeAttributeByAccount(ctx, user.Account)
		if err == nil {
			return eerrs.ErrAccountAlreadyRegister.Wrap()
		} else if !dbutil.IsDBNotFound(err) {
			return err
		}
	}
	if user.Email != "" {
		if !stringutil.IsValidEmail(user.Email) {
			return errs.ErrArgs.WrapMsg("invalid email")
		}
		_, err := o.Database.TakeAttributeByAccount(ctx, user.Email)
		if err == nil {
			return eerrs.ErrEmailAlreadyRegister.Wrap()
		} else if !dbutil.IsDBNotFound(err) {
			return err
		}
	}
	return nil
}

func DbToPbUserFullInfosWithMapping(attributes []*table.Attribute, requestedImUserIDs []string, imUserToUserIdMap map[string]string, imUserToRoleMap map[string]string, isAdmin bool, currentUserID string) []*common.UserFullInfo {
	// 创建 UserID 到 Attribute 的映射
	userIdToAttrMap := make(map[string]*table.Attribute)
	for _, attr := range attributes {
		userIdToAttrMap[attr.UserID] = attr
	}

	// 按照请求的 ImUserID 顺序返回结果
	result := make([]*common.UserFullInfo, 0, len(requestedImUserIDs))
	for _, imUserID := range requestedImUserIDs {
		// 通过 ImUserID 找到对应的 UserID
		if userID, exists := imUserToUserIdMap[imUserID]; exists {
			// 通过 UserID 找到对应的属性
			if attr, found := userIdToAttrMap[userID]; found {
				showSensitiveInfo := isAdmin || attr.UserID == currentUserID
				userInfo := DbToPbUserFullInfo(attr, showSensitiveInfo)
				if userInfo != nil {
					// 设置 UserID 为原始的 ImUserID，保持对应关系
					userInfo.UserID = imUserID
					// 设置组织角色
					if role, roleExists := imUserToRoleMap[imUserID]; roleExists {
						userInfo.OrgRole = role
					}
				}
				result = append(result, userInfo)
			}
		}
	}

	return result
}
