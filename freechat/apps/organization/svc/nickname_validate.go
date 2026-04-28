package svc

import (
	"context"
	"strings"
	"unicode"

	"github.com/openimsdk/chat/freechat/apps/organization/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

var orgAdminRolesForNickname = []model.OrganizationUserRole{
	model.OrganizationUserSuperAdminRole,
	model.OrganizationUserBackendAdminRole,
	model.OrganizationUserGroupManagerRole,
	model.OrganizationUserTermManagerRole,
}

// nicknameCondensedNoSeparators 去掉空白、ASCII 横线与下划线，用于识别用分隔符拆开的敏感组合（如 官_方_客_服_1）。
func nicknameCondensedNoSeparators(s string) string {
	var b strings.Builder
	b.Grow(len([]rune(s)))
	for _, r := range strings.TrimSpace(s) {
		if r == '-' || r == '_' || unicode.IsSpace(r) {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// nicknameImpersonatesOfficialCustomer 是否为「官方客服」及其变体（如 官方客服1、官方客服_1、官 方 客 服 1、官方_客服_1）。
func nicknameImpersonatesOfficialCustomer(nick string) bool {
	return strings.Contains(nicknameCondensedNoSeparators(nick), "官方客服")
}

// nicknameContainsFold 子串包含（ASCII 字母忽略大小写；中文等按原样比较）。
func nicknameContainsFold(haystack, needle string) bool {
	needle = strings.TrimSpace(needle)
	if needle == "" {
		return false
	}
	return strings.Contains(strings.ToLower(haystack), strings.ToLower(needle))
}

func nicknameEqualFold(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}

func nicknameHasHan(s string) bool {
	for _, r := range s {
		if unicode.Is(unicode.Han, r) {
			return true
		}
	}
	return false
}

func nicknameCanUseAdminSubstringRule(adminNick string) bool {
	adminNick = strings.TrimSpace(adminNick)
	return nicknameHasHan(adminNick) || len([]rune(adminNick)) >= 3
}

// nicknameHanOnly 从昵称中按顺序抽出所有汉字（CJK 统一表意文字），忽略数字、符号、空白等。
// 用于与管理员展示名比较：若管理员名称包含该连续汉字串，则普通用户不得使用该昵称（防止「张.三」「1张三」等与「张三-卫生集团」撞脸）。
func nicknameHanOnly(nick string) string {
	var b strings.Builder
	for _, r := range nick {
		if unicode.Is(unicode.Han, r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ValidateAppUserNickname 应用端昵称规则：去空白后非空；不可仿冒「官方客服」及变体；不可包含其它敏感词；
// 用户昵称不可包含本组织任一管理员（超管/后台管理员/群管理员）的 IM 昵称作为子串（忽略大小写）；
// 且用户昵称中提取出的汉字串若非空，则不可出现「任一管理员的 IM 昵称包含该汉字串」的情况（防止用符号/数字拆开仍与管理员展示名冲突）。
// excludeIMServerUserID 非空时，跳过该 IM user_id 对应的管理员（用于已登录用户改昵称时不与自身冲突）。
func ValidateAppUserNickname(ctx context.Context, db *mongo.Database, orgID primitive.ObjectID, nickname string, excludeIMServerUserID string) error {
	nick := strings.TrimSpace(nickname)
	if nick == "" {
		return freeErrors.ApiErr("用户昵称不合法")
	}
	if nicknameImpersonatesOfficialCustomer(nick) {
		return freeErrors.ApiErr("用户昵称不合法")
	}
	if strings.Contains(nick, "官方") || strings.Contains(nick, "客服") || strings.Contains(nick, "会聊") || strings.Contains(nick, "资产") {
		return freeErrors.ApiErr("用户昵称不合法")
	}
	if orgID.IsZero() {
		return nil
	}

	orgUserDao := model.NewOrganizationUserDao(db)
	admins, err := orgUserDao.Select(ctx, "", orgID, orgAdminRolesForNickname)
	if err != nil {
		return err
	}
	if len(admins) == 0 {
		return nil
	}

	imIDs := make([]string, 0, len(admins))
	for _, a := range admins {
		if a.ImServerUserId != "" {
			imIDs = append(imIDs, a.ImServerUserId)
		}
	}
	if len(imIDs) == 0 {
		return nil
	}

	userDao := openImModel.NewUserDao(db)
	users, err := userDao.FindByUserIDs(ctx, imIDs)
	if err != nil {
		return err
	}
	userHan := nicknameHanOnly(nick)
	for _, u := range users {
		if u == nil {
			continue
		}
		if excludeIMServerUserID != "" && u.UserID == excludeIMServerUserID {
			continue
		}
		adminNick := strings.TrimSpace(u.Nickname)
		if adminNick == "" {
			continue
		}
		if nicknameEqualFold(nick, adminNick) {
			return freeErrors.ApiErr("用户昵称不合法")
		}
		if nicknameCanUseAdminSubstringRule(adminNick) && nicknameContainsFold(nick, adminNick) {
			return freeErrors.ApiErr("用户昵称不合法")
		}
		if userHan != "" && strings.Contains(adminNick, userHan) {
			return freeErrors.ApiErr("用户昵称不合法")
		}
	}
	return nil
}
