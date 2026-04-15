package svc

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"time"

	defaultFriendSvc "github.com/openimsdk/chat/freechat/apps/defaultFriend/svc"
	defaultGroupSvc "github.com/openimsdk/chat/freechat/apps/defaultGroup/svc"
	chatCache "github.com/openimsdk/chat/freechat/third/chat/cache"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/protocol/sdkws"

	"github.com/openimsdk/chat/freechat/apps/user/dto"
	"github.com/openimsdk/chat/freechat/apps/user/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils/captcha"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"

	"github.com/openimsdk/tools/log"

	adminModel "github.com/openimsdk/chat/freechat/apps/admin/model"
	OrgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	orgSvc "github.com/openimsdk/chat/freechat/apps/organization/svc"
	platformCfgModel "github.com/openimsdk/chat/freechat/apps/platformConfig/model"
	"github.com/openimsdk/chat/freechat/plugin"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/constant"
	"github.com/openimsdk/chat/pkg/common/mctx"
	pkgConstant "github.com/openimsdk/chat/pkg/constant"
	"github.com/openimsdk/chat/pkg/protocol/admin"
	chatpb "github.com/openimsdk/chat/pkg/protocol/chat"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/mcontext"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// BlockUserResp 简化的封禁用户响应
type BlockUserResp struct {
	Total uint32                               `json:"total"`
	Users []*chatModel.BlockUserWithOrgAndAttr `json:"users"`
}

type UserSvc struct{}

func NewUserSvc() *UserSvc {
	return &UserSvc{}
}

type RegUserReq struct {
	chatpb.RegisterUserReq
	OrgInvitationCode string `protobuf:"bytes,1,opt,name=orgInvitationCode,proto3" json:"orgInvitationCode"`
}

type RegUserResp struct {
	UserID         string             `json:"user_id"`
	ChatToken      string             `json:"chat_token"`
	ImToken        string             `json:"im_token"`
	OrganizationId primitive.ObjectID `json:"organization_id"`
	InviteUserID   string             `json:"invite_user_id"`
}

type FindUserFullInfoReq struct {
	UserIDs []string `json:"userIDs" binding:"required"`
}

type FindUserFullInfoResp struct {
	Users []*UserFullInfo `json:"users"`
}

type SearchUserFullInfoReq struct {
	Keyword    string                   `json:"keyword" binding:"required"`
	Pagination *sdkws.RequestPagination `json:"pagination"`
	Genders    int32                    `json:"genders"`
	Normal     int32                    `json:"normal"`
}

type SearchUserFullInfoResp struct {
	Total uint32          `json:"total"`
	Users []*UserFullInfo `json:"users"`
}

type UserFullInfo struct {
	UserChatID         string `json:"userChatID,omitempty"`
	Password           string `json:"password,omitempty"`
	Account            string `json:"account,omitempty"`
	PhoneNumber        string `json:"phoneNumber,omitempty"`
	AreaCode           string `json:"areaCode,omitempty"`
	Email              string `json:"email,omitempty"`
	Nickname           string `json:"nickname,omitempty"`
	FaceURL            string `json:"faceURL,omitempty"`
	Gender             int32  `json:"gender,omitempty"`
	Level              int32  `json:"level,omitempty"`
	Birth              int64  `json:"birth,omitempty"`
	AllowAddFriend     int32  `json:"allowAddFriend,omitempty"`
	AllowBeep          int32  `json:"allowBeep,omitempty"`
	AllowVibration     int32  `json:"allowVibration,omitempty"`
	GlobalRecvMsgOpt   int32  `json:"globalRecvMsgOpt,omitempty"`
	RegisterType       int32  `json:"registerType,omitempty"`
	UserID             string `json:"userID,omitempty"`
	OrgRole            string `json:"orgRole,omitempty"`
	InvitationCode     string `json:"invitationCode,omitempty"`
	Points             int64  `json:"points,omitempty"`
	CanSendFreeMsg     int32  `json:"can_send_free_msg"`
	IsRealNameVerified bool   `json:"isRealNameVerified"` // 是否已实名认证
	RealName           string `json:"realName,omitempty"` // 真实姓名
}

// BatchForceLogoutReq 批量强制下线请求
type BatchForceLogoutReq struct {
	Items []BatchForceLogoutItem `json:"items"`
}

// BatchForceLogoutItem 强制下线项
type BatchForceLogoutItem struct {
	UserID     string `json:"userID"`
	PlatformID int32  `json:"platformID"`
}

func (w *UserSvc) RegisterUser(ctx context.Context, operationID string, req *RegUserReq) (*RegUserResp, error) {
	attributeDao := chatModel.NewAttributeDao(plugin.MongoCli().GetDB())
	accountDao := chatModel.NewAccountDao(plugin.MongoCli().GetDB())
	registerDao := chatModel.NewRegisterDao(plugin.MongoCli().GetDB())
	credentialDao := chatModel.NewCredentialDao(plugin.MongoCli().GetDB())
	orgUserDao := OrgModel.NewOrganizationUserDao(plugin.MongoCli().GetDB())
	//organizationDao := OrgModel.NewOrganizationDao(plugin.MongoCli().GetDB())

	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	imApiCallerToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return nil, err
	}
	imApiCallerCtx := mctx.WithApiToken(ctxWithOpID, imApiCallerToken)

	if req.User.Email == "" {
		return nil, freeErrors.ApiErr("email must be set")
	}
	if req.User.Account == "" {
		return nil, freeErrors.ApiErr("account must be set")
	}

	if err := checkRegisterClientIPNotFromBannedUser(ctx, plugin.MongoCli().GetDB(), req.Ip); err != nil {
		return nil, err
	}

	orgId := primitive.NilObjectID
	userId := ""
	err = plugin.MongoCli().GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		_, err := plugin.ChatClient().VerifyCode(ctx, &chatpb.VerifyCodeReq{
			AreaCode:          "",
			PhoneNumber:       "",
			VerifyCode:        req.VerifyCode,
			Email:             req.User.Email,
			UsedFor:           constant.VerificationCodeForRegister,
			DeleteAfterVerify: true,
		})
		if err != nil {
			return err
		}

		newUserID, err := utils.NewId()
		if err != nil {
			return err
		}

		if req.User.Account == "" {
			req.User.Account = "fcid_" + utils.RandomString(14)
		}
		attributes, err := attributeDao.FindAccountCaseInsensitive(sessionCtx, []string{req.User.Account})
		if err != nil {
			return err
		}
		if len(attributes) > 0 {
			return freeErrors.AccountExistsErr
		}

		//判断邮箱是否重复
		attributes, err = attributeDao.FindEmail(sessionCtx, []string{req.User.Email})
		if err != nil {
			return err
		}
		if len(attributes) > 0 {
			return freeErrors.EmailInUseErr
		}

		credentials := make([]*chatModel.Credential, 0)
		var registerType int32 = constant.AccountRegister
		// 手机号注册屏蔽
		//if req.User.PhoneNumber != "" {
		//	registerType = constant.PhoneRegister
		//	credentials = append(credentials, &chatModel.Credential{
		//		UserID:      newUserID,
		//		Account:     utils.BuildCredentialPhone(req.User.AreaCode, req.User.PhoneNumber),
		//		RewardType:        constant.CredentialPhone,
		//		AllowChange: true,
		//	})
		//}
		if req.User.Account != "" {
			credentials = append(credentials, &chatModel.Credential{
				UserID:      newUserID,
				Account:     req.User.Account,
				Type:        constant.CredentialAccount,
				AllowChange: true,
			})
			registerType = constant.AccountRegister
		}
		if req.User.Email != "" {
			registerType = constant.EmailRegister
			credentials = append(credentials, &chatModel.Credential{
				UserID:      newUserID,
				Account:     req.User.Email,
				Type:        constant.CredentialEmail,
				AllowChange: true,
			})
		}
		register := &chatModel.Register{
			UserID:      newUserID,
			DeviceID:    req.DeviceID,
			IP:          req.Ip,
			Platform:    pkgConstant.PlatformID2Name[int(req.Platform)],
			AccountType: "",
			Mode:        constant.UserMode,
			CreateTime:  time.Now(),
		}
		account := &chatModel.Account{
			UserID:         newUserID,
			Password:       req.User.Password,
			OperatorUserID: mcontext.GetOpUserID(ctx),
			ChangeTime:     register.CreateTime,
			CreateTime:     register.CreateTime,
		}

		attribute := &chatModel.Attribute{
			UserID:         newUserID,
			Account:        req.User.Account,
			PhoneNumber:    req.User.PhoneNumber,
			AreaCode:       req.User.AreaCode,
			Email:          req.User.Email,
			Nickname:       "",
			FaceURL:        "",
			Gender:         req.User.Gender,
			BirthTime:      time.UnixMilli(req.User.Birth),
			ChangeTime:     register.CreateTime,
			CreateTime:     register.CreateTime,
			AllowVibration: constant.DefaultAllowVibration,
			AllowBeep:      constant.DefaultAllowBeep,
			AllowAddFriend: constant.DefaultAllowAddFriend,
			RegisterType:   registerType,
		}

		if err := registerDao.Create(sessionCtx, register); err != nil {
			return err
		}
		if err := accountDao.Create(sessionCtx, account); err != nil {
			return err
		}
		if err := attributeDao.Create(sessionCtx, attribute); err != nil {
			return err
		}
		if err := credentialDao.Create(sessionCtx, credentials...); err != nil {
			return err
		}
		organizationSvc := orgSvc.OrganizationSvc{}
		resp, err := organizationSvc.JoinOrgUsingInvitationCodeByAttr(sessionCtx, operationID, newUserID, orgSvc.JoinOrgUsingInvitationCodeReq{
			InvitationCode: req.OrgInvitationCode,
			Nickname:       req.User.Nickname,
			FaceURL:        req.User.FaceURL,
		}, attribute)
		if err != nil {
			return err
		}

		orgId = resp.OrgId
		userId = newUserID
		return nil
	})

	if err != nil {
		return nil, errs.Unwrap(err)
	}

	orgUser, err := orgUserDao.GetByUserIdAndOrgId(context.Background(), userId, orgId)
	if err != nil {
		return nil, err
	}

	defFriendSvc := defaultFriendSvc.NewDefaultFriendSvc()
	defFriendSvc.InternalAddDefaultFriend(operationID, orgUser.OrganizationId, orgUser.ImServerUserId)

	defGroupSvc := defaultGroupSvc.NewDefaultGroupSvc()
	defGroupSvc.InternalAddDefaultGroup(operationID, orgUser.OrganizationId, orgUser.ImServerUserId)

	chatToken, err := plugin.AdminClient().CreateToken(ctx, &admin.CreateTokenReq{UserID: orgUser.UserId, UserType: constant.NormalUser})
	if err != nil {
		return nil, err
	}

	imToken, err := imApiCaller.GetUserToken(imApiCallerCtx, orgUser.ImServerUserId, req.Platform)
	if err != nil {
		return nil, err
	}
	inviteUserID := ""
	if orgUser.InviterType == OrgModel.OrganizationUserInviterTypeOrgUser {
		inviteUserID = orgUser.InviterImServerUserId
	}

	return &RegUserResp{
		UserID:         orgUser.ImServerUserId,
		ChatToken:      chatToken.Token,
		ImToken:        imToken,
		OrganizationId: orgId,
		InviteUserID:   inviteUserID,
	}, nil

}

type RegisterUserViaAccountReq struct {
	chatpb.RegisterUserReq
	OrgInvitationCode string `protobuf:"bytes,1,opt,name=orgInvitationCode,proto3" json:"orgInvitationCode"`

	CaptchaId     string `json:"captchaId" binding:"required"`
	CaptchaAnswer string `json:"captchaAnswer" binding:"required"`

	DeviceCode string `json:"deviceCode"`
}

var AccountRegexp = regexp.MustCompile(`^[a-zA-Z0-9\-_=+]{5,20}$`)

func (w *UserSvc) RegisterUserViaAccount(ctx context.Context, operationID string, req *RegisterUserViaAccountReq) (*RegUserResp, error) {
	redisCli := plugin.RedisCli()
	chatCfg := plugin.ChatCfg()

	attributeDao := chatModel.NewAttributeDao(plugin.MongoCli().GetDB())
	accountDao := chatModel.NewAccountDao(plugin.MongoCli().GetDB())
	registerDao := chatModel.NewRegisterDao(plugin.MongoCli().GetDB())
	credentialDao := chatModel.NewCredentialDao(plugin.MongoCli().GetDB())
	orgUserDao := OrgModel.NewOrganizationUserDao(plugin.MongoCli().GetDB())
	deviceRegisterNumDao := model.NewDeviceRegisterNumDao(redisCli)
	userLoginRecordDao := chatModel.NewUserLoginRecordDao(plugin.MongoCli().GetDB())

	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	imApiCallerToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return nil, err
	}
	imApiCallerCtx := mctx.WithApiToken(ctxWithOpID, imApiCallerToken)

	if !AccountRegexp.MatchString(req.User.Account) {
		return nil, freeErrors.ApiErr("account does not conform to the regexp rule")
	}

	registerSwitchDao := platformCfgModel.NewRegisterSwitchDao(redisCli)
	openRegister, err := registerSwitchDao.IsOpenRegister(ctx)
	if err != nil {
		return nil, err
	}
	if !openRegister {
		return nil, freeErrors.ApiErr("the register is closed")
	}

	isCorrect := captcha.VerifyImageCaptcha(ctx, redisCli, req.CaptchaId, req.CaptchaAnswer)
	if !isCorrect {
		return nil, freeErrors.CaptchaErr("captcha is incorrect")
	}

	if chatCfg.ApiConfig.EnableDeviceRegisterNum {
		if req.DeviceCode == "" {
			return nil, freeErrors.ParameterInvalidErr
		}
		deviceRegisterNum, err := deviceRegisterNumDao.Get(ctx, req.DeviceCode)
		if err != nil {
			return nil, freeErrors.ApiErr(err.Error())
		}

		if deviceRegisterNum >= 20 {
			return nil, freeErrors.DeviceRegisterNumExceedErr(req.DeviceCode, deviceRegisterNum)
		}
	}

	// 在事务外部检查账户是否已经存在（不区分大小写）
	attributes, err := attributeDao.FindAccountCaseInsensitive(ctx, []string{req.User.Account})
	if err != nil {
		return nil, err
	}
	if len(attributes) > 0 {
		return nil, freeErrors.AccountExistsErr
	}

	if err := checkRegisterClientIPNotFromBannedUser(ctx, plugin.MongoCli().GetDB(), req.Ip); err != nil {
		return nil, err
	}

	orgId := primitive.NilObjectID
	userId := ""
	err = plugin.MongoCli().GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		newUserID, err := utils.NewId()
		if err != nil {
			return err
		}

		credentials := make([]*chatModel.Credential, 0)
		var registerType int32 = constant.AccountRegister
		credentials = append(credentials, &chatModel.Credential{
			UserID:      newUserID,
			Account:     req.User.Account,
			Type:        constant.CredentialAccount,
			AllowChange: true,
		})

		register := &chatModel.Register{
			UserID:      newUserID,
			DeviceID:    req.DeviceID,
			IP:          req.Ip,
			Platform:    pkgConstant.PlatformID2Name[int(req.Platform)],
			AccountType: "",
			Mode:        constant.UserMode,
			CreateTime:  time.Now(),
		}
		account := &chatModel.Account{
			UserID:         newUserID,
			Password:       req.User.Password,
			OperatorUserID: mcontext.GetOpUserID(ctx),
			ChangeTime:     register.CreateTime,
			CreateTime:     register.CreateTime,
		}
		attribute := &chatModel.Attribute{
			UserID:         newUserID,
			Account:        req.User.Account,
			PhoneNumber:    "",
			AreaCode:       "",
			Email:          "",
			Nickname:       "",
			FaceURL:        "",
			Gender:         req.User.Gender,
			BirthTime:      time.UnixMilli(req.User.Birth),
			ChangeTime:     register.CreateTime,
			CreateTime:     register.CreateTime,
			AllowVibration: constant.DefaultAllowVibration,
			AllowBeep:      constant.DefaultAllowBeep,
			AllowAddFriend: constant.DefaultAllowAddFriend,
			RegisterType:   registerType,
		}

		if err := registerDao.Create(sessionCtx, register); err != nil {
			return err
		}
		if err := accountDao.Create(sessionCtx, account); err != nil {
			return err
		}
		if err := attributeDao.Create(sessionCtx, attribute); err != nil {
			return err
		}
		if err := credentialDao.Create(sessionCtx, credentials...); err != nil {
			return err
		}
		organizationSvc := orgSvc.OrganizationSvc{}
		resp, err := organizationSvc.JoinOrgUsingInvitationCodeByAttr(sessionCtx, operationID, newUserID, orgSvc.JoinOrgUsingInvitationCodeReq{
			InvitationCode: req.OrgInvitationCode,
			Nickname:       req.User.Nickname,
			FaceURL:        req.User.FaceURL,
		}, attribute)
		if err != nil {
			return err
		}

		if chatCfg.ApiConfig.EnableDeviceRegisterNum {
			err = deviceRegisterNumDao.Add(sessionCtx, req.DeviceCode)
			if err != nil {
				return err
			}
		}

		orgId = resp.OrgId
		userId = newUserID

		return nil
	})

	if err != nil {
		return nil, errs.Unwrap(err)
	}

	orgUser, err := orgUserDao.GetByUserIdAndOrgId(context.Background(), userId, orgId)
	if err != nil {
		return nil, err
	}

	defFriendSvc := defaultFriendSvc.NewDefaultFriendSvc()
	defFriendSvc.InternalAddDefaultFriend(operationID, orgUser.OrganizationId, orgUser.ImServerUserId)

	defGroupSvc := defaultGroupSvc.NewDefaultGroupSvc()
	defGroupSvc.InternalAddDefaultGroup(operationID, orgUser.OrganizationId, orgUser.ImServerUserId)

	chatToken, err := plugin.AdminClient().CreateToken(ctx, &admin.CreateTokenReq{UserID: orgUser.UserId, UserType: constant.NormalUser})
	if err != nil {
		return nil, err
	}

	imToken, err := imApiCaller.GetUserToken(imApiCallerCtx, orgUser.ImServerUserId, req.Platform)
	if err != nil {
		return nil, err
	}

	err = userLoginRecordDao.Create(context.TODO(), &chatModel.UserLoginRecord{
		UserID:    orgUser.UserId,
		LoginTime: time.Now(),
		IP:        req.Ip,
		DeviceID:  req.DeviceID,
		Platform:  pkgConstant.PlatformID2Name[int(req.Platform)],
	})
	if err != nil {
		return nil, err
	}

	inviteUserID := ""
	if orgUser.InviterType == OrgModel.OrganizationUserInviterTypeOrgUser {
		inviteUserID = orgUser.InviterImServerUserId
	}

	return &RegUserResp{
		UserID:         orgUser.ImServerUserId,
		ChatToken:      chatToken.Token,
		ImToken:        imToken,
		OrganizationId: orgId,
		InviteUserID:   inviteUserID,
	}, nil

}

func (w *UserSvc) UpdateUser(ctx context.Context, operationID, userId string, orgId primitive.ObjectID, req *chatpb.UpdateUserInfoReq) error {
	orgUserDao := OrgModel.NewOrganizationUserDao(plugin.MongoCli().GetDB())
	orgRolePermissionDao := OrgModel.NewOrganizationRolePermissionDao(plugin.MongoCli().GetDB())
	userDao := openImModel.NewUserDao(plugin.MongoCli().GetDB())

	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	imApiCallerToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return err
	}
	imApiCallerCtx := mctx.WithApiToken(ctxWithOpID, imApiCallerToken)

	// 查询组织用户信息和权限(不需要事务)
	orgUser, err := orgUserDao.GetByUserIdAndOrgId(ctx, userId, orgId)
	if err != nil {
		return err
	}

	hasPermission, err := orgRolePermissionDao.ExistPermission(ctx, orgId, orgUser.Role, OrgModel.PermissionCodeModifyNickname)
	if err != nil {
		return err
	}

	imUser, err := userDao.Take(ctx, orgUser.ImServerUserId)
	if err != nil {
		return err
	}

	updateNickname := ""
	if req.Nickname == nil {
		updateNickname = imUser.Nickname
	} else {
		updateNickname = req.Nickname.GetValue()
	}

	// 校验是否有权限修改昵称
	if !hasPermission && updateNickname != imUser.Nickname {
		return freeErrors.ApiErr("no permission")
	}

	if updateNickname != imUser.Nickname {
		if err := orgSvc.ValidateAppUserNickname(ctx, plugin.MongoCli().GetDB(), orgId, updateNickname, orgUser.ImServerUserId); err != nil {
			return err
		}
	}

	updateFaceUrl := ""
	if req.FaceURL == nil {
		updateFaceUrl = imUser.FaceURL
	} else {
		updateFaceUrl = req.FaceURL.GetValue()
	}

	// 更新 IM Server 的 nickname 和 faceURL
	err = imApiCaller.UpdateUserInfo(imApiCallerCtx, orgUser.ImServerUserId, updateNickname, updateFaceUrl)
	if err != nil {
		return err
	}

	// 设置正确的 userID (主账户 ID)
	req.UserID = userId
	// nickname 和 faceURL 已经通过 imApiCaller 更新到 IM Server,这里设为 nil 避免重复更新
	req.Nickname = nil
	req.FaceURL = nil
	req.Email = nil

	// 打印调试日志
	log.ZInfo(ctx, "更新用户扩展信息", "userID", req.UserID, "birth", req.Birth, "gender", req.Gender)

	// 创建包含正确 opUserID 的上下文,用于 RPC 调用
	// RPC 内部会创建自己的事务,所以这里不需要外层事务
	rpcCtx := mctx.WithOpUserID(ctx, userId, constant.NormalUser)

	// 调用 ChatClient 更新扩展字段(birth, gender, phoneNumber 等)
	// UpdateUserInfo 内部有自己的事务处理
	_, err = plugin.ChatClient().UpdateUserInfo(rpcCtx, req)
	if err != nil {
		log.ZError(ctx, "更新用户扩展信息失败", err, "userID", req.UserID)
		return err
	}

	log.ZInfo(ctx, "用户扩展信息更新成功", "userID", req.UserID)

	return nil
}

func (w *UserSvc) FindUserFullInfo(ctx context.Context, req *FindUserFullInfoReq) (*FindUserFullInfoResp, error) {
	// 参数验证：与原方法保持一致
	if len(req.UserIDs) == 0 {
		return &FindUserFullInfoResp{Users: []*UserFullInfo{}}, nil
	}

	// 权限验证：使用与原方法相同的逻辑
	opUserID, userType, err := mctx.Check(ctx)
	if err != nil {
		return nil, freeErrors.ApiErr("permission check failed: " + err.Error())
	}

	// 判断是否为管理员：与原方法逻辑一致
	isAdmin := userType == constant.AdminUser

	// 数据库连接
	db := plugin.MongoCli().GetDB()
	orgUserDao := OrgModel.NewOrganizationUserDao(db)
	userDao := openImModel.NewUserDao(db)

	attributeCache := chatCache.NewAttributeCacheRedis(plugin.RedisCli(), db)

	// 第一步：根据 UserIDs（ImServerUserIds）查询 organization_user 表，获取映射关系
	orgUsers, err := orgUserDao.GetByIMServerUserIds(ctx, req.UserIDs)
	if err != nil {
		return nil, freeErrors.ApiErr("failed to query organization user info: " + err.Error())
	}

	// 如果查询结果为空，直接返回空列表
	if len(orgUsers) == 0 {
		return &FindUserFullInfoResp{Users: []*UserFullInfo{}}, nil
	}

	// 创建映射关系
	imUserToUserIdMap := make(map[string]string)
	imUserToRoleMap := make(map[string]string)
	imUserToInvitationCodeMap := make(map[string]string)
	pointsMap := make(map[string]int64)
	userIDs := make([]string, 0, len(orgUsers))

	for _, orgUser := range orgUsers {
		imUserToUserIdMap[orgUser.ImServerUserId] = orgUser.UserId
		imUserToRoleMap[orgUser.ImServerUserId] = string(orgUser.Role)
		imUserToInvitationCodeMap[orgUser.ImServerUserId] = orgUser.InvitationCode
		// 同时获取积分信息
		if orgUser.Points != 0 {
			pointsMap[orgUser.ImServerUserId] = orgUser.Points
		} else {
			pointsMap[orgUser.ImServerUserId] = 0
		}
		userIDs = append(userIDs, orgUser.UserId)
	}

	// 第二步：根据 UserIDs 查询 attribute 表
	attributes, err := attributeCache.Find(ctx, userIDs)
	if err != nil {
		return nil, freeErrors.ApiErr("failed to query user attributes: " + err.Error())
	}

	// 第三步：根据 ImServerUserIds 查询 user 表，获取昵称和头像
	imUsers, err := userDao.Find(ctx, req.UserIDs)
	if err != nil {
		return nil, freeErrors.ApiErr("failed to query im user info: " + err.Error())
	}

	// 创建映射关系以便快速查找
	userIdToAttrMap := make(map[string]*chatModel.Attribute)
	for _, attr := range attributes {
		userIdToAttrMap[attr.UserID] = attr
	}

	imUserToImUserMap := make(map[string]*openImModel.User)
	for _, imUser := range imUsers {
		imUserToImUserMap[imUser.UserID] = imUser
	}

	// 预分配切片容量
	users := make([]*UserFullInfo, 0, len(req.UserIDs))

	// 按照请求的 ImUserID 顺序返回结果，确保与原始方法逻辑一致
	for _, imUserID := range req.UserIDs {
		// 通过 ImUserID 找到对应的 UserID
		userID, exists := imUserToUserIdMap[imUserID]
		if !exists {
			// 用户不存在时跳过
			continue
		}

		// 通过 UserID 找到对应的属性
		attr, found := userIdToAttrMap[userID]
		if !found {
			// 属性不存在时跳过
			continue
		}

		// 获取 IM 用户信息
		imUser := imUserToImUserMap[imUserID]

		// 敏感信息控制：与原方法保持一致的权限逻辑
		showSensitiveInfo := isAdmin || attr.UserID == opUserID

		// 获取积分信息
		points := pointsMap[imUserID]

		// 构建用户信息，字段映射与原方法保持一致
		userInfo := &UserFullInfo{
			UserID:           imUserID,    // 使用 ImServerUserId 作为返回的 UserID
			UserChatID:       attr.UserID, // 原始的 UserID 作为 UserChatID
			Password:         "",          // 密码永远不返回
			Account:          attr.Account,
			Gender:           attr.Gender,
			Level:            attr.Level,
			AllowAddFriend:   attr.AllowAddFriend,
			AllowBeep:        attr.AllowBeep,
			AllowVibration:   attr.AllowVibration,
			GlobalRecvMsgOpt: attr.GlobalRecvMsgOpt,
			RegisterType:     attr.RegisterType,
			OrgRole:          imUserToRoleMap[imUserID],           // 组织角色
			InvitationCode:   imUserToInvitationCodeMap[imUserID], // 邀请码
		}

		// 如果有 IM 用户信息，使用 IM 用户的昵称和头像
		if imUser != nil {
			userInfo.Nickname = imUser.Nickname // 使用 IM user 表的昵称
			userInfo.FaceURL = imUser.FaceURL   // 使用 IM user 表的头像
			userInfo.CanSendFreeMsg = imUser.CanSendFreeMsg
		} else {
			// 如果没有 IM 用户信息，使用 attribute 表的信息
			userInfo.Nickname = attr.Nickname
			userInfo.FaceURL = attr.FaceURL
		}

		// 敏感信息只有管理员或用户本人才能看到
		if showSensitiveInfo {
			userInfo.Birth = attr.BirthTime.UnixMilli()
			userInfo.Email = attr.Email
			userInfo.PhoneNumber = attr.PhoneNumber
			userInfo.AreaCode = attr.AreaCode
			userInfo.Points = points
		}

		users = append(users, userInfo)
	}

	return &FindUserFullInfoResp{Users: users}, nil
}

// SearchUserFullInfo 搜索用户完整信息（头像昵称从 IM user 表获取）
func (w *UserSvc) SearchUserFullInfo(ctx context.Context, req *SearchUserFullInfoReq) (*SearchUserFullInfoResp, error) {
	// 获取当前用户信息和权限
	opUserID, userType, err := mctx.Check(ctx)
	if err != nil {
		return nil, err
	}
	isAdmin := userType == constant.AdminUser

	// 从请求头中获取org-id
	orgIdStr, err := mctx.GetOrgId(ctx)
	if err != nil {
		return nil, err
	}
	orgId, err := primitive.ObjectIDFromHex(orgIdStr)
	if err != nil {
		return nil, err
	}

	// 数据库连接
	db := plugin.MongoCli().GetDB()
	attributeDao := chatModel.NewAttributeDao(db)
	attributeCache := chatCache.NewAttributeCacheRedis(plugin.RedisCli(), db)
	orgUsersDao := OrgModel.NewOrganizationUserDao(db)
	userDao := openImModel.NewUserDao(db)

	// 如果keyword为空，使用分页查询返回用户列表
	if req.Keyword == "" {
		return w.searchUserList(ctx, req, orgId, isAdmin, opUserID, attributeDao, userDao)
	}

	// 获取被禁用用户ID列表（数据层会根据normal参数决定是否使用）
	forbiddenAccountDao := chatModel.NewForbiddenAccountDao(db)
	forbiddenIDs, err := forbiddenAccountDao.FindAllIDs(ctx)
	if err != nil {
		return nil, freeErrors.ApiErr("failed to get forbidden user IDs: " + err.Error())
	}

	// 获取超管封禁用户列表
	superAdminForbiddenDao := adminModel.NewSuperAdminForbiddenDao(db)
	forbiddenUserIDs, err := superAdminForbiddenDao.GetAllForbiddenUserIDs(ctx)
	if err != nil {
		return nil, freeErrors.SystemErr(fmt.Errorf("failed to get forbidden user IDs: %v", err))
	}

	result := &SearchUserFullInfoResp{
		Total: 0,
		Users: nil,
	}

	user, err := userDao.Take(ctx, req.Keyword)
	if err != nil && !dbutil.IsDBNotFound(err) {
		return nil, err
	}
	if user != nil {
		// app端
		orgUser, err := orgUsersDao.GetByUserIMServerUserId(ctx, user.UserID)
		if err != nil {
			return nil, err
		}
		if orgUser.OrganizationId != orgId {
			return result, nil
		}

		if slices.Contains(forbiddenUserIDs, orgUser.UserId) {
			return result, nil
		}

		if slices.Contains(forbiddenIDs, orgUser.ImServerUserId) {
			return result, nil
		}

		attribute, err := attributeCache.Take(ctx, orgUser.UserId)
		if err != nil && !dbutil.IsDBNotFound(err) {
			return nil, err
		}

		userFullInfo := &UserFullInfo{
			FaceURL:        user.FaceURL,
			Nickname:       user.Nickname,
			OrgRole:        string(orgUser.Role),
			InvitationCode: orgUser.InvitationCode,
			UserID:         orgUser.ImServerUserId,
			Password:       "", // 密码永远不返回

		}

		if attribute != nil {
			userFullInfo.UserChatID = attribute.UserID
			userFullInfo.Account = attribute.Account
			userFullInfo.Gender = attribute.Gender
			userFullInfo.Level = attribute.Level
			userFullInfo.AllowAddFriend = attribute.AllowAddFriend
			userFullInfo.AllowBeep = attribute.AllowBeep
			userFullInfo.AllowVibration = attribute.AllowVibration
			userFullInfo.GlobalRecvMsgOpt = attribute.GlobalRecvMsgOpt
			userFullInfo.RegisterType = attribute.RegisterType
			userFullInfo.Password = ""
			userFullInfo.IsRealNameVerified = attribute.IsRealNameVerified
			userFullInfo.RealName = attribute.RealName
		}

		if !isAdmin && userFullInfo.UserID != opUserID {
			userFullInfo.PhoneNumber = ""
			userFullInfo.Email = ""
			userFullInfo.AreaCode = ""
		}

		result.Users = []*UserFullInfo{userFullInfo}
		result.Total = 1
		return result, nil
	} else {
		// web端
		genders := make([]int32, 0)
		if req.Genders != 0 {
			genders = []int32{req.Genders}
		}

		attribute, err := attributeDao.TakeByKeyword(ctx, req.Keyword, genders, nil)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return &SearchUserFullInfoResp{
					Total: 0,
					Users: nil,
				}, nil
			}
			return nil, err
		}

		// 转换为响应格式
		userFullInfo := &UserFullInfo{
			UserChatID:         attribute.UserID, // 原始的 UserID 作为 UserChatID
			Password:           "",               // 密码永远不返回
			Account:            attribute.Account,
			Gender:             attribute.Gender,
			Level:              attribute.Level,
			AllowAddFriend:     attribute.AllowAddFriend,
			AllowBeep:          attribute.AllowBeep,
			AllowVibration:     attribute.AllowVibration,
			GlobalRecvMsgOpt:   attribute.GlobalRecvMsgOpt,
			IsRealNameVerified: attribute.IsRealNameVerified,
			RealName:           attribute.RealName,
		}

		orgUser, err := orgUsersDao.GetByUserId(ctx, attribute.UserID)
		if err != nil {
			return nil, err
		}
		if orgUser.OrganizationId != orgId {
			return result, nil
		}
		if slices.Contains(forbiddenUserIDs, orgUser.UserId) {
			return result, nil
		}
		if slices.Contains(forbiddenIDs, orgUser.ImServerUserId) {
			return result, nil
		}

		userFullInfo.OrgRole = string(orgUser.Role)
		userFullInfo.InvitationCode = orgUser.InvitationCode
		userFullInfo.UserID = orgUser.ImServerUserId

		user, err = userDao.Take(ctx, orgUser.ImServerUserId)
		if err != nil && !dbutil.IsDBNotFound(err) {
			return nil, err
		}
		userFullInfo.Nickname = user.Nickname
		userFullInfo.FaceURL = user.FaceURL

		// 权限控制：只有管理员或用户本人能看到敏感信息
		if !isAdmin && userFullInfo.UserID != opUserID {
			userFullInfo.PhoneNumber = ""
			userFullInfo.Email = ""
			userFullInfo.AreaCode = ""
		}

		return &SearchUserFullInfoResp{
			Total: 1,
			Users: []*UserFullInfo{userFullInfo},
		}, nil
	}
}

// searchUserList 分页查询用户列表（keyword为空时使用）
func (w *UserSvc) searchUserList(ctx context.Context, req *SearchUserFullInfoReq, orgId primitive.ObjectID, isAdmin bool, opUserID string, attributeDao *chatModel.AttributeDao, userDao *openImModel.UserDao) (*SearchUserFullInfoResp, error) {
	// 构建性别过滤条件
	genders := make([]int32, 0)
	if req.Genders != 0 {
		genders = []int32{req.Genders}
	}

	// 使用SearchNormalUser方法分页查询
	total, attributesWithOrg, err := attributeDao.SearchNormalUser(ctx, req.Keyword, genders, req.Pagination, orgId)
	if err != nil {
		return nil, err
	}

	// 构建响应
	users := make([]*UserFullInfo, 0, len(attributesWithOrg))
	for _, attr := range attributesWithOrg {
		userFullInfo := &UserFullInfo{
			UserChatID:         attr.UserID,
			Account:            attr.Account,
			PhoneNumber:        attr.PhoneNumber,
			AreaCode:           attr.AreaCode,
			Email:              attr.Email,
			Gender:             attr.Gender,
			Level:              attr.Level,
			AllowAddFriend:     attr.AllowAddFriend,
			AllowBeep:          attr.AllowBeep,
			AllowVibration:     attr.AllowVibration,
			GlobalRecvMsgOpt:   attr.GlobalRecvMsgOpt,
			RegisterType:       attr.RegisterType,
			UserID:             attr.ImServerUserId,
			InvitationCode:     attr.InvitationCode,
			Password:           "", // 密码永远不返回
			IsRealNameVerified: attr.IsRealNameVerified,
			RealName:           attr.RealName,
		}

		// 从IM user表获取头像和昵称
		user, err := userDao.Take(ctx, attr.ImServerUserId)
		if err == nil {
			userFullInfo.Nickname = user.Nickname
			userFullInfo.FaceURL = user.FaceURL
		}

		// 权限控制：只有管理员或用户本人能看到敏感信息
		if !isAdmin && userFullInfo.UserID != opUserID {
			userFullInfo.PhoneNumber = ""
			userFullInfo.Email = ""
			userFullInfo.AreaCode = ""
		}

		users = append(users, userFullInfo)
	}

	return &SearchUserFullInfoResp{
		Total: uint32(total),
		Users: users,
	}, nil
}

// ChangeEmail 修改用户邮箱
func (w *UserSvc) ChangeEmail(ctx context.Context, userID, newEmail, verifyCode string) error {
	// 1. 验证验证码
	_, err := plugin.ChatClient().VerifyCode(ctx, &chatpb.VerifyCodeReq{
		VerifyCode:        verifyCode,
		Email:             newEmail,
		UsedFor:           constant.VerificationCodeForResetEmail,
		DeleteAfterVerify: true,
	})
	if err != nil {
		log.ZError(ctx, "邮箱验证码验证失败", err, "userID", userID, "newEmail", newEmail)
		return freeErrors.VerifyCodeNotMatchErr
	}

	// 2. 创建数据库连接
	db := plugin.MongoCli().GetDB()
	attributeModel := chatModel.NewAttributeDao(db)
	credentialModel := chatModel.NewCredentialDao(db)

	// 3. 检查新邮箱是否已被其他用户使用
	attributes, err := attributeModel.FindEmail(ctx, []string{newEmail})
	if err != nil {
		log.ZError(ctx, "查询邮箱是否存在失败", err, "newEmail", newEmail)
		return freeErrors.SystemErr(err)
	}
	if len(attributes) > 0 {
		// 检查是否是当前用户的邮箱
		for _, attr := range attributes {
			if attr.UserID != userID {
				log.ZWarn(ctx, "邮箱已被其他用户使用", nil, "newEmail", newEmail, "existingUserID", attr.UserID)
				return freeErrors.EmailInUseErr
			}
		}
	}

	// 4. 在事务中更新用户邮箱信息
	return plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		// 4.1 更新attribute表中的邮箱信息
		updateData := map[string]any{
			"email": newEmail,
		}

		err := attributeModel.Update(sessionCtx, userID, updateData)
		if err != nil {
			log.ZError(sessionCtx, "更新用户邮箱失败", err, "userID", userID, "newEmail", newEmail)
			return freeErrors.SystemErr(err)
		}

		// 4.2 处理credential表
		// 查找用户是否已有邮箱类型的credential
		existingCredential, err := credentialModel.GetByUserIdAndType(sessionCtx, userID, constant.CredentialEmail)

		if err == nil && existingCredential != nil {
			// 如果存在邮箱credential，更新account字段
			err = credentialModel.UpdateAccount(sessionCtx, userID, constant.CredentialEmail, newEmail)
			if err != nil {
				log.ZError(sessionCtx, "更新邮箱凭证失败", err, "userID", userID, "newEmail", newEmail)
				return freeErrors.SystemErr(err)
			}
		} else {
			// 如果不存在邮箱credential或查询出错，创建新的
			newCredential := &chatModel.Credential{
				UserID:      userID,
				Account:     newEmail,
				Type:        constant.CredentialEmail,
				AllowChange: true,
			}

			err = credentialModel.Create(sessionCtx, newCredential)
			if err != nil {
				log.ZError(sessionCtx, "创建邮箱凭证失败", err, "userID", userID, "newEmail", newEmail)
				return freeErrors.SystemErr(err)
			}
		}

		log.ZInfo(sessionCtx, "用户邮箱修改成功", "userID", userID, "newEmail", newEmail)
		return nil
	})
}

// BlackUser 拉黑用户
func (w *UserSvc) BlackUser(ctx context.Context, req *admin.BlockUserReq, orgID primitive.ObjectID) error {
	mongoCli := plugin.MongoCli()
	organizationUserDao := OrgModel.NewOrganizationUserDao(mongoCli.GetDB())
	orgUser, err := organizationUserDao.GetByUserIMServerUserId(ctx, req.UserID)
	if err != nil || orgUser == nil || orgUser.OrganizationId.Hex() != orgID.Hex() {
		return freeErrors.SystemErr(fmt.Errorf("user not in organization"))
	}

	adminClient := plugin.AdminClient()
	_, err = adminClient.BlockUser(ctx, req)
	if err != nil {
		return freeErrors.SystemErr(err)
	}
	apiCaller := plugin.ImApiCaller()
	imToken, err := apiCaller.ImAdminTokenWithDefaultAdmin(ctx)
	if err != nil {
		return freeErrors.SystemErr(err)
	}
	err = apiCaller.ForceOffLine(mctx.WithApiToken(ctx, imToken), req.UserID)
	if err != nil {
		return freeErrors.SystemErr(err)
	}
	if syncErr := syncForbiddenRegisterIPsOnBlock(ctx, mongoCli.GetDB(), orgUser.UserId, req.UserID); syncErr != nil {
		log.ZWarn(ctx, "sync forbidden_user_register_ip on block failed", syncErr, "im_server_user_id", req.UserID, "main_user_id", orgUser.UserId)
	}
	return nil
}

// UnblockUser 解禁用户
func (w *UserSvc) UnblockUser(ctx context.Context, req *admin.UnblockUserReq, orgID primitive.ObjectID) error {
	mongoCli := plugin.MongoCli()
	organizationUserDao := OrgModel.NewOrganizationUserDao(mongoCli.GetDB())
	for _, userID := range req.UserIDs {
		orgUser, err := organizationUserDao.GetByUserIMServerUserId(ctx, userID)
		if err != nil || orgUser == nil || orgUser.OrganizationId.Hex() != orgID.Hex() {
			return freeErrors.SystemErr(fmt.Errorf("user not in organization"))
		}
	}

	adminClient := plugin.AdminClient()
	_, err := adminClient.UnblockUser(ctx, req)
	if err != nil {
		return freeErrors.SystemErr(err)
	}
	deleteForbiddenRegisterIPsOnUnblock(ctx, mongoCli.GetDB(), req.UserIDs)
	return nil
}

// SearchBlockUser 查询封禁用户
func (w *UserSvc) SearchBlockUser(ctx context.Context, req *admin.SearchBlockUserReq, orgID primitive.ObjectID) (*BlockUserResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	// 使用DAO层的聚合查询方法
	forbiddenAccountDao := chatModel.NewForbiddenAccountDao(db)
	total, results, err := forbiddenAccountDao.SearchBlockUsersByOrg(ctx, orgID, req.Keyword, req.Pagination.PageNumber, req.Pagination.ShowNumber)
	if err != nil {
		return nil, freeErrors.SystemErr(fmt.Errorf("failed to search block users by org: %v", err))
	}
	return &BlockUserResp{
		Total: uint32(total),
		Users: results,
	}, nil
}

// CheckAccountExists 检查账户是否已存在（不区分大小写）
func (w *UserSvc) CheckAccountExists(ctx context.Context, account string) (bool, error) {
	// 验证账户格式
	if account == "" || !AccountRegexp.MatchString(account) {
		return false, freeErrors.ApiErr("Invalid account format.")
	}

	attributeDao := chatModel.NewAttributeDao(plugin.MongoCli().GetDB())

	// 检查账户是否存在（不区分大小写）
	return attributeDao.CheckAccountExists(ctx, account)
}

// ========================= Super Admin ==================================

// SuperAdminGetAllUsers 超级管理员查询系统所有用户
func (w *UserSvc) SuperAdminGetAllUsers(ctx context.Context, req *dto.SuperAdminGetAllUsersReq) (*paginationUtils.ListResp[*chatModel.Attribute], error) {
	db := plugin.MongoCli().GetDB()
	attributeDao := chatModel.NewAttributeDao(db)

	// 构建分页参数
	page := &paginationUtils.DepPagination{
		Page:     req.Page,
		PageSize: req.PageSize,
	}

	// 获取超管封禁用户列表
	superAdminForbiddenDao := adminModel.NewSuperAdminForbiddenDao(db)
	forbiddenUserIDs, err := superAdminForbiddenDao.GetAllForbiddenUserIDs(ctx)
	if err != nil {
		return nil, freeErrors.SystemErr(fmt.Errorf("failed to get forbidden user IDs: %v", err))
	}

	// 构建搜索条件，在数据库层面排除被封禁的用户
	total, attributes, err := attributeDao.SearchUser(ctx, req.Keyword, nil, nil, forbiddenUserIDs, page)
	if err != nil {
		return nil, err
	}

	return &paginationUtils.ListResp[*chatModel.Attribute]{
		Total: total,
		List:  attributes,
	}, nil
}

// SuperAdminGetUserDetail 超级管理员查询用户详情
func (w *UserSvc) SuperAdminGetUserDetail(ctx context.Context, req *dto.SuperAdminGetUserDetailReq) ([]interface{}, error) {
	db := plugin.MongoCli().GetDB()
	attributeDao := chatModel.NewAttributeDao(db)

	// 调用DAO层的聚合查询方法
	return attributeDao.GetUserDetailWithOrganizations(ctx, req.UserID)
}

// SuperAdminResetUserPassword 超级管理员重置用户密码
func (w *UserSvc) SuperAdminResetUserPassword(ctx context.Context, req *dto.SuperAdminResetUserPasswordReq) error {
	if req.NewPassword == "" {
		return freeErrors.ParameterInvalidErr
	}

	// 获取数据库连接和DAO
	db := plugin.MongoCli().GetDB()
	orgUserDao := OrgModel.NewOrganizationUserDao(db)
	accountDao := chatModel.NewAccountDao(db)

	// 通过userID查询用户信息
	user, err := accountDao.GetByUserId(ctx, req.UserID)
	if err != nil {
		return freeErrors.SystemErr(fmt.Errorf("failed to get user: %v", err))
	}

	// 检查新密码是否与当前密码相同
	if user.Password == req.NewPassword {
		return freeErrors.ApiErr("new password is the same as current password")
	}

	// 直接更新用户密码 - 使用AccountDao
	if err := accountDao.UpdatePassword(ctx, req.UserID, req.NewPassword); err != nil {
		return freeErrors.SystemErr(fmt.Errorf("failed to update password: %v", err))
	}

	// 获取IM管理员token
	imApiCaller := plugin.ImApiCaller()
	imToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctx)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err)
		return freeErrors.SystemErr(fmt.Errorf("failed to get admin token: %v", err))
	}
	apiCtx := mctx.WithApiToken(ctx, imToken)
	// 先使token失效 - 使用AdminClient的InvalidateToken
	adminClient := plugin.AdminClient()
	_, err = adminClient.InvalidateToken(apiCtx, &admin.InvalidateTokenReq{UserID: req.UserID})
	if err != nil {
		log.ZError(ctx, "token失效失败", err, "UserId", req.UserID)
	}

	// 查询用户的所有组织记录，获取所有 ImServerUserId
	orgUsers, err := orgUserDao.Select(ctx, req.UserID, primitive.NilObjectID, nil)
	if err != nil {
		return freeErrors.SystemErr(fmt.Errorf("failed to get org users: %v", err))
	}
	// 强制用户下线
	if len(orgUsers) > 0 {
		for _, orgUser := range orgUsers {
			imApiCaller.ForceOffLine(apiCtx, orgUser.ImServerUserId)
		}
	}
	return nil
}

// GetLoginRecordByImServerId 根据IMServerID查询用户登录记录
func (w *UserSvc) GetLoginRecord(ctx context.Context, req *dto.GetLoginRecordReq, opUserID string, orgId primitive.ObjectID) (*dto.GetLoginRecordResp, error) {
	orgUserDao := OrgModel.NewOrganizationUserDao(plugin.MongoCli().GetDB())
	orgUser, err := orgUserDao.GetByUserIdAndOrgId(context.TODO(), opUserID, orgId)
	if err != nil {
		return nil, freeErrors.SystemErr(fmt.Errorf("failed to get org user"))
	}

	orgRolePermissionDao := OrgModel.NewOrganizationRolePermissionDao(plugin.MongoCli().GetDB())
	hasPermission, err := orgRolePermissionDao.ExistPermission(context.TODO(), orgId, orgUser.Role, OrgModel.PermissionCodeLoginRecord)
	if !hasPermission || err != nil {
		return nil, freeErrors.ApiErr("no permission")
	}

	//根据IMServerID查询组织用户，获取UserID
	orgUser, err = orgUserDao.GetByUserIMServerUserId(ctx, req.UserID)
	if err != nil {
		return nil, freeErrors.NotFoundErrWithResource("userID " + req.UserID)
	}

	loginRecordCache := chatCache.NewLoginRecordCacheRedis(plugin.RedisCli(), plugin.MongoCli().GetDB())
	cacheRecord, err := loginRecordCache.GetByUserId(ctx, orgUser.UserId)
	if err != nil {
		return nil, freeErrors.NotFoundErrWithResource("userID " + req.UserID)
	}

	resp := &dto.GetLoginRecordResp{
		UserID:    cacheRecord.UserID,
		LoginTime: cacheRecord.LoginTime,
		IP:        cacheRecord.IP,
		DeviceID:  cacheRecord.DeviceID,
		Platform:  cacheRecord.Platform,
		Region:    cacheRecord.Region,
	}

	return resp, nil
}
