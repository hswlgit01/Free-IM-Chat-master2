package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	orgCache "github.com/openimsdk/chat/freechat/apps/organization/cache"

	"github.com/openimsdk/chat/freechat/apps/account/verifyCode"

	OrgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	orgSvc "github.com/openimsdk/chat/freechat/apps/organization/svc"
	"github.com/openimsdk/chat/freechat/plugin"
	chatCache "github.com/openimsdk/chat/freechat/third/chat/cache"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/constant"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/chat/pkg/common/imapi"
	"github.com/openimsdk/chat/pkg/common/mctx"
	pkgConstant "github.com/openimsdk/chat/pkg/constant"
	"github.com/openimsdk/chat/pkg/protocol/admin"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mcontext"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type OrgAccountSvc struct{}

func NewOrgAccountSvc() *OrgAccountSvc {
	return &OrgAccountSvc{}
}

// OrgLoginRequest 管理员登录请求参数
type OrgLoginRequest struct {
	//Email       string `json:"email"`
	//PhoneNumber string `json:"phone_number"`
	//VerifyCode  string `protobuf:"bytes,3,opt,name=verifyCode,proto3" json:"verifyCode"`
	Account string `json:"account"  binding:"required"`
	//Ip          string `protobuf:"bytes,8,opt,name=ip,proto3" json:"ip"`
	Password string `json:"password"`
	//DeviceID    string `json:"device_id"`
	Platform int32 `json:"platform"`
	//AreaCode    string `json:"area_code"`
}

// OrgLoginResponse 管理员登录响应
type OrgLoginResponse struct {
	UserID       string                 `json:"user_id"`
	ImUserID     string                 `json:"im_user_id"`
	AdminToken   string                 `json:"admin_token"`
	ImToken      string                 `json:"im_token"`
	Organization *OrgModel.Organization `json:"organization"`
}

// Login 管理员登录
func (a *OrgAccountSvc) Login(ctx context.Context, operationID string, req OrgLoginRequest) (*OrgLoginResponse, error) {
	mongoCli := plugin.MongoCli()
	attributeDao := chatModel.NewAttributeDao(mongoCli.GetDB())
	accountDao := chatModel.NewAccountDao(mongoCli.GetDB())
	orgUserDao := OrgModel.NewOrganizationUserDao(mongoCli.GetDB())

	attr, err := attributeDao.TakeAccount(context.Background(), req.Account)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource("account" + req.Account)
		}
		return nil, err
	}

	account, err := accountDao.GetByUserId(context.Background(), attr.UserID)
	if err != nil {
		return nil, err
	}

	if account.Password != req.Password {
		return nil, freeErrors.ApiErr("the passwords are inconsistent")
	}

	orgUser, err := orgUserDao.GetByUserId(context.Background(), attr.UserID)
	if err != nil {
		return nil, err
	}

	if orgUser.Status == OrgModel.OrganizationUserDisableStatus {
		return nil, freeErrors.ApiErr("the account is not activated")
	}

	allowRole := []OrgModel.OrganizationUserRole{OrgModel.OrganizationUserBackendAdminRole, OrgModel.OrganizationUserSuperAdminRole}
	if !slices.Contains(allowRole, orgUser.Role) {
		return nil, freeErrors.ApiErr("the account is not an admin or super admin")
	}

	if _, err := plugin.AdminClient().InvalidateToken(ctx, &admin.InvalidateTokenReq{UserID: orgUser.UserId}); err != nil {
		return nil, err
	}

	// 获取chat token
	chatToken, err := plugin.AdminClient().CreateToken(ctx, &admin.CreateTokenReq{UserID: orgUser.UserId, UserType: constant.NormalUser})
	if err != nil {
		return nil, err
	}

	org, err := orgCache.NewOrgCacheRedis(plugin.RedisCli(), mongoCli.GetDB()).GetByIdAndStatus(ctx, orgUser.OrganizationId, OrgModel.OrganizationStatusPass)
	if err != nil && !dbutil.IsDBNotFound(err) {
		return nil, errs.NewCodeError(freeErrors.ErrSystem, "failed to query org by email")
	}

	// 获取im token
	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return nil, err
	}

	imToken, err := imApiCaller.GetUserToken(mctx.WithApiToken(ctxWithOpID, adminToken), orgUser.ImServerUserId, req.Platform)
	if err != nil {
		return nil, err
	}

	return &OrgLoginResponse{
		UserID:       attr.UserID,
		AdminToken:   chatToken.Token,
		Organization: org,
		ImToken:      imToken,
		ImUserID:     orgUser.ImServerUserId,
	}, nil
}

// TwoFactorAuthSendVerifyCode 管理员二次验证获取邮箱验证码
func (a *OrgAccountSvc) TwoFactorAuthSendVerifyCode(ctx context.Context, req OrgLoginRequest) error {
	mongoCli := plugin.MongoCli()
	attributeDao := chatModel.NewAttributeDao(mongoCli.GetDB())
	accountDao := chatModel.NewAccountDao(mongoCli.GetDB())
	orgUserDao := OrgModel.NewOrganizationUserDao(mongoCli.GetDB())

	attr, err := attributeDao.TakeAccount(context.Background(), req.Account)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return freeErrors.NotFoundErrWithResource("account" + req.Account)
		}
		return err
	}

	if attr.Email == "" {
		return freeErrors.ApiErr("the account does not have an email")
	}

	account, err := accountDao.GetByUserId(context.Background(), attr.UserID)
	if err != nil {
		return err
	}

	if account.Password != req.Password {
		return freeErrors.ApiErr("the passwords are inconsistent")
	}

	orgUser, err := orgUserDao.GetByUserId(context.Background(), attr.UserID)
	if err != nil {
		return err
	}

	if orgUser.Status == OrgModel.OrganizationUserDisableStatus {
		return freeErrors.ApiErr("the account is not activated")
	}

	allowRole := []OrgModel.OrganizationUserRole{OrgModel.OrganizationUserBackendAdminRole, OrgModel.OrganizationUserSuperAdminRole}
	if !slices.Contains(allowRole, orgUser.Role) {
		return freeErrors.ApiErr("the account is not an admin or super admin")
	}

	chatCfg := plugin.ChatCfg()
	verifyCodeCfg := chatCfg.ChatRpcConfig.VerifyCode

	err = verifyCode.SendEmailVerifyCode(ctx, mongoCli.GetDB(), plugin.Mail(), &verifyCode.SendVerifyCodeReq{
		Email:         attr.Email,
		UsedFor:       constant.VerificationCodeForLogin,
		VerifyCodeCfg: verifyCodeCfg,
		Platform:      req.Platform,
	})

	//_, err = plugin.ChatClient().SendVerifyCode(ctx, &chat.SendVerifyCodeReq{
	//	Email:   attr.Email,
	//	UsedFor: constant.VerificationCodeForLogin,
	//})

	return err
}

type TwoFactorAuthLoginViaEmailReq struct {
	VerifyCode string `json:"verify_code" binding:"required"`
	Account    string `json:"account"  binding:"required"`
	Password   string `json:"password"`
	Platform   int32  `json:"platform"`
}

// TwoFactorAuthLoginViaEmail 管理员二次验证通过邮箱验证码登录
func (a *OrgAccountSvc) TwoFactorAuthLoginViaEmail(ctx context.Context, operationID string, req TwoFactorAuthLoginViaEmailReq) (*OrgLoginResponse, error) {
	mongoCli := plugin.MongoCli()
	attributeDao := chatModel.NewAttributeDao(mongoCli.GetDB())
	accountDao := chatModel.NewAccountDao(mongoCli.GetDB())
	orgUserDao := OrgModel.NewOrganizationUserDao(mongoCli.GetDB())
	organizationDao := OrgModel.NewOrganizationDao(mongoCli.GetDB())

	attr, err := attributeDao.TakeAccount(context.Background(), req.Account)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource("account" + req.Account)
		}
		return nil, err
	}

	account, err := accountDao.GetByUserId(context.Background(), attr.UserID)
	if err != nil {
		return nil, err
	}

	if account.Password != req.Password {
		return nil, freeErrors.ApiErr("the passwords are inconsistent")
	}

	orgUser, err := orgUserDao.GetByUserId(context.Background(), attr.UserID)
	if err != nil {
		return nil, err
	}

	if orgUser.Status == OrgModel.OrganizationUserDisableStatus {
		return nil, freeErrors.ApiErr("the account is not activated")
	}

	allowRole := []OrgModel.OrganizationUserRole{OrgModel.OrganizationUserBackendAdminRole, OrgModel.OrganizationUserSuperAdminRole}
	if !slices.Contains(allowRole, orgUser.Role) {
		return nil, freeErrors.ApiErr("the account is not an admin or super admin")
	}

	chatCfg := plugin.ChatCfg()
	verifyCodeCfg := chatCfg.ChatRpcConfig.VerifyCode

	_, err = verifyCode.VerifyEmailCode(ctx, mongoCli.GetDB(), &verifyCode.VerifyEmailCodeReq{
		Email:             attr.Email,
		UsedFor:           constant.VerificationCodeForLogin,
		VerifyCode:        req.VerifyCode,
		DeleteAfterVerify: true,
		VerifyCodeCfg:     verifyCodeCfg,
	})
	if err != nil {
		return nil, err
	}

	//_, err = plugin.ChatClient().VerifyCode(ctx, &chat.VerifyCodeReq{
	//	AreaCode:          "",
	//	PhoneNumber:       "",
	//	VerifyCode:        req.VerifyCode,
	//	Email:             attr.Email,
	//	UsedFor:           constant.VerificationCodeForLogin,
	//	DeleteAfterVerify: true,
	//})
	//if err != nil {
	//	return nil, err
	//}

	// 获取chat token
	chatToken, err := plugin.AdminClient().CreateToken(ctx, &admin.CreateTokenReq{UserID: orgUser.UserId, UserType: constant.NormalUser})
	if err != nil {
		return nil, err
	}

	org, err := organizationDao.GetByIdAndStatus(ctx, orgUser.OrganizationId, OrgModel.OrganizationStatusPass)
	if err != nil && !dbutil.IsDBNotFound(err) {
		return nil, errs.NewCodeError(freeErrors.ErrSystem, "failed to query org by email")
	}

	// 获取im token
	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return nil, err
	}

	imToken, err := imApiCaller.GetUserToken(mctx.WithApiToken(ctxWithOpID, adminToken), orgUser.ImServerUserId, req.Platform)
	if err != nil {
		return nil, err
	}

	return &OrgLoginResponse{
		UserID:       attr.UserID,
		AdminToken:   chatToken.Token,
		Organization: org,
		ImToken:      imToken,
		ImUserID:     orgUser.ImServerUserId,
	}, nil
}

type AccountSvc struct{}

func NewAccountSvc() *AccountSvc {
	return &AccountSvc{}
}

// SecretEmbedLoginRequest 嵌入式登录请求参数
type SecretEmbedLoginRequest struct {
	OrganizationId primitive.ObjectID `json:"app_id" binding:"required"`
	Secret         string             `json:"secret" binding:"required"`
}

func (l SecretEmbedLoginRequest) DeSecret(aesKeyBase64 string) (*EmbedLoginRequest, error) {
	secretJsonStr, err := utils.AESDecrypt(l.Secret, aesKeyBase64)
	if err != nil {
		return nil, err
	}

	data := &EmbedLoginRequest{}
	err = json.Unmarshal(secretJsonStr, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

type SecretEmbedLoginResponse struct {
	Secret string `json:"secret"`
}

// EmbedLoginResponse 普通用户登录响应
type EmbedLoginResponse struct {
	UserID         string             `json:"user_id"`
	ChatToken      string             `json:"chat_token"`
	ImToken        string             `json:"im_token"`
	OrganizationId primitive.ObjectID `json:"organization_id" binding:"required"`
}

func (a *EmbedLoginResponse) Secret(aesKeyBase64 string) (*SecretEmbedLoginResponse, error) {
	jsonStr, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}

	secretJsonStr, err := utils.AESEncrypt(jsonStr, aesKeyBase64)
	if err != nil {
		return nil, err
	}

	return &SecretEmbedLoginResponse{
		Secret: secretJsonStr,
	}, nil
}

type EmbedLoginRequest struct {
	Ip          string `json:"ip,omitempty"`
	DeviceID    string `json:"deviceID"`
	Platform    int32  `json:"platform"`
	ThirdUserId string `bson:"third_user_id" json:"third_user_id"`

	//User *chat.RegisterUserInfo `protobuf:"bytes,7,opt,name=user,proto3" json:"user"`

	User struct {
		//UserID      string `json:"userID,omitempty" binding:"required"`
		Nickname    string `json:"nickname" binding:"required"`
		FaceURL     string `json:"faceURL"`
		Birth       int64  `json:"birth"`
		Gender      int32  `json:"gender"`
		AreaCode    string `json:"areaCode"`
		PhoneNumber string `json:"phoneNumber"`
		Email       string `json:"email"`
		Account     string `json:"account"`
		Password    string `json:"password"`
	} ` json:"user" binding:"required"`
}

// EmbedLogin 嵌入式用户登录
func (a *AccountSvc) EmbedLogin(ctx context.Context, remoteAddr, operationID string, secretReq SecretEmbedLoginRequest) (*SecretEmbedLoginResponse, error) {
	orgUserDao := OrgModel.NewOrganizationUserDao(plugin.MongoCli().GetDB())
	attributeDao := chatModel.NewAttributeDao(plugin.MongoCli().GetDB())
	accountDao := chatModel.NewAccountDao(plugin.MongoCli().GetDB())
	registerDao := chatModel.NewRegisterDao(plugin.MongoCli().GetDB())
	credentialDao := chatModel.NewCredentialDao(plugin.MongoCli().GetDB())
	userDao := openImModel.NewUserDao(plugin.MongoCli().GetDB())
	userLoginRecordDao := chatModel.NewUserLoginRecordDao(plugin.MongoCli().GetDB())
	forbiddenAccountDao := chatModel.NewForbiddenAccountDao(plugin.MongoCli().GetDB())

	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	imApiCallerToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return nil, err
	}
	imApiCallerCtx := mctx.WithApiToken(ctxWithOpID, imApiCallerToken)

	org, err := orgCache.NewOrgCacheRedis(plugin.RedisCli(), plugin.MongoCli().GetDB()).GetByIdAndStatus(ctx, secretReq.OrganizationId, OrgModel.OrganizationStatusPass)
	if err != nil {
		return nil, err
	}

	// {"user":{"nickname":"asdas","faceURL":"","areaCode":"","phoneNumber":"","password":"0192023a7bbd73250516f069df18b500", "account": "xxxxxxasd", "gender": 1},"platform":11}
	req, err := secretReq.DeSecret(org.AesKeyBase64)
	if err != nil {
		return nil, err
	}

	req.Ip = remoteAddr
	req.User.Password = ""
	if req.Platform == 0 {
		req.Platform = pkgConstant.H5PlatformID
	}

	err = plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		orgUser, err := orgUserDao.GetByThirdUserIdAndOrganizationId(sessionCtx, req.ThirdUserId, secretReq.OrganizationId)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				log.ZWarn(ctx, "未找到组织用户", err, "third_user_id", req.ThirdUserId, "organization_id", secretReq.OrganizationId.Hex())
			} else {
				log.ZError(sessionCtx, "GetByThirdUserIdAndOrganizationId 查询出错", err, "third_user_id", req.ThirdUserId, "organization_id", secretReq.OrganizationId.Hex())
				return err
			}
		}
		if orgUser != nil {
			user, err := userDao.Take(sessionCtx, orgUser.ImServerUserId)
			if err != nil {
				log.ZError(sessionCtx, "userDao.Take 查询出错", err, "im_server_user_id", orgUser.ImServerUserId)
				return err
			}

			if user.Nickname != req.User.Nickname || user.FaceURL != req.User.FaceURL {
				if err := orgSvc.ValidateAppUserNickname(sessionCtx, plugin.MongoCli().GetDB(), secretReq.OrganizationId, req.User.Nickname, orgUser.ImServerUserId); err != nil {
					return err
				}
				err = imApiCaller.UpdateUserInfo(imApiCallerCtx, orgUser.ImServerUserId, req.User.Nickname, req.User.FaceURL)
				if err != nil {
					log.ZError(sessionCtx, "imApiCaller.UpdateUserInfo 更新用户信息失败", err, "im_server_user_id", orgUser.ImServerUserId)
					return err
				}
			}
			return nil
		}

		if err := orgSvc.ValidateAppUserNickname(sessionCtx, plugin.MongoCli().GetDB(), secretReq.OrganizationId, req.User.Nickname, ""); err != nil {
			return err
		}

		newUserID, err := utils.NewId()
		if err != nil {
			return err
		}
		newImServerUserID, err := utils.NewId()
		if err != nil {
			return err
		}
		//account信息 需要固定生成 不能随便传递
		req.User.Account = "fcid_" + utils.RandomString(14)

		attributes, err := attributeDao.FindAccountCaseInsensitive(sessionCtx, []string{req.User.Account})
		if err != nil {
			return err
		}
		if len(attributes) > 0 {
			return fmt.Errorf("account %s already exists", req.User.Account)
		}

		orgUser = &OrgModel.OrganizationUser{
			UserId:         newUserID,
			OrganizationId: secretReq.OrganizationId,
			ThirdUserId:    req.ThirdUserId,
			Role:           OrgModel.OrganizationUserNormalRole,
			Status:         OrgModel.OrganizationUserEnableStatus,
			RegisterType:   OrgModel.OrganizationUserRegisterTypeH5,
			ImServerUserId: newImServerUserID,
		}
		if err = orgUserDao.Create(sessionCtx, orgUser); err != nil {
			return err
		}

		credentials := make([]*chatModel.Credential, 0)
		var registerType int32 = constant.AccountRegister

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
		//	registerType = constant.EmailRegister
		//	credentials = append(credentials, &chatModel.Credential{
		//		UserID:      newUserID,
		//		Account:     req.User.Email,
		//		RewardType:        constant.CredentialEmail,
		//		AllowChange: true,
		//	})
		//}
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
			UserID:  newUserID,
			Account: req.User.Account,
			//PhoneNumber:    req.User.PhoneNumber,
			//AreaCode:       req.User.AreaCode,
			//Email:          req.User.Email,
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

		userInfo := &imapi.OrgUserInfo{
			UserID:         newImServerUserID,
			Nickname:       req.User.Nickname,
			FaceURL:        req.User.FaceURL,
			CreateTime:     time.Now().UnixMilli(),
			OrgId:          orgUser.OrganizationId.Hex(),
			OrgRole:        string(orgUser.Role),
			CanSendFreeMsg: 0,
		}
		err = imApiCaller.RegisterOrgUser(imApiCallerCtx, []*imapi.OrgUserInfo{userInfo})
		if err != nil {
			return err
		}

		if resp, err := plugin.AdminClient().FindDefaultFriend(context.Background(), &admin.FindDefaultFriendReq{}); err == nil {
			_ = imApiCaller.ImportFriend(imApiCallerCtx, newUserID, resp.UserIDs)
		}
		if resp, err := plugin.AdminClient().FindDefaultGroup(context.Background(), &admin.FindDefaultGroupReq{}); err == nil {
			_ = imApiCaller.InviteToGroup(imApiCallerCtx, newUserID, resp.GroupIDs)
		}
		return nil
	})

	if err != nil {
		return nil, errs.Unwrap(err)
	}

	orgUser, err := orgUserDao.GetByThirdUserIdAndOrganizationId(context.Background(), req.ThirdUserId, secretReq.OrganizationId)
	if err != nil {
		return nil, err
	}

	exist, err := forbiddenAccountDao.ExistByUserId(context.TODO(), orgUser.ImServerUserId)
	if err != nil {
		return nil, err
	}
	if exist {
		return nil, freeErrors.ForbiddenErr("account forbidden")
	}

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

	// 删除登录记录缓存，确保下次查询时获取最新数据
	loginRecordCache := chatCache.NewLoginRecordCacheRedis(plugin.RedisCli(), plugin.MongoCli().GetDB())
	if err := loginRecordCache.DelCache(context.TODO(), orgUser.UserId); err != nil {
		log.ZWarn(context.TODO(), "failed to delete login record cache", err, "userID", orgUser.UserId)
		// 缓存删除失败不影响主要功能，只记录日志
	}

	response := &EmbedLoginResponse{
		UserID:         orgUser.ImServerUserId,
		ChatToken:      chatToken.Token,
		ImToken:        imToken,
		OrganizationId: secretReq.OrganizationId,
	}
	return response.Secret(org.AesKeyBase64)
}
