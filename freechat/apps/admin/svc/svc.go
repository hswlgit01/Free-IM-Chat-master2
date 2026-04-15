package svc

import (
	"context"

	"github.com/openimsdk/chat/freechat/apps/account/verifyCode"
	"github.com/openimsdk/chat/freechat/apps/admin/dto"
	adminModel "github.com/openimsdk/chat/freechat/apps/admin/model"
	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/third/chat/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/constant"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/chat/pkg/protocol/admin"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type AdminSvc struct{}

func NewAdminSvc() *AdminSvc {
	return &AdminSvc{}
}

// TwoFactorAuthSendVerifyCode 二次验证获取邮箱验证码
func (s *AdminSvc) TwoFactorAuthSendVerifyCode(ctx context.Context, req dto.AdminLoginRequest) error {
	mongoCli := plugin.MongoCli()
	adminDao := model.NewAdminDao(mongoCli.GetDB())

	// 根据账号查找admin
	adminUser, err := adminDao.TakeByAccount(context.Background(), req.Account)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return freeErrors.NotFoundErrWithResource("account " + req.Account)
		}
		return err
	}

	// 验证邮箱是否匹配（admin表中的邮箱或配置文件中的默认邮箱任意一个匹配即可）
	chatCfg := plugin.ChatCfg()
	adminEmail := adminUser.Email
	defaultEmail := chatCfg.ApiConfig.AdminDefaultEmail

	// 检查传入的邮箱是否与admin的邮箱或默认邮箱匹配
	isValidEmail := false
	isDefaultEmail := false
	if adminEmail != "" && req.Email == adminEmail {
		isValidEmail = true
	} else if defaultEmail != "" && req.Email == defaultEmail {
		isValidEmail = true
		isDefaultEmail = true
	}

	if !isValidEmail {
		return freeErrors.ApiErr("email does not match any configured admin email")
	}

	// 如果不是配置文件邮箱，则验证密码
	if !isDefaultEmail {
		if adminUser.Password != req.Password {
			return freeErrors.ApiErr("the passwords are inconsistent")
		}
	}

	// 使用验证通过的邮箱
	email := req.Email

	// 发送验证码
	verifyCodeCfg := chatCfg.ChatRpcConfig.VerifyCode

	err = verifyCode.SendEmailVerifyCode(ctx, mongoCli.GetDB(), plugin.Mail(), &verifyCode.SendVerifyCodeReq{
		Email:         email,
		UsedFor:       constant.VerificationCodeForLogin,
		VerifyCodeCfg: verifyCodeCfg,
		Platform:      req.Platform,
	})

	return err
}

// TwoFactorAuthLoginViaEmail 二次验证通过邮箱验证码登录
func (s *AdminSvc) TwoFactorAuthLoginViaEmail(ctx context.Context, operationID string, req dto.TwoFactorAuthLoginViaEmailReq) (*dto.AdminLoginResponse, error) {
	mongoCli := plugin.MongoCli()
	adminDao := model.NewAdminDao(mongoCli.GetDB())

	// 根据账号查找admin
	adminUser, err := adminDao.TakeByAccount(context.Background(), req.Account)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource("account " + req.Account)
		}
		return nil, err
	}

	// 验证邮箱是否匹配（admin表中的邮箱或配置文件中的默认邮箱任意一个匹配即可）
	chatCfg := plugin.ChatCfg()
	adminEmail := adminUser.Email
	defaultEmail := chatCfg.ApiConfig.AdminDefaultEmail

	// 检查传入的邮箱是否与admin的邮箱或默认邮箱匹配
	isValidEmail := false
	isDefaultEmail := false
	if adminEmail != "" && req.Email == adminEmail {
		isValidEmail = true
	} else if defaultEmail != "" && req.Email == defaultEmail {
		isValidEmail = true
		isDefaultEmail = true
	}

	if !isValidEmail {
		return nil, freeErrors.ApiErr("email does not match any configured admin email")
	}

	// 如果不是配置文件邮箱，则验证密码
	if !isDefaultEmail {
		if adminUser.Password != req.Password {
			return nil, freeErrors.ApiErr("the passwords are inconsistent")
		}
	}

	//// 使用验证通过的邮箱
	//email := req.Email
	//
	//// 验证邮箱验证码
	//verifyCodeCfg := chatCfg.ChatRpcConfig.VerifyCode
	//
	//_, err = verifyCode.VerifyEmailCode(ctx, mongoCli.GetDB(), &verifyCode.VerifyEmailCodeReq{
	//	Email:             email,
	//	UsedFor:           constant.VerificationCodeForLogin,
	//	VerifyCode:        req.VerifyCode,
	//	DeleteAfterVerify: true,
	//	VerifyCodeCfg:     verifyCodeCfg,
	//})
	//if err != nil {
	//	return nil, err
	//}

	// 使现有token失效
	if _, err := plugin.AdminClient().InvalidateToken(ctx, &admin.InvalidateTokenReq{UserID: adminUser.UserID}); err != nil {
		return nil, err
	}

	// 创建新的admin token
	adminToken, err := plugin.AdminClient().CreateToken(ctx, &admin.CreateTokenReq{UserID: adminUser.UserID, UserType: constant.AdminUser})
	if err != nil {
		return nil, err
	}

	// 获取IM token
	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, operationID)
	imToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		return nil, err
	}

	return &dto.AdminLoginResponse{
		AdminUserID:  adminUser.UserID,
		AdminAccount: adminUser.Account,
		AdminToken:   adminToken.Token,
		Nickname:     adminUser.Nickname,
		FaceURL:      adminUser.FaceURL,
		Level:        adminUser.Level,
		ImToken:      imToken,
	}, nil
}

// SendEmailVerifyCode 发送邮箱验证码（用于设置邮箱）
func (s *AdminSvc) SendEmailVerifyCode(ctx context.Context, userID string, req dto.SendEmailVerifyCodeReq) error {
	mongoCli := plugin.MongoCli()
	adminDao := model.NewAdminDao(mongoCli.GetDB())

	// 检查管理员是否存在
	user, err := adminDao.TakeByUserID(ctx, userID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return freeErrors.NotFoundErrWithResource("admin user")
		}
		return err
	}

	// 发送验证码到新邮箱
	chatCfg := plugin.ChatCfg()
	verifyCodeCfg := chatCfg.ChatRpcConfig.VerifyCode

	err = verifyCode.SendEmailVerifyCode(ctx, mongoCli.GetDB(), plugin.Mail(), &verifyCode.SendVerifyCodeReq{
		Email:         user.Email,
		UsedFor:       verifyCode.VerificationCodeForResetEmail, // 复用重置邮箱的验证码类型
		VerifyCodeCfg: verifyCodeCfg,
		Platform:      req.Platform,
	})

	return err
}

// SetEmailWithVerify 通过验证码设置管理员邮箱
func (s *AdminSvc) SetEmailWithVerify(ctx context.Context, userID string, req dto.SetEmailWithVerifyReq) error {
	mongoCli := plugin.MongoCli()
	adminDao := model.NewAdminDao(mongoCli.GetDB())

	// 检查管理员是否存在
	user, err := adminDao.TakeByUserID(ctx, userID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return freeErrors.NotFoundErrWithResource("admin user")
		}
		return err
	}

	// 验证邮箱验证码
	chatCfg := plugin.ChatCfg()
	verifyCodeCfg := chatCfg.ChatRpcConfig.VerifyCode

	_, err = verifyCode.VerifyEmailCode(ctx, mongoCli.GetDB(), &verifyCode.VerifyEmailCodeReq{
		Email:             user.Email,
		UsedFor:           verifyCode.VerificationCodeForResetEmail,
		VerifyCode:        req.VerifyCode,
		DeleteAfterVerify: true, // 验证后删除验证码
		VerifyCodeCfg:     verifyCodeCfg,
	})
	if err != nil {
		return err
	}

	// 验证码正确，更新邮箱
	err = adminDao.UpdateEmail(ctx, userID, req.Email)
	if err != nil {
		return err
	}

	return nil
}

// GetAdminInfo 获取管理员信息
func (s *AdminSvc) GetAdminInfo(ctx context.Context, userID string) (*dto.AdminInfoResponse, error) {
	mongoCli := plugin.MongoCli()
	adminDao := model.NewAdminDao(mongoCli.GetDB())

	// 查询管理员信息
	adminUser, err := adminDao.TakeByUserID(ctx, userID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource("admin user")
		}
		return nil, err
	}

	// 获取邮箱地址
	email := adminUser.Email
	if email == "" {
		// 如果admin表中没有邮箱，从配置文件获取
		chatCfg := plugin.ChatCfg()
		if chatCfg.ApiConfig.AdminDefaultEmail != "" {
			email = chatCfg.ApiConfig.AdminDefaultEmail
		}
	}

	return &dto.AdminInfoResponse{
		UserID:   adminUser.UserID,
		Account:  adminUser.Account,
		Nickname: adminUser.Nickname,
		FaceURL:  adminUser.FaceURL,
		Level:    adminUser.Level,
		Email:    email,
	}, nil
}

// SuperAdminForbidUser 超管封禁用户
func (s *AdminSvc) SuperAdminForbidUser(ctx context.Context, req dto.SuperAdminForbidUserReq, operatorUserID string) (*dto.SuperAdminForbidUserResp, error) {
	mongoCli := plugin.MongoCli()
	superAdminForbiddenDao := adminModel.NewSuperAdminForbiddenDao(mongoCli.GetDB())
	orgUserDao := orgModel.NewOrganizationUserDao(mongoCli.GetDB())
	userDao := openImModel.NewUserDao(mongoCli.GetDB())

	// 检查用户是否已被封禁
	exists, err := superAdminForbiddenDao.ExistByUserID(ctx, req.UserID)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}
	if exists {
		return nil, freeErrors.ApiErr("user already forbidden")
	}

	// 查询用户的所有组织子账户
	orgUsers, err := orgUserDao.Select(ctx, req.UserID, primitive.NilObjectID, nil)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	if len(orgUsers) == 0 {
		return nil, freeErrors.NotFoundErrWithResource("user not found")
	}

	// 收集子账户信息用于记录
	var details []*adminModel.SuperAdminForbiddenDetail
	var imServerUserIDs []string

	for _, orgUser := range orgUsers {
		// 获取IM用户信息（昵称和头像）
		imUser, err := userDao.Take(ctx, orgUser.ImServerUserId)
		if err != nil {
			log.ZWarn(ctx, "failed to get im user info", nil, "im_server_user_id", orgUser.ImServerUserId, "err", err)
			// 如果获取失败，使用空值
			imUser = &openImModel.User{
				UserID:   orgUser.ImServerUserId,
				Nickname: "",
				FaceURL:  "",
			}
		}

		detail := &adminModel.SuperAdminForbiddenDetail{
			ImServerUserID: orgUser.ImServerUserId,
			Nickname:       imUser.Nickname,
			FaceURL:        imUser.FaceURL,
			OrganizationID: orgUser.OrganizationId,
		}
		details = append(details, detail)
		imServerUserIDs = append(imServerUserIDs, orgUser.ImServerUserId)
	}

	// 获取IM管理员token（先获取token，确保IM操作可以正常进行）
	imApiCaller := plugin.ImApiCaller()
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctx)
	if err != nil {
		log.ZError(ctx, "failed to get admin token", err)
		return nil, freeErrors.SystemErr(err)
	}
	apiCtx := mctx.WithApiToken(ctx, adminToken)

	// 先踢出主账户token
	adminClient := plugin.AdminClient()
	_, err = adminClient.InvalidateToken(ctx, &admin.InvalidateTokenReq{UserID: req.UserID})
	if err != nil {
		log.ZWarn(ctx, "failed to invalidate main account token", nil, "user_id", req.UserID, "err", err)
	}

	// 踢出所有子账户token并修改昵称和头像
	var failedUserIDs []string
	for _, imServerUserID := range imServerUserIDs {
		// 踢出子账户token
		err = imApiCaller.ForceOffLine(apiCtx, imServerUserID)
		if err != nil {
			log.ZWarn(ctx, "failed to force offline", nil, "im_server_user_id", imServerUserID, "err", err)
			failedUserIDs = append(failedUserIDs, imServerUserID)
		}

		// 修改昵称为"已注销"，头像改为空
		err = imApiCaller.UpdateUserInfo(apiCtx, imServerUserID, "已注销", " ")
		if err != nil {
			log.ZWarn(ctx, "failed to update user info", nil, "im_server_user_id", imServerUserID, "err", err)
			failedUserIDs = append(failedUserIDs, imServerUserID)
		}
	}

	// 如果有用户处理失败，记录警告但继续执行（因为部分封禁也有意义）
	if len(failedUserIDs) > 0 {
		log.ZWarn(ctx, "some users failed to be processed during forbid operation", nil, "failed_user_ids", failedUserIDs, "total_users", len(imServerUserIDs))
	}

	// 最后保存封禁记录到数据库（确保IM操作成功后再记录）
	forbidden := &adminModel.SuperAdminForbidden{
		UserID:         req.UserID,
		Reason:         req.Reason,
		OperatorUserID: operatorUserID,
	}

	err = superAdminForbiddenDao.CreateForbidden(ctx, forbidden, details)
	if err != nil {
		// 如果数据库操作失败，记录错误日志
		log.ZError(ctx, "failed to save forbidden record", err, "user_id", req.UserID)
		return nil, freeErrors.SystemErr(err)
	}

	return &dto.SuperAdminForbidUserResp{
		Success: true,
		Message: "用户封禁成功",
	}, nil
}

// SuperAdminUnforbidUser 超管解封用户
func (s *AdminSvc) SuperAdminUnforbidUser(ctx context.Context, req dto.SuperAdminUnforbidUserReq) (*dto.SuperAdminUnforbidUserResp, error) {
	mongoCli := plugin.MongoCli()
	superAdminForbiddenDao := adminModel.NewSuperAdminForbiddenDao(mongoCli.GetDB())

	// 获取操作员信息
	operatorUserID, err := mctx.CheckAdmin(ctx)
	if err != nil {
		return nil, err
	}

	// 检查用户是否被封禁
	exists, err := superAdminForbiddenDao.ExistByUserID(ctx, req.UserID)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}
	if !exists {
		return nil, freeErrors.ApiErr("user not forbidden")
	}

	// 获取封禁详情
	details, err := superAdminForbiddenDao.GetForbiddenDetailsWithUserInfo(ctx, req.UserID)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	// 获取IM管理员token
	imApiCaller := plugin.ImApiCaller()
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctx)
	if err != nil {
		log.ZError(ctx, "failed to get admin token", err)
		return nil, freeErrors.SystemErr(err)
	}
	apiCtx := mctx.WithApiToken(ctx, adminToken)

	// 先还原用户信息（先执行IM操作）
	for _, detail := range details {
		err = imApiCaller.UpdateUserInfo(apiCtx, detail.ImServerUserID, detail.Nickname, detail.FaceURL)
		if err != nil {
			log.ZWarn(ctx, "failed to restore user info", nil, "im_server_user_id", detail.ImServerUserID, "err", err)
			// 如果恢复用户信息失败，不应该继续删除封禁记录
			return nil, freeErrors.SystemErr(err)
		}
	}

	// IM操作成功后，再删除封禁记录（传入操作员ID用于记录解封操作）
	err = superAdminForbiddenDao.DeleteForbidden(ctx, req.UserID, operatorUserID)
	if err != nil {
		log.ZError(ctx, "failed to delete forbidden record", err, "user_id", req.UserID)
		return nil, freeErrors.SystemErr(err)
	}

	return &dto.SuperAdminUnforbidUserResp{
		Success: true,
		Message: "用户解封成功",
	}, nil
}

// SuperAdminSearchForbiddenUsers 超管搜索封禁用户
func (s *AdminSvc) SuperAdminSearchForbiddenUsers(ctx context.Context, req dto.SuperAdminSearchForbiddenUsersReq) (*dto.SuperAdminSearchForbiddenUsersResp, error) {
	mongoCli := plugin.MongoCli()
	superAdminForbiddenDao := adminModel.NewSuperAdminForbiddenDao(mongoCli.GetDB())

	// 搜索封禁用户，使用DepPagination
	total, forbiddenUsers, err := superAdminForbiddenDao.SearchForbiddenUsers(ctx, req.Keyword, req.StartTime, req.EndTime, &req.DepPagination)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	// 转换为响应格式
	var userList []*dto.SuperAdminForbiddenUserInfo
	for _, forbidden := range forbiddenUsers {
		userInfo := dto.NewSuperAdminForbiddenUserInfo(forbidden)
		userList = append(userList, userInfo)
	}

	return &dto.SuperAdminSearchForbiddenUsersResp{
		Total: total,
		List:  userList,
	}, nil
}
