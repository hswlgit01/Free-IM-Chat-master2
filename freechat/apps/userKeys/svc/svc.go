package svc

import (
	"context"

	organizationModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/apps/userKeys/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// UserKeysSvc 用户密钥服务
type UserKeysSvc struct{}

// NewUserKeysSvc 创建用户密钥服务实例
func NewUserKeysSvc() *UserKeysSvc {
	return &UserKeysSvc{}
}

// GetUserAESKey 获取用户特定平台的AES密钥
func (svc *UserKeysSvc) GetUserAESKey(ctx context.Context, userID, platform string) (string, error) {
	mongoDB := plugin.MongoCli().GetDB()
	userKeysDao := model.NewUserKeysDao(mongoDB)

	// 查询用户AES密钥
	aesKey, err := userKeysDao.GetUserAESKey(ctx, userID, platform)
	if err != nil {
		// 记录错误但不向上抛出具体错误
		log.ZError(ctx, "获取用户AES密钥失败", err, "user_id", userID, "platform", platform)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	return aesKey, nil
}

// GetUserRSAPublicKey 获取用户特定平台的RSA公钥
func (svc *UserKeysSvc) GetUserRSAPublicKey(ctx context.Context, userID, platform string) (string, error) {
	mongoDB := plugin.MongoCli().GetDB()
	userKeysDao := model.NewUserKeysDao(mongoDB)

	// 查询用户RSA公钥
	rsaPublicKey, err := userKeysDao.GetUserRSAPublicKey(ctx, userID, platform)
	if err != nil {
		log.ZError(ctx, "获取用户RSA公钥失败", err, "user_id", userID, "platform", platform)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	return rsaPublicKey, nil
}

// SetupUserKeys 设置用户密钥（RSA公钥由前端提供，服务端生成AES密钥并返回）
func (svc *UserKeysSvc) SetupUserKeys(ctx context.Context, userID, platform, rsaPublicKey, orgId string) (string, error) {
	mongoDB := plugin.MongoCli().GetDB()
	userKeysDao := model.NewUserKeysDao(mongoDB)

	orgUserDao := organizationModel.NewOrganizationUserDao(mongoDB)

	orgIdObj, err := primitive.ObjectIDFromHex(orgId)
	if err != nil {
		log.ZError(ctx, "invalid orgId", err, "orgId", orgId)
		return "", errs.NewCodeError(freeErrors.ErrSystem, "invalid orgId")
	}
	orgUser, err := orgUserDao.GetByUserIdAndOrgId(ctx, userID, orgIdObj)
	if err != nil {
		log.ZError(ctx, "failed to query orgUser", err, "user_id", userID, "org_id", orgId)
		return "", errs.NewCodeError(freeErrors.ErrSystem, "failed to query orgUser")
	}

	//判断请求类型是不是组织
	if orgUser.Role == organizationModel.OrganizationUserSuperAdminRole || orgUser.Role == organizationModel.OrganizationUserBackendAdminRole {
		aesKey, err := svc.GetUserAESKey(ctx, userID, platform)
		// 检查AES密钥是否存在
		if err == nil && aesKey != "" {
			// 使用RSA公钥加密AES密钥
			encryptedAESKey, err := utils.RSAEncrypt([]byte(aesKey), rsaPublicKey)
			if err != nil {
				log.ZError(ctx, "RSA加密AES密钥失败", err, "user_id", userID, "platform", platform)
				return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
			}
			return encryptedAESKey, nil
		}
	}

	// 生成新的AES密钥
	aesKey, err := utils.GenerateAESKey()
	if err != nil {
		log.ZError(ctx, "生成AES密钥失败", err, "user_id", userID, "platform", platform)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 保存RSA公钥
	if err := userKeysDao.UpdateUserRSAPublicKey(ctx, userID, platform, rsaPublicKey); err != nil {
		log.ZError(ctx, "保存用户RSA公钥失败", err, "user_id", userID, "platform", platform)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 保存AES密钥
	if err := userKeysDao.UpdateUserAESKey(ctx, userID, platform, aesKey); err != nil {
		log.ZError(ctx, "保存用户AES密钥失败", err, "user_id", userID, "platform", platform)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	// 使用RSA公钥加密AES密钥
	encryptedAESKey, err := utils.RSAEncrypt([]byte(aesKey), rsaPublicKey)
	if err != nil {
		log.ZError(ctx, "RSA加密AES密钥失败", err, "user_id", userID, "platform", platform)
		return "", errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	//log.ZInfo(ctx, "用户密钥设置成功", "user_id", userID, "platform", platform, "time", time.Now().Format(time.RFC3339))

	// 返回RSA加密后的AES密钥
	return encryptedAESKey, nil
}

// DecryptData 使用用户AES密钥解密数据
func (svc *UserKeysSvc) DecryptData(ctx context.Context, userID, platform, encryptedData, userReqType string) ([]byte, error) {
	////判断请求类型是不是组织
	//if userReqType == string(walletModel.WalletInfoOwnerTypeOrganization) {
	//	mongoDB := plugin.MongoCli().GetDB()
	//	organizationDao := organizationModel.NewOrganizationDao(mongoDB)
	//	org, err := organizationDao.GetByCreatorIdAndStatus(ctx, userID, organizationModel.OrganizationStatusPass)
	//	if err != nil && !dbutil.IsDBNotFound(err) {
	//		log.ZError(ctx, "failed to query org by email", err, "ownerId", userID)
	//		return nil, errs.NewCodeError(freeErrors.ErrSystem, "failed to query org by email")
	//	}
	//	userID = org.ID.Hex()
	//}
	// 获取用户AES密钥
	aesKey, err := svc.GetUserAESKey(ctx, userID, platform)
	if err != nil {
		return nil, err
	}

	// 检查AES密钥是否存在
	if aesKey == "" {
		log.ZError(ctx, "用户AES密钥不存在", nil, "user_id", userID, "platform", platform)
		return nil, errs.NewCodeError(freeErrors.ErrUnauthorized, freeErrors.ErrorMessages[freeErrors.ErrUnauthorized])
	}

	// 解密数据
	decryptedData, err := utils.AESDecrypt(encryptedData, aesKey)
	if err != nil {
		log.ZError(ctx, "AES解密数据失败", err, "user_id", userID, "platform", platform)
		return nil, errs.NewCodeError(freeErrors.ErrUnauthorized, freeErrors.ErrorMessages[freeErrors.ErrUnauthorized])
	}

	return decryptedData, nil
}

// VerifySignature 验证用户签名
func (svc *UserKeysSvc) VerifySignature(ctx context.Context, userID, platform string, data []byte, signature string) (bool, error) {
	// 获取用户RSA公钥
	rsaPublicKey, err := svc.GetUserRSAPublicKey(ctx, userID, platform)
	if err != nil {
		return false, err
	}

	// 检查RSA公钥是否存在
	if rsaPublicKey == "" {
		log.ZError(ctx, "用户RSA公钥不存在", nil, "user_id", userID, "platform", platform)
		return false, errs.NewCodeError(freeErrors.ErrUnauthorized, "用户公钥未设置")
	}

	// 验证签名
	valid, err := utils.RSAVerifySignature(data, signature, rsaPublicKey)
	if err != nil {
		log.ZError(ctx, "验证签名失败", err, "user_id", userID, "platform", platform)
		return false, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem])
	}

	return valid, nil
}
