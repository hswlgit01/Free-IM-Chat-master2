package userKeys

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/userKeys/dto"
	"github.com/openimsdk/chat/freechat/apps/userKeys/svc"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/errs"
)

// UserKeysCtl 用户密钥控制器
type UserKeysCtl struct{}

// NewUserKeysCtl 创建用户密钥控制器实例
func NewUserKeysCtl() *UserKeysCtl {
	return &UserKeysCtl{}
}

// SetupUserKeys 设置用户密钥
func (ctl *UserKeysCtl) SetupUserKeys(c *gin.Context) {
	var req dto.SetupUserKeysReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}

	// 获取用户ID和平台
	req.UserID = mctx.GetOpUserID(c)
	platform := c.GetHeader("Source")
	if platform == "" {
		platform = "web" // 默认为web
	}

	orgId, _ := mctx.GetOrgId(c)
	// 验证平台类型
	if platform != "web" && platform != "ios" && platform != "android" && platform != "h5" {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}

	// 调用服务层
	userKeysSvc := svc.NewUserKeysSvc()
	encryptedAESKey, err := userKeysSvc.SetupUserKeys(c, req.UserID, platform, req.RSAPublicKey, orgId)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 返回结果
	apiresp.GinSuccess(c, &dto.SetupUserKeysResp{
		EncryptedAESKey: encryptedAESKey,
	})
}
