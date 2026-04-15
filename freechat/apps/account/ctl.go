package account

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/account/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/third/chat/model"
	"github.com/openimsdk/chat/freechat/utils/captcha"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/ginUtils"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
	"time"
)

type AccountCtl struct{}

func NewAccountCtl() *AccountCtl {
	return &AccountCtl{}
}

// PostComparePwd 对比用户密码是否正确
func (w *AccountCtl) PostComparePwd(c *gin.Context) {
	var data struct {
		Pwd string `json:"pwd" form:"pwd" xml:"pwd"`
	}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	mongoCli := plugin.MongoCli()

	accountDao := model.NewAccountDao(mongoCli.GetDB())
	user, err := accountDao.GetByUserId(context.TODO(), opUserID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			apiresp.GinError(c, freeErrors.UserNotFoundErr)
			return
		}
		apiresp.GinError(c, err)
		return
	}

	if user.Password != data.Pwd {
		apiresp.GinError(c, freeErrors.UserAccountErr)
		return
	}

	apiresp.GinSuccess(c, true)
}

// PostOrgAdminLogin 管理端登录接口
func (w *AccountCtl) PostOrgAdminLogin(c *gin.Context) {
	data := svc.OrgLoginRequest{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 调用service层进行登录
	accountSvc := svc.NewOrgAccountSvc()
	resp, err := accountSvc.Login(c, operationID, data)

	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 返回登录结果
	apiresp.GinSuccess(c, resp)
}

// CmsPostTwoFactorAuthSendVerifyCode 管理端登录二次验证发送验证码
func (w *AccountCtl) CmsPostTwoFactorAuthSendVerifyCode(c *gin.Context) {
	data := svc.OrgLoginRequest{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgAccountSvc := svc.NewOrgAccountSvc()
	err := orgAccountSvc.TwoFactorAuthSendVerifyCode(c, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *AccountCtl) CmsPostTwoFactorAuthLoginViaEmail(c *gin.Context) {
	data := svc.TwoFactorAuthLoginViaEmailReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 调用service层进行登录
	accountSvc := svc.NewOrgAccountSvc()
	resp, err := accountSvc.TwoFactorAuthLoginViaEmail(c, operationID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 返回登录结果
	apiresp.GinSuccess(c, resp)
}

// PostEmbedLogin 用户嵌入式登录
func (w *AccountCtl) PostEmbedLogin(c *gin.Context) {
	data := svc.SecretEmbedLoginRequest{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	ip := ginUtils.GetClientIP(c)

	// 调用service层进行登录
	accountSvc := svc.NewAccountSvc()
	resp, err := accountSvc.EmbedLogin(c, ip, operationID, data)

	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 返回登录结果
	apiresp.GinSuccess(c, resp)
}

// PostHavePwd 是否有密码
func (w *AccountCtl) PostHavePwd(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	mongoCli := plugin.MongoCli()

	accountDao := model.NewAccountDao(mongoCli.GetDB())
	user, err := accountDao.GetByUserId(context.TODO(), opUserID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			apiresp.GinError(c, freeErrors.UserNotFoundErr)
			return
		}
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{
		"have_pwd": user.Password != "",
	})
}

type CaptchaCtl struct{}

func NewCaptchaCtl() *CaptchaCtl {
	return &CaptchaCtl{}
}

func (w *CaptchaCtl) GenCaptcha(c *gin.Context) {
	expiration := time.Second * 5 * 60
	captchaId, captchaB64Image, err := captcha.GenerateImageCaptcha(context.TODO(), plugin.RedisCli(), expiration)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{
		"id":         captchaId,
		"captcha":    captchaB64Image,
		"expiration": expiration,
	})
}
