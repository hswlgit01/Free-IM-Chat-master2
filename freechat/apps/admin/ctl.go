package admin

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/admin/dto"
	"github.com/openimsdk/chat/freechat/apps/admin/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
)

type AdminCtl struct{}

func NewAdminCtl() *AdminCtl {
	return &AdminCtl{}
}

// SuperCmsTwoFactorAuthSendVerifyCode 超管二次验证发送邮箱验证码
func (a *AdminCtl) SuperCmsTwoFactorAuthSendVerifyCode(c *gin.Context) {
	data := dto.AdminLoginRequest{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	adminSvc := svc.NewAdminSvc()
	err := adminSvc.TwoFactorAuthSendVerifyCode(c, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// SuperCmsTwoFactorAuthLoginViaEmail 超管二次验证邮箱登录
func (a *AdminCtl) SuperCmsTwoFactorAuthLoginViaEmail(c *gin.Context) {
	data := dto.TwoFactorAuthLoginViaEmailReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	adminSvc := svc.NewAdminSvc()
	resp, err := adminSvc.TwoFactorAuthLoginViaEmail(c, operationID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// SuperCmsGetAdminInfo 获取超管信息
func (a *AdminCtl) SuperCmsGetAdminInfo(c *gin.Context) {
	// 从token中获取当前管理员用户ID
	userID, err := mctx.CheckAdmin(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	adminSvc := svc.NewAdminSvc()
	resp, err := adminSvc.GetAdminInfo(c, userID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// SuperCmsPostSendEmailVerifyCode 发送邮箱验证码
func (a *AdminCtl) SuperCmsPostSendEmailVerifyCode(c *gin.Context) {
	data := dto.SendEmailVerifyCodeReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 从token中获取当前管理员用户ID
	userID, err := mctx.CheckAdmin(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	adminSvc := svc.NewAdminSvc()
	err = adminSvc.SendEmailVerifyCode(c, userID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// SuperCmsPostSetEmailWithVerify 通过验证码设置邮箱
func (a *AdminCtl) SuperCmsPostSetEmailWithVerify(c *gin.Context) {
	data := dto.SetEmailWithVerifyReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 从token中获取当前管理员用户ID
	userID, err := mctx.CheckAdmin(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	adminSvc := svc.NewAdminSvc()
	err = adminSvc.SetEmailWithVerify(c, userID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// SuperAdminForbidUser 超管封禁用户
func (a *AdminCtl) SuperAdminForbidUser(c *gin.Context) {
	data := dto.SuperAdminForbidUserReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 从token中获取当前管理员用户ID
	operatorUserID, err := mctx.CheckAdmin(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	adminSvc := svc.NewAdminSvc()
	resp, err := adminSvc.SuperAdminForbidUser(c, data, operatorUserID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// SuperAdminUnforbidUser 超管解封用户
func (a *AdminCtl) SuperAdminUnforbidUser(c *gin.Context) {
	data := dto.SuperAdminUnforbidUserReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 验证超管权限
	_, err := mctx.CheckAdmin(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	adminSvc := svc.NewAdminSvc()
	resp, err := adminSvc.SuperAdminUnforbidUser(c, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// SuperAdminSearchForbiddenUsers 超管搜索封禁用户
func (a *AdminCtl) SuperAdminSearchForbiddenUsers(c *gin.Context) {
	data := dto.SuperAdminSearchForbiddenUsersReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 验证超管权限
	_, err := mctx.CheckAdmin(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	adminSvc := svc.NewAdminSvc()
	resp, err := adminSvc.SuperAdminSearchForbiddenUsers(c, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}
