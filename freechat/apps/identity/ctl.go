package identity

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/identity/dto"
	"github.com/openimsdk/chat/freechat/apps/identity/svc"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
)

type IdentityCtl struct{}

func NewIdentityCtl() *IdentityCtl {
	return &IdentityCtl{}
}

// SubmitIdentity 提交身份认证
func (c *IdentityCtl) SubmitIdentity(ctx *gin.Context) {
	var req dto.SubmitIdentityReq
	if err := ctx.ShouldBind(&req); err != nil {
		apiresp.GinError(ctx, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取当前登录用户ID
	userID, _, err := mctx.Check(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	// 调用服务层提交认证
	identitySvc := svc.NewIdentitySvc()
	resp, err := identitySvc.SubmitIdentity(ctx, userID, &req)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	apiresp.GinSuccess(ctx, resp)
}

// GetIdentityInfo 获取身份认证信息
func (c *IdentityCtl) GetIdentityInfo(ctx *gin.Context) {
	// 获取当前登录用户ID
	userID, _, err := mctx.Check(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	// 调用服务层获取认证信息
	identitySvc := svc.NewIdentitySvc()
	resp, err := identitySvc.GetIdentityInfo(ctx, userID)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	apiresp.GinSuccess(ctx, resp)
}
