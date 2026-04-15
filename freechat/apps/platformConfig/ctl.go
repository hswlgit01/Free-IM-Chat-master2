package platformConfig

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/platformConfig/svc"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/tools/apiresp"
)

type RegisterSwitchCtl struct{}

func NewRegisterSwitchCtl() *RegisterSwitchCtl {
	return &RegisterSwitchCtl{}
}

func (w *RegisterSwitchCtl) SuperCmsPostUpdateRegisterSwitch(c *gin.Context) {
	var req svc.SuperCmsSetRegisterReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	registerSwitchSvc := svc.NewRegisterSwitchSvc()
	resp, err := registerSwitchSvc.SuperCmsSetRegister(c, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (w *RegisterSwitchCtl) SuperCmsGetRegisterSwitch(c *gin.Context) {
	registerSwitchSvc := svc.NewRegisterSwitchSvc()
	resp, err := registerSwitchSvc.SuperCmsDetailRegister(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}
