package svc

import (
	"context"
	"github.com/openimsdk/chat/freechat/apps/platformConfig/model"
	"github.com/openimsdk/chat/freechat/plugin"
)

type RegisterSwitchSvc struct {
}

func NewRegisterSwitchSvc() *RegisterSwitchSvc {
	return &RegisterSwitchSvc{}
}

type SuperCmsGetRegisterResp struct {
	Register bool `json:"register"`
}

func (w *RegisterSwitchSvc) SuperCmsDetailRegister(ctx context.Context) (*SuperCmsGetRegisterResp, error) {
	registerSwitchDao := model.NewRegisterSwitchDao(plugin.RedisCli())

	closeRegister, err := registerSwitchDao.IsOpenRegister(ctx)
	if err != nil {
		return nil, err
	}

	resp := &SuperCmsGetRegisterResp{
		Register: closeRegister,
	}
	return resp, nil
}

type SuperCmsSetRegisterReq struct {
	Register bool `json:"register"`
}

func (w *RegisterSwitchSvc) SuperCmsSetRegister(ctx context.Context, req *SuperCmsSetRegisterReq) (*SuperCmsGetRegisterResp, error) {
	registerSwitchDao := model.NewRegisterSwitchDao(plugin.RedisCli())

	if req.Register {
		err := registerSwitchDao.OpenRegister(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		err := registerSwitchDao.CloseRegister(ctx)
		if err != nil {
			return nil, err
		}
	}

	closeRegister, err := registerSwitchDao.IsOpenRegister(ctx)
	if err != nil {
		return nil, err
	}

	resp := &SuperCmsGetRegisterResp{
		Register: closeRegister,
	}
	return resp, nil
}
