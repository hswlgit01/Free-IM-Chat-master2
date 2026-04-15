package friend

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/friend/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/protocol/relation"
	"github.com/openimsdk/tools/apiresp"
)

type FriendCtl struct{}

func NewFriendCtl() *FriendCtl {
	return &FriendCtl{}
}

func (a *FriendCtl) WebPostAddFriend(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req relation.ApplyToAddFriendReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	friendSvc := svc.NewFriendSvc()
	err = friendSvc.WebApplyToAddFriend(context.TODO(), org.Organization, operationID, req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}
