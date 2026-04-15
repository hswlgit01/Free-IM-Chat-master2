package rtc

import (
	"context"
	"github.com/openimsdk/chat/freechat/apps/rtc/svc"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/errs"
)

// RtcCtl 处理RTC相关的HTTP请求
type RtcCtl struct{}

// NewRtcCtl 创建一个新的RTC控制器实例
func NewRtcCtl() *RtcCtl {
	return &RtcCtl{}
}

// PostGetTokenForVideoCall 获取一对一音视频通话的Token
func (c *RtcCtl) PostGetTokenForVideoCall(ctx *gin.Context) {
	// 1. 获取当前用户ID并验证权限
	//opUserID, _, err := mctx.Check(ctx)
	//if err != nil {
	//	apiresp.GinError(ctx, err)
	//	return
	//}

	// 2. 解析请求参数
	var req struct {
		Room     string `json:"room" form:"room" binding:"required"`
		Identity string `json:"identity" form:"identity" binding:"required"`
	}
	if err := ctx.ShouldBind(&req); err != nil {
		apiresp.GinError(ctx, errs.New("无效的请求参数"))
		return
	}

	// 3. 对音视频通话的基本验证
	// 注意：我们允许用户获取其他用户的Token，因为在音视频通话中可能需要获取对方的Token
	// 如果有更精细的权限控制需求，例如检查是否在同一房间、是否有通话权限等，应该在此处添加
	//log.ZInfo(ctx, "用户请求视频通话Token", "requestUser", opUserID, "targetUser", req.Identity, "room", req.Room)

	// 4. 创建RTC服务并获取Token
	rtcSvc := svc.NewRtcService()

	// 创建请求对象
	request := &svc.GetTokenForVideoCallRequest{
		Room:     req.Room,
		Identity: req.Identity,
	}

	// 调用服务获取Token
	resp, err := rtcSvc.GetTokenForVideoCall(context.Background(), request)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	// 5. 直接返回结果，不使用apiresp.GinSuccess包装，保持与原接口返回格式一致
	ctx.JSON(http.StatusOK, resp)
}
