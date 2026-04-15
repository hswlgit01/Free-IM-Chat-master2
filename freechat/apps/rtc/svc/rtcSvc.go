package svc

import (
	"context"
	"github.com/livekit/protocol/auth"
	livestreamModel "github.com/openimsdk/chat/freechat/apps/livestream/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"time"

	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/tools/errs"
	// 假设DTO请求/响应将在此处定义或导入
	// "github.com/path/to/your/project/freechat/dto"
	// 假设上下文助手用于用户ID
	// "github.com/path/to/your/project/pkg/common/mctx"
)

// RtcService 处理RTC相关的业务逻辑
type RtcService struct {
}

// NewRtcService 创建一个新的RtcService实例
func NewRtcService() *RtcService {
	return &RtcService{}
}

// GetTokenForVideoCallRequest 表示视频通话令牌的请求结构
type GetTokenForVideoCallRequest struct {
	Room     string // 通话的房间ID
	Identity string // 参与者的用户ID
}

// GetTokenForVideoCallResponse 表示视频通话令牌的响应结构
type GetTokenForVideoCallResponse struct {
	ServerUrl string // LiveKit服务器URL
	Token     string // 用于认证的JWT令牌
}

// GetTokenForVideoCall 处理为视频通话生成LiveKit令牌
func (s *RtcService) GetTokenForVideoCall(ctx context.Context, req *GetTokenForVideoCallRequest) (*GetTokenForVideoCallResponse, error) {
	// 输入验证
	if req.Room == "" || req.Identity == "" {
		return nil, errs.NewCodeError(freeErrors.ErrInvalidParams, "room and identity cannot be empty")
	}

	// 使用LiveKitService生成令牌
	token, err := s.InternalGenerateToken(req.Room, req.Identity)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	// 获取LiveKit服务器URL
	livestreamDao := livestreamModel.NewLivestreamUrlDao(plugin.RedisCli())
	serverURL, err := livestreamDao.AutomaticallySearchUrl(ctx, plugin.ChatCfg().ChatRpcConfig.LiveKit.BackupUrls)
	if err != nil {
		return nil, err
	}

	// 构建并返回响应
	resp := &GetTokenForVideoCallResponse{
		ServerUrl: serverURL,
		Token:     token,
	}

	return resp, nil
}

// InternalGenerateToken 生成用于加入LiveKit房间的JWT令牌
func (s *RtcService) InternalGenerateToken(room string, identity string) (string, error) {
	config := plugin.ChatCfg()

	// 定义授权权限 - 允许房间加入
	grant := &auth.VideoGrant{
		RoomJoin: true,
		Room:     room,
		// 根据需要添加其他权限（例如，CanPublish，CanSubscribe）
	}

	token := auth.NewAccessToken(
		config.ChatRpcConfig.LiveKit.Key,
		config.ChatRpcConfig.LiveKit.Secret,
	)

	// 生成带有身份、有效期和授权的令牌
	// 使用原始代码中的1小时有效期
	return token.AddGrant(grant).
		SetIdentity(identity).
		SetValidFor(time.Hour). // 令牌有效期
		ToJWT()
}
