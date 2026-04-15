package chat

import (
	"context"
	livestreamModel "github.com/openimsdk/chat/freechat/apps/livestream/model"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/chat/pkg/protocol/chat"
)

func (o *chatSvr) GetTokenForVideoMeeting(ctx context.Context, req *chat.GetTokenForVideoMeetingReq) (*chat.GetTokenForVideoMeetingResp, error) {
	if _, _, err := mctx.Check(ctx); err != nil {
		return nil, err
	}
	token, err := o.Livekit.GetLiveKitToken(req.Room, req.Identity)
	if err != nil {
		return nil, err
	}

	livestreamDao := livestreamModel.NewLivestreamUrlDao(o.redisCli)
	url, err := livestreamDao.AutomaticallySearchUrl(ctx, o.rpcChatConf.LiveKit.BackupUrls)
	if err != nil {
		return nil, err
	}

	return &chat.GetTokenForVideoMeetingResp{
		ServerUrl: url,
		Token:     token,
	}, err
}
