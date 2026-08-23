package chat

import (
	"context"

	livestreamModel "github.com/openimsdk/chat/freechat/apps/livestream/model"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/chat/pkg/common/rtc"
	"github.com/openimsdk/chat/pkg/protocol/chat"
	"github.com/openimsdk/tools/errs"
)

func (o *chatSvr) GetTokenForVideoMeeting(ctx context.Context, req *chat.GetTokenForVideoMeetingReq) (*chat.GetTokenForVideoMeetingResp, error) {
	opUserID, _, err := mctx.Check(ctx)
	if err != nil {
		return nil, err
	}
	if req.Room == "" {
		return nil, errs.ErrArgs.WrapMsg("RTC room ID cannot be empty")
	}

	identity, err := o.resolveRTCIdentity(ctx, opUserID, req.Identity)
	if err != nil {
		return nil, err
	}

	issuer, err := o.getRTCIssuer(ctx)
	if err != nil {
		return nil, err
	}
	credential, err := issuer.Issue(req.Room, identity)
	if err != nil {
		return nil, errs.WrapMsg(err, "issue RTC credential")
	}

	resp := &chat.GetTokenForVideoMeetingResp{
		ServerUrl: credential.ServerURL,
		Token:     credential.Token,
		Provider:  string(credential.Provider),
		SdkAppId:  int64(credential.SDKAppID),
		UserID:    credential.UserID,
		RoomID:    credential.RoomID,
		ExpiresAt: credential.ExpiresAt.Unix(),
	}
	if credential.Provider == rtc.ProviderTRTC {
		resp.UserSig = credential.Token
	}
	return resp, nil
}

func (o *chatSvr) resolveRTCIdentity(ctx context.Context, opUserID, requestedIdentity string) (string, error) {
	orgUsers, err := o.Database.FindOrgUserByUserIds(ctx, []string{opUserID})
	if err != nil {
		return "", errs.WrapMsg(err, "resolve authenticated RTC user")
	}
	if len(orgUsers) != 1 || orgUsers[0] == nil || orgUsers[0].ImServerUserId == "" {
		return "", errs.ErrNoPermission.WrapMsg("authenticated RTC user is not mapped to an IM user")
	}

	return validateRTCIdentity(orgUsers[0].ImServerUserId, requestedIdentity)
}

func validateRTCIdentity(authenticatedIdentity, requestedIdentity string) (string, error) {
	if authenticatedIdentity == "" {
		return "", errs.ErrNoPermission.WrapMsg("authenticated RTC identity is empty")
	}
	if requestedIdentity != "" && requestedIdentity != authenticatedIdentity {
		return "", errs.ErrNoPermission.WrapMsg("RTC identity does not match authenticated user")
	}
	return authenticatedIdentity, nil
}

func (o *chatSvr) getRTCIssuer(ctx context.Context) (rtc.Issuer, error) {
	switch o.RTCProvider {
	case rtc.ProviderTRTC:
		if o.RTCIssuer == nil {
			return nil, errs.New("TRTC credential issuer is not initialized")
		}
		return o.RTCIssuer, nil
	case rtc.ProviderLiveKit:
		livestreamDao := livestreamModel.NewLivestreamUrlDao(o.redisCli)
		url, err := livestreamDao.AutomaticallySearchPublicUrl(
			ctx,
			o.rpcChatConf.LiveKit.BackupUrls,
			o.rpcChatConf.LiveKit.URL,
		)
		if err != nil {
			return nil, err
		}
		return rtc.NewLiveKitWithTTL(
			o.rpcChatConf.LiveKit.Key,
			o.rpcChatConf.LiveKit.Secret,
			url,
			o.RTCTokenTTL,
		), nil
	default:
		return nil, errs.New("unsupported RTC provider: " + string(o.RTCProvider))
	}
}
