package rtc

import (
	"github.com/livekit/protocol/auth"
	"time"
)

func NewLiveKit(key, secret, url string) *LiveKit {
	return &LiveKit{
		key:    key,
		secret: secret,
		url:    url,
	}
}

type LiveKit struct {
	key    string
	secret string
	url    string
}

func (l *LiveKit) GetLiveKitURL() string {
	return l.url
}

func (l *LiveKit) GetLiveKitToken(room string, identity string) (string, error) {
	grant := &auth.VideoGrant{
		RoomJoin: true,
		Room:     room,
	}
	return auth.NewAccessToken(l.key, l.secret).
		AddGrant(grant).
		SetIdentity(identity).
		SetValidFor(time.Hour).
		ToJWT()
}
