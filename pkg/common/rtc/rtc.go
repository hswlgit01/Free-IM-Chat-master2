package rtc

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/livekit/protocol/auth"
	tencentyun "github.com/tencentyun/tls-sig-api-v2-golang/tencentyun"
)

type Provider string

const (
	ProviderLiveKit Provider = "livekit"
	ProviderTRTC    Provider = "trtc"

	defaultTokenTTL = time.Hour
	maxTRTCUserID   = 32
	maxTRTCRoomID   = 64
)

var trtcUserIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Credential contains the provider-neutral result of issuing credentials for
// one participant. Token is a LiveKit JWT or a TRTC UserSig, depending on
// Provider.
type Credential struct {
	Provider  Provider
	ServerURL string
	Token     string
	SDKAppID  int
	UserID    string
	RoomID    string
	ExpiresAt time.Time
}

// Issuer creates short-lived credentials for one RTC provider.
type Issuer interface {
	Provider() Provider
	Issue(roomID, userID string) (*Credential, error)
}

func NewLiveKit(key, secret, url string) *LiveKit {
	return NewLiveKitWithTTL(key, secret, url, defaultTokenTTL)
}

func NewLiveKitWithTTL(key, secret, url string, ttl time.Duration) *LiveKit {
	if ttl <= 0 {
		ttl = defaultTokenTTL
	}
	return &LiveKit{
		key:    key,
		secret: secret,
		url:    url,
		ttl:    ttl,
	}
}

type LiveKit struct {
	key    string
	secret string
	url    string
	ttl    time.Duration
}

func (l *LiveKit) Provider() Provider {
	return ProviderLiveKit
}

func (l *LiveKit) GetLiveKitURL() string {
	return l.url
}

func (l *LiveKit) GetLiveKitToken(room string, identity string) (string, error) {
	credential, err := l.Issue(room, identity)
	if err != nil {
		return "", err
	}
	return credential.Token, nil
}

func (l *LiveKit) Issue(roomID, userID string) (*Credential, error) {
	if strings.TrimSpace(roomID) == "" {
		return nil, errors.New("RTC room ID cannot be empty")
	}
	if strings.TrimSpace(userID) == "" {
		return nil, errors.New("RTC user ID cannot be empty")
	}
	if strings.TrimSpace(l.key) == "" || strings.TrimSpace(l.secret) == "" {
		return nil, errors.New("LiveKit key and secret must be configured")
	}
	if strings.TrimSpace(l.url) == "" {
		return nil, errors.New("LiveKit server URL must be configured")
	}

	grant := &auth.VideoGrant{
		RoomJoin: true,
		Room:     roomID,
	}
	token, err := auth.NewAccessToken(l.key, l.secret).
		AddGrant(grant).
		SetIdentity(userID).
		SetValidFor(l.ttl).
		ToJWT()
	if err != nil {
		return nil, err
	}

	return &Credential{
		Provider:  ProviderLiveKit,
		ServerURL: l.url,
		Token:     token,
		UserID:    userID,
		RoomID:    roomID,
		ExpiresAt: time.Now().Add(l.ttl),
	}, nil
}

type TRTC struct {
	sdkAppID int
	secret   string
	ttl      time.Duration
}

func NewTRTC(sdkAppID int, secret string, ttl time.Duration) (*TRTC, error) {
	if sdkAppID <= 0 {
		return nil, errors.New("TRTC SDKAppID must be configured")
	}
	if strings.TrimSpace(secret) == "" {
		return nil, errors.New("TRTC SDKSecretKey must be configured")
	}
	if ttl <= 0 {
		ttl = defaultTokenTTL
	}
	if ttl < time.Second {
		return nil, errors.New("TRTC token TTL must be at least one second")
	}

	return &TRTC{sdkAppID: sdkAppID, secret: secret, ttl: ttl}, nil
}

func (t *TRTC) Provider() Provider {
	return ProviderTRTC
}

func (t *TRTC) Issue(roomID, userID string) (*Credential, error) {
	if err := validateTRTCRoomID(roomID); err != nil {
		return nil, err
	}
	if err := validateTRTCUserID(userID); err != nil {
		return nil, err
	}

	ttlSeconds := int(t.ttl / time.Second)
	userSig, err := tencentyun.GenUserSig(t.sdkAppID, t.secret, userID, ttlSeconds)
	if err != nil {
		return nil, fmt.Errorf("generate TRTC UserSig: %w", err)
	}

	return &Credential{
		Provider:  ProviderTRTC,
		Token:     userSig,
		SDKAppID:  t.sdkAppID,
		UserID:    userID,
		RoomID:    roomID,
		ExpiresAt: time.Now().Add(time.Duration(ttlSeconds) * time.Second),
	}, nil
}

func validateTRTCUserID(userID string) error {
	if userID == "" {
		return errors.New("TRTC user ID cannot be empty")
	}
	if len(userID) > maxTRTCUserID {
		return fmt.Errorf("TRTC user ID exceeds %d bytes", maxTRTCUserID)
	}
	if !trtcUserIDPattern.MatchString(userID) {
		return errors.New("TRTC user ID may contain only letters, digits, underscores, and hyphens")
	}
	return nil
}

func validateTRTCRoomID(roomID string) error {
	if roomID == "" {
		return errors.New("TRTC room ID cannot be empty")
	}
	if len(roomID) > maxTRTCRoomID {
		return fmt.Errorf("TRTC string room ID exceeds %d bytes", maxTRTCRoomID)
	}
	return nil
}
