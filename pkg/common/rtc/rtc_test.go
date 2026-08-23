package rtc

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/livekit/protocol/auth"
	tencentyun "github.com/tencentyun/tls-sig-api-v2-golang/tencentyun"
)

func TestGetLiveKitTokenConcurrent(t *testing.T) {
	const (
		apiKey    = "test-key"
		apiSecret = "test-secret-with-enough-entropy"
		callers   = 100
	)
	liveKit := NewLiveKit(apiKey, apiSecret, "wss://rtc.example.com")

	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			room := fmt.Sprintf("room-%d", i)
			identity := fmt.Sprintf("user-%d", i)
			raw, err := liveKit.GetLiveKitToken(room, identity)
			if err != nil {
				errs <- err
				return
			}
			verifier, err := auth.ParseAPIToken(raw)
			if err != nil {
				errs <- err
				return
			}
			claims, err := verifier.Verify(apiSecret)
			if err != nil {
				errs <- err
				return
			}
			if verifier.Identity() != identity {
				errs <- fmt.Errorf("identity = %q, want %q", verifier.Identity(), identity)
				return
			}
			if claims.Video == nil || claims.Video.Room != room || !claims.Video.RoomJoin {
				errs <- fmt.Errorf("video grant = %#v, want room %q with join access", claims.Video, room)
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func TestTRTCIssueCredential(t *testing.T) {
	const (
		sdkAppID = 12345678
		secret   = "test-secret-with-enough-entropy"
		userID   = "user_123-abc"
		roomID   = "0db5b748-a9f2-4cf1-bd65-a3967f98d489"
	)
	ttl := time.Hour
	issuer, err := NewTRTC(sdkAppID, secret, ttl)
	if err != nil {
		t.Fatalf("NewTRTC() error = %v", err)
	}

	before := time.Now()
	credential, err := issuer.Issue(roomID, userID)
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	if credential.Provider != ProviderTRTC {
		t.Fatalf("provider = %q, want %q", credential.Provider, ProviderTRTC)
	}
	if credential.SDKAppID != sdkAppID || credential.UserID != userID || credential.RoomID != roomID {
		t.Fatalf("unexpected credential metadata: %#v", credential)
	}
	if credential.ServerURL != "" {
		t.Fatalf("TRTC server URL = %q, want empty", credential.ServerURL)
	}
	if err := tencentyun.VerifyUserSig(uint64(sdkAppID), secret, userID, credential.Token, time.Now()); err != nil {
		t.Fatalf("VerifyUserSig() error = %v", err)
	}
	if credential.ExpiresAt.Before(before.Add(ttl-time.Second)) || credential.ExpiresAt.After(time.Now().Add(ttl+time.Second)) {
		t.Fatalf("expiresAt = %v, want approximately now + %v", credential.ExpiresAt, ttl)
	}
}

func TestTRTCValidation(t *testing.T) {
	tests := []struct {
		name   string
		appID  int
		secret string
		ttl    time.Duration
		roomID string
		userID string
	}{
		{name: "missing app id", secret: "secret", ttl: time.Hour, roomID: "room", userID: "user"},
		{name: "missing secret", appID: 123, ttl: time.Hour, roomID: "room", userID: "user"},
		{name: "invalid user characters", appID: 123, secret: "secret", ttl: time.Hour, roomID: "room", userID: "user@example.com"},
		{name: "user too long", appID: 123, secret: "secret", ttl: time.Hour, roomID: "room", userID: strings.Repeat("u", maxTRTCUserID+1)},
		{name: "room too long", appID: 123, secret: "secret", ttl: time.Hour, roomID: strings.Repeat("r", maxTRTCRoomID+1), userID: "user"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			issuer, err := NewTRTC(tt.appID, tt.secret, tt.ttl)
			if err != nil {
				return
			}
			if _, err := issuer.Issue(tt.roomID, tt.userID); err == nil {
				t.Fatal("Issue() error = nil, want validation error")
			}
		})
	}
}
