package rtc

import (
	"fmt"
	"sync"
	"testing"

	"github.com/livekit/protocol/auth"
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
