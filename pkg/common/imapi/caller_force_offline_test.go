package imapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	constantpb "github.com/openimsdk/chat/pkg/constant"
	"github.com/openimsdk/protocol/auth"
)

func TestForceOffLineIncludesH5AndAggregatesErrors(t *testing.T) {
	var (
		mu        sync.Mutex
		platforms = make(map[int32]int)
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/auth/force_logout" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var req auth.ForceLogoutReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		mu.Lock()
		platforms[req.PlatformID]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if req.PlatformID == int32(constantpb.H5PlatformID) {
			_, _ = fmt.Fprint(w, `{"errCode":500,"errMsg":"h5 failure","data":{}}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"errCode":0,"errMsg":"","data":{}}`)
	}))
	defer server.Close()

	caller := &Caller{imApi: server.URL}
	err := caller.ForceOffLine(context.Background(), "im-user")
	if err == nil || !strings.Contains(err.Error(), "H5") {
		t.Fatalf("ForceOffLine() error = %v, want aggregated H5 error", err)
	}
	for platformID := range constantpb.PlatformID2Name {
		if platforms[int32(platformID)] != 1 {
			t.Errorf("platform %d calls = %d, want 1", platformID, platforms[int32(platformID)])
		}
	}
	if platforms[int32(constantpb.H5WebPlatformID)] != 1 {
		t.Fatalf("H5Web calls = %d, want 1", platforms[int32(constantpb.H5WebPlatformID)])
	}
}
