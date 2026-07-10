package ginUtils

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestGetClientIP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		realIP     string
		want       string
	}{
		{
			name:       "public direct ignores forged forwarding headers",
			remoteAddr: "8.8.8.8:43210",
			xff:        "1.2.3.4",
			realIP:     "5.6.7.8",
			want:       "8.8.8.8",
		},
		{
			name:       "loopback proxy accepts public xff",
			remoteAddr: "127.0.0.1:10008",
			xff:        "8.8.4.4",
			want:       "8.8.4.4",
		},
		{
			name:       "private proxy ignores spoofed xff prefix",
			remoteAddr: "172.18.0.5:10008",
			xff:        "1.2.3.4, 9.9.9.9",
			want:       "9.9.9.9",
		},
		{
			name:       "nginx overwritten real ip wins over xff",
			remoteAddr: "172.18.0.5:10008",
			xff:        "1.2.3.4, 9.9.9.9",
			realIP:     "8.8.8.8",
			want:       "8.8.8.8",
		},
		{
			name:       "trusted proxy chain is skipped from the right",
			remoteAddr: "10.0.0.2:10008",
			xff:        "8.8.8.8, 192.168.1.2, 127.0.0.1",
			want:       "8.8.8.8",
		},
		{
			name:       "trusted proxy falls back to real ip",
			remoteAddr: "[::1]:10008",
			realIP:     "[2001:4860:4860::8888]:443",
			want:       "2001:4860:4860::8888",
		},
		{
			name:       "all private xff preserves internal client",
			remoteAddr: "10.0.0.2:10008",
			xff:        "192.168.5.6, 172.16.0.3",
			want:       "192.168.5.6",
		},
		{
			name:       "remote ipv6 port is normalized",
			remoteAddr: "[2001:4860:4860::8844]:1234",
			xff:        "8.8.8.8",
			want:       "2001:4860:4860::8844",
		},
		{
			name:       "invalid peer cannot authorize headers",
			remoteAddr: "not-an-ip",
			xff:        "8.8.8.8",
			want:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			req.RemoteAddr = tt.remoteAddr
			if tt.xff != "" {
				req.Header.Set("X-Forwarded-For", tt.xff)
			}
			if tt.realIP != "" {
				req.Header.Set("X-Real-IP", tt.realIP)
			}
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = req
			if got := GetClientIP(ctx); got != tt.want {
				t.Fatalf("GetClientIP() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizeIP(t *testing.T) {
	tests := map[string]string{
		"1.2.3.4:80":       "1.2.3.4",
		" 1.2.3.4 ":        "1.2.3.4",
		"[2001:db8::1]:80": "2001:db8::1",
		"[2001:db8::1]":    "2001:db8::1",
		"garbage":          "",
	}
	for input, want := range tests {
		if got := NormalizeIP(input); got != want {
			t.Errorf("NormalizeIP(%q) = %q, want %q", input, got, want)
		}
	}
}
