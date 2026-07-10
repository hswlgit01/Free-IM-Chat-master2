package ginUtils

import (
	"net"
	"strings"

	"github.com/gin-gonic/gin"
)

// NormalizeIP strips an optional port/IPv6 zone and returns a canonical IP.
// Invalid input is represented as an empty string so callers never persist a
// header verbatim as an account binding.
func NormalizeIP(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(raw); err == nil {
		raw = host
	} else if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		raw = strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]")
	}
	if zoneAt := strings.LastIndexByte(raw, '%'); zoneAt >= 0 {
		raw = raw[:zoneAt]
	}
	ip := net.ParseIP(strings.TrimSpace(raw))
	if ip == nil {
		return ""
	}
	return ip.String()
}

func isTrustedProxyIP(ip net.IP) bool {
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
}

func GetClientIP(c *gin.Context) string {
	peerString := NormalizeIP(c.Request.RemoteAddr)
	peer := net.ParseIP(peerString)
	if peer == nil {
		return ""
	}

	// Forwarding headers are caller-controlled on a direct connection. Only a
	// loopback/private peer is considered infrastructure allowed to assert the
	// original client address.
	if !isTrustedProxyIP(peer) {
		return peerString
	}

	// nginx is configured to overwrite X-Real-IP with $remote_addr, making it
	// the least ambiguous single-hop source. A direct public peer never reaches
	// this branch, so it cannot forge the value.
	if realIP := NormalizeIP(c.GetHeader("X-Real-IP")); realIP != "" {
		return realIP
	}

	if forwarded := c.GetHeader("X-Forwarded-For"); forwarded != "" {
		parts := strings.Split(forwarded, ",")
		valid := make([]string, 0, len(parts))
		for _, part := range parts {
			if candidate := NormalizeIP(part); candidate != "" {
				valid = append(valid, candidate)
			}
		}
		// Walk from the proxy nearest to us towards the client and take the
		// first non-trusted hop. This prevents a public client-supplied prefix
		// from winning when nginx uses $proxy_add_x_forwarded_for.
		for i := len(valid) - 1; i >= 0; i-- {
			if !isTrustedProxyIP(net.ParseIP(valid[i])) {
				return valid[i]
			}
		}
		// Internal clients can legitimately have only private hops.
		if len(valid) > 0 {
			return valid[0]
		}
	}

	return peerString
}
