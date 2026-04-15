package ginUtils

import (
	"github.com/gin-gonic/gin"
	"strings"
)

func GetClientIP(c *gin.Context) string {
	// 尝试从 X-Forwarded-For 获取
	forwarded := c.Request.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		// X-Forwarded-For 可能包含多个IP（代理链），第一个是原始IP
		ips := strings.Split(forwarded, ",")
		return strings.TrimSpace(ips[0])
	}

	// 尝试从 X-Real-IP 获取
	realIP := c.Request.Header.Get("X-Real-IP")
	if realIP != "" {
		return realIP
	}

	// 回退到 RemoteAddr
	return c.Request.RemoteAddr
}
