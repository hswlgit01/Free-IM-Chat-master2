package netUtils

import (
	"net/url"
	"strings"
)

func ParseURL(rawURL string) (scheme, host, port string, err error) {
	// 如果 URL 没有协议前缀，默认添加 "http://" 以便正确解析
	if !strings.Contains(rawURL, "://") {
		rawURL = "http://" + rawURL
	}

	// 解析 URL
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", "", err
	}

	// 提取协议（http/https）
	scheme = u.Scheme

	// 提取域名和端口
	host = u.Hostname()
	port = u.Port()

	// 如果端口为空，根据协议设置默认端口
	if port == "" {
		switch scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}

	return scheme, host, port, nil
}
