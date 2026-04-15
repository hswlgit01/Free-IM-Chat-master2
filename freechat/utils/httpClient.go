package utils

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// HTTPClient 是HTTP客户端的接口
type HTTPClient interface {
	Get(ctx context.Context, url string) ([]byte, error)
}

// DefaultHTTPClient 是默认的HTTP客户端实现
type DefaultHTTPClient struct {
	client *http.Client
}

// NewHTTPClient 创建一个新的HTTP客户端
func NewHTTPClient() HTTPClient {
	return &DefaultHTTPClient{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Get 发送GET请求并返回响应体
func (c *DefaultHTTPClient) Get(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

// UnmarshalJSON 将JSON数据解析为目标结构
func UnmarshalJSON(data []byte, target interface{}) error {
	return json.Unmarshal(data, target)
}
