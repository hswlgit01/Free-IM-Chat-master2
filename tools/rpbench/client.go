package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// newHTTPClient 构造一个连接池足够大的 client。
// 压测 5000 并发时如果复用默认 Transport，MaxIdleConnsPerHost=2 会让绝大部分请求
// 卡在建连上，测出来的就是客户端的瓶颈而不是服务端的。
func newHTTPClient(maxConns int, timeout time.Duration) *http.Client {
	tr := &http.Transport{
		MaxIdleConns:        maxConns,
		MaxIdleConnsPerHost: maxConns,
		MaxConnsPerHost:     maxConns,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	return &http.Client{Transport: tr, Timeout: timeout}
}

var opSeq uint64

// newOperationID 生成 OpenIM / chat 侧要求的 operationID。
func newOperationID(prefix string) string {
	n := atomic.AddUint64(&opSeq, 1)
	return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatUint(n, 36)
}

// apiResp 是 chat / openim 统一的响应信封。
type apiResp struct {
	ErrCode int             `json:"errCode"`
	ErrMsg  string          `json:"errMsg"`
	ErrDlt  string          `json:"errDlt"`
	Data    json.RawMessage `json:"data"`
}

// postJSON 发一个 JSON POST 并解析统一信封。返回 data 段。
func postJSON(cli *http.Client, url string, headers map[string]string, body any) (json.RawMessage, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(raw), 300))
	}
	var out apiResp
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("bad json: %s", truncate(string(raw), 300))
	}
	if out.ErrCode != 0 {
		return nil, fmt.Errorf("errCode=%d %s %s", out.ErrCode, out.ErrMsg, truncate(out.ErrDlt, 200))
	}
	return out.Data, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ---------------------------------------------------------------- OpenIM API

type imClient struct {
	base   string
	cli    *http.Client
	secret string
	admin  string
	token  string
}

func newIMClient(cfg *Config, cli *http.Client) *imClient {
	return &imClient{base: cfg.IMAPI, cli: cli, secret: cfg.IMSecret, admin: cfg.IMAdmin}
}

// adminToken 取管理员 token 并缓存。
func (c *imClient) adminToken() (string, error) {
	if c.token != "" {
		return c.token, nil
	}
	data, err := postJSON(c.cli, c.base+"/auth/get_admin_token",
		map[string]string{"operationID": newOperationID("adm")},
		map[string]any{"secret": c.secret, "userID": c.admin})
	if err != nil {
		return "", fmt.Errorf("get_admin_token: %w", err)
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return "", err
	}
	c.token = out.Token
	return c.token, nil
}

func (c *imClient) headers() (map[string]string, error) {
	tok, err := c.adminToken()
	if err != nil {
		return nil, err
	}
	return map[string]string{"token": tok, "operationID": newOperationID("im")}, nil
}

// createGroup 建一个压测专用群，ownerUserID 为群主，memberIDs 为初始成员（IM 用户ID）。
func (c *imClient) createGroup(groupID, groupName, ownerUserID string, memberIDs []string) error {
	h, err := c.headers()
	if err != nil {
		return err
	}
	body := map[string]any{
		"memberUserIDs": memberIDs,
		"ownerUserID":   ownerUserID,
		"groupInfo": map[string]any{
			"groupID":   groupID,
			"groupName": groupName,
			"groupType": 2, // ReadGroupChatType
			"ownerUserID": ownerUserID,
		},
	}
	_, err = postJSON(c.cli, c.base+"/group/create_group", h, body)
	return err
}

// inviteToGroup 往已有群里追加成员（create_group 一次塞太多会超时，分批用这个）。
func (c *imClient) inviteToGroup(groupID string, memberIDs []string) error {
	h, err := c.headers()
	if err != nil {
		return err
	}
	_, err = postJSON(c.cli, c.base+"/group/invite_user_to_group", h, map[string]any{
		"groupID":        groupID,
		"invitedUserIDs": memberIDs,
		"reason":         "stress",
	})
	return err
}

// sendGroupText 以 sendID 的身份往群里发一条文本消息。
// 这条链路会走 toRedis -> msgtransfer -> toMongo / toPush，是我们要压的 IM 主链路。
func (c *imClient) sendGroupText(sendID, groupID, text string) error {
	h, err := c.headers()
	if err != nil {
		return err
	}
	content, _ := json.Marshal(map[string]any{"content": text})
	body := map[string]any{
		"sendID":          sendID,
		"groupID":         groupID,
		"senderNickname":  "stress",
		"senderPlatformID": 1,
		"sessionType":     3, // ReadGroupChatType
		"contentType":     101,
		"content":         map[string]any{"content": text},
		"isOnlineOnly":    false,
	}
	_ = content
	_, err = postJSON(c.cli, c.base+"/msg/send_msg", h, body)
	return err
}

// -------------------------------------------------------------- freechat API

type chatClient struct {
	base string
	cli  *http.Client
}

func newChatClient(cfg *Config, cli *http.Client) *chatClient {
	return &chatClient{base: cfg.ChatAPI, cli: cli}
}

// embedLoginResp 对应 svc.EmbedLoginResponse。
type embedLoginResp struct {
	UserID         string `json:"user_id"`
	ChatToken      string `json:"chat_token"`
	ImToken        string `json:"im_token"`
	OrganizationID string `json:"organization_id"`
}

// embedLogin 走嵌入式登录换一个 chat token。
// 请求体 {app_id, secret}，secret 是用组织 aesKeyBase64 加密的 EmbedLoginRequest；
// 响应体同样是加密的，需要用同一把 key 解回来。
func (c *chatClient) embedLogin(orgID, aesKey, thirdUserID, nickname string) (*embedLoginResp, error) {
	payload := map[string]any{
		"deviceID":      "stress-" + thirdUserID,
		"platform":      5, // Web
		"third_user_id": thirdUserID,
		"user": map[string]any{
			"nickname": nickname,
		},
	}
	plain, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	sec, err := aesEncrypt(plain, aesKey)
	if err != nil {
		return nil, err
	}
	data, err := postJSON(c.cli, c.base+"/third/account/embed/login",
		map[string]string{"operationID": newOperationID("login")},
		map[string]any{"app_id": orgID, "secret": sec})
	if err != nil {
		return nil, err
	}
	var wrapped struct {
		Secret string `json:"secret"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return nil, err
	}
	decoded, err := aesDecrypt(wrapped.Secret, aesKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt login resp: %w", err)
	}
	out := &embedLoginResp{}
	if err := json.Unmarshal(decoded, out); err != nil {
		return nil, err
	}
	return out, nil
}

// createRedPacket 发一个群红包，返回 transaction_id。
// transactionType: 2=普通红包(均分) 3=拼手气红包
func (c *chatClient) createRedPacket(token, orgID, groupID, currencyID, amount string, count, transactionType int, payPwd string) (string, error) {
	h := map[string]string{
		"token":       token,
		"orgid":       orgID,
		"operationID": newOperationID("create"),
		"Source":      "web",
	}
	body := map[string]any{
		"target_id":        groupID,
		"transaction_type": transactionType,
		"total_amount":     amount,
		"total_count":      count,
		"greeting":         "stress test",
		"pay_password":     payPwd,
		"currency_id":      currencyID,
		"wallet_info_owner_type": "ordinary",
	}
	data, err := postJSON(c.cli, c.base+"/third/transaction/create", h, body)
	if err != nil {
		return "", err
	}
	var out struct {
		TransactionID string `json:"transaction_id"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return "", err
	}
	return out.TransactionID, nil
}
