package message

import "testing"

// dawn 2026-05-06 修复消息查询 ArgsError：覆盖 CMS 搜索参数归一化。
func TestNormalizeSearchRequest(t *testing.T) {
	req := map[string]any{
		"sendID":      "",
		"recvID":      "  group001 ",
		"contentType": "101",
		"sessionType": float64(3),
		"sendTime":    "2026-05-06T00:00:00.000Z",
		"pagination": map[string]any{
			"pageNumber": "2",
			"showNumber": float64(20),
		},
	}

	got := normalizeSearchRequest(req)
	if _, ok := got["sendID"]; ok {
		t.Fatal("sendID should be omitted when empty")
	}
	if got["recvID"] != "group001" {
		t.Fatalf("recvID = %v, want group001", got["recvID"])
	}
	if got["contentType"] != int64(101) {
		t.Fatalf("contentType = %v, want 101", got["contentType"])
	}
	if got["sessionType"] != int64(3) {
		t.Fatalf("sessionType = %v, want 3", got["sessionType"])
	}
	if got["sendTime"] != "2026-05-06" {
		t.Fatalf("sendTime = %v, want 2026-05-06", got["sendTime"])
	}

	pagination, ok := got["pagination"].(map[string]any)
	if !ok {
		t.Fatalf("pagination has unexpected type %T", got["pagination"])
	}
	if pagination["pageNumber"] != int64(2) || pagination["showNumber"] != int64(20) {
		t.Fatalf("pagination = %v, want pageNumber=2 showNumber=20", pagination)
	}
}

// dawn 2026-05-06 修复聊天记录名称查询：覆盖发送者和接收者昵称过滤。
func TestFilterSearchResultByNickname(t *testing.T) {
	resp := map[string]any{
		"chatLogs": []any{
			map[string]any{
				"chatLog": map[string]any{
					"senderNickname": "zz",
					"recvNickname":   "aa",
				},
			},
			map[string]any{
				"chatLog": map[string]any{
					"senderNickname": "bb",
					"recvNickname":   "cc",
				},
			},
		},
		"chatLogsNum": 2,
	}

	got := filterSearchResult(resp, "z", "a", 1, 10).(map[string]any)
	logs := got["chatLogs"].([]any)
	if len(logs) != 1 {
		t.Fatalf("filtered log count = %d, want 1", len(logs))
	}
	if got["chatLogsNum"] != 1 {
		t.Fatalf("chatLogsNum = %v, want 1", got["chatLogsNum"])
	}
}

// dawn 2026-05-06 修复消息管理排序：覆盖发送者升序、同发送者时间倒序。
func TestSortSearchResult(t *testing.T) {
	resp := map[string]any{
		"chatLogs": []any{
			map[string]any{"chatLog": map[string]any{"senderNickname": "zz", "sendID": "2", "createTime": int64(1)}},
			map[string]any{"chatLog": map[string]any{"senderNickname": "aa", "sendID": "1", "createTime": int64(2)}},
			map[string]any{"chatLog": map[string]any{"senderNickname": "zz", "sendID": "2", "createTime": int64(3)}},
		},
	}

	got := sortSearchResult(resp).(map[string]any)
	logs := got["chatLogs"].([]any)
	first := logs[0].(map[string]any)["chatLog"].(map[string]any)
	second := logs[1].(map[string]any)["chatLog"].(map[string]any)
	third := logs[2].(map[string]any)["chatLog"].(map[string]any)

	if first["senderNickname"] != "aa" {
		t.Fatalf("first sender = %v, want aa", first["senderNickname"])
	}
	if second["createTime"] != int64(3) || third["createTime"] != int64(1) {
		t.Fatalf("zz rows not sorted by time desc: second=%v third=%v", second["createTime"], third["createTime"])
	}
}
