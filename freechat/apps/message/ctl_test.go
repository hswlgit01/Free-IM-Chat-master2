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
