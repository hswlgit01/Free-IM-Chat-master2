// Package debugLog
// dawn 2026-04-27 新增：客户端 bug 排查用的轻量日志收集端点。
//
// 为什么不写库：
//   - 仅用于一次性排查（撤回 detail 字段名定位等），不希望多一个 Mongo TTL 索引和迁移负担
//   - 进程重启丢日志可接受，反正用户测一次就来取
//
// 安全：
//   - POST /debug/log 公开，但限制：单条 payload <= 8KB；环形缓冲只保留最近 200 条
//   - GET /debug/log 必须带 ?key=<DebugReadKey> 才返回，避免任何人都能拉
package debugLog

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	maxEntries     = 200
	maxPayloadSize = 8 * 1024
	// dawn 2026-04-27 临时调试 key，本批排查完后整模块直接删
	DebugReadKey = "im-revoke-debug-2026-04-27"
)

type entry struct {
	At     int64                  `json:"at"`
	Tag    string                 `json:"tag"`
	Data   map[string]interface{} `json:"data"`
	Source string                 `json:"source"`
}

var (
	mu      sync.Mutex
	entries = make([]entry, 0, maxEntries)
)

func push(e entry) {
	mu.Lock()
	defer mu.Unlock()
	entries = append(entries, e)
	if len(entries) > maxEntries {
		entries = entries[len(entries)-maxEntries:]
	}
}

// PostHandler 接收客户端日志。
func PostHandler(c *gin.Context) {
	if c.Request.ContentLength > maxPayloadSize {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"err": "payload too large"})
		return
	}
	var body struct {
		Tag  string                 `json:"tag" binding:"required"`
		Data map[string]interface{} `json:"data"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"err": err.Error()})
		return
	}
	if len(body.Tag) > 64 {
		c.JSON(http.StatusBadRequest, gin.H{"err": "tag too long"})
		return
	}
	push(entry{
		At:     time.Now().UnixMilli(),
		Tag:    body.Tag,
		Data:   body.Data,
		Source: c.ClientIP(),
	})
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// GetHandler 拉取最近的客户端日志。
func GetHandler(c *gin.Context) {
	if c.Query("key") != DebugReadKey {
		c.JSON(http.StatusUnauthorized, gin.H{"err": "bad key"})
		return
	}
	mu.Lock()
	out := make([]entry, len(entries))
	copy(out, entries)
	mu.Unlock()
	c.JSON(http.StatusOK, gin.H{"count": len(out), "entries": out})
}
