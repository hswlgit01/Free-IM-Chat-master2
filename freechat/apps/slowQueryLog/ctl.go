package slowQueryLog

import (
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/slowQueryLog/dto"
	"github.com/openimsdk/chat/freechat/apps/slowQueryLog/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/tools/apiresp"
)

const maxSlowQueryPageSize int32 = 5000

type SlowQueryLogCtl struct{}

func NewSlowQueryLogCtl() *SlowQueryLogCtl {
	return &SlowQueryLogCtl{}
}

// dawn 2026-06-16 新增慢查询日志页面接口：后台查询 slow_query_log 并支持前端导出。
func (ctl *SlowQueryLogCtl) CmsList(c *gin.Context) {
	if _, err := middleware.GetOrgInfoFromCtx(c); err != nil {
		apiresp.GinError(c, err)
		return
	}
	page, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, freeErrors.PageParameterInvalidErr)
		return
	}
	normalizeSlowQueryPage(page)

	minDuration, err := queryInt64(c, "min_duration_ms")
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	startTime, err := queryTime(c, "start_time", "startTime")
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	endTime, err := queryTime(c, "end_time", "endTime")
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	resp, err := svc.NewSlowQueryLogSvc().CmsList(c.Request.Context(), dto.ListSlowQueryLogReq{
		Keyword:       c.Query("keyword"),
		Collection:    c.Query("collection"),
		Operation:     c.Query("operation"),
		MinDurationMS: minDuration,
		StartTime:     startTime,
		EndTime:       endTime,
	}, page)
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	apiresp.GinSuccess(c, resp)
}

func normalizeSlowQueryPage(page *paginationUtils.DepPagination) {
	if page.Page <= 0 {
		page.Page = 1
	}
	if page.PageSize <= 0 {
		page.PageSize = 10
	}
	if page.PageSize > maxSlowQueryPageSize {
		page.PageSize = maxSlowQueryPageSize
	}
}

func queryInt64(c *gin.Context, key string) (int64, error) {
	raw := strings.TrimSpace(c.Query(key))
	if raw == "" {
		return 0, nil
	}
	return strconv.ParseInt(raw, 10, 64)
}

func queryTime(c *gin.Context, keys ...string) (time.Time, error) {
	raw := ""
	for _, key := range keys {
		raw = strings.TrimSpace(c.Query(key))
		if raw != "" {
			break
		}
	}
	if raw == "" {
		return time.Time{}, nil
	}
	if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if v > 1_000_000_000_000 {
			return time.UnixMilli(v).UTC(), nil
		}
		return time.Unix(v, 0).UTC(), nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, raw, time.Local); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, freeErrors.ParameterInvalidErr
}
