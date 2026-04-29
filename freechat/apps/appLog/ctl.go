package appLog

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/appLog/dto"
	"github.com/openimsdk/chat/freechat/apps/appLog/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/tools/apiresp"
)

const uploadBodyLimitBytes int64 = 1024 * 1024

type AppLogCtl struct{}

func NewAppLogCtl() *AppLogCtl {
	return &AppLogCtl{}
}

func (ctl *AppLogCtl) Upload(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, uploadBodyLimitBytes)

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.UploadAppLogReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	err = svc.NewAppLogSvc().Upload(c.Request.Context(), org.ID, org.OrgUser, c.ClientIP(), &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]any{})
}

func (ctl *AppLogCtl) CmsList(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	page, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, freeErrors.PageParameterInvalidErr)
		return
	}

	platform, err := queryInt32(c, "platform")
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

	resp, err := svc.NewAppLogSvc().CmsList(c.Request.Context(), org.ID, dto.ListAppLogReq{
		Keyword:        c.Query("keyword"),
		UserID:         c.Query("user_id"),
		ImServerUserID: c.Query("im_server_user_id"),
		Level:          c.Query("level"),
		DeviceID:       c.Query("device_id"),
		SessionID:      c.Query("session_id"),
		AppVersion:     c.Query("app_version"),
		Reason:         c.Query("reason"),
		Platform:       platform,
		StartTime:      startTime,
		EndTime:        endTime,
	}, page)
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}

	apiresp.GinSuccess(c, resp)
}

func queryInt32(c *gin.Context, key string) (int32, error) {
	raw := strings.TrimSpace(c.Query(key))
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(v), nil
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
