package svc

import (
	"context"
	"strings"
	"time"

	"github.com/openimsdk/chat/freechat/apps/appLog/dto"
	"github.com/openimsdk/chat/freechat/apps/appLog/model"
	organizationModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	maxUploadBatchSize = 500
	maxMessageRunes    = 4000
	maxStackRunes      = 12000
	maxTagRunes        = 80
	maxReasonRunes     = 80
)

type AppLogSvc struct{}

func NewAppLogSvc() *AppLogSvc {
	return &AppLogSvc{}
}

func (s *AppLogSvc) Upload(ctx context.Context, orgID primitive.ObjectID, orgUser *organizationModel.OrganizationUser, sourceIP string, req *dto.UploadAppLogReq) error {
	if req == nil || len(req.Logs) == 0 {
		return nil
	}
	if len(req.Logs) > maxUploadBatchSize {
		return freeErrors.ApiErr("App log batch is too large")
	}

	now := time.Now().UTC()
	rows := make([]*model.AppLog, 0, len(req.Logs))
	for _, item := range req.Logs {
		if item == nil || strings.TrimSpace(item.Message) == "" {
			continue
		}
		rows = append(rows, &model.AppLog{
			ID:             primitive.NewObjectID(),
			OrgID:          orgID,
			UserID:         orgUser.UserId,
			ImServerUserID: orgUser.ImServerUserId,
			BatchID:        trimRunes(req.BatchID, 80),
			SessionID:      trimRunes(req.SessionID, 120),
			DeviceID:       trimRunes(req.DeviceID, 160),
			Platform:       req.Platform,
			SystemType:     trimRunes(req.SystemType, 80),
			AppVersion:     trimRunes(req.AppVersion, 80),
			Level:          normalizeLevel(item.Level),
			Tag:            trimRunes(item.Tag, maxTagRunes),
			Message:        trimRunes(item.Message, maxMessageRunes),
			Stack:          trimRunes(item.Stack, maxStackRunes),
			Extra:          normalizeExtra(item.Extra),
			Reason:         trimRunes(req.Reason, maxReasonRunes),
			SourceIP:       trimRunes(sourceIP, 80),
			ClientTime:     parseClientTime(item.ClientTime, now),
			ServerTime:     now,
		})
	}
	if len(rows) == 0 {
		return nil
	}

	dao := model.NewAppLogDao(plugin.MongoCli().GetDB())
	return dao.InsertMany(ctx, rows)
}

func (s *AppLogSvc) CmsList(ctx context.Context, orgID primitive.ObjectID, req dto.ListAppLogReq, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.AppLogResp], error) {
	dao := model.NewAppLogDao(plugin.MongoCli().GetDB())
	level := strings.TrimSpace(req.Level)
	if level != "" {
		level = normalizeLevel(level)
	}
	total, rows, err := dao.Search(ctx, model.AppLogSearchFilter{
		OrgID:          orgID,
		Keyword:        strings.TrimSpace(req.Keyword),
		UserID:         strings.TrimSpace(req.UserID),
		ImServerUserID: strings.TrimSpace(req.ImServerUserID),
		Level:          level,
		DeviceID:       strings.TrimSpace(req.DeviceID),
		SessionID:      strings.TrimSpace(req.SessionID),
		AppVersion:     strings.TrimSpace(req.AppVersion),
		Reason:         strings.TrimSpace(req.Reason),
		Platform:       req.Platform,
		StartTime:      req.StartTime,
		EndTime:        req.EndTime,
	}, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.AppLogResp]{
		Total: total,
		List:  make([]*dto.AppLogResp, 0, len(rows)),
	}
	for _, row := range rows {
		resp.List = append(resp.List, dto.NewAppLogResp(row))
	}
	return resp, nil
}

func normalizeLevel(level string) string {
	level = strings.ToUpper(strings.TrimSpace(level))
	switch level {
	case "D", "DEBUG":
		return "DEBUG"
	case "I", "INFO":
		return "INFO"
	case "W", "WARN", "WARNING":
		return "WARN"
	case "E", "ERROR":
		return "ERROR"
	case "F", "FATAL":
		return "FATAL"
	default:
		if level == "" {
			return "INFO"
		}
		return trimRunes(level, 20)
	}
}

func parseClientTime(raw int64, fallback time.Time) time.Time {
	if raw <= 0 {
		return fallback
	}
	if raw > 1_000_000_000_000 {
		return time.UnixMilli(raw).UTC()
	}
	return time.Unix(raw, 0).UTC()
}

func normalizeExtra(extra map[string]any) bson.M {
	if len(extra) == 0 {
		return nil
	}
	out := bson.M{}
	i := 0
	for k, v := range extra {
		if i >= 30 {
			break
		}
		key := trimRunes(strings.TrimSpace(k), 80)
		if key == "" {
			continue
		}
		out[key] = normalizeExtraValue(v)
		i++
	}
	return out
}

func normalizeExtraValue(v any) any {
	switch val := v.(type) {
	case string:
		return trimRunes(val, 1000)
	case map[string]any:
		return normalizeExtra(val)
	case []any:
		if len(val) > 30 {
			val = val[:30]
		}
		out := make([]any, 0, len(val))
		for _, item := range val {
			out = append(out, normalizeExtraValue(item))
		}
		return out
	default:
		return val
	}
}

func trimRunes(s string, limit int) string {
	if limit <= 0 {
		return ""
	}
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit])
}
