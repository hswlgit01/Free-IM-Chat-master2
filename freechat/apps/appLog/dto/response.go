package dto

import (
	"github.com/openimsdk/chat/freechat/apps/appLog/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type AppLogResp struct {
	ID             primitive.ObjectID     `json:"id,omitempty"`
	OrgID          primitive.ObjectID     `json:"org_id"`
	UserID         string                 `json:"user_id"`
	ImServerUserID string                 `json:"im_server_user_id"`
	BatchID        string                 `json:"batch_id"`
	SessionID      string                 `json:"session_id"`
	DeviceID       string                 `json:"device_id"`
	Platform       int32                  `json:"platform"`
	SystemType     string                 `json:"system_type"`
	AppVersion     string                 `json:"app_version"`
	Level          string                 `json:"level"`
	Tag            string                 `json:"tag"`
	Message        string                 `json:"message"`
	Stack          string                 `json:"stack,omitempty"`
	Extra          map[string]interface{} `json:"extra,omitempty"`
	Reason         string                 `json:"reason"`
	SourceIP       string                 `json:"source_ip"`
	ClientTime     time.Time              `json:"client_time"`
	ServerTime     time.Time              `json:"server_time"`
	User           map[string]interface{} `json:"user"`
	Attribute      map[string]interface{} `json:"attribute"`
	OrgUser        map[string]interface{} `json:"org_user"`
}

func NewAppLogResp(row *model.AppLogJoinAll) *AppLogResp {
	extra := map[string]interface{}{}
	if row.Extra != nil {
		extra = bsonMToMap(row.Extra)
	}
	return &AppLogResp{
		ID:             row.ID,
		OrgID:          row.OrgID,
		UserID:         row.UserID,
		ImServerUserID: row.ImServerUserID,
		BatchID:        row.BatchID,
		SessionID:      row.SessionID,
		DeviceID:       row.DeviceID,
		Platform:       row.Platform,
		SystemType:     row.SystemType,
		AppVersion:     row.AppVersion,
		Level:          row.Level,
		Tag:            row.Tag,
		Message:        row.Message,
		Stack:          row.Stack,
		Extra:          extra,
		Reason:         row.Reason,
		SourceIP:       row.SourceIP,
		ClientTime:     row.ClientTime,
		ServerTime:     row.ServerTime,
		User:           row.User,
		Attribute:      row.Attribute,
		OrgUser:        row.OrgUser,
	}
}

func bsonMToMap(in bson.M) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
