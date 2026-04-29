package dto

import "time"

type UploadAppLogReq struct {
	BatchID    string              `json:"batch_id"`
	Reason     string              `json:"reason"`
	DeviceID   string              `json:"device_id"`
	Platform   int32               `json:"platform"`
	SystemType string              `json:"system_type"`
	AppVersion string              `json:"app_version"`
	SessionID  string              `json:"session_id"`
	Logs       []*UploadAppLogItem `json:"logs" binding:"required"`
}

type UploadAppLogItem struct {
	Level      string         `json:"level"`
	Tag        string         `json:"tag"`
	Message    string         `json:"message" binding:"required"`
	Stack      string         `json:"stack"`
	ClientTime int64          `json:"client_time"`
	Extra      map[string]any `json:"extra"`
}

type ListAppLogReq struct {
	Keyword        string
	UserID         string
	ImServerUserID string
	Level          string
	DeviceID       string
	SessionID      string
	AppVersion     string
	Reason         string
	Platform       int32
	StartTime      time.Time
	EndTime        time.Time
}
