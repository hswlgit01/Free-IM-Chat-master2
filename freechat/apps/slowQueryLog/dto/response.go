package dto

import (
	"time"

	"github.com/openimsdk/chat/freechat/apps/slowQueryLog/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type SlowQueryLogResp struct {
	ID            primitive.ObjectID `json:"id,omitempty"`
	CreatedAt     time.Time          `json:"created_at"`
	Timestamp     string             `json:"timestamp"`
	Collection    string             `json:"collection"`
	Operation     string             `json:"operation"`
	CompleteQuery string             `json:"complete_query"`
	Duration      string             `json:"duration"`
	DurationMS    int64              `json:"duration_ms"`
	Error         string             `json:"error,omitempty"`
}

func NewSlowQueryLogResp(row *model.SlowQueryLog) *SlowQueryLogResp {
	return &SlowQueryLogResp{
		ID:            row.ID,
		CreatedAt:     row.CreatedAt,
		Timestamp:     row.Timestamp,
		Collection:    row.Collection,
		Operation:     row.Operation,
		CompleteQuery: row.CompleteQuery,
		Duration:      row.Duration,
		DurationMS:    row.DurationMS,
		Error:         row.Error,
	}
}
