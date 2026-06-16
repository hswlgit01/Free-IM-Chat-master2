package dto

import "time"

type ListSlowQueryLogReq struct {
	Keyword       string
	Collection    string
	Operation     string
	MinDurationMS int64
	StartTime     time.Time
	EndTime       time.Time
}
