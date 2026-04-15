package utils

import (
	"time"
)

// 时区处理：
// 2024-01-12修改：签到功能全部使用中国标准时间(CST, UTC+8)处理，
// 解决了24点后签到按钮状态不更新的问题。此问题的根源是之前系统使用UTC时间判断"今天"，
// 而客户端使用本地时间，导致在00:00-08:00之间出现不一致。
// 现在系统统一使用CST时区，确保前后端时间处理一致。
//
// 重要更新 2026-01-13：
// 1. 签到系统时区处理规范：
//    - 所有时间戳参数转换为CST时区处理 (使用QueryToCstTime函数)
//    - API过滤器使用"date"字段而非"created_at"字段进行时间范围查询
//    - 返回给前端的所有时间字段都经过TimeToCST转换，确保时区一致
// 2. 重复签到记录处理：
//    - 在返回给前端前，过滤同一天的重复签到记录，只保留最新的记录
// 3. 命名规范：
//    - 废弃旧函数名QueryToUtcTime (实际返回CST)，新代码使用QueryToCstTime

// CST 中国标准时间 (China Standard Time), UTC+8
var CST *time.Location

func init() {
	var err error
	CST, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		// 如果无法加载时区，使用固定偏移量
		CST = time.FixedZone("CST", 8*60*60) // UTC+8小时
	}
}

// NowCST 获取当前中国标准时间
func NowCST() time.Time {
	return time.Now().In(CST)
}

// TodayCST 获取今天的日期（中国标准时间）
func TodayCST() time.Time {
	now := NowCST()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, CST)
}

// YesterdayCST 获取昨天的日期（中国标准时间）
func YesterdayCST() time.Time {
	yesterday := NowCST().AddDate(0, 0, -1)
	return time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, CST)
}

// TimeToCST 将任意时间转换为中国标准时间
func TimeToCST(t time.Time) time.Time {
	return t.In(CST)
}
