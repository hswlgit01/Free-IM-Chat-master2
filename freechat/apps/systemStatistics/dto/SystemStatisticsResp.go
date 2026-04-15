package dto

// SystemStatisticsResp 是平台累计统计数据的对外返回结构。
// 兼容老字段（date/register/login/sign），并为新后台页面提供更丰富的统计字段。
type SystemStatisticsResp struct {
	// 原有字段（保持兼容）
	Date     string `json:"date" bson:"date"`         // 日期（格式：20060102）
	Register int32  `json:"register" bson:"register"` // 注册去重人数
	Login    int32  `json:"login" bson:"login"`       // 登录去重人数
	Sign     int32  `json:"sign" bson:"sign"`         // 签到去重人数

	// 新增字段：组织/用户维度信息
	UserName string `json:"user_name"` // 用户名称（此处为组织名称）
	UserID   string `json:"user_id"`   // 用户ID（此处为组织ID）

	// 新增字段：平台累计统计页面使用的指标
	NewRegisterCount int32 `json:"new_register_count"` // 新增人数（当日新增注册用户数）
	VerifiedCount    int32 `json:"verified_count"`     // 实名人数（当日完成实名认证的去重用户数）
	UnverifiedCount  int32 `json:"unverified_count"`   // 未实名人数（新增人数 - 实名人数，最低为0）
	CheckinCount     int32 `json:"checkin_count"`      // 签到人数（当日签到去重人数）
	SignByCreatedAt  int32 `json:"sign_by_created_at"` // 按创建时间统计的签到人数
	AllCheckinCount  int32 `json:"all_checkin_count"`  // 下级所有签到人数（当日所有被邀请用户签到总人数）

}
