package dto

// SubmitIdentityReq 提交身份认证请求
type SubmitIdentityReq struct {
	RealName     string `json:"realName" binding:"required"`     // 真实姓名
	IDCardNumber string `json:"idCardNumber" binding:"required"` // 身份证号
	IDCardFront  string `json:"idCardFront" binding:"required"`  // 身份证正面URL
	IDCardBack   string `json:"idCardBack" binding:"required"`   // 身份证反面URL
}

// SubmitIdentityResp 提交身份认证响应
type SubmitIdentityResp struct {
	Status int32 `json:"status"` // 提交后的状态（1-审核中）
}

// GetIdentityInfoResp 获取身份认证信息响应
type GetIdentityInfoResp struct {
	Status       int32  `json:"status"`       // 认证状态 0-待认证 1-审核中 2-已认证 3-已拒绝
	RealName     string `json:"realName"`     // 真实姓名
	IDCardNumber string `json:"idCardNumber"` // 身份证号
	IDCardFront  string `json:"idCardFront"`  // 身份证正面URL
	IDCardBack   string `json:"idCardBack"`   // 身份证反面URL
	RejectReason string `json:"rejectReason"` // 拒绝原因
	ApplyTime    int64  `json:"applyTime"`    // 申请时间（秒时间戳）
	VerifyTime   int64  `json:"verifyTime"`   // 审核时间（秒时间戳）
}

// AdminGetIdentityListReq 管理员获取认证列表请求
type AdminGetIdentityListReq struct {
	Status          *int32 `form:"status"`                                      // 状态筛选（可选）
	Keyword         string `form:"keyword"`                                     // 关键词搜索（可选）
	PageNumber      int32  `form:"pageNumber" binding:"required,min=1"`         // 页码
	ShowNumber      int32  `form:"showNumber" binding:"required,min=1,max=100"` // 每页数量
	OrderKey        string `form:"orderKey"`                                    // 排序字段
	OrderDirection  string `form:"orderDirection"`                              // 排序方向："asc"升序，"desc"降序
	StartTime       int64  `form:"start_time"`                                  // 提交开始时间（秒时间戳）
	EndTime         int64  `form:"end_time"`                                    // 提交结束时间（秒时间戳）
	VerifyStartTime int64  `form:"verify_start_time"`                           // 审核开始时间（秒时间戳）
	VerifyEndTime   int64  `form:"verify_end_time"`                             // 审核结束时间（秒时间戳）
}

func (r *AdminGetIdentityListReq) GetPageNumber() int32 {
	return r.PageNumber
}

func (r *AdminGetIdentityListReq) GetShowNumber() int32 {
	return r.ShowNumber
}

// AdminIdentityItem 管理员认证列表项
type AdminIdentityItem struct {
	UserID          string `json:"userID"`          // 用户ID
	Account         string `json:"account"`         // 账号
	Nickname        string `json:"nickname"`        // 昵称
	FaceURL         string `json:"faceURL"`         // 头像
	RealName        string `json:"realName"`        // 真实姓名
	IDCardNumber    string `json:"idCardNumber"`    // 身份证号（完整）
	IDCardFront     string `json:"idCardFront"`     // 身份证正面URL
	IDCardBack      string `json:"idCardBack"`      // 身份证反面URL
	Status          int32  `json:"status"`          // 状态
	ApplyTime       int64  `json:"applyTime"`       // 申请时间（秒时间戳，0表示无）
	VerifyTime      int64  `json:"verifyTime"`      // 审核时间（秒时间戳，0表示无）
	VerifyAdmin     string `json:"verifyAdmin"`     // 审核管理员ID
	VerifyAdminName string `json:"verifyAdminName"` // 审核管理员昵称
	RejectReason    string `json:"rejectReason"`    // 拒绝原因
}

// AdminGetIdentityListResp 管理员获取认证列表响应
type AdminGetIdentityListResp struct {
	Total int64                `json:"total"` // 总数
	List  []*AdminIdentityItem `json:"list"`  // 列表
}

// AdminGetIdentityDetailReq 按关键词查询单条实名详情（返回结构与 list 相同：total + list，通常 0 或 1 条）
// 详情接口实现为：organization_user 归属 + identity_verifications.Take；keyword 支持 chat user_id 或账号（account），不支持昵称模糊搜。
type AdminGetIdentityDetailReq struct {
	Keyword string `form:"keyword" binding:"required"` // chat user_id 或 attribute.account
}

// AdminApproveReq 管理员审核通过请求
type AdminApproveReq struct {
	UserID string `json:"userID" binding:"required"` // 用户ID
}

// AdminRejectReq 管理员审核拒绝请求
type AdminRejectReq struct {
	UserID       string `json:"userID" binding:"required"` // 用户ID
	RejectReason string `json:"rejectReason"`              // 拒绝原因（可选）
}

// AdminCancelVerificationReq 管理员取消实名认证请求
type AdminCancelVerificationReq struct {
	UserID string `json:"userID" binding:"required"` // 用户ID
}

// AdminGetPendingIdentityListReq 待审核实名列表（固定 status=审核中，单页最多 500 条，需翻页取全量）
type AdminGetPendingIdentityListReq struct {
	Keyword        string `form:"keyword"`
	PageNumber     int32  `form:"pageNumber"`
	ShowNumber     int32  `form:"showNumber"`
	OrderKey       string `form:"orderKey"`
	OrderDirection string `form:"orderDirection"`
	StartTime      int64  `form:"start_time"`
	EndTime        int64  `form:"end_time"`
}

// AdminApproveBatchReq 批量审核通过（与单条 POST /approve 逻辑一致）
type AdminApproveBatchReq struct {
	UserIDs []string `json:"userIDs" binding:"required,min=1,max=200"` // chat 侧 user_id
}

// AdminApproveBatchResp 批量审核结果
type AdminApproveBatchResp struct {
	Success int                   `json:"success"`
	Failed  []AdminApproveFailure `json:"failed"`
}

// AdminApproveFailure 单条失败原因
type AdminApproveFailure struct {
	UserID string `json:"userID"`
	ErrMsg string `json:"errMsg"`
}
