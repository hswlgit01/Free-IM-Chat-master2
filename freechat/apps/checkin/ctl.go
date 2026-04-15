package checkin

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/checkin/model"
	"github.com/openimsdk/chat/freechat/apps/checkin/svc"
	opModel "github.com/openimsdk/chat/freechat/apps/operationLog/model"
	opSvc "github.com/openimsdk/chat/freechat/apps/operationLog/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/ginUtils"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/log"
)

type CheckinCtl struct{}

func NewCheckinCtl() *CheckinCtl {
	return &CheckinCtl{}
}

func (w *CheckinCtl) WebPostCreateCheckin(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	checkInSvc := svc.NewCheckinSvc()
	resp, err := checkInSvc.WebCreateCheckin(c, opUserID, org.Organization)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (w *CheckinCtl) WebGetDetailCheckin(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	startTimeCST, err := ginUtils.QueryToCstTime(c, "startTime")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	endTimeCST, err := ginUtils.QueryToCstTime(c, "endTime")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	checkInSvc := svc.NewCheckinSvc()
	resp, err := checkInSvc.WebDetailUserCheckin(c, org.OrgUser, startTimeCST, endTimeCST)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (w *CheckinCtl) CmsGetListOrgUserCheckin(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	pagination, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 解析时间参数
	startTimeCST, err := ginUtils.QueryToCstTime(c, "startTime")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	endTimeCST, err := ginUtils.QueryToCstTime(c, "endTime")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	keyword := strings.TrimSpace(c.Query("keyword"))
	imServerUserId := c.Query("imServerUserId")

	// 如果前端未传时间范围，并且未指定具体用户，则默认限制为最近30天，避免全表扫描
	// 当指定了 imServerUserId 时，不强制时间范围，允许查看该用户的全部签到历史
	if startTimeCST.IsZero() && endTimeCST.IsZero() && imServerUserId == "" {
		endTimeCST = time.Now()
		startTimeCST = endTimeCST.AddDate(0, 0, -30)
	}
	checkInSvc := svc.NewCheckinSvc()
	// 传递用户ID与时间参数给服务层；未传 imServerUserId 时仅按 keyword / 时间筛选
	resp, err := checkInSvc.CmsListUserCheckin(c, imServerUserId, keyword, org.ID, startTimeCST, endTimeCST, pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

type CheckinRewardCfgCtl struct{}

func NewCheckinRewardCfgCtl() *CheckinRewardCfgCtl {
	return &CheckinRewardCfgCtl{}
}

func (w *CheckinRewardCfgCtl) CmsPostCreateCheckinRewardCfg(c *gin.Context) {
	var req svc.CmsCreateCheckinRewardCfgReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	checkInSvc := svc.NewCheckinRewardConfigSvc()
	err = checkInSvc.CmsCreateCheckinRewardCfg(c, opUserID, org.Organization, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeCreateCheckinRewardCfg,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *CheckinRewardCfgCtl) CmsPostDeleteCheckinRewardCfg(c *gin.Context) {
	var req svc.CmsDeleteCheckinRewardCfgReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	checkInSvc := svc.NewCheckinRewardConfigSvc()
	err = checkInSvc.CmsDeleteCheckinRewardCfg(c, opUserID, org.Organization, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeDeleteCheckinRewardCfg,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *CheckinRewardCfgCtl) CmsGetListCheckinRewardCfg(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	pagination, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	checkInCfgSvc := svc.NewCheckinRewardConfigSvc()
	resp, err := checkInCfgSvc.CmsListCheckinRewardCfg(c, org.ID, pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

type CheckinRewardCtl struct{}

func NewCheckinRewardCtl() *CheckinRewardCtl {
	return &CheckinRewardCtl{}
}

func (w *CheckinRewardCtl) CmsPostUpdateCheckinRewardApply(c *gin.Context) {
	var req svc.CmsUpdateCheckinRewardStatusReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	req.Status = model.CheckinRewardStatusApply
	checkinRewardSvc := svc.NewCheckinRewardSvc()
	err = checkinRewardSvc.CmsUpdateCheckinRewardStatus(c, org.OrgUser, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeApproveUserCheckinReward,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// CmsPostFixUserRewards 修复指定用户的签到奖励数据（去重连续签到阶段奖励）
func (w *CheckinRewardCtl) CmsPostFixUserRewards(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req struct {
		ImServerUserId string `json:"im_server_user_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.ImServerUserId == "" {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	checkinRewardSvc := svc.NewCheckinRewardSvc()
	resp, err := checkinRewardSvc.CmsFixUserRewards(c, org.ID, req.ImServerUserId)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CmsPostFixContinuousRewards 修复当前组织下所有「阶段性奖励」去重（删除重复的 15 元等阶段奖励）
func (w *CheckinRewardCtl) CmsPostFixContinuousRewards(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	checkinRewardSvc := svc.NewCheckinRewardSvc()
	resp, err := checkinRewardSvc.CmsFixContinuousRewards(c, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (w *CheckinRewardCtl) CmsGetListCheckinReward(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	pagination, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	statusStr := c.Query("status")
	status := model.CheckinRewardStatus(statusStr)

	// 将Unix时间戳转换为CST时区的时间对象（前端参数为 checkin_start_time / checkin_end_time）
	checkinStartTimeCST, err := ginUtils.QueryToCstTime(c, "checkin_start_time")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	checkinEndTimeCST, err := ginUtils.QueryToCstTime(c, "checkin_end_time")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	checkinRewardSvc := svc.NewCheckinRewardSvc()
	keyword := strings.TrimSpace(c.Query("keyword"))
	resp, err := checkinRewardSvc.CmsListCheckinReward(c, org.ID, status, keyword, checkinStartTimeCST, checkinEndTimeCST, pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (w *CheckinRewardCtl) WebGetListCheckinReward(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	pagination, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	statusStr := c.Query("status")
	status := model.CheckinRewardStatus(statusStr)

	checkinRewardSvc := svc.NewCheckinRewardSvc()
	resp, err := checkinRewardSvc.WebListCheckinReward(c, org.OrgUser, status, pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

type DailyCheckinRewardCfgCtl struct{}

func NewDailyCheckinRewardCfgCtl() *DailyCheckinRewardCfgCtl {
	return &DailyCheckinRewardCfgCtl{}
}

// CmsGetDailyCheckinRewardCfg 获取日常签到奖励配置
func (w *DailyCheckinRewardCfgCtl) CmsGetDailyCheckinRewardCfg(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	dailyRewardCfgSvc := svc.NewDailyCheckinRewardConfigSvc()
	resp, err := dailyRewardCfgSvc.CmsGetDailyCheckinRewardCfg(c, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CmsPostCreateOrUpdateDailyCheckinRewardCfg 创建或更新日常签到奖励配置
func (w *DailyCheckinRewardCfgCtl) CmsPostCreateOrUpdateDailyCheckinRewardCfg(c *gin.Context) {
	var req svc.CmsCreateOrUpdateDailyCheckinRewardCfgReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	dailyRewardCfgSvc := svc.NewDailyCheckinRewardConfigSvc()
	err = dailyRewardCfgSvc.CmsCreateOrUpdateDailyCheckinRewardCfg(c, opUserID, org.Organization, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeCreateOrUpdateDailyCheckinRewardCfg,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// CmsPostDeleteDailyCheckinRewardCfg 删除(禁用)日常签到奖励配置
func (w *DailyCheckinRewardCfgCtl) CmsPostDeleteDailyCheckinRewardCfg(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	dailyRewardCfgSvc := svc.NewDailyCheckinRewardConfigSvc()
	err = dailyRewardCfgSvc.CmsDeleteDailyCheckinRewardCfg(c, opUserID, org.Organization)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        nil,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeDeleteDailyCheckinRewardCfg,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// WebGetCheckinRule 获取签到规则
func (w *CheckinCtl) WebGetCheckinRule(c *gin.Context) {
	// 从上下文中获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 返回组织中的签到规则说明
	response := gin.H{
		"checkin_rule": org.Organization.CheckinRuleDescription,
	}

	apiresp.GinSuccess(c, response)
}

// WebGetCheckinRecordsForFix 获取用户最近一段连续签到记录，用于修复
func (w *CheckinCtl) WebGetCheckinRecordsForFix(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 从请求参数中获取用户ID
	targetImServerUserId := c.Query("im_server_user_id")
	if targetImServerUserId == "" {
		apiresp.GinError(c, freeErrors.ApiErr("user im_server_user_id is required"))
		return
	}

	log.ZInfo(c, "获取用户签到记录用于修复", "target_user_id", targetImServerUserId, "admin_user_id", org.OrgUser.ImServerUserId)

	checkInSvc := svc.NewCheckinSvc()
	resp, err := checkInSvc.WebGetCheckinRecordsForFix(c, targetImServerUserId, org.OrgUser.OrganizationId)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// WebPostFixCheckinRecords 修复用户最近一段连续签到记录
func (w *CheckinCtl) WebPostFixCheckinRecords(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 解析请求体
	var req struct {
		ImServerUserId string `json:"im_server_user_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ApiErr("invalid request body"))
		return
	}

	if req.ImServerUserId == "" {
		apiresp.GinError(c, freeErrors.ApiErr("user im_server_user_id is required"))
		return
	}

	log.ZInfo(c, "修复用户签到记录", "target_user_id", req.ImServerUserId, "admin_user_id", org.OrgUser.ImServerUserId)

	checkInSvc := svc.NewCheckinSvc()
	resp, err := checkInSvc.WebFixCheckinRecords(c, req.ImServerUserId, org.OrgUser.OrganizationId)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 记录操作日志
	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details: map[string]interface{}{
			"records_fixed":  resp.RecordsFixed,
			"rewards_added":  resp.RewardsAdded,
			"target_user_id": req.ImServerUserId,
		},
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeFixCheckinRecords,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, resp)
}

// CmsPostSupplementCheckin 管理员补签功能
// 允许管理员为指定用户补签一段时间内的打卡记录
// 主要用于修复用户漏签的情况，确保连续签到奖励正常发放
func (w *CheckinCtl) CmsPostSupplementCheckin(c *gin.Context) {
	var req svc.CmsSupplementCheckinReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取管理员信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 调用服务层进行补签
	checkInSvc := svc.NewCheckinSvc()
	resp, err := checkInSvc.CmsSupplementCheckin(c, org.OrgUser, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 记录操作日志
	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details: map[string]interface{}{
			"im_server_user_id": req.ImServerUserId,
			"start_date":        req.StartDate.Format("2006-01-02"),
			"end_date":          req.EndDate.Format("2006-01-02"),
			"success_count":     resp.SuccessCount,
			"skipped_dates":     len(resp.SkippedDates),
		},
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeSupplementCheckin, // 需要在operationLog模型中添加此类型
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	// 补签完成后，自动执行一次修复逻辑，确保连续签到天数与奖励完全正确
	if _, fixErr := checkInSvc.WebFixCheckinRecords(c, req.ImServerUserId, org.OrgUser.OrganizationId); fixErr != nil {
		log.ZError(c, "auto fix checkin records after CmsSupplementCheckin failed", fixErr)
	}

	apiresp.GinSuccess(c, resp)
}

// CmsPostSupplementMultipleDates 管理员多日期补签功能
// 允许管理员为指定用户补签多个指定日期的打卡记录
// 所有选择的日期与已签到日期必须形成一个连续的序列，不能有签到空洞
func (w *CheckinCtl) CmsPostSupplementMultipleDates(c *gin.Context) {
	var req svc.CmsSupplementMultipleDatesReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取管理员信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 调用服务层进行多日期补签
	checkInSvc := svc.NewCheckinSvc()
	resp, err := checkInSvc.CmsSupplementMultipleDates(c, org.OrgUser, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 记录操作日志
	dateStrings := make([]string, 0, len(req.Dates))
	for _, date := range req.Dates {
		dateStrings = append(dateStrings, date.Format("2006-01-02"))
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details: map[string]interface{}{
			"im_server_user_id": req.ImServerUserId,
			"dates":             dateStrings,
			"success_count":     resp.SuccessCount,
			"skipped_dates":     len(resp.SkippedDates),
		},
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeSupplementCheckin, // 与普通补签使用相同的操作类型
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	// 多日期补签完成后，同样自动执行一次修复逻辑
	if _, fixErr := checkInSvc.WebFixCheckinRecords(c, req.ImServerUserId, org.OrgUser.OrganizationId); fixErr != nil {
		log.ZError(c, "auto fix checkin records after CmsSupplementMultipleDates failed", fixErr)
	}

	apiresp.GinSuccess(c, resp)
}
