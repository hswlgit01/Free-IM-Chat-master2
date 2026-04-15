package systemStatistics

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/systemStatistics/dto"
	"github.com/openimsdk/chat/freechat/apps/systemStatistics/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	openImUserModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/tools/apiresp"
)

type SystemStatisticsCtl struct {
}

func NewSystemStatisticsCtl() *SystemStatisticsCtl {
	return &SystemStatisticsCtl{}
}

// GetSystemStatistics 平台维度的按日统计（日期、登录人数、注册人数、签到人数）
func (s *SystemStatisticsCtl) GetSystemStatistics(c *gin.Context) {
	// 获取组织信息
	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 解析时间参数（都是可选的）
	var startTime, endTime *time.Time

	// 解析开始时间戳
	if startTimeStr := c.Query("start_time"); startTimeStr != "" {
		timestamp, err := strconv.ParseInt(startTimeStr, 10, 64)
		if err != nil {
			apiresp.GinError(c, fmt.Errorf("invalid start_time format: %s", startTimeStr))
			return
		}
		t := time.Unix(timestamp, 0)
		startTime = &t
	}

	// 解析结束时间戳
	if endTimeStr := c.Query("end_time"); endTimeStr != "" {
		timestamp, err := strconv.ParseInt(endTimeStr, 10, 64)
		if err != nil {
			apiresp.GinError(c, fmt.Errorf("invalid end_time format: %s", endTimeStr))
			return
		}
		t := time.Unix(timestamp, 0)
		endTime = &t
	}

	// 验证时间范围合理性
	if startTime != nil && endTime != nil && startTime.After(*endTime) {
		apiresp.GinError(c, fmt.Errorf("start_time cannot be after end_time"))
		return
	}

	// 调用服务层（平台维度统计）
	statisticsSvc := svc.NewSystemStatisticsSvc()
	statistics, err := statisticsSvc.GetSystemStatistics(c, orgInfo.ID, startTime, endTime)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 平台统计不区分业务员，userName/userID 置空即可
	respList := make([]*dto.SystemStatisticsResp, 0, len(statistics))
	for _, s := range statistics {
		item := &dto.SystemStatisticsResp{
			Date:     s.Date,
			Register: s.Register,
			Login:    s.Login,
			Sign:     s.Sign,

			UserName: "",
			UserID:   "",

			NewRegisterCount: s.Register,
			VerifiedCount:    0,
			UnverifiedCount:  0,
			CheckinCount:     s.Sign,
			SignByCreatedAt:  s.SignByCreatedAt,
		}
		respList = append(respList, item)
	}

	apiresp.GinSuccess(c, respList)
}

// GetSalesDailyStatistics 业务员（邀请人）每日新增统计
func (s *SystemStatisticsCtl) GetSalesDailyStatistics(c *gin.Context) {
	// 获取组织信息
	orgInfo, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 解析时间参数（都是可选的）
	var startTime, endTime *time.Time

	// 解析开始时间戳
	if startTimeStr := c.Query("start_time"); startTimeStr != "" {
		timestamp, err := strconv.ParseInt(startTimeStr, 10, 64)
		if err != nil {
			apiresp.GinError(c, fmt.Errorf("invalid start_time format: %s", startTimeStr))
			return
		}
		t := time.Unix(timestamp, 0)
		startTime = &t
	}

	// 解析结束时间戳
	if endTimeStr := c.Query("end_time"); endTimeStr != "" {
		timestamp, err := strconv.ParseInt(endTimeStr, 10, 64)
		if err != nil {
			apiresp.GinError(c, fmt.Errorf("invalid end_time format: %s", endTimeStr))
			return
		}
		t := time.Unix(timestamp, 0)
		endTime = &t
	}

	// 验证时间范围合理性
	if startTime != nil && endTime != nil && startTime.After(*endTime) {
		apiresp.GinError(c, fmt.Errorf("start_time cannot be after end_time"))
		return
	}

	// 调用服务层（业务员维度统计）
	// 为避免时区边界导致的遗漏，这里在传给服务层的时间范围上做一定放宽，
	// 后续再根据聚合后的「日期字符串」进行精确过滤。
	statisticsSvc := svc.NewSystemStatisticsSvc()
	var svcStart, svcEnd *time.Time
	if startTime != nil {
		t := startTime.Add(-24 * time.Hour)
		svcStart = &t
	}
	if endTime != nil {
		t := endTime.Add(24 * time.Hour)
		svcEnd = &t
	}

	statistics, err := statisticsSvc.GetSalesDailyStatistics(c, orgInfo.ID, svcStart, svcEnd)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 批量获取所有邀请人的 user_id & nickname（从 user 表）
	userNameMap := map[string]string{} // inviterImUserId -> nickname
	userIDMap := map[string]string{}   // inviterImUserId -> user.user_id
	if db := plugin.MongoCli().GetDB(); db != nil {
		userDao := openImUserModel.NewUserDao(db)

		// 收集所有邀请人的 IM 用户ID
		imIDsSet := make(map[string]struct{})
		for _, s := range statistics {
			if s.InviterImUserId != "" {
				imIDsSet[s.InviterImUserId] = struct{}{}
			}
		}

		if len(imIDsSet) > 0 {
			imIDs := make([]string, 0, len(imIDsSet))
			for id := range imIDsSet {
				imIDs = append(imIDs, id)
			}

			if users, uErr := userDao.FindByUserIDs(c, imIDs); uErr == nil {
				for _, u := range users {
					if u == nil || u.UserID == "" {
						continue
					}
					userIDMap[u.UserID] = u.UserID
					if u.Nickname != "" {
						userNameMap[u.UserID] = u.Nickname
					}
				}
			}
		}
	}

	// 封装为对外返回的 DTO
	respList := make([]*dto.SystemStatisticsResp, 0, len(statistics))
	for _, s := range statistics {
		userID := s.InviterImUserId
		if v, ok := userIDMap[s.InviterImUserId]; ok {
			userID = v
		}
		userName := userID
		if v, ok := userNameMap[s.InviterImUserId]; ok && v != "" {
			userName = v
		}

		item := &dto.SystemStatisticsResp{
			Date:     s.Date,
			Register: s.Register,
			Login:    s.Login,
			Sign:     s.Sign,

			UserName: userName,
			UserID:   userID,

			NewRegisterCount: s.Register,
			VerifiedCount:    s.Verified,
			UnverifiedCount:  s.Unverified,
			CheckinCount:     s.Sign,
			AllCheckinCount:  s.AllCheckin,
			TeamCheckinCount: s.TeamCheckin,
		}
		respList = append(respList, item)
	}

	// 按关键字过滤（支持前后空格处理，匹配用户名称或用户ID）
	keyword := strings.TrimSpace(c.Query("user_name"))
	if keyword != "" {
		filtered := make([]*dto.SystemStatisticsResp, 0, len(respList))
		for _, item := range respList {
			if strings.Contains(item.UserName, keyword) || strings.Contains(item.UserID, keyword) {
				filtered = append(filtered, item)
			}
		}
		respList = filtered
	}

	// 再按日期范围过滤（基于聚合后的日期字符串，避免时区边界引起的偏差）
	var startDateStr, endDateStr string
	if startTime != nil {
		startDateStr = startTime.In(time.FixedZone("CST", 8*3600)).Format("20060102")
	}
	if endTime != nil {
		endDateStr = endTime.In(time.FixedZone("CST", 8*3600)).Format("20060102")
	}

	if startDateStr != "" || endDateStr != "" {
		filtered := make([]*dto.SystemStatisticsResp, 0, len(respList))
		for _, item := range respList {
			if startDateStr != "" && item.Date < startDateStr {
				continue
			}
			if endDateStr != "" && item.Date > endDateStr {
				continue
			}
			filtered = append(filtered, item)
		}
		respList = filtered
	}

	apiresp.GinSuccess(c, respList)
}
