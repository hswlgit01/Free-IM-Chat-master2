package svc

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	OrgModel "github.com/openimsdk/chat/freechat/apps/organization/model"

	"github.com/openimsdk/chat/freechat/apps/systemStatistics/model"
	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type SystemStatisticsSvc struct{}

func NewSystemStatisticsSvc() *SystemStatisticsSvc {
	return &SystemStatisticsSvc{}
}

// GetSystemStatistics 获取系统统计数据
func (s *SystemStatisticsSvc) GetSystemStatistics(ctx context.Context, orgId primitive.ObjectID, startTime, endTime *time.Time) ([]*model.SystemStatistics, error) {
	// 计算时间范围
	start, end := s.calculateTimeRange(startTime, endTime)

	// 生成日期列表
	dates := s.generateDateList(start, end)

	// 并发统计四个指标（注册、登录、签到按date、签到按created_at）
	statisticsDao := model.NewStatisticsDao(plugin.MongoCli().GetDB())

	resultChan := make(chan *model.StatisticsResult, 4)
	var wg sync.WaitGroup

	// 1. 注册统计 - 需要排除 SuperAdmin 和 BackendAdmin 角色（使用 created_at，不修改）
	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := statisticsDao.GetDailyUniqueUserCountExcludeRoles(ctx, orgId,
			constant.CollectionOrganizationUser, "organization_id", start, end,
			[]string{string(OrgModel.OrganizationUserBackendAdminRole), string(OrgModel.OrganizationUserSuperAdminRole)})
		if err != nil {
			log.ZError(ctx, "注册统计查询失败", err, "collection", constant.CollectionOrganizationUser, "orgId", orgId.Hex())
		}
		resultChan <- &model.StatisticsResult{
			Collection: constant.CollectionOrganizationUser,
			Data:       data,
			Err:        err,
		}
	}()

	// 2. 登录统计（使用 created_at，不修改）
	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := statisticsDao.GetDailyUniqueUserCount(ctx, orgId, constant.CollectionChangeOrgRecord, "org_id", start, end)
		if err != nil {
			log.ZError(ctx, "登录统计查询失败", err, "collection", constant.CollectionChangeOrgRecord, "orgId", orgId.Hex())
		}
		resultChan <- &model.StatisticsResult{
			Collection: constant.CollectionChangeOrgRecord,
			Data:       data,
			Err:        err,
		}
	}()

	// 3. 签到统计 - 使用 date 字段过滤，分组时使用上海时区
	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := statisticsDao.GetDailyUniqueCheckinCountByDate(ctx, orgId, constant.CollectionCheckin, start, end)
		if err != nil {
			log.ZError(ctx, "签到统计查询失败", err, "collection", constant.CollectionCheckin, "orgId", orgId.Hex())
		}
		resultChan <- &model.StatisticsResult{
			Collection: constant.CollectionCheckin,
			Data:       data,
			Err:        err,
		}
	}()

	// 4. 签到统计（按 created_at）- 页面展示用
	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := statisticsDao.GetDailyUniqueUserCount(ctx, orgId, constant.CollectionCheckin, "org_id", start, end)
		if err != nil {
			log.ZError(ctx, "签到(按创建时间)统计查询失败", err, "collection", constant.CollectionCheckin, "orgId", orgId.Hex())
		}
		resultChan <- &model.StatisticsResult{
			Collection: "checkin_by_created_at", // 虚拟标识，用于区分签到(按created_at)统计
			Data:       data,
			Err:        err,
		}
	}()

	// 等待所有查询完成，添加超时保护
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 预分配容量优化内存
	resultMap := make(map[string]*model.SystemStatistics, len(dates))
	collectionMap := map[string]string{
		constant.CollectionOrganizationUser: "register",
		constant.CollectionChangeOrgRecord:  "login",
		constant.CollectionCheckin:          "sign",
		"checkin_by_created_at":             "sign_by_created_at",
	}

	// 添加超时控制
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// 处理查询结果
	for {
		select {
		case result, ok := <-resultChan:
			if !ok {
				// channel已关闭，所有结果处理完毕
				goto processResults
			}

			if result.Err != nil {
				continue // 忽略错误的查询，继续处理其他结果
			}

			fieldName := collectionMap[result.Collection]
			for _, stat := range result.Data {
				dateStr := stat.Date
				count := stat.Count

				// 获取或创建统计对象
				statObj, exists := resultMap[dateStr]
				if !exists {
					statObj = &model.SystemStatistics{Date: dateStr}
					resultMap[dateStr] = statObj
				}

				// 设置对应字段的值
				switch fieldName {
				case "register":
					statObj.Register = count
				case "login":
					statObj.Login = count
				case "sign":
					statObj.Sign = count
				case "sign_by_created_at":
					statObj.SignByCreatedAt = count
				}
			}
		case <-timeoutCtx.Done():
			return nil, fmt.Errorf("统计查询超时")
		}
	}

processResults:
	// 预分配结果数组容量
	result := make([]*model.SystemStatistics, 0, len(resultMap))
	for _, stat := range resultMap {
		result = append(result, stat)
	}

	sort.Slice(result, func(i, j int) bool {
		// 按日期倒序
		return result[i].Date > result[j].Date
	})

	return result, nil
}

// GetSalesDailyStatistics 获取按业务员（邀请人）维度的每日统计数据
// 统计口径：每个邀请人在某天邀请注册 / 实名 / 签到的下级人数。
func (s *SystemStatisticsSvc) GetSalesDailyStatistics(ctx context.Context, orgId primitive.ObjectID, startTime, endTime *time.Time) ([]*model.SystemStatistics, error) {
	// 计算时间范围
	start, end := s.calculateTimeRange(startTime, endTime)

	// 生成日期列表（仅用于预估容量）
	dates := s.generateDateList(start, end)

	// 并发统计多个指标（按邀请人+日期维度）
	statisticsDao := model.NewStatisticsDao(plugin.MongoCli().GetDB())

	type inviteResult struct {
		metric string
		data   []*model.InviteStatisticsCount
		err    error
	}

	resultChan := make(chan *inviteResult, 5)
	var wg sync.WaitGroup

	// 1. 注册统计（按邀请人）
	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := statisticsDao.GetInviteDailyRegisterStats(ctx, orgId, start, end)
		if err != nil {
			log.ZError(ctx, "邀请注册统计查询失败", err, "orgId", orgId.Hex())
		}
		resultChan <- &inviteResult{metric: "register", data: data, err: err}
	}()

	// 2. 实名统计（按邀请人）
	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := statisticsDao.GetInviteDailyVerifiedStats(ctx, orgId, start, end)
		if err != nil {
			log.ZError(ctx, "邀请实名认证统计查询失败", err, "orgId", orgId.Hex())
		}
		resultChan <- &inviteResult{metric: "verified", data: data, err: err}
	}()

	// 3. 当日新增用户签到统计（按邀请人）
	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := statisticsDao.GetInviteDailyNewRegisterCheckinStats(ctx, orgId, start, end)
		if err != nil {
			log.ZError(ctx, "邀请【当日新增】签到统计查询失败", err, "orgId", orgId.Hex())
		}
		resultChan <- &inviteResult{metric: "sign", data: data, err: err}
	}()

	// 4. 下级所有签到统计（按邀请人）
	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := statisticsDao.GetInviteDailyCheckinStats(ctx, orgId, start, end)
		if err != nil {
			log.ZError(ctx, "邀请【所有下级】签到统计查询失败", err, "orgId", orgId.Hex())
		}
		resultChan <- &inviteResult{metric: "all_sign", data: data, err: err}
	}()

	// 5. 整个团队签到统计（按祖先业务员）
	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := statisticsDao.GetInviteDailyTeamCheckinStats(ctx, orgId, start, end)
		if err != nil {
			log.ZError(ctx, "邀请【整个团队】签到统计查询失败", err, "orgId", orgId.Hex())
		}
		resultChan <- &inviteResult{metric: "team_sign", data: data, err: err}
	}()

	// 等待所有查询完成，添加超时保护
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 预分配容量优化内存，key = inviterImUserId + "|" + date
	resultMap := make(map[string]*model.SystemStatistics, len(dates))

	// 添加超时控制
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// 处理查询结果
	for {
		select {
		case result, ok := <-resultChan:
			if !ok {
				// channel已关闭，所有结果处理完毕
				goto processResultsSales
			}

			if result.err != nil {
				continue // 忽略错误的查询，继续处理其他结果
			}

			for _, stat := range result.data {
				dateStr := stat.Date
				count := stat.Count
				inviterImUserId := stat.InviterImUserId

				key := inviterImUserId + "|" + dateStr
				// 获取或创建统计对象
				statObj, exists := resultMap[key]
				if !exists {
					statObj = &model.SystemStatistics{
						Date:            dateStr,
						InviterImUserId: inviterImUserId,
					}
					resultMap[key] = statObj
				}

				// 设置对应字段的值
				switch result.metric {
				case "register":
					statObj.Register = count
				case "sign":
					statObj.Sign = count
				case "all_sign":
					statObj.AllCheckin = count
				case "team_sign":
					statObj.TeamCheckin = count
				case "verified":
					statObj.Verified = count
				}
			}
		case <-timeoutCtx.Done():
			return nil, fmt.Errorf("统计查询超时")
		}
	}

processResultsSales:
	// 计算未实名人数（保证不为负数）
	for _, stat := range resultMap {
		if stat.Verified >= 0 && stat.Register > stat.Verified {
			stat.Unverified = stat.Register - stat.Verified
		} else {
			stat.Unverified = 0
		}
	}

	// 预分配结果数组容量
	result := make([]*model.SystemStatistics, 0, len(resultMap))
	for _, stat := range resultMap {
		result = append(result, stat)
	}

	sort.Slice(result, func(i, j int) bool {
		// 先按日期倒序，再按邀请人ID排序，保证稳定性
		if result[i].Date == result[j].Date {
			return result[i].InviterImUserId < result[j].InviterImUserId
		}
		return result[i].Date > result[j].Date
	})

	return result, nil
}

// calculateTimeRange 计算时间范围，如果没有指定则默认最近30天
func (s *SystemStatisticsSvc) calculateTimeRange(startTime, endTime *time.Time) (time.Time, time.Time) {
	now := time.Now()

	if startTime != nil && endTime != nil {
		// 用户指定了完整的时间范围，直接使用
		return *startTime, *endTime
	} else if startTime != nil {
		// 只指定了开始时间，结束时间为现在
		return *startTime, now
	} else if endTime != nil {
		// 只指定了结束时间，开始时间为30天前
		start := endTime.AddDate(0, 0, -6)
		return start, *endTime
	} else {
		// 都没指定，默认最近30天
		start := now.AddDate(0, 0, -6)
		return start, now
	}
}

// generateDateList 生成日期列表
func (s *SystemStatisticsSvc) generateDateList(start, end time.Time) []string {
	var dates []string

	// 按天遍历，生成日期字符串
	for d := start; !d.After(end); d = d.Add(24 * time.Hour) {
		dates = append(dates, d.Format("20060102"))
	}

	return dates
}
