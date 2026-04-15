package svc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/openimsdk/chat/freechat/apps/checkin/dto"
	"github.com/openimsdk/chat/freechat/apps/checkin/model"
	lotteryModel "github.com/openimsdk/chat/freechat/apps/lottery/model"
	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	pointsModel "github.com/openimsdk/chat/freechat/apps/points/model"
	pointsSvc "github.com/openimsdk/chat/freechat/apps/points/svc"
	transactionSvc "github.com/openimsdk/chat/freechat/apps/transaction/svc"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	walletSvc "github.com/openimsdk/chat/freechat/apps/wallet/svc"
	walletTsModel "github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type CheckinSvc struct{}

func NewCheckinSvc() *CheckinSvc {
	return &CheckinSvc{}
}

// RecalculateAllStreaks 重新计算所有用户的所有签到记录的连续签到天数
// 此函数用于修复已存在的数据，确保所有streak值正确
// 参数：
//   - ctx: 上下文
//   - userId: 可选，如果提供则只计算指定用户的记录
//   - dryRun: 如果为true，只计算不更新数据库
//
// 返回：
//   - 已处理的签到记录总数
//   - 已修正的签到记录数
//   - 错误
func (w *CheckinSvc) RecalculateAllStreaks(ctx context.Context, userId string, dryRun bool) (int, int, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	checkinDao := model.NewCheckinDao(db)

	logCtx := ctx

	// 计数器
	totalProcessed := 0
	totalFixed := 0

	// 按 (user, org) 分组修复，确保 streak 正确
	pairs, err := checkinDao.GetAllUserOrgPairs(logCtx)
	if err != nil {
		return 0, 0, err
	}
	if userId != "" {
		filtered := make([]model.UserOrgPair, 0)
		for _, p := range pairs {
			if p.ImServerUserId == userId {
				filtered = append(filtered, p)
			}
		}
		pairs = filtered
		log.ZInfo(logCtx, "仅处理指定用户", "userID", userId, "组织数", len(pairs))
	} else {
		log.ZInfo(logCtx, "获取 (user, org) 对成功", "数量", len(pairs))
	}

	// 不使用事务：批量修复涉及大量 (user,org) 对，事务易超时或触发 session 限制
	for _, pair := range pairs {
		userCheckins, err := checkinDao.GetAllByImServerUserIdAndOrgId(logCtx, pair.ImServerUserId, pair.OrgId)
		if err != nil {
			log.ZError(logCtx, "获取签到记录失败", err, "userID", pair.ImServerUserId, "orgID", pair.OrgId.Hex())
			continue
		}
		if len(userCheckins) == 0 {
			continue
		}

		for i, checkin := range userCheckins {
			totalProcessed++
			correctStreak, err := calculateCorrectStreak(logCtx, userCheckins, checkin.Date)
			if err != nil {
				log.ZError(logCtx, "计算连续签到天数失败", err, "userID", pair.ImServerUserId, "日期", checkin.Date.Format("2006-01-02"))
				continue
			}
			if checkin.Streak != correctStreak {
				totalFixed++
				log.ZInfo(logCtx, "发现不正确的streak", "userID", pair.ImServerUserId, "日期", checkin.Date.Format("2006-01-02"), "当前", checkin.Streak, "正确", correctStreak)
				if !dryRun {
					if err := checkinDao.UpdateStreak(logCtx, checkin.ID, correctStreak); err != nil {
						log.ZError(logCtx, "更新streak失败", err, "记录ID", checkin.ID.Hex())
						continue
					}
					userCheckins[i].Streak = correctStreak
				}
			}
		}
	}

	return totalProcessed, totalFixed, nil
}

// calculateCorrectStreak 计算指定日期在签到序列中的正确连续天数
// 核心算法：
// 1. 获取所有签到记录（包括当前日期如果尚未在列表中）
// 2. 按日期排序
// 3. 确保所有日期形成连续序列（无间隔）
// 4. 计算指定日期在序列中的位置（索引+1）
// 这个函数用于所有签到场景：常规签到、补签单日、补签多日
// calculateCorrectStreak 计算给定日期的连续签到天数
// 算法原理:
// 1. 收集所有签到日期并转换为YYYYMMDD格式的整数
// 2. 按日期排序并识别连续序列
// 3. 找到目标日期所在的连续序列
// 4. 计算目标日期在该序列中的位置+1作为连续签到天数
// 返回值是目标日期的连续签到天数(从1开始)
func calculateCorrectStreak(ctx context.Context, allCheckins []*model.Checkin, targetDate time.Time) (int, error) {
	// 规范化目标日期为午夜时间（CST时区）
	targetDate = time.Date(
		targetDate.Year(),
		targetDate.Month(),
		targetDate.Day(),
		0, 0, 0, 0,
		utils.CST,
	)

	// 将所有签到记录转换为日期整数映射 (YYYYMMDD -> 是否存在)
	dateIntMap := make(map[int]bool)

	// 添加目标日期到映射
	targetDateInt := targetDate.Year()*10000 + int(targetDate.Month())*100 + targetDate.Day()
	dateIntMap[targetDateInt] = true

	// 遍历所有签到记录，提取日期整数
	for _, checkin := range allCheckins {
		// 规范化日期并转换为整数
		normalizedDate := utils.TimeToCST(checkin.Date)
		dateInt := normalizedDate.Year()*10000 + int(normalizedDate.Month())*100 + normalizedDate.Day()
		dateIntMap[dateInt] = true
	}

	// 将日期整数转换为有序整数数组
	dateInts := make([]int, 0, len(dateIntMap))
	for dateInt := range dateIntMap {
		dateInts = append(dateInts, dateInt)
	}

	// 按整数值排序（升序，从最早到最晚）
	slices.Sort(dateInts)

	// 找出目标日期在排序后列表中的位置
	targetIndex := -1
	for i, dateInt := range dateInts {
		if dateInt == targetDateInt {
			targetIndex = i
			break
		}
	}

	// 如果未找到目标日期，返回错误
	if targetIndex == -1 {
		return 0, errors.New("target date not found in date list")
	}

	// 识别所有连续序列
	var sequences [][]int
	currentSequence := []int{dateInts[0]}

	// 从第二个日期开始遍历，构建连续序列（使用日历“下一天”判断，支持跨月/跨年）
	for i := 1; i < len(dateInts); i++ {
		if dateInts[i] == nextDayDateInt(dateInts[i-1]) {
			currentSequence = append(currentSequence, dateInts[i])
		} else {
			sequences = append(sequences, currentSequence)
			currentSequence = []int{dateInts[i]}
		}
	}

	// 添加最后一个序列
	sequences = append(sequences, currentSequence)

	// 查找目标日期所在的连续序列
	var targetSequence []int
	for _, seq := range sequences {
		for _, d := range seq {
			if d == targetDateInt {
				targetSequence = seq
				break
			}
		}
		if len(targetSequence) > 0 {
			break
		}
	}

	// 计算目标日期在序列中的位置
	targetPositionInSequence := -1
	for i, d := range targetSequence {
		if d == targetDateInt {
			targetPositionInSequence = i
			break
		}
	}

	// 计算连续签到天数（当前位置+1）
	return targetPositionInSequence + 1, nil
}

// dateIntsToDateStr 将YYYYMMDD格式的整数转换为YYYY-MM-DD格式的日期字符串
func dateIntsToDateStr(dateInt int) string {
	year := dateInt / 10000
	month := (dateInt % 10000) / 100
	day := dateInt % 100
	return fmt.Sprintf("%d-%02d-%02d", year, month, day)
}

// nextDayDateInt 返回 YYYYMMDD 日期的下一天（按日历），用于跨月/跨年连续判断
func nextDayDateInt(dateInt int) int {
	year := dateInt / 10000
	month := (dateInt % 10000) / 100
	day := dateInt % 100
	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, utils.CST)
	next := t.AddDate(0, 0, 1)
	return next.Year()*10000 + int(next.Month())*100 + next.Day()
}

// daysBetweenCST 计算两个日期在 CST 下“仅日期”相差的天数（b - a），避免 Sub().Hours()/24 的浮点误差导致跨月计算错误
func daysBetweenCST(a, b time.Time) int {
	aNorm := time.Date(a.Year(), a.Month(), a.Day(), 0, 0, 0, 0, utils.CST)
	bNorm := time.Date(b.Year(), b.Month(), b.Day(), 0, 0, 0, 0, utils.CST)
	return int(bNorm.Sub(aNorm) / (24 * time.Hour))
}

type WebCreateCheckinResp struct {
	CheckinRewards []*dto.CheckinRewardResp `json:"checkin_rewards"`
	Streak         int                      `json:"streak"`        // 本次签到后的连续签到天数（数据库已更新，供前端回显）
	TodayCheckin   *model.Checkin           `json:"today_checkin"` // 本次创建的今日签到记录
}

// CmsSupplementCheckinReq 管理员补签请求参数
type CmsSupplementCheckinReq struct {
	ImServerUserId string    `json:"im_server_user_id"` // 用户ImServerUserId
	StartDate      time.Time `json:"start_date"`        // 补签开始日期
	EndDate        time.Time `json:"end_date"`          // 补签结束日期
}

// CmsSupplementCheckinResp 管理员补签响应
type CmsSupplementCheckinResp struct {
	SuccessCount int              `json:"success_count"` // 成功补签天数
	SkippedDates []time.Time      `json:"skipped_dates"` // 跳过的日期（已有签到记录）
	Rewards      []*model.Checkin `json:"rewards"`       // 生成的签到记录
}

// CmsSupplementMultipleDatesReq 管理员多日期补签请求
type CmsSupplementMultipleDatesReq struct {
	ImServerUserId string      `json:"im_server_user_id"` // 用户ImServerUserId
	Dates          []time.Time `json:"dates"`             // 要补签的日期列表
}

// CmsSupplementMultipleDatesResp 管理员多日期补签响应
type CmsSupplementMultipleDatesResp struct {
	SuccessCount int              `json:"success_count"` // 成功补签天数
	SkippedDates []time.Time      `json:"skipped_dates"` // 跳过的日期（已有签到记录）
	Rewards      []*model.Checkin `json:"rewards"`       // 生成的签到记录
}

// CmsSupplementCheckin 管理员补签功能
// 为用户补签指定日期范围内的签到记录，并生成对应的奖励
// 如果日期范围内某天已有签到记录，则跳过该日期
// 补签必须与用户最后一次签到连续，不允许有间隔
func (w *CheckinSvc) CmsSupplementCheckin(ctx context.Context, adminOrgUser *orgModel.OrganizationUser, req *CmsSupplementCheckinReq) (*CmsSupplementCheckinResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	orgUserDao := orgModel.NewOrganizationUserDao(db)
	checkinDao := model.NewCheckinDao(db)
	checkinRewardCfgDao := model.NewCheckinRewardConfigDao(db)
	dailyCheckinRewardCfgDao := model.NewDailyCheckinRewardConfigDao(db)
	checkinRewardDao := model.NewCheckinRewardDao(db)

	// 检查管理员权限
	allowRole := []orgModel.OrganizationUserRole{
		orgModel.OrganizationUserSuperAdminRole,
		orgModel.OrganizationUserBackendAdminRole,
		orgModel.OrganizationUserGroupManagerRole,
		orgModel.OrganizationUserTermManagerRole,
	}
	if !slices.Contains(allowRole, adminOrgUser.Role) {
		return nil, freeErrors.ApiErr("no permission")
	}

	// 查找需要补签的用户
	orgUser, err := orgUserDao.GetByImServerUserId(ctx, req.ImServerUserId)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErrWithResource("user")
		}
		return nil, err
	}

	// 验证补签用户和管理员是否在同一组织
	if orgUser.OrganizationId != adminOrgUser.OrganizationId {
		return nil, freeErrors.ApiErr("user not in the same organization")
	}

	// 验证补签日期范围
	now := utils.NowCST()
	todayStart := utils.TodayCST()

	// 将开始和结束日期规范化为CST午夜时间
	startDate := time.Date(req.StartDate.Year(), req.StartDate.Month(), req.StartDate.Day(), 0, 0, 0, 0, utils.CST)
	endDate := time.Date(req.EndDate.Year(), req.EndDate.Month(), req.EndDate.Day(), 0, 0, 0, 0, utils.CST)

	// 验证日期范围
	if startDate.After(endDate) {
		return nil, freeErrors.ApiErr("start date must be before or equal to end date")
	}

	// 日期不能在未来
	if startDate.After(todayStart) || endDate.After(todayStart) {
		return nil, freeErrors.ApiErr("cannot supplement checkin for future dates")
	}

	// 获取用户在本组织下最近一次签到记录（按签到日期）
	latestCheckin, err := checkinDao.GetLatestCheckInByDateByImServerUserIdAndOrgId(ctx, req.ImServerUserId, adminOrgUser.OrganizationId)
	if err != nil && !dbutil.IsDBNotFound(err) {
		return nil, err
	}

	// 如果没有找到签到记录，这将是用户的第一次签到
	if latestCheckin == nil {
		// 允许从任何日期开始补签
	} else {
		// 确保时区正确
		latestCheckin.Date = utils.TimeToCST(latestCheckin.Date)

		// 计算最近签到日期（只保留年月日）
		latestDate := time.Date(
			latestCheckin.Date.Year(),
			latestCheckin.Date.Month(),
			latestCheckin.Date.Day(),
			0, 0, 0, 0,
			utils.CST,
		)

		// 补签必须与最近一次签到连续，开始日期应为最近签到日期的下一天
		expectedStartDate := latestDate.AddDate(0, 0, 1)
		if startDate.Before(expectedStartDate) {
			// 如果开始日期早于预期，说明要求补签的是已经签过的日期，这是不允许的
			return nil, freeErrors.ApiErr("start date must be after the latest checkin date")
		} else if startDate.After(expectedStartDate) {
			// 如果开始日期晚于预期，说明中间有间隔，这也是不允许的
			return nil, freeErrors.ApiErr("cannot have gap between latest checkin and supplement start date")
		}
	}

	resp := &CmsSupplementCheckinResp{
		SuccessCount: 0,
		SkippedDates: make([]time.Time, 0),
		Rewards:      make([]*model.Checkin, 0),
	}

	err = mongoCli.GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		// 获取用户最近一次签到记录，作为初始streak值的基础
		var currentStreak int
		if latestCheckin != nil {
			currentStreak = latestCheckin.Streak
		} else {
			currentStreak = 0
		}

		// 遍历每一天进行补签
		currentDate := startDate
		for !currentDate.After(endDate) {
			// 检查当天在本组织是否已经签到
			existingCheckin, err := checkinDao.GetByImServerUserIdAndDateAndOrgId(sessionCtx, req.ImServerUserId, adminOrgUser.OrganizationId, currentDate)
			if err != nil && !dbutil.IsDBNotFound(err) {
				return err
			}

			if existingCheckin != nil {
				// 已经有签到记录，跳过这一天
				resp.SkippedDates = append(resp.SkippedDates, currentDate)
			} else {
				// 当前需要补签，streak加1
				currentStreak++

				// 创建签到记录
				checkin := &model.Checkin{
					ID:             primitive.NewObjectID(),
					ImServerUserId: req.ImServerUserId,
					OrgId:          adminOrgUser.OrganizationId,
					Date:           currentDate,
					Streak:         currentStreak,
					CreatedAt:      now, // 所有补签记录使用同一个创建时间
				}

				err = checkinDao.Create(sessionCtx, checkin)
				if err != nil {
					return err
				}

				resp.SuccessCount++
				resp.Rewards = append(resp.Rewards, checkin)

				// 处理签到奖励

				// 1. 日常签到奖励
				dailyRewardCfg, err := dailyCheckinRewardCfgDao.GetByOrgId(sessionCtx, adminOrgUser.OrganizationId)
				if err != nil && !dbutil.IsDBNotFound(err) {
					return err
				}

				if dailyRewardCfg != nil {
					dailyCheckinReward := &model.CheckinReward{
						CheckinId:             checkin.ID,
						CheckinRewardConfigId: dailyRewardCfg.ID,
						ImServerUserId:        req.ImServerUserId,
						RewardAmount:          dailyRewardCfg.RewardAmount,
						RewardId:              dailyRewardCfg.RewardId,
						RewardType:            dailyRewardCfg.RewardType,
						OrgID:                 adminOrgUser.OrganizationId,
						CheckinDate:           checkin.Date,
						Source:                model.CheckinRewardSourceDaily,
						Description:           strconv.Itoa(currentStreak), // 存连续天数，列表可准确展示
						Status:                model.CheckinRewardStatusPending,
					}

					// 自动发放奖励
					err = NewCheckinRewardSvc().InternalDistributeReward(sessionCtx, adminOrgUser.OrganizationId, dailyCheckinReward)
					if err != nil {
						// 如果是余额奖励且余额不足,记录日志但不阻止签到
						if errs.Unwrap(err).Error() == freeErrors.ErrorMessages[freeErrors.ErrInsufficientBalance] {
							log.ZWarn(sessionCtx, "组织余额不足,跳过日常奖励发放", err,
								"org_id", adminOrgUser.OrganizationId.Hex(),
								"user_id", orgUser.UserId,
								"reward_amount", dailyRewardCfg.RewardAmount.String())
							// 保持状态为 Pending,不设置为 Apply
						} else {
							return err
						}
					} else {
						dailyCheckinReward.Status = model.CheckinRewardStatusApply
					}

					err = checkinRewardDao.Create(sessionCtx, dailyCheckinReward)
					if err != nil {
						return err
					}
				}

				// 2. 连续签到奖励（阶段奖励每个配置只发一次）
				checkinRewardCfg, err := checkinRewardCfgDao.SelectByOrgIdAndStreak(sessionCtx, adminOrgUser.OrganizationId, currentStreak)
				if err != nil && !dbutil.IsDBNotFound(err) {
					return err
				}

				for _, rewardCfg := range checkinRewardCfg {
					exists, err := checkinRewardDao.ExistContinuousByOrgIdAndImServerUserIdAndConfigId(sessionCtx, adminOrgUser.OrganizationId, req.ImServerUserId, rewardCfg.ID)
					if err != nil {
						return err
					}
					if exists {
						continue
					}

					checkinReward := &model.CheckinReward{
						CheckinId:             checkin.ID,
						CheckinRewardConfigId: rewardCfg.ID,
						ImServerUserId:        req.ImServerUserId,
						RewardAmount:          rewardCfg.RewardAmount,
						RewardId:              rewardCfg.RewardId,
						RewardType:            rewardCfg.RewardType,
						OrgID:                 adminOrgUser.OrganizationId,
						CheckinDate:           checkin.Date,
						Source:                model.CheckinRewardSourceContinuous,
						Description:           strconv.Itoa(currentStreak),
						Status:                model.CheckinRewardStatusPending,
					}

					if rewardCfg.Auto {
						// 自动发放奖励
						err = NewCheckinRewardSvc().InternalDistributeReward(sessionCtx, adminOrgUser.OrganizationId, checkinReward)
						if err != nil {
							// 如果是余额奖励且余额不足,记录日志但不阻止签到
							if rewardCfg.RewardType == model.CheckinRewardTypeCash && errs.Unwrap(err).Error() == freeErrors.ErrorMessages[freeErrors.ErrInsufficientBalance] {
								log.ZWarn(sessionCtx, "组织余额不足,跳过余额奖励发放", err,
									"org_id", adminOrgUser.OrganizationId.Hex(),
									"user_id", orgUser.UserId,
									"reward_amount", rewardCfg.RewardAmount.String())
								// 保持状态为 Pending,不设置为 Apply
							} else {
								return err
							}
						} else {
							checkinReward.Status = model.CheckinRewardStatusApply
						}
					}

					err = checkinRewardDao.Create(sessionCtx, checkinReward)
					if err != nil {
						return err
					}
				}
			}

			// 移动到下一天
			currentDate = currentDate.AddDate(0, 0, 1)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (w *CheckinSvc) WebCreateCheckin(ctx context.Context, userId string, org *orgModel.Organization) (*WebCreateCheckinResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	orgUserDao := orgModel.NewOrganizationUserDao(db)
	orgRolePermissionDao := orgModel.NewOrganizationRolePermissionDao(db)
	checkinDao := model.NewCheckinDao(db)
	checkinRewardCfgDao := model.NewCheckinRewardConfigDao(db)
	dailyCheckinRewardCfgDao := model.NewDailyCheckinRewardConfigDao(db)
	checkinRewardDao := model.NewCheckinRewardDao(db)
	walletInfoDao := walletModel.NewWalletInfoDao(db)

	resp := &WebCreateCheckinResp{
		CheckinRewards: make([]*dto.CheckinRewardResp, 0),
	}

	// 创建上下文
	logCtx := context.Background()

	err := mongoCli.GetTx().Transaction(logCtx, func(sessionCtx context.Context) error {
		orgUser, err := orgUserDao.GetByUserIdAndOrgId(sessionCtx, userId, org.ID)
		if err != nil {
			return err
		}

		hasPermission, err := orgRolePermissionDao.ExistPermission(sessionCtx, orgUser.OrganizationId, orgUser.Role, orgModel.PermissionCodeCheckin)
		if err != nil {
			return err
		}
		if !hasPermission {
			return freeErrors.ApiErr("no permission")
		}

		// 检查是否有余额奖励配置,如果有则需要验证钱包是否开通
		// 使用中国时区(CST)替代UTC
		checkinDate := utils.TodayCST()

		// 判断今天在本组织是否已经打过卡 - 按组织检查
		latestCheckIn, err := checkinDao.GetByImServerUserIdAndDateAndOrgId(sessionCtx, orgUser.ImServerUserId, org.ID, checkinDate)
		if !dbutil.IsDBNotFound(err) {
			if err != nil {
				return err
			}
			log.ZError(sessionCtx, "今天已经签到过", nil,
				"userID", orgUser.ImServerUserId,
				"日期", checkinDate.Format("2006-01-02"))
			return freeErrors.ApiErr("You have already signed in today: " + latestCheckIn.Date.Format("2006-01-02"))
		}

		// 在数据库已有连续天数基础上计算：取本组织下「签到日期」最新一条的 streak，若昨日已签则今日 = streak+1，否则从 1 开始
		latestCheckin, err := checkinDao.GetLatestCheckInByDateByImServerUserIdAndOrgId(sessionCtx, orgUser.ImServerUserId, org.ID)
		if err != nil && !dbutil.IsDBNotFound(err) {
			log.ZError(sessionCtx, "获取用户最近签到记录失败", err,
				"userID", orgUser.ImServerUserId, "orgID", org.ID.Hex())
			return err
		}

		log.ZInfo(sessionCtx, "开始计算连续签到天数",
			"userID", orgUser.ImServerUserId,
			"orgID", org.ID.Hex(),
			"今日签到日期", checkinDate.Format("2006-01-02"))

		var tempStreak int
		if latestCheckin == nil {
			tempStreak = 1
		} else {
			latestDateCST := utils.TimeToCST(latestCheckin.Date)
			yesterdayCST := checkinDate.AddDate(0, 0, -1)
			latestDay := time.Date(latestDateCST.Year(), latestDateCST.Month(), latestDateCST.Day(), 0, 0, 0, 0, utils.CST)
			yesterdayDay := time.Date(yesterdayCST.Year(), yesterdayCST.Month(), yesterdayCST.Day(), 0, 0, 0, 0, utils.CST)
			if latestDay.Equal(yesterdayDay) {
				tempStreak = latestCheckin.Streak + 1
			} else {
				tempStreak = 1
			}
		}

		// 查询日常签到奖励配置
		dailyRewardCfg, err := dailyCheckinRewardCfgDao.GetByOrgId(sessionCtx, org.ID)
		if err != nil && !dbutil.IsDBNotFound(err) {
			return err
		}

		// 获取今天的连续签到奖励配置
		tempCheckinRewardCfg, err := checkinRewardCfgDao.SelectByOrgIdAndStreak(sessionCtx, org.ID, tempStreak)
		if err != nil && !dbutil.IsDBNotFound(err) {
			return err
		}

		// 检查是否有余额奖励（日常+连续）
		hasCashReward := false
		if dailyRewardCfg != nil {
			hasCashReward = true
		}
		if !hasCashReward {
			for _, cfg := range tempCheckinRewardCfg {
				if cfg.RewardType == model.CheckinRewardTypeCash {
					hasCashReward = true
					break
				}
			}
		}

		// 如果有余额奖励,检查用户钱包是否开通
		if hasCashReward {
			_, err := walletInfoDao.GetByOwnerIdAndOwnerType(sessionCtx, userId, walletModel.WalletInfoOwnerTypeOrdinary)
			if err != nil {
				if dbutil.IsDBNotFound(err) {
					return freeErrors.WalletNotOpenErr
				}
				return err
			}
		}

		// 已经提前检查今天是否签到，此处无需重复检查

		// 使用之前计算的连续签到数
		streak := tempStreak

		// 显式跟踪处理前的连续签到天数
		initialStreak := streak
		log.ZInfo(sessionCtx, "准备创建签到记录 - 初始连续签到天数",
			"userID", orgUser.ImServerUserId,
			"initialStreak", initialStreak,
			"date", checkinDate.Format("2006-01-02"))

		// 预先创建完整的签到对象并生成ID
		checkinObj := &model.Checkin{
			ID:             primitive.NewObjectID(), // 预先生成ID
			Date:           checkinDate,
			ImServerUserId: orgUser.ImServerUserId,
			OrgId:          org.ID,
			Streak:         streak,
			CreatedAt:      utils.NowCST(), // 设置创建时间，避免DAO内再次设置
		}

		err = checkinDao.Create(sessionCtx, checkinObj)
		if err != nil {
			log.ZError(sessionCtx, "创建签到记录失败", err,
				"userID", orgUser.ImServerUserId,
				"streak", streak)
			return err
		}

		// 直接使用已创建的对象，避免再次查询
		checkin := checkinObj

		// 用全量签到记录重算今日的连续天数，避免「最近一条 streak+1」取错或历史错误导致 99→10 等异常
		allCheckins, err := checkinDao.GetAllByImServerUserIdAndOrgId(sessionCtx, orgUser.ImServerUserId, org.ID)
		if err != nil {
			log.ZError(sessionCtx, "获取全量签到记录用于校验 streak 失败", err, "userID", orgUser.ImServerUserId)
			return err
		}
		for _, c := range allCheckins {
			c.Date = utils.TimeToCST(c.Date)
		}
		correctStreak, err := calculateCorrectStreak(sessionCtx, allCheckins, checkinDate)
		if err != nil {
			log.ZError(sessionCtx, "计算今日连续签到天数失败", err, "userID", orgUser.ImServerUserId, "date", checkinDate.Format("2006-01-02"))
			return err
		}
		if correctStreak != checkin.Streak {
			if err := checkinDao.UpdateStreak(sessionCtx, checkin.ID, correctStreak); err != nil {
				log.ZError(sessionCtx, "修正新建签到的 streak 失败", err, "checkinID", checkin.ID.Hex())
				return err
			}
			checkin.Streak = correctStreak
			streak = correctStreak
			log.ZInfo(sessionCtx, "已按全量序列修正新建签到的连续天数",
				"userID", orgUser.ImServerUserId, "date", checkinDate.Format("2006-01-02"),
				"原值", checkinObj.Streak, "修正值", correctStreak)
		}

		// 后续奖励、回显均使用修正后的 streak
		log.ZInfo(sessionCtx, "创建签到记录成功",
			"ID", checkin.ID.Hex(),
			"UserID", checkin.ImServerUserId,
			"Date", checkin.Date.Format("2006-01-02"),
			"Streak", checkin.Streak)

		// 【新增】发放日常签到奖励
		if dailyRewardCfg != nil {
			dailyCheckinReward := &model.CheckinReward{
				CheckinId:             checkin.ID,
				CheckinRewardConfigId: dailyRewardCfg.ID,
				ImServerUserId:        orgUser.ImServerUserId,
				RewardAmount:          dailyRewardCfg.RewardAmount,
				RewardId:              dailyRewardCfg.RewardId,
				RewardType:            dailyRewardCfg.RewardType,
				OrgID:                 orgUser.OrganizationId,
				CheckinDate:           checkin.Date,
				Source:                model.CheckinRewardSourceDaily,
				Description:           strconv.Itoa(checkin.Streak), // 存连续天数，列表可准确展示
				Status:                model.CheckinRewardStatusPending,
			}

			// 日常奖励永远自动发放
			err = NewCheckinRewardSvc().InternalDistributeReward(sessionCtx, org.ID, dailyCheckinReward)
			if err != nil {
				// 如果是余额奖励且余额不足,记录日志但不阻止签到
				if errs.Unwrap(err).Error() == freeErrors.ErrorMessages[freeErrors.ErrInsufficientBalance] {
					log.ZWarn(sessionCtx, "组织余额不足,跳过日常奖励发放", err,
						"org_id", org.ID.Hex(),
						"user_id", orgUser.UserId,
						"reward_amount", dailyRewardCfg.RewardAmount.String())
					// 保持状态为 Pending,不设置为 Apply
				} else {
					return err
				}
			} else {
				dailyCheckinReward.Status = model.CheckinRewardStatusApply
			}

			err = checkinRewardDao.Create(sessionCtx, dailyCheckinReward)
			if err != nil {
				return err
			}

			rewardResp, err := dto.NewCheckinRewardResp(db, dailyCheckinReward)
			if err != nil {
				return err
			}
			resp.CheckinRewards = append(resp.CheckinRewards, rewardResp)
		}

		// 创建连续签到奖励（阶段奖励每个配置只发一次，判重后跳过）
		checkinRewardCfg, err := checkinRewardCfgDao.SelectByOrgIdAndStreak(sessionCtx, org.ID, streak)
		if err != nil {
			return err
		}

		for _, rewardCfg := range checkinRewardCfg {
			exists, err := checkinRewardDao.ExistContinuousByOrgIdAndImServerUserIdAndConfigId(sessionCtx, org.ID, orgUser.ImServerUserId, rewardCfg.ID)
			if err != nil {
				return err
			}
			if exists {
				log.ZInfo(sessionCtx, "已存在该阶段连续签到奖励，跳过", "configId", rewardCfg.ID.Hex(), "streak", streak)
				continue
			}

			checkinReward := &model.CheckinReward{
				CheckinId:             checkin.ID,
				CheckinRewardConfigId: rewardCfg.ID,
				ImServerUserId:        orgUser.ImServerUserId,
				RewardAmount:          rewardCfg.RewardAmount,
				RewardId:              rewardCfg.RewardId,
				RewardType:            rewardCfg.RewardType,
				OrgID:                 orgUser.OrganizationId,
				CheckinDate:           checkin.Date,
				Source:                model.CheckinRewardSourceContinuous,
				Description:           strconv.Itoa(streak),
				Status:                model.CheckinRewardStatusPending,
			}

			if rewardCfg.Auto {
				// 自动发放奖励
				err = NewCheckinRewardSvc().InternalDistributeReward(sessionCtx, org.ID, checkinReward)
				if err != nil {
					// 如果是余额奖励且余额不足,记录日志但不阻止签到
					if rewardCfg.RewardType == model.CheckinRewardTypeCash && errs.Unwrap(err).Error() == freeErrors.ErrorMessages[freeErrors.ErrInsufficientBalance] {
						log.ZWarn(sessionCtx, "组织余额不足,跳过余额奖励发放", err,
							"org_id", org.ID.Hex(),
							"user_id", orgUser.UserId,
							"reward_amount", rewardCfg.RewardAmount.String())
						// 保持状态为 Pending,不设置为 Apply
					} else {
						return err
					}
				} else {
					checkinReward.Status = model.CheckinRewardStatusApply
				}
			}

			err = checkinRewardDao.Create(sessionCtx, checkinReward)
			if err != nil {
				return err
			}

			rewardResp, err := dto.NewCheckinRewardResp(db, checkinReward)
			if err != nil {
				return err
			}
			resp.CheckinRewards = append(resp.CheckinRewards, rewardResp)
		}

		// 回填本次签到后的连续签到天数与今日签到记录，供前端直接回显，无需再请求详情
		resp.Streak = checkin.Streak
		checkinForResp := *checkin
		checkinForResp.Date = utils.TimeToCST(checkinForResp.Date)
		checkinForResp.CreatedAt = utils.TimeToCST(checkinForResp.CreatedAt)
		resp.TodayCheckin = &checkinForResp

		return nil
	})
	return resp, errs.Unwrap(err)
}

type DetailUserCheckinResp struct {
	Streak        int              `json:"streak"`
	TodayCheckin  *model.Checkin   `json:"today_checkin"`
	CheckinRecord []*model.Checkin `json:"checkin_record"`
}

// filterDuplicateCheckins 过滤同一天的重复签到数据，每天只保留一条记录
func (w *CheckinSvc) filterDuplicateCheckins(ctx context.Context, records []*model.Checkin) []*model.Checkin {
	if len(records) == 0 {
		return records
	}

	// 使用map记录每个日期的签到记录，如果有重复的日期，保留最新的记录（创建时间最新的）
	dateMap := make(map[string]*model.Checkin)
	duplicatesCount := make(map[string]int)

	for _, record := range records {
		// 将日期格式化为YYYY-MM-DD字符串作为键
		dateKey := record.Date.Format("2006-01-02")

		if existing, exists := dateMap[dateKey]; exists {
			// 记录重复次数
			duplicatesCount[dateKey]++

			// 如果已存在该日期的记录，比较创建时间，保留较新的记录
			if record.CreatedAt.After(existing.CreatedAt) {
				log.ZInfo(ctx, "发现重复签到数据，保留较新记录",
					"日期", dateKey,
					"旧记录ID", existing.ID.Hex(),
					"旧记录时间", existing.CreatedAt.Format("2006-01-02 15:04:05"),
					"新记录ID", record.ID.Hex(),
					"新记录时间", record.CreatedAt.Format("2006-01-02 15:04:05"))
				dateMap[dateKey] = record
			} else {
				log.ZInfo(ctx, "发现重复签到数据，保留较早记录",
					"日期", dateKey,
					"保留记录ID", existing.ID.Hex(),
					"保留记录时间", existing.CreatedAt.Format("2006-01-02 15:04:05"),
					"忽略记录ID", record.ID.Hex(),
					"忽略记录时间", record.CreatedAt.Format("2006-01-02 15:04:05"))
			}
		} else {
			// 如果不存在该日期的记录，直接添加
			dateMap[dateKey] = record
			duplicatesCount[dateKey] = 0
		}
	}

	// 输出重复记录的统计信息
	duplicateDays := 0
	totalDuplicates := 0
	for date, count := range duplicatesCount {
		if count > 0 {
			duplicateDays++
			totalDuplicates += count
			log.ZInfo(ctx, "日期存在重复签到记录",
				"日期", date,
				"重复数", count)
		}
	}

	// 如果有重复记录，输出汇总信息
	if totalDuplicates > 0 {
		log.ZWarn(ctx, "签到数据中存在重复记录", nil,
			"总签到记录数", len(records),
			"去重后记录数", len(dateMap),
			"存在重复记录的天数", duplicateDays,
			"重复记录总数", totalDuplicates)
	}

	// 将map中的记录转换回切片
	filteredRecords := make([]*model.Checkin, 0, len(dateMap))
	for _, record := range dateMap {
		filteredRecords = append(filteredRecords, record)
	}

	return filteredRecords
}

func (w *CheckinSvc) WebDetailUserCheckin(ctx context.Context, orgUser *orgModel.OrganizationUser,
	startTime time.Time, endTime time.Time) (*DetailUserCheckinResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	checkinDao := model.NewCheckinDao(db)

	resp := &DetailUserCheckinResp{}

	// 使用中国时区(CST)替代UTC
	checkinDate := utils.TodayCST()

	todayCheckin, err := checkinDao.GetByImServerUserIdAndDateAndOrgId(context.TODO(), orgUser.ImServerUserId, orgUser.OrganizationId, checkinDate)
	if !dbutil.IsDBNotFound(err) {
		if err != nil {
			return nil, err
		}
		// 确保今日签到记录的时区为CST
		if todayCheckin != nil {
			todayCheckin.Date = utils.TimeToCST(todayCheckin.Date)
			todayCheckin.CreatedAt = utils.TimeToCST(todayCheckin.CreatedAt)
		}
		resp.TodayCheckin = todayCheckin
	}

	// 历史打卡记录
	_, records, err := checkinDao.SelectJoinOgrUserAndUser(context.TODO(), orgUser.ImServerUserId, "", orgUser.OrganizationId, startTime, endTime, nil)
	if err != nil {
		return nil, err
	}

	// 确保所有记录使用CST时区
	for _, record := range records {
		record.Date = utils.TimeToCST(record.Date)
		record.CreatedAt = utils.TimeToCST(record.CreatedAt)
	}

	// 过滤重复的签到数据（同一天只保留一条记录）
	filteredRecords := w.filterDuplicateCheckins(ctx, records)

	// 手动按签到日期降序排列（因为filterDuplicateCheckins使用map不保证顺序）
	sort.Slice(filteredRecords, func(i, j int) bool {
		return filteredRecords[i].Date.After(filteredRecords[j].Date)
	})

	resp.CheckinRecord = filteredRecords

	// 连续签到天数：以数据库记录为准，取本组织下「签到日期」最新的一条记录的 streak（在已有连续天数基础上展示）
	latestCheckin, err := checkinDao.GetLatestCheckInByDateByImServerUserIdAndOrgId(context.Background(), orgUser.ImServerUserId, orgUser.OrganizationId)
	if err != nil && !dbutil.IsDBNotFound(err) {
		return nil, err
	}
	if latestCheckin != nil {
		latestCheckin.Date = utils.TimeToCST(latestCheckin.Date)
		latestCheckin.CreatedAt = utils.TimeToCST(latestCheckin.CreatedAt)
		resp.Streak = latestCheckin.Streak
	}

	return resp, nil
}

// CmsSupplementMultipleDates 管理员多日期补签功能
// 功能:
// 1. 为用户补签多个指定日期的签到记录
// 2. 为每个补签日期生成相应的日常和连续签到奖励
// 3. 自动更新所有受影响的签到记录的连续签到天数
// 约束条件:
// - 如果日期列表中某天已有签到记录，则跳过该日期
// - 补签后的所有签到记录必须形成一个连续的序列，不允许出现"签到空洞"
func (w *CheckinSvc) CmsSupplementMultipleDates(ctx context.Context, adminOrgUser *orgModel.OrganizationUser, req *CmsSupplementMultipleDatesReq) (*CmsSupplementMultipleDatesResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	// 验证管理员权限
	allowRoles := []orgModel.OrganizationUserRole{
		orgModel.OrganizationUserSuperAdminRole,
		orgModel.OrganizationUserBackendAdminRole,
		orgModel.OrganizationUserGroupManagerRole,
		orgModel.OrganizationUserTermManagerRole,
	}

	if !slices.Contains(allowRoles, adminOrgUser.Role) {
		return nil, freeErrors.ApiErr("no permission to supplement checkin")
	}

	// 验证用户是否在该组织内
	orgUserDao := orgModel.NewOrganizationUserDao(db)
	targetOrgUser, err := orgUserDao.GetByImServerUserId(ctx, req.ImServerUserId)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.ApiErr("user not found in organization")
		}
		return nil, err
	}

	if targetOrgUser.OrganizationId != adminOrgUser.OrganizationId {
		return nil, freeErrors.ApiErr("user not in the same organization")
	}

	// 初始化响应对象
	resp := &CmsSupplementMultipleDatesResp{
		SuccessCount: 0,
		SkippedDates: []time.Time{},
		Rewards:      []*model.Checkin{},
	}

	// 如果没有日期，直接返回
	if len(req.Dates) == 0 {
		return resp, nil
	}

	// 获取今天的开始时间（CST时区）
	todayStart := utils.TodayCST()

	// 检查并规范化所有日期（去除时间部分，只保留日期）
	normalizedDates := make([]time.Time, 0, len(req.Dates))
	dateMap := make(map[string]bool)

	for _, date := range req.Dates {
		// 规范化为CST午夜时间
		normalizedDate := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, utils.CST)

		// 检查日期是否在未来
		if normalizedDate.After(todayStart) || normalizedDate.Equal(todayStart) {
			return nil, freeErrors.ApiErr("cannot supplement checkin for today or future dates")
		}

		// 检查是否有重复日期
		dateKey := normalizedDate.Format("2006-01-02")
		if !dateMap[dateKey] {
			dateMap[dateKey] = true
			normalizedDates = append(normalizedDates, normalizedDate)
		}
	}

	// 按日期排序
	slices.SortFunc(normalizedDates, func(a, b time.Time) int {
		if a.Before(b) {
			return -1
		}
		if a.After(b) {
			return 1
		}
		return 0
	})

	// 验证连续性
	err = mongoCli.GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		checkinDao := model.NewCheckinDao(db)
		dailyCheckinRewardCfgDao := model.NewDailyCheckinRewardConfigDao(db)
		checkinRewardCfgDao := model.NewCheckinRewardConfigDao(db)

		// 不需要获取组织信息
		// 之前的代码中获取组织信息是为了传递给InternalDistributeReward方法
		// 现在直接传递组织ID即可

		// 获取用户在本组织下的已签到记录（按组织过滤，与创建签到一致）
		userCheckins, err := checkinDao.GetAllByImServerUserIdAndOrgId(sessionCtx, req.ImServerUserId, targetOrgUser.OrganizationId)
		if err != nil {
			return err
		}

		// 构建已签到日期映射
		checkinDateMap := make(map[string]bool)
		for _, checkin := range userCheckins {
			// 先将UTC时间转换为CST时区
			cstDate := utils.TimeToCST(checkin.Date)
			dateStr := cstDate.Format("2006-01-02")
			checkinDateMap[dateStr] = true
		}

		// 找出本组织下最近一次签到记录（按签到日期），用于确定当前streak值
		latestCheckin, err := checkinDao.GetLatestCheckInByDateByImServerUserIdAndOrgId(sessionCtx, req.ImServerUserId, targetOrgUser.OrganizationId)
		if err != nil && !dbutil.IsDBNotFound(err) {
			return err
		}

		// 初始化当前连续签到天数
		currentStreak := 0

		if latestCheckin != nil {
			currentStreak = latestCheckin.Streak
			// 不需要latestDate，我们直接使用日期比较
		}

		// 构建所有日期（已签到 + 待补签）
		allDates := make([]time.Time, 0)

		// 添加已有的签到日期
		for _, checkin := range userCheckins {
			normalizedDate := time.Date(
				checkin.Date.Year(),
				checkin.Date.Month(),
				checkin.Date.Day(),
				0, 0, 0, 0, utils.CST,
			)
			allDates = append(allDates, normalizedDate)
		}

		// 添加待补签日期
		for _, date := range normalizedDates {
			dateStr := date.Format("2006-01-02")
			if !checkinDateMap[dateStr] {
				allDates = append(allDates, date)
			}
		}

		// 排序所有日期
		slices.SortFunc(allDates, func(a, b time.Time) int {
			if a.Before(b) {
				return -1
			}
			if a.After(b) {
				return 1
			}
			return 0
		})

		// 记录签到+补签总日期数
		log.ZInfo(sessionCtx, "合并后的签到日期",
			"日期总数", len(allDates))

		// 新的验证逻辑：
		// 1. 将现有签到日期和待补签日期分开处理
		// 2. 检查补签后的结果是否能形成连续序列

		// 创建一个映射，标记哪些日期是现有的签到，哪些是待补签的
		existingDatesMap := make(map[string]bool)
		pendingDatesMap := make(map[string]bool)

		// 标记现有签到日期
		for _, checkin := range userCheckins {
			dateStr := utils.TimeToCST(checkin.Date).Format("2006-01-02")
			existingDatesMap[dateStr] = true
		}

		// 标记待补签日期
		for _, date := range normalizedDates {
			dateStr := date.Format("2006-01-02")
			if !checkinDateMap[dateStr] { // 只添加不存在的日期
				pendingDatesMap[dateStr] = true
			}
		}

		// 检查补签后的结果是否能形成完整的连续序列
		// 我们需要确保：所有空洞都被填补，或者在一个连续序列中

		// 将allDates转换为日期字符串数组，方便查找
		allDatesStr := make([]string, len(allDates))
		for i, date := range allDates {
			allDatesStr[i] = date.Format("2006-01-02")
		}

		// 检查待补签日期是否能填补空洞
		validSupplements := true

		for i := 1; i < len(allDates); i++ {
			// 计算相邻两天的间隔（使用日期差避免浮点误差，跨月时更准确）
			dayDiff := daysBetweenCST(allDates[i-1], allDates[i])

			if dayDiff > 1 {
				// 存在间隔超过1天的情况，检查是否所有间隔都在待补签列表中
				// 计算需要填补的日期
				current := allDates[i-1]
				for j := 1; j < dayDiff; j++ {
					gapDate := current.AddDate(0, 0, j)
					gapDateStr := gapDate.Format("2006-01-02")

					// 如果这个日期不在待补签列表中，那么补签后仍然会有空洞
					if !pendingDatesMap[gapDateStr] && !existingDatesMap[gapDateStr] {
						validSupplements = false
						log.ZInfo(sessionCtx, "Gap remains after supplementing",
							"date", gapDateStr,
							"previous_day", allDates[i-1].Format("2006-01-02"),
							"next_day", allDates[i].Format("2006-01-02"))
						break
					}
				}

				if !validSupplements {
					break
				}
			}
		}

		// 同一批多日期补签，所有记录的 created_at 使用同一个时间
		batchCreatedAt := utils.NowCST()

		// 遍历待补签日期（不再因为“仍有空洞”而报错，只禁止补今天/未来或已签到的日期）
		for _, date := range normalizedDates {
			// 检查是否已签到
			dateStr := date.Format("2006-01-02")
			if checkinDateMap[dateStr] {
				resp.SkippedDates = append(resp.SkippedDates, date)
				log.ZInfo(sessionCtx, "跳过已签到日期", "日期", dateStr)
				continue
			}

			// 构建「已有 + 本次待补签」的完整日期集合，确保 streak 计算包含整段连续链
			fullTempCheckins := make([]*model.Checkin, 0, len(userCheckins)+len(normalizedDates))
			fullTempCheckins = append(fullTempCheckins, userCheckins...)
			for _, d := range normalizedDates {
				ds := d.Format("2006-01-02")
				if !checkinDateMap[ds] {
					fullTempCheckins = append(fullTempCheckins, &model.Checkin{
						Date: d, ImServerUserId: req.ImServerUserId, OrgId: adminOrgUser.OrganizationId,
					})
				}
			}

			calculatedStreak, err := calculateCorrectStreak(sessionCtx, fullTempCheckins, date)
			if err != nil {
				log.ZError(sessionCtx, "计算连续签到天数失败", err,
					"日期", date.Format("2006-01-02"),
					"所有日期数量", len(fullTempCheckins))
				// 使用累加的streak作为后备计算方法
				currentStreak++
			} else {
				// 使用计算得到的正确streak值
				currentStreak = calculatedStreak
			}

			// 创建签到记录（同一批补签共用 batchCreatedAt）
			checkin := &model.Checkin{
				OrgId:          adminOrgUser.OrganizationId,
				ImServerUserId: req.ImServerUserId,
				Date:           date,
				Streak:         currentStreak,
				CreatedAt:      batchCreatedAt,
			}

			// 记录补签日志
			log.ZInfo(sessionCtx, "创建补签记录",
				"用户ID", req.ImServerUserId,
				"日期", date.Format("2006-01-02"),
				"连续签到天数", currentStreak)

			// 存储签到记录
			err = checkinDao.Create(sessionCtx, checkin)
			if err != nil {
				return err
			}

			// 添加到响应中
			resp.Rewards = append(resp.Rewards, checkin)
			resp.SuccessCount++

			// 重要：将新创建的签到记录添加到 userCheckins
			// 这样后续日期的计算才会考虑刚创建的记录
			userCheckins = append(userCheckins, checkin)

			// 处理日常签到奖励
			dailyRewardCfg, err := dailyCheckinRewardCfgDao.GetByOrgId(sessionCtx, adminOrgUser.OrganizationId)
			if err != nil && !dbutil.IsDBNotFound(err) {
				return err
			}

			if dailyRewardCfg != nil {
				dailyCheckinReward := &model.CheckinReward{
					ID:                    primitive.NewObjectID(),
					OrgID:                 adminOrgUser.OrganizationId,
					ImServerUserId:        req.ImServerUserId,
					CheckinId:             checkin.ID,
					CheckinRewardConfigId: dailyRewardCfg.ID,
					CheckinDate:           date,
					RewardType:            dailyRewardCfg.RewardType,
					RewardId:              dailyRewardCfg.RewardId,
					RewardAmount:          dailyRewardCfg.RewardAmount,
					Status:                model.CheckinRewardStatusPending,
					Source:                model.CheckinRewardSourceDaily,
					Description:           strconv.Itoa(currentStreak), // 存连续天数，列表可准确展示
					CreatedAt:             date,
				}

				// 保存奖励记录
				err = model.NewCheckinRewardDao(db).Create(sessionCtx, dailyCheckinReward)
				if err != nil {
					return err
				}

				// 自动发放奖励
				err = NewCheckinRewardSvc().InternalDistributeReward(sessionCtx, adminOrgUser.OrganizationId, dailyCheckinReward)
				if err != nil {
					// 余额不足时记录日志但不阻止补签
					if errs.Unwrap(err).Error() == freeErrors.ErrorMessages[freeErrors.ErrInsufficientBalance] {
						log.ZWarn(sessionCtx, "组织余额不足,跳过日常奖励发放", err, map[string]interface{}{
							"ImServerUserId": req.ImServerUserId,
							"date":           date,
						})
					} else {
						return err
					}
				} else {
					dailyCheckinReward.Status = model.CheckinRewardStatusApply
					err = model.NewCheckinRewardDao(db).UpdateStatus(sessionCtx, dailyCheckinReward.ID, dailyCheckinReward.Status)
					if err != nil {
						return err
					}
				}
			}

			// 处理连续签到奖励：仅精确匹配当前连续天数的阶段（7/30/90/180/365），且每阶段只发一次
			checkinRewardCfg, err := checkinRewardCfgDao.SelectByOrgIdAndStreak(sessionCtx, adminOrgUser.OrganizationId, currentStreak)
			if err != nil && !dbutil.IsDBNotFound(err) {
				return err
			}

			log.ZInfo(sessionCtx, "查询到连续签到奖励配置",
				"用户ID", req.ImServerUserId,
				"连续签到天数", currentStreak,
				"查询方式", "精确匹配(streak=当前天数)",
				"配置数量", len(checkinRewardCfg))

			checkinRewardDao := model.NewCheckinRewardDao(db)
			for _, rewardCfg := range checkinRewardCfg {
				// 已领过该阶段则跳过
				exists, err := checkinRewardDao.ExistContinuousByOrgIdAndImServerUserIdAndConfigId(sessionCtx, adminOrgUser.OrganizationId, req.ImServerUserId, rewardCfg.ID)
				if err != nil {
					return err
				}
				if exists {
					log.ZInfo(sessionCtx, "已存在该阶段连续签到奖励，跳过",
						"用户ID", req.ImServerUserId,
						"配置ID", rewardCfg.ID.Hex(),
						"连续天数", currentStreak)
					continue
				}

				checkinReward := &model.CheckinReward{
					ID:                    primitive.NewObjectID(),
					OrgID:                 adminOrgUser.OrganizationId,
					ImServerUserId:        req.ImServerUserId,
					CheckinId:             checkin.ID,
					CheckinRewardConfigId: rewardCfg.ID,
					CheckinDate:           date,
					RewardType:            rewardCfg.RewardType,
					RewardId:              rewardCfg.RewardId,
					RewardAmount:          rewardCfg.RewardAmount,
					Status:                model.CheckinRewardStatusPending,
					Source:                model.CheckinRewardSourceContinuous,
					Description:           strconv.Itoa(currentStreak),
					CreatedAt:             date,
				}

				// 保存奖励记录
				err = checkinRewardDao.Create(sessionCtx, checkinReward)
				if err != nil {
					return err
				}

				if rewardCfg.Auto {
					// 自动发放奖励
					err = NewCheckinRewardSvc().InternalDistributeReward(sessionCtx, adminOrgUser.OrganizationId, checkinReward)
					if err != nil {
						// 余额不足时记录日志但不阻止补签
						if errs.Unwrap(err).Error() == freeErrors.ErrorMessages[freeErrors.ErrInsufficientBalance] {
							log.ZWarn(sessionCtx, "组织余额不足,跳过连续奖励发放", err, map[string]interface{}{
								"ImServerUserId": req.ImServerUserId,
								"date":           date,
								"streak":         currentStreak,
							})
						} else {
							return err
						}
					} else {
						checkinReward.Status = model.CheckinRewardStatusApply
						err = model.NewCheckinRewardDao(db).UpdateStatus(sessionCtx, checkinReward.ID, checkinReward.Status)
						if err != nil {
							return err
						}
					}
				}
			}
		}

		// 如果有成功补签，对「所有」签到记录（含本次新建的）重新计算并更新 streak，确保签到列表展示正确
		if resp.SuccessCount > 0 {
			// 获取用户在本组织下的所有签到记录（包括刚补签的）
			allCheckins, err := checkinDao.GetAllByImServerUserIdAndOrgId(sessionCtx, req.ImServerUserId, targetOrgUser.OrganizationId)
			if err != nil {
				log.ZError(sessionCtx, "获取用户本组织签到记录失败", err, "userID", req.ImServerUserId)
				return err
			}

			// 收集需要更新的日期：本次补签涉及日期区间内的所有签到记录（含新建），全部重算
			needToUpdateDates := make([]time.Time, 0, len(allCheckins))
			for _, checkin := range allCheckins {
				checkinDate := utils.TimeToCST(checkin.Date)
				checkinDate = time.Date(checkinDate.Year(), checkinDate.Month(), checkinDate.Day(), 0, 0, 0, 0, utils.CST)
				needToUpdateDates = append(needToUpdateDates, checkinDate)
			}

			// 对需要更新的日期按时间排序
			slices.SortFunc(needToUpdateDates, func(a, b time.Time) int {
				if a.Before(b) {
					return -1
				}
				if a.After(b) {
					return 1
				}
				return 0
			})

			log.ZInfo(sessionCtx, "找到需要更新的所有受影响的签到日期",
				"用户ID", req.ImServerUserId,
				"日期数量", len(needToUpdateDates))

			// 更新每个后续日期的连续签到天数
			for _, updateDate := range needToUpdateDates {
				// 查找对应的签到记录ID
				var recordToUpdate *model.Checkin
				for _, checkin := range allCheckins {
					checkinDate := utils.TimeToCST(checkin.Date)
					checkinDate = time.Date(checkinDate.Year(), checkinDate.Month(), checkinDate.Day(), 0, 0, 0, 0, utils.CST)

					if checkinDate.Equal(updateDate) {
						recordToUpdate = checkin
						break
					}
				}

				if recordToUpdate == nil {
					log.ZError(sessionCtx, "无法找到需要更新的签到记录", nil,
						"日期", updateDate.Format("2006-01-02"),
						"用户ID", req.ImServerUserId)
					continue
				}

				// 重新计算连续签到天数
				correctStreak, err := calculateCorrectStreak(sessionCtx, allCheckins, updateDate)
				if err != nil {
					log.ZError(sessionCtx, "计算更新日期的连续签到天数失败", err,
						"日期", updateDate.Format("2006-01-02"),
						"记录ID", recordToUpdate.ID.Hex())
					continue
				}

				// 如果计算结果与当前值不同，则更新
				if recordToUpdate.Streak != correctStreak {
					log.ZInfo(sessionCtx, "更新受影响日期的连续签到天数",
						"日期", updateDate.Format("2006-01-02"),
						"记录ID", recordToUpdate.ID.Hex(),
						"当前值", recordToUpdate.Streak,
						"新值", correctStreak)

					err = checkinDao.UpdateStreak(sessionCtx, recordToUpdate.ID, correctStreak)
					if err != nil {
						log.ZError(sessionCtx, "更新签到记录streak值失败", err,
							"日期", updateDate.Format("2006-01-02"),
							"记录ID", recordToUpdate.ID.Hex())
						continue
					}

					// 为更新后的签到记录重新检查奖励条件
					// 仅检查具体连续签到天数的奖励（不使用阈值查询）
					additionalRewardCfg, err := checkinRewardCfgDao.SelectByOrgIdAndStreak(
						sessionCtx, adminOrgUser.OrganizationId, correctStreak)
					if err != nil && !dbutil.IsDBNotFound(err) {
						log.ZError(sessionCtx, "查询更新后日期的奖励配置失败", err,
							"日期", updateDate.Format("2006-01-02"),
							"连续签到天数", correctStreak)
						continue
					}

					log.ZInfo(sessionCtx, "为更新后的签到记录检查奖励条件",
						"日期", updateDate.Format("2006-01-02"),
						"连续签到天数", correctStreak,
						"符合条件的奖励配置数", len(additionalRewardCfg))

					// 如果找到了匹配的奖励配置，创建奖励记录并发放奖励
					for _, rewardCfg := range additionalRewardCfg {
						// 检查是否已存在相同streak值的奖励记录
						// 这一步是为了避免重复发放奖励
						checkinRewardDao := model.NewCheckinRewardDao(db)
						existingRewards, err := checkinRewardDao.SelectByCheckinId(sessionCtx, recordToUpdate.ID)
						if err != nil && !dbutil.IsDBNotFound(err) {
							log.ZError(sessionCtx, "查询已有奖励记录失败", err,
								"日期", updateDate.Format("2006-01-02"),
								"签到记录ID", recordToUpdate.ID.Hex())
							continue
						}

						// 检查是否已存在相同streak值的连续签到奖励
						rewardExists := false
						for _, existingReward := range existingRewards {
							// 只检查连续签到奖励，不考虑日常奖励
							if existingReward.Source == model.CheckinRewardSourceContinuous &&
								existingReward.Description == strconv.Itoa(correctStreak) {
								rewardExists = true
								log.ZInfo(sessionCtx, "已存在相同连续签到天数的奖励，跳过",
									"日期", updateDate.Format("2006-01-02"),
									"签到记录ID", recordToUpdate.ID.Hex(),
									"连续签到天数", correctStreak,
									"奖励ID", existingReward.ID.Hex())
								break
							}
						}

						if rewardExists {
							continue
						}

						// 创建新的奖励记录
						checkinReward := &model.CheckinReward{
							ID:                    primitive.NewObjectID(),
							OrgID:                 adminOrgUser.OrganizationId,
							ImServerUserId:        req.ImServerUserId,
							CheckinId:             recordToUpdate.ID, // 关联到更新的签到记录
							CheckinDate:           updateDate,
							RewardType:            rewardCfg.RewardType,
							RewardId:              rewardCfg.RewardId,
							RewardAmount:          rewardCfg.RewardAmount,
							Status:                model.CheckinRewardStatusPending,
							Source:                model.CheckinRewardSourceContinuous,
							Description:           strconv.Itoa(correctStreak), // 记录连续签到天数
							CreatedAt:             utils.NowCST(),              // 使用当前时间
							CheckinRewardConfigId: rewardCfg.ID,
						}

						log.ZInfo(sessionCtx, "为更新后的签到记录创建新的奖励",
							"日期", updateDate.Format("2006-01-02"),
							"连续签到天数", correctStreak,
							"奖励类型", string(rewardCfg.RewardType),
							"奖励金额", rewardCfg.RewardAmount.String())

						// 保存奖励记录
						err = checkinRewardDao.Create(sessionCtx, checkinReward)
						if err != nil {
							log.ZError(sessionCtx, "创建奖励记录失败", err,
								"日期", updateDate.Format("2006-01-02"),
								"签到记录ID", recordToUpdate.ID.Hex())
							continue
						}

						if rewardCfg.Auto {
							// 自动发放奖励
							err = NewCheckinRewardSvc().InternalDistributeReward(sessionCtx, adminOrgUser.OrganizationId, checkinReward)
							if err != nil {
								// 余额不足时记录日志但不阻止流程
								if errs.Unwrap(err).Error() == freeErrors.ErrorMessages[freeErrors.ErrInsufficientBalance] {
									log.ZWarn(sessionCtx, "组织余额不足,跳过更新日期的连续奖励发放", err,
										"ImServerUserId", req.ImServerUserId,
										"date", updateDate.Format("2006-01-02"),
										"streak", correctStreak)
								} else {
									log.ZError(sessionCtx, "发放更新日期的奖励失败", err,
										"日期", updateDate.Format("2006-01-02"),
										"连续签到天数", correctStreak)
									continue
								}
							} else {
								checkinReward.Status = model.CheckinRewardStatusApply
								err = checkinRewardDao.UpdateStatus(sessionCtx, checkinReward.ID, checkinReward.Status)
								if err != nil {
									log.ZError(sessionCtx, "更新奖励状态失败", err,
										"奖励ID", checkinReward.ID.Hex())
									continue
								}

								log.ZInfo(sessionCtx, "成功发放更新日期的连续签到奖励",
									"日期", updateDate.Format("2006-01-02"),
									"连续签到天数", correctStreak,
									"奖励ID", checkinReward.ID.Hex())
							}
						}
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (w *CheckinSvc) CmsListUserCheckin(ctx context.Context, imServerUserId string, keyword string, orgId primitive.ObjectID,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.CheckinResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	checkInDao := model.NewCheckinDao(db)

	// 传递用户ID与时间参数到DAO层；当 imServerUserId 为空时，保持原有仅按 keyword 查询的行为
	total, records, err := checkInDao.SelectJoinOgrUserAndUser(context.TODO(), imServerUserId, keyword, orgId, startTime, endTime, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.CheckinResp]{
		List:  make([]*dto.CheckinResp, 0),
		Total: total,
	}

	for _, record := range records {
		checkinResp, err := dto.NewCheckinResp(db, record)
		if err != nil {
			return nil, err
		}
		resp.List = append(resp.List, checkinResp)
	}
	return resp, nil

}

type CheckinRewardConfigSvc struct{}

func NewCheckinRewardConfigSvc() *CheckinRewardConfigSvc {
	return &CheckinRewardConfigSvc{}
}

type CmsCreateCheckinRewardCfgReq struct {
	Streak       int                     `json:"streak"`
	RewardType   model.CheckinRewardType `json:"reward_type"`
	RewardId     string                  `json:"reward_id"`
	RewardAmount decimal.Decimal         `json:"reward_amount"`
	Auto         bool                    `json:"auto"`
}

func (w *CheckinRewardConfigSvc) CmsCreateCheckinRewardCfg(ctx context.Context, userId string, org *orgModel.Organization, req *CmsCreateCheckinRewardCfgReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	orgUserDao := orgModel.NewOrganizationUserDao(db)
	checkinRewardCfgDao := model.NewCheckinRewardConfigDao(db)
	walletCurrencyDao := walletModel.NewWalletCurrencyDao(db)
	lotteryDao := lotteryModel.NewLotteryDao(db)

	orgUser, err := orgUserDao.GetByUserIdAndOrgId(context.TODO(), userId, org.ID)
	if err != nil {
		return err
	}

	allowRole := []orgModel.OrganizationUserRole{orgModel.OrganizationUserSuperAdminRole, orgModel.OrganizationUserBackendAdminRole}
	if !slices.Contains(allowRole, orgUser.Role) {
		return freeErrors.ApiErr("the account is not an admin or super admin")
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		rewardId := primitive.NilObjectID
		if req.RewardId != "" {
			rewardId, err = primitive.ObjectIDFromHex(req.RewardId)
			if err != nil {
				return freeErrors.ApiErr("invalid reward id")
			}
		}

		if req.RewardType == model.CheckinRewardTypeCash {
			exist, err := walletCurrencyDao.ExistByIdAndOrgID(sessionCtx, rewardId, org.ID)
			if err != nil {
				return err
			}
			if !exist {
				return freeErrors.ApiErr("currency not found")
			}
		} else if req.RewardType == model.CheckinRewardTypeLottery {
			_, err = lotteryDao.GetByIdAndOrgId(context.TODO(), rewardId, org.ID)
			if err != nil {
				if !dbutil.IsDBNotFound(err) {
					return freeErrors.ApiErr("lottery not found")
				}
				return err
			}
		} else if req.RewardType == model.CheckinRewardTypeIntegral {
			// todo 创建签到奖励配置是否需要校验积分
		} else {
			return freeErrors.ApiErr("Invalid reward type")
		}

		rewardAmount, err := primitive.ParseDecimal128(req.RewardAmount.String())
		err = checkinRewardCfgDao.Create(sessionCtx, &model.CheckinRewardConfig{
			OrgId:        org.ID,
			Streak:       req.Streak,
			RewardType:   req.RewardType,
			RewardId:     req.RewardId,
			RewardAmount: rewardAmount,
			Auto:         req.Auto,
		})
		return err
	})
	return errs.Unwrap(err)
}

type CmsDeleteCheckinRewardCfgReq struct {
	Id primitive.ObjectID `json:"id"`
}

func (w *CheckinRewardConfigSvc) CmsDeleteCheckinRewardCfg(ctx context.Context, userId string, org *orgModel.Organization, req *CmsDeleteCheckinRewardCfgReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	orgUserDao := orgModel.NewOrganizationUserDao(db)
	checkinRewardCfgDao := model.NewCheckinRewardConfigDao(db)

	orgUser, err := orgUserDao.GetByUserIdAndOrgId(context.TODO(), userId, org.ID)
	if err != nil {
		return err
	}

	allowRole := []orgModel.OrganizationUserRole{orgModel.OrganizationUserSuperAdminRole, orgModel.OrganizationUserBackendAdminRole}
	if !slices.Contains(allowRole, orgUser.Role) {
		return freeErrors.ApiErr("the account is not an admin or super admin")
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		err = checkinRewardCfgDao.DeleteById(sessionCtx, req.Id)
		return err
	})
	return errs.Unwrap(err)
}

func (w *CheckinRewardConfigSvc) CmsListCheckinRewardCfg(ctx context.Context, orgId primitive.ObjectID,
	page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.CheckinRewardConfigResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	checkinCfgDao := model.NewCheckinRewardConfigDao(db)

	total, records, err := checkinCfgDao.Select(context.TODO(), orgId, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.CheckinRewardConfigResp]{
		List:  []*dto.CheckinRewardConfigResp{},
		Total: total,
	}

	// 转换为响应对象
	for _, record := range records {
		item, err := dto.NewCheckinRewardConfigResp(db, record)
		if err != nil {
			return nil, errs.Unwrap(err)
		}
		resp.List = append(resp.List, item)
	}

	return resp, nil

}

type CheckinRewardSvc struct{}

func NewCheckinRewardSvc() *CheckinRewardSvc {
	return &CheckinRewardSvc{}
}

// CmsFixUserRewardsResp 修复用户签到奖励数据的响应
type CmsFixUserRewardsResp struct {
	DeletedContinuousRewards int64 `json:"deleted_continuous_rewards"` // 删除的重复连续奖励条数
}

// CmsFixDailyRewardsResp 修复组织下所有“非阶段性奖励”（daily）的响应
type CmsFixDailyRewardsResp struct {
	UpdatedDailyRewards int64 `json:"updated_daily_rewards"` // 更新（补全/纠正）的日常奖励条数
}

type CmsUpdateCheckinRewardStatusReq struct {
	Id     primitive.ObjectID        `json:"id"`
	Status model.CheckinRewardStatus `json:"status"`
}

func (w *CheckinRewardSvc) CmsUpdateCheckinRewardStatus(ctx context.Context, opOgrUser *orgModel.OrganizationUser, req *CmsUpdateCheckinRewardStatusReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	checkinRewardDao := model.NewCheckinRewardDao(db)

	allowRole := []orgModel.OrganizationUserRole{orgModel.OrganizationUserSuperAdminRole, orgModel.OrganizationUserBackendAdminRole}
	if !slices.Contains(allowRole, opOgrUser.Role) {
		return freeErrors.ApiErr("the account is not an admin or super admin")
	}

	err := mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		checkinReward, err := checkinRewardDao.GetByIdAndOrgId(sessionCtx, req.Id, opOgrUser.OrganizationId)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.NotFoundErrWithResource("checkin reward")
			}
			return err
		}

		err = w.InternalDistributeReward(sessionCtx, opOgrUser.OrganizationId, checkinReward)
		if err != nil {
			return err
		}

		err = checkinRewardDao.UpdateStatusById(sessionCtx, req.Id, req.Status)
		return err
	})
	return errs.Unwrap(err)
}

func (w *CheckinRewardSvc) CmsListCheckinReward(ctx context.Context, orgId primitive.ObjectID,
	status model.CheckinRewardStatus, keyword string, startTime time.Time, stopTime time.Time, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.CheckinRewardJoinAllResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	checkinRewardDao := model.NewCheckinRewardDao(db)

	total, records, err := checkinRewardDao.SelectJoinAll(context.TODO(), "", keyword, orgId, status, startTime, stopTime, page)
	if err != nil {
		return nil, err
	}

	//log.ZInfo(ctx, "list checkin reward", "total", total, "records", records)

	resp := &paginationUtils.ListResp[*dto.CheckinRewardJoinAllResp]{
		List:  []*dto.CheckinRewardJoinAllResp{},
		Total: total,
	}

	for _, record := range records {
		item, err := dto.NewCheckinRewardJoinAllResp(db, record)
		if err != nil {
			return nil, errs.Unwrap(err)
		}
		resp.List = append(resp.List, item)
	}

	return resp, nil

}

// CmsFixUserRewards 修复指定用户在本组织下的签到奖励数据（目前主要是去重连续签到阶段奖励）
func (w *CheckinRewardSvc) CmsFixUserRewards(ctx context.Context, orgId primitive.ObjectID, imServerUserId string) (*CmsFixUserRewardsResp, error) {
	if orgId.IsZero() || imServerUserId == "" {
		return &CmsFixUserRewardsResp{DeletedContinuousRewards: 0}, nil
	}

	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	checkinRewardDao := model.NewCheckinRewardDao(db)

	deleted, err := checkinRewardDao.FixUserContinuousRewards(ctx, orgId, imServerUserId)
	if err != nil {
		return nil, err
	}

	return &CmsFixUserRewardsResp{
		DeletedContinuousRewards: deleted,
	}, nil
}

// CmsFixDailyRewards 修复本组织下所有“非阶段性奖励”（daily）的数据
func (w *CheckinRewardSvc) CmsFixDailyRewards(ctx context.Context, orgId primitive.ObjectID) (*CmsFixDailyRewardsResp, error) {
	if orgId.IsZero() {
		return &CmsFixDailyRewardsResp{UpdatedDailyRewards: 0}, nil
	}

	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	checkinRewardDao := model.NewCheckinRewardDao(db)

	updated, err := checkinRewardDao.FixDailyRewardsByOrg(ctx, orgId)
	if err != nil {
		return nil, err
	}

	return &CmsFixDailyRewardsResp{
		UpdatedDailyRewards: updated,
	}, nil
}

// CmsFixContinuousRewardsResp 修复组织下「阶段性奖励」去重的响应
type CmsFixContinuousRewardsResp struct {
	DeletedContinuousRewards int64 `json:"deleted_continuous_rewards"` // 删除的重复阶段奖励条数
}

// CmsFixContinuousRewards 对本组织下所有「阶段性奖励」（continuous）去重，同一用户同一阶段配置只保留一条
func (w *CheckinRewardSvc) CmsFixContinuousRewards(ctx context.Context, orgId primitive.ObjectID) (*CmsFixContinuousRewardsResp, error) {
	if orgId.IsZero() {
		return &CmsFixContinuousRewardsResp{DeletedContinuousRewards: 0}, nil
	}

	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	checkinRewardDao := model.NewCheckinRewardDao(db)

	deleted, err := checkinRewardDao.FixContinuousRewardsByOrg(ctx, orgId)
	if err != nil {
		return nil, err
	}

	return &CmsFixContinuousRewardsResp{
		DeletedContinuousRewards: deleted,
	}, nil
}

// CmsCleanupInvalidContinuousRewardsResp 清理非法阶段奖励并冲减钱包的响应
type CmsCleanupInvalidContinuousRewardsResp struct {
	DeletedCount  int64 `json:"deleted_count"`  // 删除的奖励条数
	ReversedCount int64 `json:"reversed_count"` // 已冲回钱包的条数（已发放的现金奖励）
}

// CmsCleanupInvalidContinuousRewards 只保留 7/30/90/180/365 阶段且每阶段每用户一条，其余删除；对已发放的现金奖励做钱包冲回
func (w *CheckinRewardSvc) CmsCleanupInvalidContinuousRewards(ctx context.Context, orgId primitive.ObjectID) (*CmsCleanupInvalidContinuousRewardsResp, error) {
	if orgId.IsZero() {
		return &CmsCleanupInvalidContinuousRewardsResp{}, nil
	}
	db := plugin.MongoCli().GetDB()
	checkinRewardDao := model.NewCheckinRewardDao(db)
	toDelete, err := checkinRewardDao.CleanupInvalidContinuousRewardsByOrg(ctx, orgId)
	if err != nil {
		return nil, err
	}
	if len(toDelete) == 0 {
		return &CmsCleanupInvalidContinuousRewardsResp{DeletedCount: 0, ReversedCount: 0}, nil
	}

	tsSvc := transactionSvc.NewTransactionService()
	orgIdHex := orgId.Hex()
	var reversed int64
	for _, r := range toDelete {
		if r.Status != model.CheckinRewardStatusApply || r.RewardType != model.CheckinRewardTypeCash || r.RewardId == "" {
			continue
		}
		err := tsSvc.InternalOrganizationSignInRewardReversal(ctx, orgIdHex, r.ImServerUserId, r.RewardId, r.RewardAmount.String())
		if err != nil {
			log.ZWarn(ctx, "签到奖励冲回失败，跳过", err, "reward_id", r.ID.Hex(), "user", r.ImServerUserId, "amount", r.RewardAmount.String())
			continue
		}
		reversed++
	}

	ids := make([]primitive.ObjectID, 0, len(toDelete))
	for _, r := range toDelete {
		ids = append(ids, r.ID)
	}
	res, err := checkinRewardDao.Collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return nil, err
	}
	return &CmsCleanupInvalidContinuousRewardsResp{
		DeletedCount:  res.DeletedCount,
		ReversedCount: reversed,
	}, nil
}

func (w *CheckinRewardSvc) WebListCheckinReward(ctx context.Context, orgUser *orgModel.OrganizationUser,
	status model.CheckinRewardStatus, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.CheckinRewardResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	checkinRewardDao := model.NewCheckinRewardDao(db)

	total, records, err := checkinRewardDao.SelectJoinOgrUser(context.TODO(), orgUser.ImServerUserId, orgUser.OrganizationId, status, time.Time{}, time.Time{}, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.CheckinRewardResp]{
		List:  []*dto.CheckinRewardResp{},
		Total: total,
	}

	for _, record := range records {
		item, err := dto.NewCheckinRewardResp(db, record)
		if err != nil {
			return nil, errs.Unwrap(err)
		}
		resp.List = append(resp.List, item)
	}

	return resp, nil

}

// InternalDistributeReward 分发奖励
func (w *CheckinRewardSvc) InternalDistributeReward(ctx context.Context, orgId primitive.ObjectID, checkinReward *model.CheckinReward) error {
	db := plugin.MongoCli().GetDB()
	lotteryUserTicketDao := lotteryModel.NewLotteryUserTicketDao(db)

	var err error
	rewardId := primitive.NilObjectID
	if checkinReward.RewardId != "" {
		rewardId, err = primitive.ObjectIDFromHex(checkinReward.RewardId)
		if err != nil {
			return errors.New("invalid reward_id")
		}
	}

	if checkinReward.RewardType == model.CheckinRewardTypeLottery {
		// 检查奖励数量是否为正整数
		rewardAmountStr := checkinReward.RewardAmount.String()
		rewardCount, err := strconv.Atoi(rewardAmountStr)
		if err != nil {
			return errors.New("lottery reward amount must be a valid integer")
		}
		if rewardCount <= 0 {
			return errors.New("lottery reward amount must be positive")
		}

		// 批量插入抽奖券
		err = lotteryUserTicketDao.CreateBatch(ctx, checkinReward.ImServerUserId, rewardId, rewardCount)
		if err != nil {
			return err
		}
	} else if checkinReward.RewardType == model.CheckinRewardTypeCash {
		// 尝试从补偿金中扣除
		walletInfoDao := walletModel.NewWalletInfoDao(db)
		compensationSvc := walletSvc.NewCompensationService()

		// 获取用户id
		orgUserDao := orgModel.NewOrganizationUserDao(db)
		orgUser, err := orgUserDao.GetByImServerUserId(ctx, checkinReward.ImServerUserId)
		if err != nil {
			log.ZError(ctx, "Get org user error", err)
			// 如果获取用户失败，回退到原来的逻辑
			tsSvc := transactionSvc.NewTransactionService()
			_, err = tsSvc.InternalOrganizationSignInRewardTransfer(ctx, orgId.Hex(), checkinReward.ImServerUserId, checkinReward.RewardId, checkinReward.RewardAmount.String(), "")
			return err
		}

		// 获取用户钱包
		walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, orgUser.UserId, walletModel.WalletInfoOwnerTypeOrdinary)
		if err != nil {
			if !dbutil.IsDBNotFound(err) {
				log.ZError(ctx, "Get wallet info error", err)
			}
			// 如果获取钱包失败，回退到原来的逻辑
			tsSvc := transactionSvc.NewTransactionService()
			_, err = tsSvc.InternalOrganizationSignInRewardTransfer(ctx, orgId.Hex(), checkinReward.ImServerUserId, checkinReward.RewardId, checkinReward.RewardAmount.String(), "")
			return err
		}

		// 获取奖励金额
		rewardAmount, err := decimal.NewFromString(checkinReward.RewardAmount.String())
		if err != nil {
			log.ZError(ctx, "Parse reward amount error", err)
			// 如果解析奖励金额失败，回退到原来的逻辑
			tsSvc := transactionSvc.NewTransactionService()
			_, err = tsSvc.InternalOrganizationSignInRewardTransfer(ctx, orgId.Hex(), checkinReward.ImServerUserId, checkinReward.RewardId, checkinReward.RewardAmount.String(), "")
			return err
		}

		// 检查货币ID格式 (即使补偿金不再与币种关联，仍需验证格式)
		_, err = primitive.ObjectIDFromHex(checkinReward.RewardId)
		if err != nil {
			log.ZError(ctx, "Parse currency id error", err)
			// 如果解析货币ID失败，回退到原来的逻辑
			tsSvc := transactionSvc.NewTransactionService()
			_, err = tsSvc.InternalOrganizationSignInRewardTransfer(ctx, orgId.Hex(), checkinReward.ImServerUserId, checkinReward.RewardId, checkinReward.RewardAmount.String(), "")
			return err
		}

		// 尝试从补偿金中扣除 - 无需传递币种ID，因为补偿金与币种无关
		deducted, err := compensationSvc.DeductCompensationForCheckin(ctx, walletInfo.ID, rewardAmount)
		if err != nil {
			log.ZError(ctx, "Deduct compensation error", err)
			// 如果扣除补偿金失败，回退到原来的逻辑
			tsSvc := transactionSvc.NewTransactionService()
			_, err = tsSvc.InternalOrganizationSignInRewardTransfer(ctx, orgId.Hex(), checkinReward.ImServerUserId, checkinReward.RewardId, checkinReward.RewardAmount.String(), "")
			return err
		}

		// 如果从补偿金中扣除成功，不需要从组织扣款
		if deducted {
			log.ZInfo(ctx, "Deducted from compensation balance", "walletId", walletInfo.ID, "amount", rewardAmount)

			// 解析货币ID
			currencyId, err := primitive.ObjectIDFromHex(checkinReward.RewardId)
			if err != nil {
				log.ZError(ctx, "Parse currency id error when updating available balance", err)
				return err
			}

			// 增加用户可用余额并创建交易记录
			walletBalanceDao := walletModel.NewWalletBalanceDao(db)
			err = walletBalanceDao.UpdateAvailableBalanceAndAddTsRecord(
				ctx,
				walletInfo.ID,
				currencyId,
				rewardAmount,
				walletTsModel.TsRecordTypeSignInRewardReceive, // Type 42: 签到奖励领取
				"checkin",
				"签到奖励领取",
			)
			if err != nil {
				log.ZError(ctx, "Update available balance error", err,
					"walletId", walletInfo.ID,
					"currencyId", currencyId.Hex(),
					"amount", rewardAmount.String())
				return err
			}

			log.ZInfo(ctx, "Successfully updated available balance for checkin reward",
				"walletId", walletInfo.ID,
				"currencyId", currencyId.Hex(),
				"amount", rewardAmount.String())

			return nil
		}

		// 如果补偿金不足，从组织扣款
		tsSvc := transactionSvc.NewTransactionService()
		_, err = tsSvc.InternalOrganizationSignInRewardTransfer(ctx, orgId.Hex(), checkinReward.ImServerUserId, checkinReward.RewardId, checkinReward.RewardAmount.String(), "")
		if err != nil {
			return err
		}
	} else if checkinReward.RewardType == model.CheckinRewardTypeIntegral {
		points, err := strconv.Atoi(checkinReward.RewardAmount.String())
		if err != nil {
			return err
		}
		err = pointsSvc.NewPointsSvc().IssuePoints(ctx, &pointsSvc.IssuePointsReq{
			ImServerUserId: checkinReward.ImServerUserId,
			OrganizationId: orgId,
			Points:         int64(points),
			PointsType:     pointsModel.PointsTypeCheckin,
			Description:    "签到奖励",
		})
		if err != nil {
			return err
		}

	}
	return nil
}

// DailyCheckinRewardConfigSvc 日常签到奖励配置服务
type DailyCheckinRewardConfigSvc struct{}

func NewDailyCheckinRewardConfigSvc() *DailyCheckinRewardConfigSvc {
	return &DailyCheckinRewardConfigSvc{}
}

type CmsCreateOrUpdateDailyCheckinRewardCfgReq struct {
	RewardId     string          `json:"reward_id"`     // 货币ID
	RewardAmount decimal.Decimal `json:"reward_amount"` // 奖励金额
}

// CmsCreateOrUpdateDailyCheckinRewardCfg 创建或更新日常签到奖励配置
func (w *DailyCheckinRewardConfigSvc) CmsCreateOrUpdateDailyCheckinRewardCfg(ctx context.Context, userId string, org *orgModel.Organization, req *CmsCreateOrUpdateDailyCheckinRewardCfgReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	orgUserDao := orgModel.NewOrganizationUserDao(db)
	dailyCheckinRewardCfgDao := model.NewDailyCheckinRewardConfigDao(db)
	walletCurrencyDao := walletModel.NewWalletCurrencyDao(db)

	orgUser, err := orgUserDao.GetByUserIdAndOrgId(context.TODO(), userId, org.ID)
	if err != nil {
		return err
	}

	allowRole := []orgModel.OrganizationUserRole{orgModel.OrganizationUserSuperAdminRole, orgModel.OrganizationUserBackendAdminRole}
	if !slices.Contains(allowRole, orgUser.Role) {
		return freeErrors.ApiErr("the account is not an admin or super admin")
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		rewardId := primitive.NilObjectID
		if req.RewardId != "" {
			rewardId, err = primitive.ObjectIDFromHex(req.RewardId)
			if err != nil {
				return freeErrors.ApiErr("invalid reward id")
			}
		}

		// 验证货币存在
		exist, err := walletCurrencyDao.ExistByIdAndOrgID(sessionCtx, rewardId, org.ID)
		if err != nil {
			return err
		}
		if !exist {
			return freeErrors.ApiErr("currency not found")
		}

		rewardAmount, err := primitive.ParseDecimal128(req.RewardAmount.String())
		if err != nil {
			return err
		}

		err = dailyCheckinRewardCfgDao.CreateOrUpdate(sessionCtx, &model.DailyCheckinRewardConfig{
			OrgId:        org.ID,
			RewardType:   model.CheckinRewardTypeCash, // 固定为cash
			RewardId:     req.RewardId,
			RewardAmount: rewardAmount,
		})
		return err
	})
	return errs.Unwrap(err)
}

// CmsGetDailyCheckinRewardCfg 查询日常签到奖励配置
func (w *DailyCheckinRewardConfigSvc) CmsGetDailyCheckinRewardCfg(ctx context.Context, orgId primitive.ObjectID) (*dto.DailyCheckinRewardConfigResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	dailyCheckinRewardCfgDao := model.NewDailyCheckinRewardConfigDao(db)

	record, err := dailyCheckinRewardCfgDao.GetByOrgId(context.TODO(), orgId)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, nil // 未配置返回nil
		}
		return nil, err
	}

	return dto.NewDailyCheckinRewardConfigResp(db, record)
}

// CmsDeleteDailyCheckinRewardCfg 删除日常签到奖励配置
func (w *DailyCheckinRewardConfigSvc) CmsDeleteDailyCheckinRewardCfg(ctx context.Context, userId string, org *orgModel.Organization) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	orgUserDao := orgModel.NewOrganizationUserDao(db)
	dailyCheckinRewardCfgDao := model.NewDailyCheckinRewardConfigDao(db)

	orgUser, err := orgUserDao.GetByUserIdAndOrgId(context.TODO(), userId, org.ID)
	if err != nil {
		return err
	}

	allowRole := []orgModel.OrganizationUserRole{orgModel.OrganizationUserSuperAdminRole, orgModel.OrganizationUserBackendAdminRole}
	if !slices.Contains(allowRole, orgUser.Role) {
		return freeErrors.ApiErr("the account is not an admin or super admin")
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		return dailyCheckinRewardCfgDao.DeleteByOrgId(sessionCtx, org.ID)
	})
	return errs.Unwrap(err)
}

// WebGetCheckinRecordsForFixResp 获取签到记录用于修复的响应
type WebGetCheckinRecordsForFixResp struct {
	Records []*model.Checkin `json:"records"` // 最近一段连续签到记录，按日期升序排序
}

// WebFixCheckinRecordsResp 修复签到记录的响应
type WebFixCheckinRecordsResp struct {
	RecordsFixed int `json:"records_fixed"` // 修复的记录数量
	RewardsAdded int `json:"rewards_added"` // 添加的奖励数量
}

// findLatestContinuousCheckins 查找最近一段连续的签到记录
// 输入参数：
//   - ctx：上下文
//   - checkins：已排序的签到记录（按日期降序）
//
// 返回值：
//   - 连续的签到记录，按日期升序排序
func findLatestContinuousCheckins(ctx context.Context, checkins []*model.Checkin) []*model.Checkin {
	if len(checkins) == 0 {
		return []*model.Checkin{}
	}

	// 确保记录按日期降序排列（最近的日期在前）
	slices.SortFunc(checkins, func(a, b *model.Checkin) int {
		if a.Date.After(b.Date) {
			return -1
		}
		if a.Date.Before(b.Date) {
			return 1
		}
		return 0
	})

	// 从最新的日期开始，查找连续签到记录
	var result []*model.Checkin
	result = append(result, checkins[0])

	for i := 1; i < len(checkins); i++ {
		currentDate := checkins[i].Date
		previousDate := checkins[i-1].Date

		// 转换为CST时区并规范化为午夜时间
		currentDate = time.Date(
			currentDate.Year(),
			currentDate.Month(),
			currentDate.Day(),
			0, 0, 0, 0,
			utils.CST,
		)

		previousDate = time.Date(
			previousDate.Year(),
			previousDate.Month(),
			previousDate.Day(),
			0, 0, 0, 0,
			utils.CST,
		)

		// 计算日期差（使用日期差避免浮点误差，跨月时更准确）
		dayDiff := daysBetweenCST(currentDate, previousDate)

		// 如果日期差是1天，说明是连续的
		if dayDiff == 1 {
			result = append(result, checkins[i])
		} else {
			// 找到不连续的地方，停止查找
			break
		}
	}

	// 反转列表，使其按日期升序排序
	slices.Reverse(result)

	return result
}

// WebGetCheckinRecordsForFix 获取签到记录用于修复
// 获取用户最近一段连续的签到记录，用于前端展示和检测异常
func (w *CheckinSvc) WebGetCheckinRecordsForFix(ctx context.Context, targetImServerUserId string, orgId primitive.ObjectID) (*WebGetCheckinRecordsForFixResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	checkinDao := model.NewCheckinDao(db)

	log.ZInfo(ctx, "开始获取用户签到记录用于修复",
		"targetImServerUserId", targetImServerUserId, "orgId", orgId.Hex())

	// 获取目标用户在本组织下的签到记录（按组织过滤）
	allCheckins, err := checkinDao.GetAllByImServerUserIdAndOrgId(ctx, targetImServerUserId, orgId)
	if err != nil {
		return nil, err
	}

	// 确保所有记录使用CST时区
	for _, record := range allCheckins {
		record.Date = utils.TimeToCST(record.Date)
		record.CreatedAt = utils.TimeToCST(record.CreatedAt)
	}

	// 过滤重复的签到数据（同一天只保留一条记录）
	filteredCheckins := w.filterDuplicateCheckins(ctx, allCheckins)

	// 找到最近一段连续的签到记录
	continuousCheckins := findLatestContinuousCheckins(ctx, filteredCheckins)

	log.ZInfo(ctx, "获取用户最近连续签到记录完成",
		"targetUserId", targetImServerUserId,
		"总记录数", len(allCheckins),
		"过滤后记录数", len(filteredCheckins),
		"连续记录数", len(continuousCheckins))

	// 对连续签到记录按日期降序排序（最近的日期在前面）
	slices.SortFunc(continuousCheckins, func(a, b *model.Checkin) int {
		// 降序排列，所以a的日期晚于b时返回-1
		if a.Date.After(b.Date) {
			return -1
		}
		if a.Date.Before(b.Date) {
			return 1
		}
		return 0
	})

	return &WebGetCheckinRecordsForFixResp{
		Records: continuousCheckins,
	}, nil
}

// WebFixCheckinRecords 修复签到记录
// 修复用户最近一段连续签到记录的连续签到天数，并添加缺失的奖励
func (w *CheckinSvc) WebFixCheckinRecords(ctx context.Context, targetImServerUserId string, orgId primitive.ObjectID) (*WebFixCheckinRecordsResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	checkinDao := model.NewCheckinDao(db)
	checkinRewardCfgDao := model.NewCheckinRewardConfigDao(db)
	checkinRewardDao := model.NewCheckinRewardDao(db)

	resp := &WebFixCheckinRecordsResp{
		RecordsFixed: 0,
		RewardsAdded: 0,
	}

	log.ZInfo(ctx, "开始修复用户签到记录",
		"targetImServerUserId", targetImServerUserId)

	// 验证用户在当前组织内
	orgUserDao := orgModel.NewOrganizationUserDao(db)
	targetOrgUser, err := orgUserDao.GetByImServerUserId(ctx, targetImServerUserId)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.ApiErr("user not found in organization")
		}
		return nil, err
	}

	// 确保用户在同一组织
	if targetOrgUser.OrganizationId != orgId {
		return nil, freeErrors.ApiErr("user is not in the same organization")
	}

	err = mongoCli.GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		// 获取目标用户在本组织下的签到记录（按组织过滤）
		allCheckins, err := checkinDao.GetAllByImServerUserIdAndOrgId(sessionCtx, targetImServerUserId, orgId)
		if err != nil {
			return err
		}

		// 确保所有记录使用CST时区
		for _, record := range allCheckins {
			record.Date = utils.TimeToCST(record.Date)
			record.CreatedAt = utils.TimeToCST(record.CreatedAt)
		}

		// 过滤重复的签到数据（同一天只保留一条记录）
		filteredCheckins := w.filterDuplicateCheckins(sessionCtx, allCheckins)

		// 找到最近一段连续的签到记录
		continuousCheckins := findLatestContinuousCheckins(sessionCtx, filteredCheckins)

		log.ZInfo(sessionCtx, "开始修复用户连续签到记录",
			"userID", targetImServerUserId,
			"连续记录数", len(continuousCheckins))

		if len(continuousCheckins) == 0 {
			return freeErrors.ApiErr("no continuous checkin records found")
		}

		// 收集需要添加奖励的记录
		type RewardToAdd struct {
			CheckinID primitive.ObjectID
			Date      time.Time
			Streak    int
		}
		rewardsToAdd := make([]RewardToAdd, 0)

		// 修复每条记录的连续签到天数（使用全量签到记录计算，确保连续链正确）
		for _, checkin := range continuousCheckins {
			correctStreak, err := calculateCorrectStreak(sessionCtx, filteredCheckins, checkin.Date)
			if err != nil {
				log.ZError(sessionCtx, "计算连续签到天数失败", err,
					"日期", checkin.Date.Format("2006-01-02"),
					"记录ID", checkin.ID.Hex())
				continue
			}

			log.ZInfo(sessionCtx, "检查签到记录",
				"日期", checkin.Date.Format("2006-01-02"),
				"当前streak", checkin.Streak,
				"正确streak", correctStreak)

			// 如果连续签到天数不正确，进行修复
			if checkin.Streak != correctStreak {
				err = checkinDao.UpdateStreak(sessionCtx, checkin.ID, correctStreak)
				if err != nil {
					log.ZError(sessionCtx, "更新签到记录streak值失败", err,
						"记录ID", checkin.ID.Hex(),
						"日期", checkin.Date.Format("2006-01-02"))
					return err
				}

				// 计数器+1
				resp.RecordsFixed++

				log.ZInfo(sessionCtx, "已修复签到记录的streak值",
					"记录ID", checkin.ID.Hex(),
					"日期", checkin.Date.Format("2006-01-02"),
					"旧值", checkin.Streak,
					"新值", correctStreak)

				// 检查是否已存在该streak值的奖励
				checkinRewards, err := checkinRewardDao.SelectByCheckinId(sessionCtx, checkin.ID)
				if err != nil && !dbutil.IsDBNotFound(err) {
					return err
				}

				// 检查是否存在对应的连续签到奖励
				hasCorrectStreakReward := false
				for _, reward := range checkinRewards {
					// 检查是否已有该streak值的连续签到奖励
					if reward.Source == model.CheckinRewardSourceContinuous &&
						reward.Description == strconv.Itoa(correctStreak) {
						hasCorrectStreakReward = true
						break
					}
				}

				// 如果没有对应的连续签到奖励，记录需要添加奖励的信息
				if !hasCorrectStreakReward {
					rewardsToAdd = append(rewardsToAdd, RewardToAdd{
						CheckinID: checkin.ID,
						Date:      checkin.Date,
						Streak:    correctStreak,
					})
				}
			}
		}

		// 处理需要添加的奖励
		for _, rewardToAdd := range rewardsToAdd {
			// 查询对应streak值的奖励配置
			rewardConfigs, err := checkinRewardCfgDao.SelectByOrgIdAndStreak(
				sessionCtx, orgId, rewardToAdd.Streak)
			if err != nil && !dbutil.IsDBNotFound(err) {
				return err
			}

			// 如果找到奖励配置，创建并发放奖励（已领过该阶段则跳过）
			for _, config := range rewardConfigs {
				exists, err := checkinRewardDao.ExistContinuousByOrgIdAndImServerUserIdAndConfigId(sessionCtx, orgId, targetImServerUserId, config.ID)
				if err != nil {
					return err
				}
				if exists {
					log.ZInfo(sessionCtx, "已存在该阶段连续签到奖励，跳过",
						"用户ID", targetImServerUserId,
						"配置ID", config.ID.Hex(),
						"连续天数", rewardToAdd.Streak)
					continue
				}

				// 创建新的奖励记录
				checkinReward := &model.CheckinReward{
					ID:                    primitive.NewObjectID(),
					OrgID:                 orgId,
					ImServerUserId:        targetImServerUserId,
					CheckinId:             rewardToAdd.CheckinID,
					CheckinDate:           rewardToAdd.Date,
					RewardType:            config.RewardType,
					RewardId:              config.RewardId,
					RewardAmount:          config.RewardAmount,
					Status:                model.CheckinRewardStatusPending,
					Source:                model.CheckinRewardSourceContinuous,
					Description:           strconv.Itoa(rewardToAdd.Streak),
					CreatedAt:             utils.NowCST(),
					CheckinRewardConfigId: config.ID,
				}

				// 保存奖励记录
				err = checkinRewardDao.Create(sessionCtx, checkinReward)
				if err != nil {
					return err
				}

				// 如果配置为自动发放，则发放奖励
				if config.Auto {
					err = NewCheckinRewardSvc().InternalDistributeReward(
						sessionCtx, orgId, checkinReward)
					if err != nil {
						// 余额不足时记录日志但不阻止流程
						if errs.Unwrap(err).Error() == freeErrors.ErrorMessages[freeErrors.ErrInsufficientBalance] {
							log.ZWarn(sessionCtx, "组织余额不足,跳过连续奖励发放", err,
								"userID", targetImServerUserId,
								"date", rewardToAdd.Date.Format("2006-01-02"),
								"streak", rewardToAdd.Streak)
						} else {
							log.ZError(sessionCtx, "发放奖励失败", err,
								"date", rewardToAdd.Date.Format("2006-01-02"),
								"streak", rewardToAdd.Streak)
							return err
						}
					} else {
						// 更新奖励状态
						checkinReward.Status = model.CheckinRewardStatusApply
						err = checkinRewardDao.UpdateStatus(sessionCtx, checkinReward.ID, checkinReward.Status)
						if err != nil {
							return err
						}
					}
				}

				// 计数器+1
				resp.RewardsAdded++

				log.ZInfo(sessionCtx, "已添加缺失的奖励",
					"日期", rewardToAdd.Date.Format("2006-01-02"),
					"连续签到天数", rewardToAdd.Streak,
					"奖励类型", string(config.RewardType),
					"奖励金额", config.RewardAmount.String())
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return resp, nil
}
