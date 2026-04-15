package systemStatistics

import (
	"time"

	"github.com/robfig/cron/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// SalesDailyStatisticsCronJob 业务员每日统计 Redis 缓存：每日 0 点（上海）刷新「昨日」数据并修剪过期 field
type SalesDailyStatisticsCronJob struct {
	cron *cron.Cron
}

func NewSalesDailyStatisticsCronJob() *SalesDailyStatisticsCronJob {
	loc := time.FixedZone("CST", 8*3600)
	c := cron.New(cron.WithSeconds(), cron.WithLocation(loc))
	return &SalesDailyStatisticsCronJob{cron: c}
}

//func (j *SalesDailyStatisticsCronJob) Start() {
//	ctx := context.Background()
//	// 每天 0 点 0 分 0 秒
//	_, err := j.cron.AddFunc("0 0 0 * * *", func() {
//		dailyRefreshSalesDailyStats(context.Background())
//	})
//	if err != nil {
//		log.ZError(ctx, "添加业务员每日统计定时任务失败", err)
//		return
//	}
//	j.cron.Start()
//	log.ZInfo(ctx, "业务员每日统计缓存定时任务已启动（每日0点刷新昨日数据）")
//}

func (j *SalesDailyStatisticsCronJob) Stop() {
	if j.cron != nil {
		j.cron.Stop()
	}
}

// dailyRefreshSalesDailyStats 与每日 0 点（CST）定时任务相同：全组织刷新昨日/当日 Redis、修剪过期 field
//func dailyRefreshSalesDailyStats(ctx context.Context) {
//	loc := time.FixedZone("CST", 8*3600)
//	now := time.Now().In(loc)
//	yesterday := now.AddDate(0, 0, -1)
//	yDay := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, loc)
//
//	db := plugin.MongoCli().GetDB()
//	if db == nil {
//		return
//	}
//	orgDao := model.NewOrganizationDao(db)
//	orgIDs, err := orgDao.FindAllIdsByStatus(ctx, model.OrganizationStatusPass)
//	if err != nil {
//		log.ZError(ctx, "业务员每日统计定时任务：查询组织列表失败", err)
//		return
//	}
//
//	statSvc := svc.NewSystemStatisticsSvc()
//	rdb := plugin.RedisCli()
//	cutoff := now.AddDate(0, 0, -statsCache.SalesDailyRetentionDays()).Format("20060102")
//
//	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
//
//	for _, oid := range orgIDs {
//		o := oid
//		if err := statSvc.RefreshSalesDailyCacheForOrgDay(ctx, o, yDay); err != nil {
//			log.ZWarn(ctx, "业务员每日统计：刷新昨日缓存失败", err, "orgId", o.Hex())
//		}
//		// 同时写入「当日」field，避免查询区间含今天时 HMGet 缺 key 导致整段缓存不命中
//		if err := statSvc.RefreshSalesDailyCacheForOrgDay(ctx, o, todayStart); err != nil {
//			log.ZWarn(ctx, "业务员每日统计：刷新当日缓存失败", err, "orgId", o.Hex())
//		}
//		if rdb != nil {
//			if err := statsCache.TrimOlderThan(ctx, rdb, o.Hex(), cutoff); err != nil {
//				log.ZWarn(ctx, "业务员每日统计：修剪过期缓存失败", err, "orgId", o.Hex())
//			}
//		}
//	}
//	log.ZInfo(ctx, "业务员每日统计定时任务：昨日缓存刷新完成", "组织数", len(orgIDs), "昨日日期", yDay.Format("20060102"))
//}

// RunManualDailyRefresh 手动触发与定时任务相同的 Redis 刷新逻辑（全组织，可能耗时较长，建议由 HTTP 异步调用）
//func RunManualDailyRefresh(ctx context.Context) {
//	dailyRefreshSalesDailyStats(ctx)
//}

// NotifySalesDailyStatsChangedAsync 新增人数/实名/签到等变动后异步刷新「当日」缓存（CST）
func NotifySalesDailyStatsChangedAsync(orgID primitive.ObjectID) {
	//go func() {
	//	defer func() { recover() }()
	//	ctx := context.Background()
	//	loc := time.FixedZone("CST", 8*3600)
	//	today := time.Now().In(loc)
	//	s := svc.NewSystemStatisticsSvc()
	//	if err := s.RefreshSalesDailyCacheForOrgDay(ctx, orgID, today); err != nil {
	//		log.ZWarn(ctx, "业务员每日统计：异步刷新当日缓存失败", err, "orgId", orgID.Hex())
	//	}
	//}()
}
