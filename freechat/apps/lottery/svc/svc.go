package svc

import (
	"context"
	"math/rand/v2"

	"errors"
	"time"

	"github.com/openimsdk/chat/freechat/apps/lottery/dto"
	"github.com/openimsdk/chat/freechat/apps/lottery/model"
	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/plugin"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/tools/errs"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type LotterySvc struct{}

func NewLotterySvc() *LotterySvc {
	return &LotterySvc{}
}

type CmsCreateLotteryReq struct {
	Name      string `bson:"name" json:"name"`
	Desc      string `bson:"desc" json:"desc"`
	ValidDays int    `bson:"valid_days" json:"valid_days"`

	LotteryConfig []struct {
		LotteryRewardId primitive.ObjectID `bson:"lottery_reward_id" json:"lottery_reward_id"`
		Probability     decimal.Decimal    `bson:"probability" json:"probability"`
	} `json:"lottery_config"`
}

func (w *LotterySvc) CmsCreateLottery(ctx context.Context, orgUser *orgModel.OrganizationUser, req *CmsCreateLotteryReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	lotteryDao := model.NewLotteryDao(db)
	lotteryConfigDao := model.NewLotteryConfigDao(db)

	// 检查抽奖活动名称是否已存在
	existingLottery, err := lotteryDao.GetByNameAndOrgId(ctx, req.Name, orgUser.OrganizationId)
	if err != nil && !dbutil.IsDBNotFound(err) {
		return err
	}
	if existingLottery != nil {
		return freeErrors.LotteryNameExistsErr
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		err := lotteryDao.Create(sessionCtx, &model.Lottery{
			Name:      req.Name,
			Desc:      req.Desc,
			ValidDays: req.ValidDays,
			OrgId:     orgUser.OrganizationId,
		})
		if err != nil {
			return err
		}

		lottery, err := lotteryDao.GetByNameAndOrgId(sessionCtx, req.Name, orgUser.OrganizationId)
		if err != nil {
			return err
		}

		sumProbability := decimal.NewFromInt(0)
		for _, lotteryConfig := range req.LotteryConfig {
			left, err := primitive.ParseDecimal128(sumProbability.String())
			if err != nil {
				return err
			}

			sumProbability = sumProbability.Add(lotteryConfig.Probability)
			if sumProbability.Cmp(decimal.NewFromInt(100)) >= 1 {
				return freeErrors.ApiErr("Probability sum cannot exceed 100")
			}

			right, err := primitive.ParseDecimal128(sumProbability.String())
			if err != nil {
				return err
			}

			err = lotteryConfigDao.Create(sessionCtx, &model.LotteryConfig{
				LotteryId:       lottery.ID,
				LotteryRewardId: lotteryConfig.LotteryRewardId,
				Left:            left,
				Right:           right,
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
	return errs.Unwrap(err)
}

type CmsUpdateLotteryReq struct {
	ID        primitive.ObjectID `bson:"id" json:"id"`
	Name      string             `bson:"name" json:"name"`
	Desc      string             `bson:"desc" json:"desc"`
	ValidDays int                `bson:"valid_days" json:"valid_days"`

	LotteryConfig []struct {
		LotteryRewardId primitive.ObjectID `bson:"lottery_reward_id" json:"lottery_reward_id"`
		Probability     decimal.Decimal    `bson:"probability" json:"probability"`
	} `json:"lottery_config"`
}

func (w *LotterySvc) CmsUpdateLottery(ctx context.Context, orgUser *orgModel.OrganizationUser, req *CmsUpdateLotteryReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	lotteryDao := model.NewLotteryDao(db)
	lotteryConfigDao := model.NewLotteryConfigDao(db)

	// 检查是否存在同名的其他抽奖活动
	existingLottery, err := lotteryDao.GetByNameAndOrgId(ctx, req.Name, orgUser.OrganizationId)
	if err != nil && !dbutil.IsDBNotFound(err) {
		return err
	}
	// 如果存在同名活动且不是当前正在更新的活动，则返回错误
	if existingLottery != nil && existingLottery.ID != req.ID {
		return freeErrors.LotteryNameExistsErr
	}

	err = mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		lottery, err := lotteryDao.GetByIdAndOrgId(sessionCtx, req.ID, orgUser.OrganizationId)
		if err != nil {
			return err
		}

		err = lotteryDao.UpdateById(sessionCtx, req.ID, &model.LotteryUpdateFieldParam{
			Name:      req.Name,
			Desc:      req.Desc,
			ValidDays: req.ValidDays,
		})
		if err != nil {
			return err
		}

		err = lotteryConfigDao.DeleteByLotteryId(sessionCtx, lottery.ID)
		if err != nil {
			return err
		}

		sumProbability := decimal.NewFromInt(0)
		for _, lotteryConfig := range req.LotteryConfig {
			left, err := primitive.ParseDecimal128(sumProbability.String())
			if err != nil {
				return err
			}

			sumProbability = sumProbability.Add(lotteryConfig.Probability)
			if sumProbability.Cmp(decimal.NewFromInt(100)) >= 1 {
				return freeErrors.ApiErr("Probability sum cannot exceed 100")
			}

			right, err := primitive.ParseDecimal128(sumProbability.String())
			if err != nil {
				return err
			}

			err = lotteryConfigDao.Create(sessionCtx, &model.LotteryConfig{
				LotteryId:       lottery.ID,
				LotteryRewardId: lotteryConfig.LotteryRewardId,
				Left:            left,
				Right:           right,
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
	return errs.Unwrap(err)
}

func (w *LotterySvc) CmsListSearchLottery(ctx context.Context, orgUser *orgModel.OrganizationUser, keyword string,
	page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.LotterySimpleResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	lotteryDao := model.NewLotteryDao(db)

	total, result, err := lotteryDao.Select(ctx, keyword, orgUser.OrganizationId, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.LotterySimpleResp]{
		List:  []*dto.LotterySimpleResp{},
		Total: total,
	}

	for _, record := range result {
		item := dto.NewLotterySimpleResp(record)
		resp.List = append(resp.List, item)
	}
	return resp, nil
}

func (w *LotterySvc) WebDetailLottery(ctx context.Context, id primitive.ObjectID) (*dto.DetailLotteryResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	lotteryDao := model.NewLotteryDao(db)
	//lotteryConfigDao := model.NewLotteryConfigDao(db)

	lottery, err := lotteryDao.GetById(ctx, id)
	if err != nil {
		return nil, err
	}

	resp, err := dto.NewDetailLotteryResp(db, lottery)
	return resp, err
}

func (w *LotterySvc) CmsListLottery(ctx context.Context, orgUser *orgModel.OrganizationUser, keyword string,
	page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.DetailLotteryResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	lotteryDao := model.NewLotteryDao(db)

	total, result, err := lotteryDao.Select(ctx, keyword, orgUser.OrganizationId, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.DetailLotteryResp]{
		List:  []*dto.DetailLotteryResp{},
		Total: total,
	}

	for _, record := range result {
		item, err := dto.NewDetailLotteryResp(db, record)
		if err != nil {
			return nil, err
		}
		resp.List = append(resp.List, item)
	}
	return resp, nil
}

type LotteryUserTicketSvc struct{}

func NewLotteryUserTicketSvc() *LotteryUserTicketSvc {
	return &LotteryUserTicketSvc{}
}

func (w *LotteryUserTicketSvc) WebListLotteryUserTicket(ctx context.Context, orgUser *orgModel.OrganizationUser,
	page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.LotteryUserTicketResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	lotteryUserTicketDao := model.NewLotteryUserTicketDao(db)

	total, result, err := lotteryUserTicketDao.Select(ctx, orgUser.ImServerUserId, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.LotteryUserTicketResp]{
		List:  []*dto.LotteryUserTicketResp{},
		Total: total,
	}

	for _, record := range result {
		item, err := dto.NewLotteryUserTicketResp(db, record)
		if err != nil {
			return nil, errs.Unwrap(err)
		}
		resp.List = append(resp.List, item)
	}
	return resp, nil
}

type WebUseLotteryRewardResp struct {
	RandomNum    decimal.Decimal        `json:"random_num"`
	RewardConfig *dto.LotteryConfigResp ` json:"reward_config"`
}

func (w *LotteryUserTicketSvc) WebUseLotteryReward(ctx context.Context, orgUser *orgModel.OrganizationUser, req *WebUseLotteryRewardReq) (*WebUseLotteryRewardResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	lotteryUserTicketDao := model.NewLotteryUserTicketDao(db)
	lotteryDao := model.NewLotteryDao(db)
	lotteryCfgDao := model.NewLotteryConfigDao(db)
	lotteryUserRecordDao := model.NewLotteryUserRecordDao(db)
	//lotteryRewardDao := model.NewLotteryRewardDao(db)
	orgRolePermissionDao := orgModel.NewOrganizationRolePermissionDao(db)

	var (
		lotteryRewardConfig *model.LotteryConfig
		randomNum           decimal.Decimal
	)

	err := mongoCli.GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		userTicket, err := lotteryUserTicketDao.GetById(sessionCtx, req.LotteryTicketId)
		if err != nil {
			return err
		}
		if orgUser.ImServerUserId != userTicket.ImServerUserId {
			return freeErrors.ApiErr("you are not the owner of this ticket")
		}

		hasPermission, err := orgRolePermissionDao.ExistPermission(sessionCtx, orgUser.OrganizationId, orgUser.Role, orgModel.PermissionCodeLottery)
		if err != nil {
			return err
		}
		if !hasPermission {
			return freeErrors.ApiErr("no permission")
		}

		lottery, err := lotteryDao.GetByIdAndOrgId(sessionCtx, userTicket.LotteryId, orgUser.OrganizationId)
		if err != nil {
			return err
		}

		date := userTicket.CreatedAt.AddDate(0, 0, lottery.ValidDays)
		expired := time.Now().UTC().Compare(date) > 0
		if expired {
			return freeErrors.ApiErr("ticket expired")
		}

		if userTicket.Use {
			return freeErrors.ApiErr("ticket already used")
		}

		randomNum = decimal.NewFromFloat(rand.Float64() * 100)

		_, lotteryCfgs, err := lotteryCfgDao.Select(sessionCtx, userTicket.LotteryId, nil)
		if err != nil {
			return err
		}

		for _, cfg := range lotteryCfgs {
			left, err := decimal.NewFromString(cfg.Left.String())
			if err != nil {
				return freeErrors.ApiErr("Internal error: invalid left amount constant cfg id: " + cfg.ID.Hex())
			}
			right, err := decimal.NewFromString(cfg.Right.String())
			if err != nil {
				return freeErrors.ApiErr("Internal error: invalid right amount constant cfg id: " + cfg.ID.Hex())
			}
			// 抽中
			if randomNum.GreaterThanOrEqual(left) && randomNum.LessThan(right) {
				// 添加抽奖记录
				err = lotteryUserRecordDao.Create(sessionCtx, &model.LotteryUserRecord{
					ImServerUserId:      orgUser.ImServerUserId,
					LotteryId:           userTicket.LotteryId,
					LotteryUserTicketId: userTicket.ID,
					RewardId:            cfg.LotteryRewardId,
				})
				if err != nil {
					return err
				}
				lotteryRewardConfig = cfg
				break
			}
		}

		// 未抽到奖励
		if lotteryRewardConfig == nil {
			err = lotteryUserRecordDao.Create(sessionCtx, &model.LotteryUserRecord{
				ImServerUserId:      orgUser.ImServerUserId,
				LotteryId:           userTicket.LotteryId,
				LotteryUserTicketId: userTicket.ID,
				RewardId:            primitive.NilObjectID,
			})
			if err != nil {
				return err
			}
		}

		err = lotteryUserTicketDao.UpdateUseById(sessionCtx, userTicket.ID)
		return err
	})
	if err != nil {
		return nil, errors.Unwrap(err)
	}

	resp := &WebUseLotteryRewardResp{
		RandomNum: randomNum,
	}

	if lotteryRewardConfig != nil {
		resp.RewardConfig, err = dto.NewLotteryConfigResp(db, lotteryRewardConfig)
		if err != nil {
			return nil, err
		}
	}

	return resp, errors.Unwrap(err)
}

type CMSCreateLotteryRewardReq struct {
	Name   string `json:"name" binding:"required"`
	Img    string `json:"img"`
	Remark string `json:"remark"`
	Entity *bool  `json:"entity"`
	Type   string `json:"type"`
}
type CMSUpdateLotteryRewardReq struct {
	CMSDeleteLotteryRewardReq
	CMSCreateLotteryRewardReq
}

type CMSQueryLotteryRewardReq struct {
	Name   string `form:"name"`
	Type   string `form:"type"`
	Entity *bool  `form:"entity"`
	Page   *paginationUtils.DepPagination
}

type CMSDeleteLotteryRewardReq struct {
	ID primitive.ObjectID `json:"id" form:"id" uri:"id" binding:"required"`
}

// CMSCreateLotteryReward CMS创建抽奖奖品
func (w *LotterySvc) CMSCreateLotteryReward(ctx context.Context, orgUser *orgModel.OrganizationUser, req *CMSCreateLotteryRewardReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	lotteryRewardDao := model.NewLotteryRewardDao(db)

	return lotteryRewardDao.CreateLotteryReward(ctx, &model.LotteryReward{
		Name:     req.Name,
		Img:      req.Img,
		Remark:   req.Remark,
		Entity:   req.Entity,
		Type:     req.Type,
		OrgId:    orgUser.OrganizationId,
		CreateAt: time.Now(),
		Status:   1,
	})
}

func (w *LotterySvc) CMSDeleteLotteryReward(ctx context.Context, orgUser *orgModel.OrganizationUser, req *CMSDeleteLotteryRewardReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	// 检查当前奖品是否被抽奖配置使用
	lotteryConfigDao := model.NewLotteryConfigDao(db)
	inUse, err := lotteryConfigDao.CheckRewardInUse(ctx, req.ID)
	if err != nil {
		return errs.Wrap(err)
	}

	if inUse {
		return freeErrors.LotteryRewardInUseErr
	}

	lotteryRewardDao := model.NewLotteryRewardDao(db)
	return lotteryRewardDao.DeleteLotteryReward(ctx, orgUser.OrganizationId, req.ID)
}

func (w *LotterySvc) CMSUpdateLotteryReward(ctx context.Context, orgUser *orgModel.OrganizationUser, req *CMSUpdateLotteryRewardReq) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	lotteryRewardDao := model.NewLotteryRewardDao(db)

	return lotteryRewardDao.UpdateLotteryReward(ctx, orgUser.OrganizationId, &model.LotteryReward{
		ID:     req.ID,
		Name:   req.Name,
		Img:    req.Img,
		Remark: req.Remark,
		Entity: req.Entity,
		Type:   req.Type,
		Status: 1,
	})
}

type CMSQueryLotteryRewardResp struct {
	Name     string `json:"name"`
	Img      string `json:"img"`
	Remark   string `json:"remark"`
	Entity   *bool  `json:"entity"`
	Type     string `json:"type"`
	ID       primitive.ObjectID
	CreateAt time.Time
}

func (w *LotterySvc) CMSFindLotteryReward(ctx context.Context, orgUser *orgModel.OrganizationUser,
	req *CMSQueryLotteryRewardReq) *paginationUtils.ListResp[*CMSQueryLotteryRewardResp] {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	lotteryRewardDao := model.NewLotteryRewardDao(db)

	total, items, err := lotteryRewardDao.FindLotteryReward(ctx, orgUser.OrganizationId, model.LotteryReward{
		Name:   req.Name,
		Entity: req.Entity,
		Type:   req.Type,
		Status: 1,
	}, req.Page)
	if err != nil || 0 == total {
		return &paginationUtils.ListResp[*CMSQueryLotteryRewardResp]{
			Total: 0,
			List:  nil,
		}
	}
	result := &paginationUtils.ListResp[*CMSQueryLotteryRewardResp]{
		Total: total,
		List:  []*CMSQueryLotteryRewardResp{},
	}
	for _, item := range items {
		result.List = append(result.List, &CMSQueryLotteryRewardResp{
			Name:     item.Name,
			Img:      item.Img,
			Remark:   item.Remark,
			Entity:   item.Entity,
			Type:     item.Type,
			ID:       item.ID,
			CreateAt: item.CreateAt,
		})
	}

	return result
}

// ================== 用户抽奖记录相关服务 ==================

type LotteryUserRecordSvc struct{}

func NewLotteryUserRecordSvc() *LotteryUserRecordSvc {
	return &LotteryUserRecordSvc{}
}

// 1. 用户端查询请求结构
type UserQueryLotteryRecordReq struct {
	LotteryId    primitive.ObjectID             `json:"lottery_id,omitempty"`
	IsWin        *bool                          `json:"is_win,omitempty"`         // 使用指针类型，nil表示不筛选
	Status       *int                           `json:"status,omitempty"`         // 使用指针类型，nil表示不筛选
	WinStartTime int64                          `json:"win_start_time,omitempty"` // 时间戳
	WinEndTime   int64                          `json:"win_end_time,omitempty"`   // 时间戳
	Pagination   *paginationUtils.DepPagination `json:"pagination"`
}

// WebListUserLotteryRecords 1. 用户端查询抽奖记录
func (w *LotteryUserRecordSvc) WebListUserLotteryRecords(ctx context.Context,
	orgUser *orgModel.OrganizationUser, req *UserQueryLotteryRecordReq) (*paginationUtils.ListResp[*dto.LotteryUserRecordResp], error) {

	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	recordDao := model.NewLotteryUserRecordDao(db)

	// 转换参数：零值转为nil
	var lotteryId *primitive.ObjectID
	if !req.LotteryId.IsZero() {
		lotteryId = &req.LotteryId
	}

	// 直接使用指针类型，nil表示不筛选
	isWin := req.IsWin
	status := req.Status

	var winStartTime *time.Time
	if req.WinStartTime != 0 {
		t := time.Unix(req.WinStartTime, 0).UTC()
		winStartTime = &t
	}

	var winEndTime *time.Time
	if req.WinEndTime != 0 {
		t := time.Unix(req.WinEndTime, 0).UTC()
		winEndTime = &t
	}

	total, result, err := recordDao.SelectUserRecordsWithReward(ctx, orgUser.ImServerUserId,
		lotteryId, isWin, status, winStartTime, winEndTime, req.Pagination)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.LotteryUserRecordResp]{
		List:  []*dto.LotteryUserRecordResp{},
		Total: total,
	}

	for _, record := range result {
		item := dto.NewLotteryUserRecordWithRewardResp(record)
		resp.List = append(resp.List, item)
	}

	return resp, nil
}

// 2. 管理端查询请求结构
type AdminQueryLotteryRecordReq struct {
	LotteryId           primitive.ObjectID             `json:"lottery_id,omitempty"`
	IsWin               *bool                          `json:"is_win,omitempty"`         // 使用指针类型，nil表示不筛选
	Status              *int                           `json:"status,omitempty"`         // 使用指针类型，nil表示不筛选
	WinStartTime        int64                          `json:"win_start_time,omitempty"` // 时间戳
	WinEndTime          int64                          `json:"win_end_time,omitempty"`   // 时间戳
	Keyword             string                         `json:"keyword,omitempty"`
	RewardId            primitive.ObjectID             `json:"reward_id,omitempty"`
	DistributeStartTime int64                          `json:"distribute_start_time,omitempty"` // 时间戳
	DistributeEndTime   int64                          `json:"distribute_end_time,omitempty"`   // 时间戳
	Pagination          *paginationUtils.DepPagination `json:"pagination"`
}

// CmsListUserLotteryRecords 2. 管理端查询抽奖记录
func (w *LotteryUserRecordSvc) CmsListUserLotteryRecords(ctx context.Context,
	orgUser *orgModel.OrganizationUser, req *AdminQueryLotteryRecordReq) (*paginationUtils.ListResp[*dto.AdminLotteryUserRecordResp], error) {

	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	recordDao := model.NewLotteryUserRecordDao(db)

	// 转换参数：零值转为nil
	var lotteryId *primitive.ObjectID
	if !req.LotteryId.IsZero() {
		lotteryId = &req.LotteryId
	}

	// 直接使用指针类型，nil表示不筛选
	isWin := req.IsWin
	status := req.Status

	var winStartTime *time.Time
	if req.WinStartTime != 0 {
		t := time.Unix(req.WinStartTime, 0).UTC()
		winStartTime = &t
	}

	var winEndTime *time.Time
	if req.WinEndTime != 0 {
		t := time.Unix(req.WinEndTime, 0).UTC()
		winEndTime = &t
	}

	var rewardId *primitive.ObjectID
	if !req.RewardId.IsZero() {
		rewardId = &req.RewardId
	}

	var distributeStartTime *time.Time
	if req.DistributeStartTime != 0 {
		t := time.Unix(req.DistributeStartTime, 0).UTC()
		distributeStartTime = &t
	}

	var distributeEndTime *time.Time
	if req.DistributeEndTime != 0 {
		t := time.Unix(req.DistributeEndTime, 0).UTC()
		distributeEndTime = &t
	}

	total, result, err := recordDao.SelectAdminRecords(ctx,
		orgUser.OrganizationId, // 添加组织过滤
		lotteryId, isWin, status, winStartTime, winEndTime,
		req.Keyword, rewardId, distributeStartTime, distributeEndTime, req.Pagination)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.AdminLotteryUserRecordResp]{
		List:  []*dto.AdminLotteryUserRecordResp{},
		Total: total,
	}

	for _, record := range result {
		item := dto.NewAdminLotteryUserRecordResp(record)
		resp.List = append(resp.List, item)
	}

	return resp, nil
}

// 3. 管理员审核请求结构
type AdminAuditLotteryRecordReq struct {
	ID     primitive.ObjectID `json:"id" binding:"required"`
	Status int                `json:"status"` // 0-未发放，1-已发放
}

// CmsAuditLotteryRecord 3. 管理员审核接口 - 更新发放状态
func (w *LotteryUserRecordSvc) CmsAuditLotteryRecord(ctx context.Context,
	orgUser *orgModel.OrganizationUser, req *AdminAuditLotteryRecordReq) error {

	// 验证状态值
	if req.Status != 0 && req.Status != 1 {
		return freeErrors.ApiErr("Invalid status value, must be 0 or 1")
	}

	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	recordDao := model.NewLotteryUserRecordDao(db)

	// 权限验证：根据ID查询抽奖记录详情
	record, err := recordDao.GetRecordById(ctx, req.ID)
	if err != nil {
		return err
	}

	// 验证抽奖记录的用户是否属于当前管理员的组织（通过user表的org_id字段）
	userDao := openImModel.NewUserDao(db)
	user, err := userDao.Take(ctx, record.ImServerUserId)
	if err != nil {
		return freeErrors.ApiErr("User not found")
	}

	if user.OrgId != orgUser.OrganizationId.Hex() {
		return freeErrors.ApiErr("Permission denied: You can only audit records within your organization")
	}

	return recordDao.UpdateDistributeStatus(ctx, req.ID, req.Status)
}

type WebUseLotteryRewardReq struct {
	LotteryTicketId primitive.ObjectID `json:"lottery_ticket_id"`
}
