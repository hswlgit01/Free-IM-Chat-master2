package lottery

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/openimsdk/chat/freechat/apps/lottery/svc"
	opModel "github.com/openimsdk/chat/freechat/apps/operationLog/model"
	opSvc "github.com/openimsdk/chat/freechat/apps/operationLog/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/ginUtils"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type LotteryCtl struct{}

func NewLotteryCtl() *LotteryCtl {
	return &LotteryCtl{}
}

func (w *LotteryCtl) CmsPostCreateLottery(c *gin.Context) {
	var req svc.CmsCreateLotteryReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lotterySvc := svc.NewLotterySvc()
	err = lotterySvc.CmsCreateLottery(c, org.OrgUser, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeCreateLottery,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *LotteryCtl) CmsPostUpdateLottery(c *gin.Context) {
	var req svc.CmsUpdateLotteryReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	lotterySvc := svc.NewLotterySvc()
	err = lotterySvc.CmsUpdateLottery(c, org.OrgUser, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeUpdateLottery,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *LotteryCtl) CmsGetSearchLottery(c *gin.Context) {
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

	keyword := c.Query("keyword")

	lotterySvc := svc.NewLotterySvc()
	resp, err := lotterySvc.CmsListSearchLottery(c, org.OrgUser, keyword, pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (w *LotteryCtl) CmsGetListLottery(c *gin.Context) {
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

	keyword := c.Query("keyword")

	lotterySvc := svc.NewLotterySvc()
	resp, err := lotterySvc.CmsListLottery(c, org.OrgUser, keyword, pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (w *LotteryCtl) WebGetDetailLottery(c *gin.Context) {
	id, err := ginUtils.QueryToObjectId(c, "id")
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	lotterySvc := svc.NewLotterySvc()
	resp, err := lotterySvc.WebDetailLottery(c, id)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

type LotteryUserTicketCtl struct{}

func NewLotteryUserTicketCtl() *LotteryUserTicketCtl {
	return &LotteryUserTicketCtl{}
}

func (w *LotteryUserTicketCtl) WebGetListLotteryUserTicket(c *gin.Context) {
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

	lotterySvc := svc.NewLotteryUserTicketSvc()
	resp, err := lotterySvc.WebListLotteryUserTicket(c, org.OrgUser, pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (w *LotteryUserTicketCtl) WebPostUseLotteryUserTicket(c *gin.Context) {
	var req svc.WebUseLotteryRewardReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	lotteryUserTicketSvc := svc.NewLotteryUserTicketSvc()
	resp, err := lotteryUserTicketSvc.WebUseLotteryReward(c, org.OrgUser, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

type LotteryRewardCtl struct{}

func NewLotteryRewardCtl() *LotteryRewardCtl {
	return &LotteryRewardCtl{}
}

func (w *LotteryRewardCtl) WebPostCreateLotteryReward(c *gin.Context) {
	var req svc.CMSCreateLotteryRewardReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	lotteryRewardSvc := svc.NewLotterySvc()
	err = lotteryRewardSvc.CMSCreateLotteryReward(c, org.OrgUser, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *LotteryRewardCtl) WebPostUpdateLotteryReward(c *gin.Context) {
	var req svc.CMSUpdateLotteryRewardReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	lotteryRewardSvc := svc.NewLotterySvc()
	err = lotteryRewardSvc.CMSUpdateLotteryReward(c, org.OrgUser, &req)
	if err != nil {
		apiresp.GinError(c, err)
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *LotteryRewardCtl) WebPostDeleteLotteryReward(c *gin.Context) {
	id, err := primitive.ObjectIDFromHex(c.Param("id"))
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	req := &svc.CMSDeleteLotteryRewardReq{
		ID: id,
	}
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
	}
	lotteryRewardSvc := svc.NewLotterySvc()
	err = lotteryRewardSvc.CMSDeleteLotteryReward(c, org.OrgUser, req)
	if err != nil {
		apiresp.GinError(c, err)
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *LotteryRewardCtl) WebGetSearchLotteryReward(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
	}

	var req svc.CMSQueryLotteryRewardReq
	err = c.ShouldBindWith(&req, binding.Query)
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
	}
	lotteryRewardSvc := svc.NewLotterySvc()
	page := lotteryRewardSvc.CMSFindLotteryReward(c, org.OrgUser, &req)
	apiresp.GinSuccess(c, page)
}

// ================== 用户抽奖记录相关控制器 ==================

type LotteryUserRecordCtl struct{}

func NewLotteryUserRecordCtl() *LotteryUserRecordCtl {
	return &LotteryUserRecordCtl{}
}

// WebGetUserLotteryRecords 1. 用户端查询抽奖记录
func (w *LotteryUserRecordCtl) WebGetUserLotteryRecords(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req svc.UserQueryLotteryRecordReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	recordSvc := svc.NewLotteryUserRecordSvc()
	resp, err := recordSvc.WebListUserLotteryRecords(c, org.OrgUser, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CmsGetUserLotteryRecords 2. 管理端查询抽奖记录
func (w *LotteryUserRecordCtl) CmsGetUserLotteryRecords(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req svc.AdminQueryLotteryRecordReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	recordSvc := svc.NewLotteryUserRecordSvc()
	resp, err := recordSvc.CmsListUserLotteryRecords(c, org.OrgUser, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CmsPostAuditLotteryRecord 3. 管理员审核接口 - 更新发放状态
func (w *LotteryUserRecordCtl) CmsPostAuditLotteryRecord(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req svc.AdminAuditLotteryRecordReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	recordSvc := svc.NewLotteryUserRecordSvc()
	err = recordSvc.CmsAuditLotteryRecord(c, org.OrgUser, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeAuditLotteryRecord,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}
