package organization

import (
	"context"
	"errors"
	"slices"

	opModel "github.com/openimsdk/chat/freechat/apps/operationLog/model"
	opSvc "github.com/openimsdk/chat/freechat/apps/operationLog/svc"
	orgCache "github.com/openimsdk/chat/freechat/apps/organization/cache"
	"github.com/openimsdk/tools/log"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/organization/dto"
	"github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/apps/organization/svc"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/ginUtils"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type OrganizationCtl struct{}

func NewOrganizationCtl() *OrganizationCtl {
	return &OrganizationCtl{}
}

func (w *OrganizationCtl) SuperCmsGetListOrg(c *gin.Context) {
	pagination, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	keyword := c.Query("keyword")

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	startTimeUtc, err := ginUtils.QueryToUtcTime(c, "star_time")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	endTimeUtc, err := ginUtils.QueryToUtcTime(c, "end_time")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	checkInSvc := svc.NewOrganizationService()
	resp, err := checkInSvc.SuperCmsListOrg(c, keyword, operationID, startTimeUtc, endTimeUtc, pagination)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// GetOrganizationInfo 获取组织信息
func (w *OrganizationCtl) GetOrganizationInfo(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	resp, err := dto.NewOrganizationResp(org.Organization, operationID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// GetOrganizationInfoById 获取组织信息
func (w *OrganizationCtl) GetOrganizationInfoById(c *gin.Context) {
	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgId, err := ginUtils.QueryToObjectId(c, "org_id")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	if orgId == primitive.NilObjectID {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	db := plugin.MongoCli().GetDB()
	org, err := orgCache.NewOrgCacheRedis(plugin.RedisCli(), db).GetByIdAndStatus(context.TODO(), orgId, model.OrganizationStatusPass)
	if err != nil {
		apiresp.GinError(c, errors.Unwrap(err))
		return
	}

	resp, err := dto.NewOrganizationResp(org, operationID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// PostTestCreate 创建组织-测试接口
func (w *OrganizationCtl) PostTestCreate(c *gin.Context) {
	data := svc.CreateTestOrganizationReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgSvc := svc.NewOrganizationService()
	err = orgSvc.CreateTestOrganization(operationID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// SuperCmsPostCreateOrg 超管创建组织
func (w *OrganizationCtl) SuperCmsPostCreateOrg(c *gin.Context) {
	data := svc.CreateTestOrganizationReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	data.Secret = plugin.ChatCfg().Share.OpenIM.Secret
	orgSvc := svc.NewOrganizationService()
	err = orgSvc.CreateTestOrganization(operationID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// SuperCmsPostUpdateOrg 超管编辑组织
func (w *OrganizationCtl) SuperCmsPostUpdateOrg(c *gin.Context) {
	data := svc.SuperAdminUpdateOrganizationReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgSvc := svc.NewOrganizationService()
	err := orgSvc.SuperAdminUpdateOrganization(c, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, map[string]interface{}{})
}

// PostUpdate 更新组织
func (w *OrganizationCtl) PostUpdate(c *gin.Context) {
	data := svc.UpdateOrganization{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgSvc := svc.NewOrganizationService()
	_, err = orgSvc.UpdateOrganization(org.ID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &data,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeUpdateOrgInfo,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *OrganizationCtl) PostCreateOrganizationWallet(c *gin.Context) {
	data := svc.CreateOrganizationWalletReq{}
	if err := c.ShouldBind(&data); err != nil {
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

	orgSvc := svc.NewOrganizationService()
	err = orgSvc.CreateOrganizationWallet(opUserID, org.ID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *OrganizationCtl) PostUpdateOrganizationWallet(c *gin.Context) {
	data := svc.UpdateOrganizationWalletPayPwdReq{}
	if err := c.ShouldBind(&data); err != nil {
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

	orgSvc := svc.NewOrganizationService()
	err = orgSvc.UpdateOrganizationWalletPayPwd(opUserID, org.ID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &data,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeUpdateWalletPassword,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// GetWalletExist 查询当前组织是否开通钱包
func (w *OrganizationCtl) GetWalletExist(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	mongoCli := plugin.MongoCli()
	walletInfoDao := walletModel.NewWalletInfoDao(mongoCli.GetDB())
	exist, err := walletInfoDao.ExistByOwnerIdAndOwnerType(context.TODO(), org.ID.Hex(), walletModel.WalletInfoOwnerTypeOrganization)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, exist)
}

func (w *OrganizationCtl) PostCreateOrganizationCurrency(c *gin.Context) {
	data := svc.CreateOrganizationCurrencyReq{}
	if err := c.ShouldBind(&data); err != nil {
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

	orgSvc := svc.NewOrganizationService()
	err = orgSvc.CreateOrganizationCurrency(opUserID, org.ID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &data,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeCreateOrgCurrency,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *OrganizationCtl) PostUpdateOrganizationCurrency(c *gin.Context) {
	data := svc.UpdateOrganizationCurrencyReq{}
	if err := c.ShouldBind(&data); err != nil {
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

	orgSvc := svc.NewOrganizationService()
	err = orgSvc.UpdateOrganizationCurrency(opUserID, org.ID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &data,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeUpdateOrgCurrency,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *OrganizationCtl) PostJoinOrgUsingInvitationCode(c *gin.Context) {
	data := svc.JoinOrgUsingInvitationCodeReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationId, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgSvc := svc.NewOrganizationService()
	resp, err := orgSvc.JoinOrgUsingInvitationCode(operationId, opUserID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

type OrganizationUserCtl struct{}

func NewOrganizationUserCtl() *OrganizationUserCtl {
	return &OrganizationUserCtl{}
}

func (w *OrganizationUserCtl) GetInfo(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, org.OrgUser)
	return
}

func (w *OrganizationUserCtl) GetSelfAllOrg(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgUserSvc := svc.NewOrganizationUserService()
	resp, err := orgUserSvc.GetUserAllOrg("", []string{opUserID})
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
	return
}

func (w *OrganizationUserCtl) PostAddBackendAdmin(c *gin.Context) {
	data := svc.CreateOrganizationBackendAdminReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgUserSvc := svc.NewOrganizationUserService()
	err = orgUserSvc.CreateOrganizationBackendAdmin(operationID, org.ID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &data,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeCreateBackendAdmin,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *OrganizationUserCtl) PostUpdateWebUserRole(c *gin.Context) {
	data := svc.UpdateWebUserRoleReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgUserSvc := svc.NewOrganizationUserService()
	err = orgUserSvc.UpdateWebUserRole(c, operationID, org.ID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &data,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeUpdateUserRole,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *OrganizationUserCtl) PostUpdateUserCanSendFreeMsg(c *gin.Context) {
	data := svc.UpdateUserCanSendFreeMsgReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgUserSvc := svc.NewOrganizationUserService()
	err = orgUserSvc.UpdateUserCanSendFreeMsg(c, operationID, org.ID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &data,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeUpdateUserCanSendMsg,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *OrganizationUserCtl) PostResetOrgUserPassword(c *gin.Context) {
	data := svc.ResetOrganizationUserPasswordReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgUserSvc := svc.NewOrganizationUserService()
	err = orgUserSvc.ResetOrganizationUserPassword(c, operationID, org.ID, org.OrgUser, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *OrganizationUserCtl) PostUpdateUserStatus(c *gin.Context) {
	data := svc.UpdateUserStatusReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgUserSvc := svc.NewOrganizationUserService()
	err = orgUserSvc.UpdateUserStatus(org.ID, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

func (w *OrganizationUserCtl) PostChangeOrgUser(c *gin.Context) {
	data := svc.ChangeOrgUserReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	userId, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	orgUserSvc := svc.NewOrganizationUserService()
	resp, err := orgUserSvc.ChangeOrgUser(context.TODO(), operationID, userId, data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// GetOrgByImServerUserId 根据用户的imServerUserId查询组织信息
func (w *OrganizationUserCtl) GetOrgByImServerUserId(c *gin.Context) {
	var req svc.GetOrgByImServerUserIdReq
	req.ImServerUserId = c.Query("im_server_user_id")

	if req.ImServerUserId == "" {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	orgUserSvc := svc.NewOrganizationUserService()
	resp, err := orgUserSvc.GetOrgByImServerUserId(c, req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// PostGetOrgUser 查询组织用户（新版POST接口）
func (w *OrganizationUserCtl) PostGetOrgUser(c *gin.Context) {
	var req dto.GetOrgUserReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 构建分页参数
	page := &paginationUtils.DepPagination{
		Page:     int32(req.Page),
		PageSize: int32(req.PageSize),
	}

	orgUserSvc := svc.NewOrganizationUserService()
	resp, err := orgUserSvc.GetOrgUserWithFilters(org.ID, &req, page)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// PostOrgUserWalletSnapshot 批量获取本组织用户钱包/补偿金（与 post_org_user omit_wallet 配套）
func (w *OrganizationUserCtl) PostOrgUserWalletSnapshot(c *gin.Context) {
	var req dto.OrgUserWalletSnapshotReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgUserSvc := svc.NewOrganizationUserService()
	resp, err := orgUserSvc.BatchOrgUserWalletSnapshot(org.ID, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// 用户标签管理相关接口

// CreateUserTag 创建标签
func (w *OrganizationUserCtl) CreateUserTag(ctx *gin.Context) {
	var req dto.CreateUserTagReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(ctx, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	orgUserSvc := svc.NewOrganizationUserService()
	resp, err := orgUserSvc.CreateUserTag(ctx, org.ID, &req)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(ctx, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeCreateUserTag,
	})
	if err != nil {
		log.ZError(ctx, ctx.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(ctx, resp)
}

// UpdateUserTag 更新标签
func (w *OrganizationUserCtl) UpdateUserTag(ctx *gin.Context) {
	var req dto.UpdateUserTagReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(ctx, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	orgUserSvc := svc.NewOrganizationUserService()
	resp, err := orgUserSvc.UpdateUserTag(ctx, org.ID, &req)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}
	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(ctx, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeUpdateUserTag,
	})
	if err != nil {
		log.ZError(ctx, ctx.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(ctx, resp)
}

// GetUserTagList 获取标签列表
func (w *OrganizationUserCtl) GetUserTagList(ctx *gin.Context) {
	page, err := paginationUtils.QueryToDepPagination(ctx)
	if err != nil {
		apiresp.GinError(ctx, freeErrors.PageParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	orgUserSvc := svc.NewOrganizationUserService()
	resp, err := orgUserSvc.GetUserTagList(ctx, org.ID, page)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	apiresp.GinSuccess(ctx, resp)
}

// AssignUserTags 给用户打标签
func (w *OrganizationUserCtl) AssignUserTags(ctx *gin.Context) {
	var req dto.AssignUserTagsReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(ctx, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(ctx)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	orgUserSvc := svc.NewOrganizationUserService()
	err = orgUserSvc.AssignUserTags(ctx, org.ID, &req)
	if err != nil {
		apiresp.GinError(ctx, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(ctx, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeUpdateUserTagAssign,
	})
	if err != nil {
		log.ZError(ctx, ctx.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(ctx, "success")
}

type OrgRolePermissionCtl struct{}

func NewOrgRolePermissionCtl() *OrgRolePermissionCtl {
	return &OrgRolePermissionCtl{}
}

// WebGetOrgRolePermission web端获取角色权限
func (w *OrgRolePermissionCtl) WebGetOrgRolePermission(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	resp, err := svc.NewOrgRolePermissionService().DetailOrgRolePermission(org.ID, org.OrgUser.Role)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CmsGetOrgRolePermission 后台管理端获取角色权限
func (w *OrgRolePermissionCtl) CmsGetOrgRolePermission(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	role := model.OrganizationUserRole(c.Query("role"))
	if !slices.Contains(model.AllOrganizationUserRole, role) {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	resp, err := svc.NewOrgRolePermissionService().DetailOrgRolePermission(org.ID, role)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CmsPostUpdateOrgRolePermission 后台管理端修改角色权限
func (w *OrgRolePermissionCtl) CmsPostUpdateOrgRolePermission(c *gin.Context) {
	data := svc.UpdateOrgRolePermissionReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	data.OrgId = org.ID

	rolePerSvc := svc.NewOrgRolePermissionService()
	err = rolePerSvc.UpdateOrgRolePermission(c, data, operationID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &data,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeUpdateUserRolePermission,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// NotificationAccountCtl 通知账户控制器
type NotificationAccountCtl struct{}

func NewNotificationAccountCtl() *NotificationAccountCtl {
	return &NotificationAccountCtl{}
}

// CmsPostCreateNotificationAccount 创建通知账户
func (w *NotificationAccountCtl) CmsPostCreateNotificationAccount(c *gin.Context) {
	// 解析请求参数
	var req dto.CreateNotificationAccountReq
	if err := c.ShouldBind(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 获取操作ID
	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 调用服务层
	svc := svc.NewNotificationAccountService()
	resp, err := svc.CreateNotificationAccount(c, operationID, org.ID, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CmsPostUpdateNotificationAccount 更新通知账户
func (w *NotificationAccountCtl) CmsPostUpdateNotificationAccount(c *gin.Context) {
	// 解析请求参数
	var req dto.UpdateNotificationAccountReq
	if err := c.ShouldBind(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 获取操作ID
	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 调用服务层
	svc := svc.NewNotificationAccountService()
	err = svc.UpdateNotificationAccount(c, operationID, org.ID, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// CmsPostSearchNotificationAccount 搜索通知账户
func (w *NotificationAccountCtl) CmsPostSearchNotificationAccount(c *gin.Context) {
	// 解析请求参数
	var req dto.SearchNotificationAccountReq
	if err := c.ShouldBind(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 调用服务层
	svc := svc.NewNotificationAccountService()
	resp, err := svc.SearchNotificationAccount(c, org.ID, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CmsPostSendBannerNotification 发送图文通知
func (w *NotificationAccountCtl) CmsPostSendBannerNotification(c *gin.Context) {
	// 解析请求参数
	var req dto.SendBannerNotificationReq
	if err := c.ShouldBind(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 获取操作ID
	operationID, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 调用服务层
	svc := svc.NewNotificationAccountService()
	err = svc.SendBannerNotification(c, org.ID, operationID, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// InternalCheckUserProtection 内部API：检查用户是否拥有官方账号保护权限
// 供Free-IM-Server等内部服务调用
func (w *OrganizationCtl) InternalCheckUserProtection(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	orgSvc := &svc.OrganizationSvc{}
	hasProtection, err := orgSvc.CheckUserHasProtection(c, userID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{
		"user_id":        userID,
		"has_protection": hasProtection,
	})
}

// CmsPostUpdateCheckinRuleDescription 更新签到规则说明
func (w *OrganizationCtl) CmsPostUpdateCheckinRuleDescription(c *gin.Context) {
	var req svc.UpdateCheckinRuleReq
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

	orgSvc := svc.NewOrganizationService()
	err = orgSvc.CmsUpdateCheckinRuleDescription(c, opUserID, org.Organization, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &req,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeUpdateCheckinRuleDescription,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}
