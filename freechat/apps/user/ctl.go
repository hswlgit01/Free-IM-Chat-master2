package user

import (
	_ "embed"
	"fmt"

	"github.com/gin-gonic/gin"
	opModel "github.com/openimsdk/chat/freechat/apps/operationLog/model"
	opSvc "github.com/openimsdk/chat/freechat/apps/operationLog/svc"
	"github.com/openimsdk/chat/freechat/apps/user/dto"
	"github.com/openimsdk/chat/freechat/apps/user/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/ginUtils"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/chat/pkg/protocol/admin"
	chatpb "github.com/openimsdk/chat/pkg/protocol/chat"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type UserCtl struct{}

func NewUserCtl() *UserCtl {
	return &UserCtl{}
}

func (w *UserCtl) WebPostRegisterUserByEmail(c *gin.Context) {
	data := svc.RegUserReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	data.Ip = ginUtils.GetClientIP(c)

	operationId, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	userSvc := svc.NewUserSvc()
	resp, err := userSvc.RegisterUser(c, operationId, &data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (w *UserCtl) WebPostRegisterUserByAccount(c *gin.Context) {
	data := svc.RegisterUserViaAccountReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	operationId, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	data.Ip = ginUtils.GetClientIP(c)

	userSvc := svc.NewUserSvc()
	resp, err := userSvc.RegisterUserViaAccount(c, operationId, &data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

//go:embed importUserTemplate.xlsx
var ImportUserTemplateExcelFile []byte

func (w *UserCtl) CmsGetImportUserExcelTemplateFile(c *gin.Context) {

	fileBytes := ImportUserTemplateExcelFile
	fileName := "importUserTemplate.xlsx"

	// 设置响应头
	c.Header("Content-Disposition", "attachment; filename="+fileName)
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Length", fmt.Sprintf("%d", len(fileBytes)))

	// 写入响应体
	c.Writer.Write(fileBytes)

	//// 设置响应头，告诉浏览器这是一个文件下载
	//c.Header("Content-Disposition", "attachment; filename=downloaded.pdf")
	//c.Header("Content-Type", "application/octet-stream")
	//
	//apiresp.GinSuccess(c, map[string]string{
	//	"url": plugin.ChatCfg().ApiConfig.ImportUserTemplateExcelUrl,
	//})
}

func (w *UserCtl) CmsPostImportUserViaExcel(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		apiresp.GinError(c, freeErrors.ApiErr(err.Error()))
		return
	}

	operationId, err := middleware.GetOperationId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	importUserSvc := svc.NewImportUserSvc()
	resp, err := importUserSvc.CmsImportUserViaExcel(c, operationId, org.Organization, file)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

func (w *UserCtl) FindUserFullInfo(c *gin.Context) {
	data := svc.FindUserFullInfoReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	userSvc := svc.NewUserSvc()
	resp, err := userSvc.FindUserFullInfo(c, &data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// SearchUserFullInfo 搜索用户完整信息
func (w *UserCtl) SearchUserFullInfo(c *gin.Context) {
	data := svc.SearchUserFullInfoReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	userSvc := svc.NewUserSvc()
	resp, err := userSvc.SearchUserFullInfo(c, &data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// ChangeEmail 修改用户邮箱
func (w *UserCtl) ChangeEmail(c *gin.Context) {
	data := dto.ChangeEmailReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	userID := mctx.GetOpUserID(c)
	userSvc := svc.NewUserSvc()
	err := userSvc.ChangeEmail(c, userID, data.NewEmail, data.VerifyCode)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, "success")
}

// 拉黑用户
func (w *UserCtl) BlackUser(c *gin.Context) {
	data := admin.BlockUserReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	userSvc := svc.NewUserSvc()
	err = userSvc.BlackUser(c, &data, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &data,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeAddBlockUser,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, "success")
}

// 查询封禁用户
func (w *UserCtl) SearchBlockUser(c *gin.Context) {
	data := admin.SearchBlockUserReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	userSvc := svc.NewUserSvc()

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	resp, err := userSvc.SearchBlockUser(c, &data, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

// 解禁用户
func (w *UserCtl) UnblockUser(c *gin.Context) {
	data := admin.UnblockUserReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	userSvc := svc.NewUserSvc()
	err = userSvc.UnblockUser(c, &data, org.ID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	err = opSvc.NewOperationLogSvc().InternalCreateOperationLog(c, &opSvc.InternalCreateOperationLogReq{
		Details:        &data,
		UserId:         org.OrgUser.UserId,
		ImServerUserId: org.OrgUser.ImServerUserId,
		OrgId:          org.ID,
		OperationType:  opModel.OpTypeUnBlockUser,
	})
	if err != nil {
		log.ZError(c, c.Request.URL.Path+" :CreateOperationLog", err)
	}

	apiresp.GinSuccess(c, "success")
}

func (w *UserCtl) PostUpdateUserInfo(c *gin.Context) {
	data := chatpb.UpdateUserInfoReq{}
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

	orgIdStr, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgId, err := primitive.ObjectIDFromHex(orgIdStr)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	userSvc := svc.NewUserSvc()
	err = userSvc.UpdateUser(c, operationId, opUserID, orgId, &data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, map[string]interface{}{})
}

// CheckAccountExists 检查账户是否已存在
func (w *UserCtl) CheckAccountExists(c *gin.Context) {
	var req struct {
		Account string `json:"account" binding:"required"`
	}

	if err := c.ShouldBind(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	userSvc := svc.NewUserSvc()
	exists, err := userSvc.CheckAccountExists(c, req.Account)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, gin.H{"exists": exists})
}

// SuperAdminGetAllUsers 超级管理员查询系统所有用户
func (w *UserCtl) SuperAdminGetAllUsers(c *gin.Context) {
	data := dto.SuperAdminGetAllUsersReq{}
	if err := c.ShouldBindJSON(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	userSvc := svc.NewUserSvc()
	resp, err := userSvc.SuperAdminGetAllUsers(c, &data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// SuperAdminGetUserDetail 超级管理员查询用户详情
func (w *UserCtl) SuperAdminGetUserDetail(c *gin.Context) {
	data := dto.SuperAdminGetUserDetailReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	userSvc := svc.NewUserSvc()
	resp, err := userSvc.SuperAdminGetUserDetail(c, &data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// SuperAdminResetUserPassword 超级管理员重置用户密码
func (w *UserCtl) SuperAdminResetUserPassword(c *gin.Context) {
	data := dto.SuperAdminResetUserPasswordReq{}
	if err := c.ShouldBind(&data); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	userSvc := svc.NewUserSvc()
	err := userSvc.SuperAdminResetUserPassword(c, &data)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, "密码重置成功")
}

// GetLoginRecordByImServerId 根据IMServerID查询用户登录记录
func (w *UserCtl) GetLoginRecordByImServerId(c *gin.Context) {
	var req dto.GetLoginRecordReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgIdStr, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	orgId, err := primitive.ObjectIDFromHex(orgIdStr)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	userSvc := svc.NewUserSvc()
	resp, err := userSvc.GetLoginRecord(c, &req, opUserID, orgId)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}
