package svc

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	defaultFriendSvc "github.com/openimsdk/chat/freechat/apps/defaultFriend/svc"
	defaultGroupSvc "github.com/openimsdk/chat/freechat/apps/defaultGroup/svc"
	OrgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	orgSvc "github.com/openimsdk/chat/freechat/apps/organization/svc"
	"github.com/openimsdk/chat/freechat/plugin"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/constant"
	pkgConstant "github.com/openimsdk/chat/pkg/constant"
	"github.com/openimsdk/tools/mcontext"
	"github.com/xuri/excelize/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"mime/multipart"
	"sync"
	"time"
)

type ImportUserSvc struct{}

func NewImportUserSvc() *ImportUserSvc {
	return &ImportUserSvc{}
}

type CmsImportUserViaExcelRowErrType string

const (
	ExcelRowErrSystem              CmsImportUserViaExcelRowErrType = "system"              // 系统错误
	ExcelRowErrExcelFormat         CmsImportUserViaExcelRowErrType = "ExcelFormat"         // excel格式错误
	ExcelRowErrAccountRegexp       CmsImportUserViaExcelRowErrType = "AccountRegexp"       // 账号格式错误
	ExcelRowErrMissingPwd          CmsImportUserViaExcelRowErrType = "MissingPwd"          // 密码为空
	ExcelRowErrMissingNickname     CmsImportUserViaExcelRowErrType = "MissingNickname"     // 昵称为空
	ExcelRowErrAccountAlreadyExist CmsImportUserViaExcelRowErrType = "AccountAlreadyExist" // 账号已存在
)

type CmsImportUserViaExcelErr struct {
	Row  int                             `json:"row"`
	Type CmsImportUserViaExcelRowErrType `json:"type"`
	Data interface{}                     `json:"data"`
	Msg  string                          `json:"msg"`
}

type CmsImportUserViaExcelSuccess struct {
	Row            int                `json:"row"`
	UserId         string             `json:"user_id"`
	ImServerUserId string             `json:"im_server_user_id"`
	OrgId          primitive.ObjectID `json:"org_id"`
	Account        string             `json:"account"`
	Nickname       string             `json:"nickname"`
}

type CmsImportUserViaExcelErrResp struct {
	Error   []*CmsImportUserViaExcelErr     `json:"error"`
	Success []*CmsImportUserViaExcelSuccess `json:"success"`
}

var lock sync.Mutex

func (w *ImportUserSvc) CmsImportUserViaExcel(ctx context.Context, operationId string, org *OrgModel.Organization, excelF *multipart.FileHeader) (*CmsImportUserViaExcelErrResp, error) {
	db := plugin.MongoCli().GetDB()
	attributeDao := chatModel.NewAttributeDao(db)
	accountDao := chatModel.NewAccountDao(db)
	registerDao := chatModel.NewRegisterDao(db)
	credentialDao := chatModel.NewCredentialDao(db)

	lock.Lock()
	defer lock.Unlock()

	f, err := excelF.Open()
	if err != nil {
		return nil, freeErrors.ApiErr(err.Error())
	}
	defer f.Close()

	excelFile, err := excelize.OpenReader(f)
	if err != nil {
		return nil, err
	}
	defer excelFile.Close()

	rows, err := excelFile.GetRows("Sheet1")
	if err != nil {
		return nil, err
	}

	if len(rows)-1 > 500 {
		return nil, freeErrors.ImportUserExcelRowExceedErr(len(rows) - 1)
	}

	resp := &CmsImportUserViaExcelErrResp{
		Error: make([]*CmsImportUserViaExcelErr, 0),
	}

	err = plugin.MongoCli().GetTx().Transaction(context.TODO(), func(sessionCtx context.Context) error {
		type ImportExcelUser struct {
			Account  string `json:"account"`
			Nickname string `json:"nickname"`
			Password string `json:"password"`
		}

		createUsers := make([]*ImportExcelUser, 0)

		// 数据校验
		for i, row := range rows {
			if i == 0 {
				continue
			}

			currentRow := i + 1
			if len(row) < 3 {
				resp.Error = append(resp.Error, &CmsImportUserViaExcelErr{
					Row:  currentRow,
					Type: ExcelRowErrExcelFormat,
					Msg:  "row format error",
				})
				continue
			}

			registerUser := &ImportExcelUser{
				Account:  row[0],
				Nickname: row[1],
				Password: row[2],
			}
			createUsers = append(createUsers, registerUser)

			if !AccountRegexp.MatchString(registerUser.Account) {
				resp.Error = append(resp.Error, &CmsImportUserViaExcelErr{
					Row:  currentRow,
					Type: ExcelRowErrAccountRegexp,
					Data: registerUser,
					Msg:  "account does not conform to the regexp rule",
				})
				continue
			}

			attributes, err := attributeDao.FindAccountCaseInsensitive(sessionCtx, []string{registerUser.Account})
			if err != nil {
				return err
			}
			if len(attributes) > 0 {
				resp.Error = append(resp.Error, &CmsImportUserViaExcelErr{
					Row:  currentRow,
					Type: ExcelRowErrAccountAlreadyExist,
					Data: registerUser,
					Msg:  fmt.Sprintf("Account : %s already exists", registerUser.Account),
				})
				continue
			}

			if registerUser.Nickname == "" {
				resp.Error = append(resp.Error, &CmsImportUserViaExcelErr{
					Row:  currentRow,
					Type: ExcelRowErrMissingNickname,
					Data: registerUser,
					Msg:  "nickname cannot be empty",
				})
				continue
			}

			if registerUser.Password == "" {
				resp.Error = append(resp.Error, &CmsImportUserViaExcelErr{
					Row:  currentRow,
					Type: ExcelRowErrMissingPwd,
					Data: registerUser,
					Msg:  "password cannot be empty",
				})
				continue
			}

			h := md5.New()
			h.Write([]byte(registerUser.Password))
			registerUser.Password = hex.EncodeToString(h.Sum(nil))

			// 数据入库
			newUserID, err := utils.NewId()
			if err != nil {
				resp.Error = append(resp.Error, &CmsImportUserViaExcelErr{
					Row:  currentRow,
					Type: ExcelRowErrSystem,
					Data: registerUser,
					Msg:  "new user id error: " + newUserID,
				})
				continue
				//return err
			}

			credentials := make([]*chatModel.Credential, 0)
			var registerType int32 = constant.AccountRegister
			credentials = append(credentials, &chatModel.Credential{
				UserID:      newUserID,
				Account:     registerUser.Account,
				Type:        constant.CredentialAccount,
				AllowChange: true,
			})

			register := &chatModel.Register{
				UserID:      newUserID,
				DeviceID:    "",
				IP:          "",
				Platform:    pkgConstant.WebPlatformStr,
				AccountType: "",
				Mode:        constant.UserMode,
				CreateTime:  time.Now(),
			}
			account := &chatModel.Account{
				UserID:         newUserID,
				Password:       registerUser.Password,
				OperatorUserID: mcontext.GetOpUserID(ctx),
				ChangeTime:     register.CreateTime,
				CreateTime:     register.CreateTime,
			}
			attribute := &chatModel.Attribute{
				UserID:         newUserID,
				Account:        registerUser.Account,
				PhoneNumber:    "",
				AreaCode:       "",
				Email:          "",
				Nickname:       "",
				FaceURL:        "",
				Gender:         1,
				BirthTime:      time.Time{},
				ChangeTime:     register.CreateTime,
				CreateTime:     register.CreateTime,
				AllowVibration: constant.DefaultAllowVibration,
				AllowBeep:      constant.DefaultAllowBeep,
				AllowAddFriend: constant.DefaultAllowAddFriend,
				RegisterType:   registerType,
			}

			if err = registerDao.Create(sessionCtx, register); err != nil {
				resp.Error = append(resp.Error, &CmsImportUserViaExcelErr{
					Row:  currentRow,
					Type: ExcelRowErrSystem,
					Data: registerUser,
					Msg:  "create register error:" + err.Error(),
				})
				continue
				//return err
			}
			if err = accountDao.Create(sessionCtx, account); err != nil {
				resp.Error = append(resp.Error, &CmsImportUserViaExcelErr{
					Row:  currentRow,
					Type: ExcelRowErrSystem,
					Data: registerUser,
					Msg:  "create account error:" + err.Error(),
				})
				continue
				//return err
			}
			if err = attributeDao.Create(sessionCtx, attribute); err != nil {
				resp.Error = append(resp.Error, &CmsImportUserViaExcelErr{
					Row:  currentRow,
					Type: ExcelRowErrSystem,
					Data: registerUser,
					Msg:  "create attribute error",
				})
				continue
				//return err
			}
			if err = credentialDao.Create(sessionCtx, credentials...); err != nil {
				resp.Error = append(resp.Error, &CmsImportUserViaExcelErr{
					Row:  currentRow,
					Type: ExcelRowErrSystem,
					Data: registerUser,
					Msg:  "create credential error: " + err.Error(),
				})
				continue
				//return err
			}

			joinOrgResp := &orgSvc.JoinOrgUsingInvitationCodeResp{}
			if len(resp.Error) <= 0 {
				organizationSvc := orgSvc.OrganizationSvc{}
				joinOrgResp, err = organizationSvc.JoinOrgUsingInvitationCodeByAttr(sessionCtx, operationId, newUserID, orgSvc.JoinOrgUsingInvitationCodeReq{
					InvitationCode: org.InvitationCode,
					Nickname:       registerUser.Nickname,
					FaceURL:        "",
				}, attribute)
				if err != nil {
					resp.Error = append(resp.Error, &CmsImportUserViaExcelErr{
						Row:  currentRow,
						Type: ExcelRowErrSystem,
						Data: registerUser,
						Msg:  "join org error: " + err.Error(),
					})
					continue
					//return err
				}
			}

			resp.Success = append(resp.Success, &CmsImportUserViaExcelSuccess{
				Account:        registerUser.Account,
				Nickname:       registerUser.Nickname,
				Row:            currentRow,
				UserId:         newUserID,
				ImServerUserId: joinOrgResp.ImServerUserId,
				OrgId:          joinOrgResp.OrgId,
			})

		}

		if len(resp.Error) > 0 {
			return errors.New("import user error")
		}
		return nil
	})

	if len(resp.Error) <= 0 {
		go func() {
			defFriendSvc := defaultFriendSvc.NewDefaultFriendSvc()
			defGroupSvc := defaultGroupSvc.NewDefaultGroupSvc()
			for _, success := range resp.Success {
				defFriendSvc.InternalAddDefaultFriend(operationId, success.OrgId, success.ImServerUserId)
				defGroupSvc.InternalAddDefaultGroup(operationId, success.OrgId, success.ImServerUserId)
			}
		}()
	}

	return resp, nil
}
