package walletTransactionRecord

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/dto"
	"github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/model"
	"github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/svc"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/ginUtils"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"strconv"
	"strings"
)

type WalletTsRecordCtl struct{}

func NewWalletTsRecordCtl() *WalletTsRecordCtl {
	return &WalletTsRecordCtl{}
}

// GetWalletTsRecord 查询交易记录详情
func (w *WalletTsRecordCtl) GetWalletTsRecord(c *gin.Context) {
	id := strings.TrimSpace(c.Query("id"))

	objId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	walletTsSvc := &svc.WalletTsRecordSvc{}
	resp, err := walletTsSvc.DetailWalletTsRecordSvc(objId)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

// ListWalletTsRecord 批量获取交易记录详情
func (w *WalletTsRecordCtl) ListWalletTsRecord(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 处理单一类型查询
	typeStr := strings.TrimSpace(c.Query("type"))
	var tsRecordType model.TsRecordType
	if typeStr != "" {
		typeInt, err := strconv.Atoi(typeStr)
		if err != nil {
			apiresp.GinError(c, freeErrors.ParameterInvalidErr)
			return
		}
		tsRecordType = model.TsRecordType(typeInt)
	}

	// 处理多类型查询
	typeInStr := strings.TrimSpace(c.Query("type_in"))
	var tsRecordTypes []model.TsRecordType
	if typeInStr != "" {
		typeStrArr := strings.Split(typeInStr, ",")
		for _, str := range typeStrArr {
			typeInt, err := strconv.Atoi(str)
			if err != nil {
				apiresp.GinError(c, freeErrors.ParameterInvalidErr)
				return
			}
			tsRecordTypes = append(tsRecordTypes, model.TsRecordType(typeInt))
		}
	}

	currencyId, err := ginUtils.QueryToObjectId(c, "currency_id")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	page, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, freeErrors.PageParameterInvalidErr)
		return
	}

	startTimeUtc, err := ginUtils.QueryToUtcTime(c, "startTime")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	endTimeUtc, err := ginUtils.QueryToUtcTime(c, "endTime")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	walletTsSvc := &svc.WalletTsRecordSvc{}
	var resp *paginationUtils.ListResp[*dto.SimpleWalletTsRecordResp]

	// 根据参数选择查询方法
	if len(tsRecordTypes) > 0 {
		// 使用多类型查询
		resp, err = walletTsSvc.ListWalletTsRecordByTypesSvc(opUserID, currencyId, tsRecordTypes, startTimeUtc, endTimeUtc, page)
	} else {
		// 使用单一类型或不指定类型查询
		resp, err = walletTsSvc.ListWalletTsRecordSvc(opUserID, currencyId, tsRecordType, startTimeUtc, endTimeUtc, page)
	}

	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

type DepAdminWalletTsRecordCtl struct{}

func NewDepAdminWalletTsRecordCtl() *DepAdminWalletTsRecordCtl {
	return &DepAdminWalletTsRecordCtl{}
}

func (w *DepAdminWalletTsRecordCtl) GetOrgWalletTsRecord(c *gin.Context) {
	id := strings.TrimSpace(c.Query("id"))

	objId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	walletTsSvc := &svc.OrgWalletTsRecordSvc{}
	resp, err := walletTsSvc.DetailOrgWalletTsRecordSvc(objId)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

func (w *DepAdminWalletTsRecordCtl) ListOrgWalletTsRecord(c *gin.Context) {
	typeStr := strings.TrimSpace(c.Query("type"))
	var tsRecordType model.TsRecordType
	if typeStr != "" {
		typeInt, err := strconv.Atoi(typeStr)
		if err != nil {
			apiresp.GinError(c, freeErrors.ParameterInvalidErr)
			return
		}
		tsRecordType = model.TsRecordType(typeInt)
	}

	currencyId, err := ginUtils.QueryToObjectId(c, "currency_id")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	page, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, freeErrors.PageParameterInvalidErr)
		return
	}

	startTimeUtc, err := ginUtils.QueryToUtcTime(c, "startTime")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	endTimeUtc, err := ginUtils.QueryToUtcTime(c, "endTime")
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	// 从上下文中获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	walletTsSvc := &svc.OrgWalletTsRecordSvc{}
	resp, err := walletTsSvc.ListOrgWalletTsRecordSvc(org.ID, currencyId, tsRecordType, startTimeUtc, endTimeUtc, page)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}
