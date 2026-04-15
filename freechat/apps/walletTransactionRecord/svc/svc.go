package svc

import (
	"context"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	"github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/dto"
	"github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/model"
	"github.com/openimsdk/tools/log"

	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type WalletTsRecordSvc struct{}

func NewWalletTsRecordService() *WalletTsRecordSvc {
	return &WalletTsRecordSvc{}
}

func (w *WalletTsRecordSvc) DetailWalletTsRecordSvc(id primitive.ObjectID) (*dto.DetailWalletTsRecordResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	walletTsDao := model.NewWalletTsRecordDao(db)
	walletTsRecord, err := walletTsDao.GetById(context.TODO(), id)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErr
		}
		return nil, err
	}
	detailWalletTsRecord, err := dto.NewDetailWalletTsRecord(context.TODO(), db, walletTsRecord)
	if err != nil {
		return nil, err
	}
	return detailWalletTsRecord, nil
}

func (w *WalletTsRecordSvc) ListWalletTsRecordSvc(userId string, currencyId primitive.ObjectID, tsRecordType model.TsRecordType,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.SimpleWalletTsRecordResp], error) {
	db := plugin.MongoCli().GetDB()

	log.ZInfo(context.TODO(), "ListWalletTsRecordSvc", "tsRecordType", tsRecordType, "page", page,
		"startTime", startTime, "endTime", endTime)

	walletTsDao := model.NewWalletTsRecordDao(db)
	walletInfoDao := walletModel.NewWalletInfoDao(db)

	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(context.TODO(), userId, walletModel.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		return nil, err
	}

	total, walletTsRecords, err := walletTsDao.Select(context.TODO(), walletInfo.ID, currencyId, tsRecordType, startTime, endTime, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.SimpleWalletTsRecordResp]{
		Total: total,
		List:  []*dto.SimpleWalletTsRecordResp{},
	}

	for _, record := range walletTsRecords {
		tsRecord, err := dto.NewSimpleWalletTsRecord(context.TODO(), db, record)
		if err != nil {
			return nil, err
		}
		resp.List = append(resp.List, tsRecord)
	}

	return resp, nil
}

// ListWalletTsRecordByTypesSvc 根据多个交易类型查询交易记录
func (w *WalletTsRecordSvc) ListWalletTsRecordByTypesSvc(userId string, currencyId primitive.ObjectID, tsRecordTypes []model.TsRecordType,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.SimpleWalletTsRecordResp], error) {
	db := plugin.MongoCli().GetDB()

	log.ZInfo(context.TODO(), "ListWalletTsRecordByTypesSvc", "tsRecordTypes", tsRecordTypes, "page", page,
		"startTime", startTime, "endTime", endTime)

	walletTsDao := model.NewWalletTsRecordDao(db)
	walletInfoDao := walletModel.NewWalletInfoDao(db)

	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(context.TODO(), userId, walletModel.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		return nil, err
	}

	total, walletTsRecords, err := walletTsDao.SelectByTypes(context.TODO(), walletInfo.ID, currencyId, tsRecordTypes, startTime, endTime, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.SimpleWalletTsRecordResp]{
		Total: total,
		List:  []*dto.SimpleWalletTsRecordResp{},
	}

	for _, record := range walletTsRecords {
		tsRecord, err := dto.NewSimpleWalletTsRecord(context.TODO(), db, record)
		if err != nil {
			return nil, err
		}
		resp.List = append(resp.List, tsRecord)
	}

	return resp, nil
}

type OrgWalletTsRecordSvc struct{}

func NewOrgWalletTsRecordService() *OrgWalletTsRecordSvc {
	return &OrgWalletTsRecordSvc{}
}

func (w *OrgWalletTsRecordSvc) DetailOrgWalletTsRecordSvc(id primitive.ObjectID) (*dto.DetailWalletTsRecordResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	walletTsDao := model.NewWalletTsRecordDao(db)
	walletTsRecord, err := walletTsDao.GetById(context.TODO(), id)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErr
		}
		return nil, err
	}
	detailWalletTsRecord, err := dto.NewDetailWalletTsRecord(context.TODO(), db, walletTsRecord)
	if err != nil {
		return nil, err
	}
	return detailWalletTsRecord, nil
}

func (w *OrgWalletTsRecordSvc) ListOrgWalletTsRecordSvc(orgId primitive.ObjectID, currencyId primitive.ObjectID, tsRecordType model.TsRecordType,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.SimpleWalletTsRecordResp], error) {
	db := plugin.MongoCli().GetDB()

	log.ZInfo(context.TODO(), "ListOrgWalletTsRecordSvc", "tsRecordType", tsRecordType, "page", page,
		"startTime", startTime, "endTime", endTime)

	walletTsDao := model.NewWalletTsRecordDao(db)
	walletInfoDao := walletModel.NewWalletInfoDao(db)

	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(context.TODO(), orgId.Hex(), walletModel.WalletInfoOwnerTypeOrganization)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.WalletNotOpenErr
		}
		return nil, err
	}

	total, walletTsRecords, err := walletTsDao.Select(context.TODO(), walletInfo.ID, currencyId, tsRecordType, startTime, endTime, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.SimpleWalletTsRecordResp]{
		Total: total,
		List:  []*dto.SimpleWalletTsRecordResp{},
	}

	for _, record := range walletTsRecords {
		tsRecord, err := dto.NewSimpleWalletTsRecord(context.TODO(), db, record)
		if err != nil {
			return nil, err
		}
		resp.List = append(resp.List, tsRecord)
	}

	return resp, nil
}
