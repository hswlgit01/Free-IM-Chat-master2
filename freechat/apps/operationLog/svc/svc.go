package svc

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/openimsdk/chat/freechat/apps/operationLog/dto"
	"github.com/openimsdk/chat/freechat/apps/operationLog/model"
	"github.com/openimsdk/chat/freechat/plugin"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type GroupOperationLogSvc struct{}

func NewGroupOperationLogSvc() *GroupOperationLogSvc {
	return &GroupOperationLogSvc{}
}

func (w *GroupOperationLogSvc) ListGroupOperationLogSvc(orgId primitive.ObjectID, groupId string,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.GroupOperationLogResp], error) {
	db := plugin.MongoCli().GetDB()
	//dao := openImModel.NewGroupDao(db)
	//
	//_, err := dao.GetByGroupIDAndOrgID(context.TODO(), groupId, orgId.Hex())
	//if err != nil {
	//	if dbutil.IsDBNotFound(err) {
	//		return nil, freeErrors.NotFoundErrWithResource(orgId.String())
	//	}
	//	return nil, freeErrors.SystemErr(err)
	//}

	log.ZInfo(context.TODO(), "ListGroupOperationLogSvc", "page", page, "startTime", startTime, "endTime", endTime)

	groupOperLogDao := openImModel.NewGroupOperationLogDao(db)

	total, items, err := groupOperLogDao.Select(context.TODO(), groupId, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.GroupOperationLogResp]{
		Total: total,
		List:  []*dto.GroupOperationLogResp{},
	}

	for _, record := range items {
		r, err := dto.NewGroupOperationLogResp(context.TODO(), db, record)
		if err != nil {
			return nil, err
		}
		resp.List = append(resp.List, r)
	}

	return resp, nil
}

type OperationLogSvc struct{}

func NewOperationLogSvc() *OperationLogSvc {
	return &OperationLogSvc{}
}

type InternalCreateOperationLogReq struct {
	OrgId          primitive.ObjectID `json:"org_id"`
	UserId         string             `json:"user_id"`
	ImServerUserId string             `json:"im_server_user_id" `

	OperationType model.OperationLogType `json:"operation_type"`
	Details       interface{}            `json:"json_details"`
}

func (w *OperationLogSvc) InternalCreateOperationLog(ctx context.Context, req *InternalCreateOperationLogReq) error {
	db := plugin.MongoCli().GetDB()
	opLogDao := model.NewOperationLogDao(db)

	marshal, err := json.Marshal(req.Details)
	if err != nil {
		return err
	}

	err = plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		obj := &model.OperationLog{
			OrgId:          req.OrgId,
			OperationType:  req.OperationType,
			ImServerUserId: req.ImServerUserId,
			UserId:         req.UserId,
			Details:        req.Details,
			DetailsRaw:     string(marshal),
		}
		err = opLogDao.Create(sessionCtx, obj)
		return err
	})

	return errors.Unwrap(err)
}

func (w *OperationLogSvc) CmsListOperationLog(ctx context.Context, orgId primitive.ObjectID,
	operationLogType model.OperationLogType, keyword string, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.OperationLogJoinAllResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	operationLogDao := model.NewOperationLogDao(db)

	total, records, err := operationLogDao.SelectJoinAll(context.TODO(), keyword, orgId, operationLogType, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.OperationLogJoinAllResp]{
		List:  []*dto.OperationLogJoinAllResp{},
		Total: total,
	}

	for _, record := range records {
		item := dto.NewOperationLogJoinAllResp(db, record)
		resp.List = append(resp.List, item)
	}

	return resp, nil

}
