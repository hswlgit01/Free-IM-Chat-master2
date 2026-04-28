package svc

import (
	"context"
	"errors"
	"github.com/openimsdk/chat/freechat/apps/defaultFriend/dto"
	"github.com/openimsdk/chat/freechat/apps/defaultFriend/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/mctx"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"slices"
)

type DefaultFriendSvc struct{}

func NewDefaultFriendSvc() *DefaultFriendSvc {
	return &DefaultFriendSvc{}
}

type SuperCmsAddDefaultFriendReq struct {
	ImUserIDs []string           `json:"im_user_ids"`
	OrgId     primitive.ObjectID `json:"org_id"`
}

func (w *DefaultFriendSvc) SuperCmsAddDefaultFriend(ctx context.Context, req *SuperCmsAddDefaultFriendReq) error {
	db := plugin.MongoCli().GetDB()
	friendDao := model.NewDefaultFriendDao(db)
	if len(req.ImUserIDs) <= 0 {
		return freeErrors.ApiErr("im_user_ids is empty")
	}

	err := plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		imUserIds := make([]string, 0)
		for _, id := range req.ImUserIDs {
			if slices.Contains(imUserIds, id) {
				continue
			}

			exists, err := friendDao.ExistByImUserId(sessionCtx, id)
			if err != nil {
				return err
			}
			if exists {
				continue
			}

			imUserIds = append(imUserIds, id)
		}

		defaultFriends := make([]*model.DefaultFriend, 0)
		for _, id := range imUserIds {
			defaultFriends = append(defaultFriends, &model.DefaultFriend{
				ImServerUserId: id,
				OrgId:          req.OrgId,
			})
		}

		if len(defaultFriends) > 0 {
			err := friendDao.Add(sessionCtx, defaultFriends)
			return err
		}

		return nil
	})

	return err
}

type SuperCmsDelDefaultFriendReq struct {
	ImUserIDs []string           `json:"im_user_ids"`
	OrgId     primitive.ObjectID `json:"org_id"`
}

func (w *DefaultFriendSvc) SuperCmsDelDefaultFriend(ctx context.Context, req *SuperCmsDelDefaultFriendReq) error {
	db := plugin.MongoCli().GetDB()
	friendDao := model.NewDefaultFriendDao(db)
	if len(req.ImUserIDs) <= 0 {
		return freeErrors.ApiErr("im_user_ids is empty")
	}

	err := plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		err := friendDao.Del(sessionCtx, req.OrgId, req.ImUserIDs)
		return err
	})

	return errors.Unwrap(err)
}

type SuperCmsSearchDefaultFriendResp struct {
	ImUserIDs []string `json:"im_user_ids"`
}

func (w *DefaultFriendSvc) SuperCmsSearchDefaultFriend(ctx context.Context, orgId primitive.ObjectID) (*SuperCmsSearchDefaultFriendResp, error) {
	db := plugin.MongoCli().GetDB()
	friendDao := model.NewDefaultFriendDao(db)

	imUserIDs, err := friendDao.SelectByOrgIdAndImUserIds(ctx, orgId, nil)
	if err != nil {
		return nil, err
	}

	return &SuperCmsSearchDefaultFriendResp{
		ImUserIDs: imUserIDs,
	}, nil
}

func (w *DefaultFriendSvc) SuperCmsListDefaultFriend(ctx context.Context, orgId primitive.ObjectID, keyword string,
	page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.RegisterAddFriendJoinAllResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	friendDao := model.NewDefaultFriendDao(db)

	total, records, err := friendDao.SelectJoinAll(context.TODO(), keyword, orgId, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.RegisterAddFriendJoinAllResp]{
		List:  make([]*dto.RegisterAddFriendJoinAllResp, 0),
		Total: total,
	}

	for _, record := range records {
		respListItem := dto.NewRegisterAddFriendJoinAllResp(record)
		resp.List = append(resp.List, respListItem)
	}
	return resp, nil
}

func (w *DefaultFriendSvc) InternalAddDefaultFriend(operationID string, orgId primitive.ObjectID, imServerUserId string) []string {
	registerAddFriendDao := model.NewDefaultFriendDao(plugin.MongoCli().GetDB())
	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(context.Background(), constantpb.OperationID, operationID)
	imApiCallerToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctxWithOpID, "imApiCaller.ImAdminTokenWithDefaultAdmin error", err)
		return nil
	}
	imApiCallerCtx := mctx.WithApiToken(ctxWithOpID, imApiCallerToken)

	imUserIds, err := registerAddFriendDao.SelectByOrgIdAndImUserIds(context.TODO(), orgId, nil)
	if err == nil {
		friendIDs := make([]string, 0, len(imUserIds))
		for _, userID := range imUserIds {
			if userID == "" || userID == imServerUserId {
				continue
			}
			friendIDs = append(friendIDs, userID)
		}
		if len(friendIDs) == 0 {
			return nil
		}
		if err = imApiCaller.ImportFriend(imApiCallerCtx, imServerUserId, friendIDs); err != nil {
			log.ZError(ctxWithOpID, "imApiCaller.ImportFriend default friends error", err,
				"userID", imServerUserId, "defaultFriendIDs", friendIDs)
		}
		for _, friendID := range friendIDs {
			if err = imApiCaller.ImportFriend(imApiCallerCtx, friendID, []string{imServerUserId}); err != nil {
				log.ZError(ctxWithOpID, "imApiCaller.ImportFriend new user to default friend error", err,
					"defaultFriendID", friendID, "newUserID", imServerUserId)
			}
		}
		return friendIDs
	} else {
		log.ZError(ctxWithOpID, "registerAddFriendDao.SelectByOrgIdAndImUserIds error", err)
	}
	return nil
}
