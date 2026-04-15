package svc

import (
	"context"
	"errors"
	"github.com/openimsdk/chat/freechat/apps/defaultGroup/dto"
	"github.com/openimsdk/chat/freechat/apps/defaultGroup/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/mctx"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"slices"
)

type DefaultGroupSvc struct{}

func NewDefaultGroupSvc() *DefaultGroupSvc {
	return &DefaultGroupSvc{}
}

type SuperCmsAddDefaultGroupReq struct {
	GroupIDs []string           `json:"group_ids"`
	OrgId    primitive.ObjectID `json:"org_id"`
}

func (w *DefaultGroupSvc) SuperCmsAddDefaultGroup(ctx context.Context, req *SuperCmsAddDefaultGroupReq) error {
	db := plugin.MongoCli().GetDB()
	groupDao := model.NewDefaultGroupDao(db)
	if len(req.GroupIDs) <= 0 {
		return freeErrors.ApiErr("group_ids is empty")
	}

	err := plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		groupIds := make([]string, 0)
		for _, id := range req.GroupIDs {
			if slices.Contains(groupIds, id) {
				continue
			}

			exists, err := groupDao.ExistByGroupId(sessionCtx, id)
			if err != nil {
				return err
			}
			if exists {
				continue
			}

			groupIds = append(groupIds, id)
		}

		defaultGroups := make([]*model.DefaultGroup, 0)
		for _, id := range groupIds {
			defaultGroups = append(defaultGroups, &model.DefaultGroup{
				GroupID: id,
				OrgId:   req.OrgId,
			})
		}

		if len(defaultGroups) > 0 {
			err := groupDao.Add(sessionCtx, defaultGroups)
			return err
		}

		return nil
	})

	return err
}

type SuperCmsDelDefaultGroupReq struct {
	GroupIDs []string           `json:"group_ids"`
	OrgId    primitive.ObjectID `json:"org_id"`
}

func (w *DefaultGroupSvc) SuperCmsDelDefaultGroup(ctx context.Context, req *SuperCmsDelDefaultGroupReq) error {
	db := plugin.MongoCli().GetDB()
	groupDao := model.NewDefaultGroupDao(db)
	if len(req.GroupIDs) <= 0 {
		return freeErrors.ApiErr("group_ids is empty")
	}

	err := plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		err := groupDao.Del(sessionCtx, req.OrgId, req.GroupIDs)
		return err
	})

	return errors.Unwrap(err)
}

type SuperCmsSearchDefaultGroupResp struct {
	GroupIDs []string `json:"group_ids"`
}

func (w *DefaultGroupSvc) SuperCmsSearchDefaultGroup(ctx context.Context, orgId primitive.ObjectID) (*SuperCmsSearchDefaultGroupResp, error) {
	db := plugin.MongoCli().GetDB()
	groupDao := model.NewDefaultGroupDao(db)

	groupIDs, err := groupDao.SelectByOrgIdAndGroupIds(ctx, orgId, nil)
	if err != nil {
		return nil, err
	}

	return &SuperCmsSearchDefaultGroupResp{
		GroupIDs: groupIDs,
	}, nil
}

func (w *DefaultGroupSvc) SuperCmsListDefaultGroup(ctx context.Context, orgId primitive.ObjectID, keyword string,
	page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.RegisterAddGroupJoinAllResp], error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	groupDao := model.NewDefaultGroupDao(db)

	total, records, err := groupDao.SelectJoinAll(context.TODO(), keyword, orgId, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.RegisterAddGroupJoinAllResp]{
		List:  make([]*dto.RegisterAddGroupJoinAllResp, 0),
		Total: total,
	}

	groupIDs := make([]string, 0, len(records))
	for _, record := range records {
		if record != nil && record.GroupID != "" {
			groupIDs = append(groupIDs, record.GroupID)
		}
	}

	groupNameMap := make(map[string]string, len(groupIDs))
	if len(groupIDs) > 0 {
		imApiCaller := plugin.ImApiCaller()
		ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, "default-group-list")
		imApiCallerToken, tokenErr := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
		if tokenErr != nil {
			log.ZWarn(ctx, "get im admin token for default group list failed", tokenErr)
		} else {
			groups, findErr := imApiCaller.FindGroupInfo(mctx.WithApiToken(ctxWithOpID, imApiCallerToken), groupIDs)
			if findErr != nil {
				log.ZWarn(ctx, "find default group info failed", findErr, "group_ids", groupIDs)
			} else {
				for _, group := range groups {
					if group == nil || group.GroupID == "" {
						continue
					}
					groupNameMap[group.GroupID] = group.GroupName
				}
			}
		}
	}

	for _, record := range records {
		respListItem := dto.NewRegisterAddGroupJoinAllResp(record)
		respListItem.GroupName = groupNameMap[record.GroupID]
		resp.List = append(resp.List, respListItem)
	}
	return resp, nil
}

func (w *DefaultGroupSvc) InternalAddDefaultGroup(operationID string, orgId primitive.ObjectID, imServerUserId string) {
	registerAddGroupDao := model.NewDefaultGroupDao(plugin.MongoCli().GetDB())
	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(context.Background(), constantpb.OperationID, operationID)
	imApiCallerToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctxWithOpID, "imApiCaller.ImAdminTokenWithDefaultAdmin error", err)
		return
	}
	imApiCallerCtx := mctx.WithApiToken(ctxWithOpID, imApiCallerToken)

	groupIDs, err := registerAddGroupDao.SelectByOrgIdAndGroupIds(context.TODO(), orgId, nil)
	if err == nil && len(groupIDs) > 0 {
		// 邀请用户加入所有默认群
		for _, groupID := range groupIDs {
			err = imApiCaller.InviteToGroup(imApiCallerCtx, imServerUserId, []string{groupID})
			if err != nil {
				log.ZError(ctxWithOpID, "imApiCaller.InviteToGroup error", err, "groupID", groupID, "userID", imServerUserId)
			}
		}
	} else if err != nil {
		log.ZError(ctxWithOpID, "registerAddGroupDao.SelectByOrgIdAndGroupIds error", err)
	}
}
