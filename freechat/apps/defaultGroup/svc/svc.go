package svc

import (
	"context"
	"github.com/openimsdk/chat/freechat/apps/defaultGroup/dto"
	"github.com/openimsdk/chat/freechat/apps/defaultGroup/model"
	organizationModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/plugin"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/mctx"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"slices"
	"strings"
)

type DefaultGroupSvc struct{}

func NewDefaultGroupSvc() *DefaultGroupSvc {
	return &DefaultGroupSvc{}
}

type SuperCmsAddDefaultGroupReq struct {
	GroupIDs []string `json:"group_ids"`
	// dawn 2026-06-14 默认群可绑定二级业务员：空值表示全组织新会员都加入。
	SalespersonUserID string             `json:"salesperson_user_id"`
	OrgId             primitive.ObjectID `json:"org_id"`
}

func (w *DefaultGroupSvc) SuperCmsAddDefaultGroup(ctx context.Context, req *SuperCmsAddDefaultGroupReq) error {
	db := plugin.MongoCli().GetDB()
	groupDao := model.NewDefaultGroupDao(db)
	req.SalespersonUserID = strings.TrimSpace(req.SalespersonUserID)
	if len(req.GroupIDs) <= 0 {
		return freeErrors.ApiErr("group_ids is empty")
	}
	if _, err := w.validateSalesperson(ctx, db, req.OrgId, req.SalespersonUserID); err != nil {
		return err
	}

	err := plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		groupIds := make([]string, 0)
		for _, id := range req.GroupIDs {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			if slices.Contains(groupIds, id) {
				continue
			}

			exists, err := groupDao.Exist(sessionCtx, req.OrgId, id, req.SalespersonUserID)
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
				GroupID:           id,
				OrgId:             req.OrgId,
				SalespersonUserID: req.SalespersonUserID,
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
	IDs      []string           `json:"ids"`
	GroupIDs []string           `json:"group_ids"`
	OrgId    primitive.ObjectID `json:"org_id"`
}

func (w *DefaultGroupSvc) SuperCmsDelDefaultGroup(ctx context.Context, req *SuperCmsDelDefaultGroupReq) error {
	db := plugin.MongoCli().GetDB()
	groupDao := model.NewDefaultGroupDao(db)
	if len(req.IDs) <= 0 && len(req.GroupIDs) <= 0 {
		return freeErrors.ApiErr("ids or group_ids is empty")
	}

	ids := make([]primitive.ObjectID, 0, len(req.IDs))
	for _, rawID := range req.IDs {
		rawID = strings.TrimSpace(rawID)
		if rawID == "" {
			continue
		}
		id, err := primitive.ObjectIDFromHex(rawID)
		if err != nil {
			return freeErrors.ApiErr("invalid default group id")
		}
		ids = append(ids, id)
	}

	err := plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		err := groupDao.Del(sessionCtx, req.OrgId, ids, req.GroupIDs)
		return err
	})

	return err
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
	salespersonIDs := uniqueSalespersonIDs(records)

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

	salespersonMap, err := w.loadSalespersonDisplay(ctx, db, orgId, salespersonIDs)
	if err != nil {
		log.ZWarn(ctx, "load default group salesperson display failed", err, "salesperson_user_ids", salespersonIDs)
	}

	for _, record := range records {
		respListItem := dto.NewRegisterAddGroupJoinAllResp(record)
		respListItem.GroupName = groupNameMap[record.GroupID]
		if display, ok := salespersonMap[record.SalespersonUserID]; ok {
			respListItem.SalespersonNickname = display.Nickname
			respListItem.SalespersonAccount = display.Account
			respListItem.SalespersonImServerUserID = display.ImServerUserID
		}
		resp.List = append(resp.List, respListItem)
	}
	return resp, nil
}

func (w *DefaultGroupSvc) InternalAddDefaultGroup(operationID string, orgId primitive.ObjectID, imServerUserId string) {
	db := plugin.MongoCli().GetDB()
	registerAddGroupDao := model.NewDefaultGroupDao(db)
	imApiCaller := plugin.ImApiCaller()
	ctxWithOpID := context.WithValue(context.Background(), constantpb.OperationID, operationID)
	imApiCallerToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctxWithOpID, "imApiCaller.ImAdminTokenWithDefaultAdmin error", err)
		return
	}
	imApiCallerCtx := mctx.WithApiToken(ctxWithOpID, imApiCallerToken)

	// dawn 2026-06-14 注册默认入群：全局默认群全部加入，绑定业务员的默认群只按新用户上级链路命中后加入。
	salespersonUserIDs := make([]string, 0)
	orgUserDao := organizationModel.NewOrganizationUserDao(db)
	orgUser, orgUserErr := orgUserDao.GetByImServerUserIdAndOrgID(ctxWithOpID, imServerUserId, orgId.Hex())
	if orgUserErr != nil {
		log.ZWarn(ctxWithOpID, "get org user for default group match failed, only global default groups will be used", orgUserErr, "org_id", orgId.Hex(), "im_server_user_id", imServerUserId)
	} else {
		salespersonUserIDs = collectDefaultGroupSalespersonIDs(orgUser)
	}

	groupIDs, err := registerAddGroupDao.SelectEligibleGroupIDs(context.TODO(), orgId, salespersonUserIDs)
	if err == nil && len(groupIDs) > 0 {
		for _, groupID := range groupIDs {
			err = imApiCaller.InviteToGroup(imApiCallerCtx, imServerUserId, []string{groupID})
			if err != nil {
				log.ZError(ctxWithOpID, "imApiCaller.InviteToGroup error", err, "groupID", groupID, "userID", imServerUserId)
			}
		}
	} else if err != nil {
		log.ZError(ctxWithOpID, "registerAddGroupDao.SelectEligibleGroupIDs error", err)
	}
}

type salespersonDisplay struct {
	Account        string
	Nickname       string
	ImServerUserID string
}

func (w *DefaultGroupSvc) validateSalesperson(ctx context.Context, db *mongo.Database, orgId primitive.ObjectID, salespersonUserID string) (*organizationModel.OrganizationUser, error) {
	if salespersonUserID == "" {
		return nil, nil
	}
	orgUserDao := organizationModel.NewOrganizationUserDao(db)
	orgUser, err := orgUserDao.GetByUserIdAndOrgId(ctx, salespersonUserID, orgId)
	if err != nil {
		return nil, freeErrors.ApiErr("所属业务员不存在")
	}
	if orgUser.Status == organizationModel.OrganizationUserDisableStatus {
		return nil, freeErrors.ApiErr("所属业务员已禁用")
	}
	if orgUser.Level != 2 {
		return nil, freeErrors.ApiErr("所属业务员必须是2级业务员")
	}
	return orgUser, nil
}

func uniqueSalespersonIDs(records []*model.DefaultGroup) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, record := range records {
		if record == nil {
			continue
		}
		id := strings.TrimSpace(record.SalespersonUserID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func (w *DefaultGroupSvc) loadSalespersonDisplay(ctx context.Context, db *mongo.Database, orgId primitive.ObjectID, salespersonIDs []string) (map[string]salespersonDisplay, error) {
	out := make(map[string]salespersonDisplay, len(salespersonIDs))
	if len(salespersonIDs) == 0 {
		return out, nil
	}

	orgUserDao := organizationModel.NewOrganizationUserDao(db)
	orgUsers, err := orgUserDao.ListByOrgIdAndUserIDs(ctx, orgId, salespersonIDs)
	if err != nil {
		return out, err
	}
	orgUserMap := make(map[string]*organizationModel.OrganizationUser, len(orgUsers))
	imServerUserIDs := make([]string, 0, len(orgUsers))
	for _, orgUser := range orgUsers {
		if orgUser == nil || orgUser.UserId == "" {
			continue
		}
		orgUserMap[orgUser.UserId] = orgUser
		if orgUser.ImServerUserId != "" {
			imServerUserIDs = append(imServerUserIDs, orgUser.ImServerUserId)
		}
	}

	attrMap := make(map[string]*chatModel.Attribute, len(salespersonIDs))
	attrs, attrErr := chatModel.NewAttributeDao(db).Find(ctx, salespersonIDs)
	if attrErr != nil {
		log.ZWarn(ctx, "find default group salesperson attributes failed", attrErr, "salesperson_user_ids", salespersonIDs)
	} else {
		for _, attr := range attrs {
			if attr != nil && attr.UserID != "" {
				attrMap[attr.UserID] = attr
			}
		}
	}

	imUserMap := make(map[string]*openImModel.User, len(imServerUserIDs))
	imUsers, imErr := openImModel.NewUserDao(db).FindByUserIDs(ctx, imServerUserIDs)
	if imErr != nil {
		log.ZWarn(ctx, "find default group salesperson im users failed", imErr, "im_server_user_ids", imServerUserIDs)
	} else {
		for _, imUser := range imUsers {
			if imUser != nil && imUser.UserID != "" {
				imUserMap[imUser.UserID] = imUser
			}
		}
	}

	for _, salespersonID := range salespersonIDs {
		display := salespersonDisplay{}
		if orgUser := orgUserMap[salespersonID]; orgUser != nil {
			display.ImServerUserID = orgUser.ImServerUserId
			if imUser := imUserMap[orgUser.ImServerUserId]; imUser != nil {
				display.Nickname = strings.TrimSpace(imUser.Nickname)
			}
		}
		if attr := attrMap[salespersonID]; attr != nil {
			display.Account = strings.TrimSpace(attr.Account)
			if display.Nickname == "" {
				display.Nickname = strings.TrimSpace(attr.Nickname)
			}
		}
		if display.Nickname == "" {
			display.Nickname = salespersonID
		}
		out[salespersonID] = display
	}
	return out, nil
}

func collectDefaultGroupSalespersonIDs(orgUser *organizationModel.OrganizationUser) []string {
	if orgUser == nil {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]string, 0, len(orgUser.AncestorPath)+5)
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	// 注册默认群按业务员匹配时，优先使用注册邀请人。部分老数据或异常注册链路
	// 可能还没完整写入 ancestor_path，但 inviter 已经能准确表示本次邀请码归属。
	if orgUser.InviterType == organizationModel.OrganizationUserInviterTypeOrgUser {
		add(orgUser.Inviter)
	}
	add(orgUser.Level1Parent)
	add(orgUser.Level2Parent)
	add(orgUser.Level3Parent)
	for _, ancestorID := range orgUser.AncestorPath {
		add(ancestorID)
	}
	if orgUser.Level == 2 {
		add(orgUser.UserId)
	}
	return out
}
