// Copyright © 2023 OpenIM open source community. All rights reserved.

package svc

import (
	"context"
	"regexp"
	"strings"

	adminModel "github.com/openimsdk/chat/freechat/apps/admin/model"
	"github.com/openimsdk/chat/freechat/apps/organization/dto"
	"github.com/openimsdk/chat/freechat/third/chat/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	chat "github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (s *HierarchyService) mergeForbiddenImServerUserIDs(ctx context.Context) map[string]struct{} {
	out := make(map[string]struct{})
	faDao := model.NewForbiddenAccountDao(s.db)
	if ids, err := faDao.FindAllIDs(ctx); err != nil {
		log.ZWarn(ctx, "search_panel: forbidden_account 列表加载失败", err)
	} else {
		for _, id := range ids {
			if id != "" {
				out[id] = struct{}{}
			}
		}
	}
	saDao := adminModel.NewSuperAdminForbiddenDao(s.db)
	if saIDs, err := saDao.DistinctForbiddenImServerUserIDs(ctx); err != nil {
		log.ZWarn(ctx, "search_panel: super_admin_forbidden_detail 加载失败", err)
	} else {
		for _, id := range saIDs {
			if id != "" {
				out[id] = struct{}{}
			}
		}
	}
	return out
}

const (
	// searchPanelMaxResults 单次返回上限
	searchPanelMaxResults int64 = 500
	// searchPanelMaxCandidateIDs attribute / user 侧前缀扫描最多收集的 user_id 数，避免 $in 过大与全表扫过久
	searchPanelMaxCandidateIDs int64 = 3000
)

// SearchHierarchyPanel 管理后台层级搜索：keyword 对 account / nickname 均为前缀匹配（^ + QuoteMeta，忽略大小写）。
// 查询结构：先在 attribute、user 上筛出候选 user_id / im_server_user_id，再 $in 查 organization_user，最后批量补全展示字段（避免对全组织逐条 $lookup）。
// 排除组织禁用、账号封禁、IM 昵称为「已注销」；最多返回 searchPanelMaxResults 条。
func (s *HierarchyService) SearchHierarchyPanel(ctx context.Context, organizationID primitive.ObjectID, req *dto.SearchHierarchyReq) (*dto.SearchHierarchyPanelResp, error) {
	kw := strings.TrimSpace(req.Keyword)
	if kw == "" {
		return &dto.SearchHierarchyPanelResp{Users: []dto.HierarchyPanelSearchUser{}, Total: 0}, nil
	}
	escaped := regexp.QuoteMeta(kw)
	prefixRegex := "^" + escaped
	regex := bson.M{"$regex": prefixRegex, "$options": "i"}

	attrColl := s.db.Collection("attribute")
	userColl := s.db.Collection(openImModel.User{}.TableName())
	orgColl := s.db.Collection(chat.OrganizationUser{}.TableName())

	// 1) attribute：账号或昵称前缀匹配 → organization_user.user_id
	attrFilter := bson.M{
		"$or": []bson.M{
			{"account": regex},
			{"nickname": regex},
		},
	}
	attrCur, err := attrColl.Find(ctx, attrFilter, options.Find().
		SetProjection(bson.M{"user_id": 1, "account": 1, "nickname": 1, "face_url": 1, "_id": 0}).
		SetLimit(searchPanelMaxCandidateIDs))
	if err != nil {
		return nil, err
	}
	attrByOrgUserID := make(map[string]struct {
		Account  string `bson:"account"`
		Nickname string `bson:"nickname"`
		FaceURL  string `bson:"face_url"`
	})
	for attrCur.Next(ctx) {
		var row struct {
			UserID   string `bson:"user_id"`
			Account  string `bson:"account"`
			Nickname string `bson:"nickname"`
			FaceURL  string `bson:"face_url"`
		}
		if err := attrCur.Decode(&row); err != nil {
			_ = attrCur.Close(ctx)
			return nil, err
		}
		if row.UserID == "" {
			continue
		}
		if _, ok := attrByOrgUserID[row.UserID]; !ok {
			attrByOrgUserID[row.UserID] = struct {
				Account  string `bson:"account"`
				Nickname string `bson:"nickname"`
				FaceURL  string `bson:"face_url"`
			}{row.Account, row.Nickname, row.FaceURL}
		}
	}
	_ = attrCur.Close(ctx)

	attrUserIDs := make([]string, 0, len(attrByOrgUserID))
	for id := range attrByOrgUserID {
		attrUserIDs = append(attrUserIDs, id)
	}

	// 2) OpenIM user：昵称前缀匹配且非「已注销」→ organization_user.im_server_user_id
	userFilter := bson.M{
		"$and": []bson.M{
			{"nickname": regex},
			{"nickname": bson.M{"$ne": imNicknameDeregistered}},
		},
	}
	userCur, err := userColl.Find(ctx, userFilter, options.Find().
		SetProjection(bson.M{"user_id": 1, "nickname": 1, "face_url": 1, "_id": 0}).
		SetLimit(searchPanelMaxCandidateIDs))
	if err != nil {
		return nil, err
	}
	imByID := make(map[string]struct {
		Nickname string `bson:"nickname"`
		FaceURL  string `bson:"face_url"`
	})
	for userCur.Next(ctx) {
		var row struct {
			UserID   string `bson:"user_id"`
			Nickname string `bson:"nickname"`
			FaceURL  string `bson:"face_url"`
		}
		if err := userCur.Decode(&row); err != nil {
			_ = userCur.Close(ctx)
			return nil, err
		}
		if row.UserID == "" {
			continue
		}
		if _, ok := imByID[row.UserID]; !ok {
			imByID[row.UserID] = struct {
				Nickname string `bson:"nickname"`
				FaceURL  string `bson:"face_url"`
			}{row.Nickname, row.FaceURL}
		}
	}
	_ = userCur.Close(ctx)

	imServerUserIDs := make([]string, 0, len(imByID))
	for id := range imByID {
		imServerUserIDs = append(imServerUserIDs, id)
	}

	if len(attrUserIDs) == 0 && len(imServerUserIDs) == 0 {
		return &dto.SearchHierarchyPanelResp{Users: []dto.HierarchyPanelSearchUser{}, Total: 0}, nil
	}

	orConds := make([]bson.M, 0, 2)
	if len(attrUserIDs) > 0 {
		orConds = append(orConds, bson.M{"user_id": bson.M{"$in": attrUserIDs}})
	}
	if len(imServerUserIDs) > 0 {
		orConds = append(orConds, bson.M{"im_server_user_id": bson.M{"$in": imServerUserIDs}})
	}

	match := bson.M{
		"organization_id": organizationID,
		"status":          bson.M{"$ne": chat.OrganizationUserDisableStatus},
		"$or":             orConds,
	}
	if !req.IncludeOrgNodes {
		match["user_type"] = bson.M{"$ne": chat.OrganizationUserTypeOrganization}
	}
	if req.Level > 0 {
		match["level"] = req.Level
	}
	if req.AncestorID != "" {
		if len(req.AncestorID) >= len(OrgRootNodePrefix) && req.AncestorID[:len(OrgRootNodePrefix)] == OrgRootNodePrefix {
			match["level"] = 1
		} else {
			ancestor, err := s.getUserByID(ctx, organizationID, req.AncestorID)
			if err == nil && ancestor.UserType == chat.OrganizationUserTypeOrganization {
				match["level"] = 1
			} else {
				match["ancestor_path"] = req.AncestorID
			}
		}
	}

	forbidden := s.mergeForbiddenImServerUserIDs(ctx)
	if len(forbidden) > 0 {
		ids := make([]string, 0, len(forbidden))
		for id := range forbidden {
			ids = append(ids, id)
		}
		match["im_server_user_id"] = bson.M{"$nin": ids}
	}

	sortField := "created_at"
	if req.SortByField != "" {
		switch req.SortByField {
		case "created_at", "level", "team_size", "direct_downline_count":
			sortField = req.SortByField
		}
	}
	sortOrder := int32(-1)
	if req.SortOrder == "asc" {
		sortOrder = 1
	}

	findOpts := options.Find().
		SetSort(bson.D{{Key: sortField, Value: sortOrder}}).
		SetLimit(searchPanelMaxResults)

	orgCur, err := orgColl.Find(ctx, match, findOpts)
	if err != nil {
		return nil, err
	}
	var orgUsers []*chat.OrganizationUser
	if err = orgCur.All(ctx, &orgUsers); err != nil {
		_ = orgCur.Close(ctx)
		return nil, err
	}
	_ = orgCur.Close(ctx)

	// 当前结果涉及的全部 IM user_id 批量拉取（用于「已注销」过滤 + 展示，避免逐条 FindOne）
	imNeedSeen := make(map[string]struct{}, len(orgUsers))
	imNeed := make([]string, 0, len(orgUsers))
	for _, ou := range orgUsers {
		im := strings.TrimSpace(ou.ImServerUserId)
		if im == "" {
			continue
		}
		if _, ok := imNeedSeen[im]; ok {
			continue
		}
		imNeedSeen[im] = struct{}{}
		imNeed = append(imNeed, im)
	}
	if len(imNeed) > 0 {
		uCur, qErr := userColl.Find(ctx, bson.M{"user_id": bson.M{"$in": imNeed}}, options.Find().SetProjection(bson.M{"user_id": 1, "nickname": 1, "face_url": 1, "_id": 0}))
		if qErr == nil {
			for uCur.Next(ctx) {
				var row struct {
					UserID   string `bson:"user_id"`
					Nickname string `bson:"nickname"`
					FaceURL  string `bson:"face_url"`
				}
				if err := uCur.Decode(&row); err != nil {
					break
				}
				if row.UserID != "" {
					imByID[row.UserID] = struct {
						Nickname string `bson:"nickname"`
						FaceURL  string `bson:"face_url"`
					}{row.Nickname, row.FaceURL}
				}
			}
			_ = uCur.Close(ctx)
		}
	}

	// 仅 attribute 命中但 IM 昵称为「已注销」的剔除（与旧聚合 $nor 一致）
	filtered := orgUsers[:0]
	for _, ou := range orgUsers {
		im := strings.TrimSpace(ou.ImServerUserId)
		if im != "" {
			if u, ok := imByID[im]; ok && strings.TrimSpace(u.Nickname) == imNicknameDeregistered {
				continue
			}
		}
		filtered = append(filtered, ou)
	}
	orgUsers = filtered

	// 批量补全 attribute（仅 im_server_user_id 命中时，第一步 attr 扫描可能未带出行）
	missingAttrIDs := make([]string, 0, len(orgUsers))
	for _, ou := range orgUsers {
		if _, ok := attrByOrgUserID[ou.UserId]; !ok {
			missingAttrIDs = append(missingAttrIDs, ou.UserId)
		}
	}
	if len(missingAttrIDs) > 0 {
		aCur, err := attrColl.Find(ctx, bson.M{"user_id": bson.M{"$in": missingAttrIDs}}, options.Find().SetProjection(bson.M{"user_id": 1, "account": 1, "nickname": 1, "face_url": 1, "_id": 0}))
		if err == nil {
			for aCur.Next(ctx) {
				var row struct {
					UserID   string `bson:"user_id"`
					Account  string `bson:"account"`
					Nickname string `bson:"nickname"`
					FaceURL  string `bson:"face_url"`
				}
				if err := aCur.Decode(&row); err != nil {
					break
				}
				if row.UserID != "" {
					attrByOrgUserID[row.UserID] = struct {
						Account  string `bson:"account"`
						Nickname string `bson:"nickname"`
						FaceURL  string `bson:"face_url"`
					}{row.Account, row.Nickname, row.FaceURL}
				}
			}
			_ = aCur.Close(ctx)
		}
	}

	out := make([]dto.HierarchyPanelSearchUser, 0, len(orgUsers))
	for _, ou := range orgUsers {
		attr := attrByOrgUserID[ou.UserId]
		var nickIM, faceIM string
		if im := strings.TrimSpace(ou.ImServerUserId); im != "" {
			if u, ok := imByID[im]; ok {
				nickIM = strings.TrimSpace(u.Nickname)
				faceIM = strings.TrimSpace(u.FaceURL)
			}
		}

		nick := nickIM
		if nick == "" {
			nick = strings.TrimSpace(attr.Nickname)
		}
		if nick == "" {
			nick = strings.TrimSpace(attr.Account)
		}
		if nick == "" && len(ou.UserId) > 6 {
			nick = "用户" + ou.UserId[len(ou.UserId)-6:]
		} else if nick == "" {
			nick = ou.UserId
		}
		face := faceIM
		if face == "" {
			face = strings.TrimSpace(attr.FaceURL)
		}

		out = append(out, dto.HierarchyPanelSearchUser{
			UserID:              ou.UserId,
			Account:             attr.Account,
			Nickname:            nick,
			FaceURL:             face,
			InvitationCode:      ou.InvitationCode,
			Level:               ou.Level,
			TeamSize:            ou.TeamSize,
			DirectDownlineCount: ou.DirectDownlineCount,
		})
	}

	return &dto.SearchHierarchyPanelResp{Users: out, Total: int64(len(out))}, nil
}
