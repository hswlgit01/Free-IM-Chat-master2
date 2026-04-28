// Copyright © 2023 OpenIM open source community. All rights reserved.

package svc

import (
	"context"
	"strings"

	adminModel "github.com/openimsdk/chat/freechat/apps/admin/model"
	"github.com/openimsdk/chat/freechat/apps/organization/dto"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// imNicknameDeregistered 超管封禁等流程会将 IM 昵称改为该值，视为已注销展示口径。
const imNicknameDeregistered = "已注销"

// hierarchyEffectiveStats 层级管理展示用：team_size / direct_downline_count 排除封禁、组织禁用、已注销（IM 昵称），
// 被封禁节点本身不计入上级人数，但其下级中非封禁成员仍计入（递归仍穿过被封禁节点）。
type hierarchyEffectiveStats struct {
	team       map[string]int
	direct     map[string]int
	excluded   map[string]bool
	excludedIM map[string]struct{}
}

func (st *hierarchyEffectiveStats) applyUser(p *dto.UserHierarchyInfo) {
	if st == nil || p == nil {
		return
	}
	if v, ok := st.team[p.UserID]; ok {
		p.TeamSize = v
	}
	if v, ok := st.direct[p.UserID]; ok {
		p.DirectDownlineCount = v
	}
}

func (st *hierarchyEffectiveStats) applyUserPtr(p *dto.UserHierarchyInfo) {
	if st == nil || p == nil {
		return
	}
	st.applyUser(p)
}

func (st *hierarchyEffectiveStats) applyUserSlice(slice []dto.UserHierarchyInfo) {
	if st == nil {
		return
	}
	for i := range slice {
		st.applyUser(&slice[i])
	}
}

func (st *hierarchyEffectiveStats) applyTreeNode(n *dto.HierarchyTreeNode) {
	if st == nil || n == nil {
		return
	}
	if v, ok := st.team[n.UserID]; ok {
		n.TeamSize = v
	}
	if v, ok := st.direct[n.UserID]; ok {
		n.DirectDownlineCount = v
	}
}

func (st *hierarchyEffectiveStats) applyTreeRecursive(n *dto.HierarchyTreeNode) {
	if st == nil || n == nil {
		return
	}
	st.applyTreeNode(n)
	for _, ch := range n.Children {
		st.applyTreeRecursive(ch)
	}
}

func (st *hierarchyEffectiveStats) totalTeamSizeFor(userID string) int {
	if st == nil {
		return 0
	}
	return st.team[userID]
}

func (st *hierarchyEffectiveStats) directDownlineCountFor(userID string) int {
	if st == nil {
		return 0
	}
	return st.direct[userID]
}

func (st *hierarchyEffectiveStats) isExcludedUserID(userID string) bool {
	if st == nil {
		return false
	}
	return st.excluded[userID]
}

func (st *hierarchyEffectiveStats) excludedImServerUserIDs() []string {
	if st == nil || len(st.excludedIM) == 0 {
		return nil
	}
	ids := make([]string, 0, len(st.excludedIM))
	for id := range st.excludedIM {
		if id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// loadHierarchyEffectiveStats 按组织拉全量 organization_user 后在内存中重算有效团队人数。
func (s *HierarchyService) loadHierarchyEffectiveStats(ctx context.Context, orgID primitive.ObjectID) (*hierarchyEffectiveStats, error) {
	coll := s.db.Collection(chat.OrganizationUser{}.TableName())
	cur, err := coll.Find(ctx, bson.M{"organization_id": orgID}, options.Find())
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var all []*chat.OrganizationUser
	if err := cur.All(ctx, &all); err != nil { // todo  不需要把所有的字段都查询出来. 不需要全量的数据
		return nil, err
	}

	orgRootID := ""
	for _, ou := range all {
		if ou.UserType == chat.OrganizationUserTypeOrganization {
			orgRootID = ou.UserId
			break
		}
	}

	forbiddenIm := make(map[string]struct{})
	faDao := chatModel.NewForbiddenAccountDao(s.db)
	if ids, err := faDao.FindAllIDs(ctx); err != nil {
		log.ZWarn(ctx, "forbidden_account 列表加载失败，层级有效人数可能偏大", err)
	} else {
		for _, id := range ids {
			if id != "" {
				forbiddenIm[id] = struct{}{}
			}
		}
	}

	saDao := adminModel.NewSuperAdminForbiddenDao(s.db)
	if saIDs, err := saDao.DistinctForbiddenImServerUserIDs(ctx); err != nil {
		log.ZWarn(ctx, "super_admin_forbidden_detail 加载失败，层级有效人数可能偏大", err)
	} else {
		for _, id := range saIDs {
			if id != "" {
				forbiddenIm[id] = struct{}{}
			}
		}
	}

	imIDs := make([]string, 0, len(all))
	seenIM := make(map[string]struct{}, len(all))
	for _, ou := range all {
		im := strings.TrimSpace(ou.ImServerUserId)
		if im == "" {
			continue
		}
		if _, ok := seenIM[im]; ok {
			continue
		}
		seenIM[im] = struct{}{}
		imIDs = append(imIDs, im)
	}

	nickByIm := make(map[string]string, len(imIDs))
	if len(imIDs) > 0 {
		userColl := s.db.Collection(openImModel.User{}.TableName())
		uCur, qErr := userColl.Find(ctx, bson.M{"user_id": bson.M{"$in": imIDs}}, options.Find().SetProjection(bson.M{"user_id": 1, "nickname": 1, "_id": 0}))
		if qErr != nil {
			log.ZWarn(ctx, "OpenIM user 昵称批量加载失败，已注销判断可能不完整", qErr)
		} else {
			defer uCur.Close(ctx)
			for uCur.Next(ctx) {
				var row struct {
					UserID   string `bson:"user_id"`
					Nickname string `bson:"nickname"`
				}
				if err := uCur.Decode(&row); err != nil {
					continue
				}
				nickByIm[row.UserID] = row.Nickname
			}
		}
	}

	excluded := make(map[string]bool, len(all))
	excludedIM := make(map[string]struct{})
	for _, ou := range all {
		if ou.UserType == chat.OrganizationUserTypeOrganization {
			continue
		}
		if ou.Status == chat.OrganizationUserDisableStatus {
			excluded[ou.UserId] = true
			continue
		}
		im := strings.TrimSpace(ou.ImServerUserId)
		if im != "" {
			if _, ok := forbiddenIm[im]; ok {
				excluded[ou.UserId] = true
				excludedIM[im] = struct{}{}
				continue
			}
			if strings.TrimSpace(nickByIm[im]) == imNicknameDeregistered {
				excluded[ou.UserId] = true
				excludedIM[im] = struct{}{}
			}
		}
	}

	children := make(map[string][]string)
	for _, ou := range all {
		if ou.UserType == chat.OrganizationUserTypeOrganization {
			continue
		}
		parent := strings.TrimSpace(ou.Level1Parent)
		if parent == "" && ou.Level == 1 && orgRootID != "" {
			parent = orgRootID
		}
		if parent != "" {
			children[parent] = append(children[parent], ou.UserId)
		}
	}

	memoTeam := make(map[string]int)
	var teamFn func(uid string) int
	teamFn = func(uid string) int {
		if v, ok := memoTeam[uid]; ok {
			return v
		}
		sum := 0
		for _, cid := range children[uid] {
			if !excluded[cid] {
				sum++
			}
			sum += teamFn(cid)
		}
		memoTeam[uid] = sum
		return sum
	}

	teamMap := make(map[string]int, len(all))
	directMap := make(map[string]int, len(all))
	for _, ou := range all {
		uid := ou.UserId
		n := 0
		for _, cid := range children[uid] {
			if !excluded[cid] {
				n++
			}
		}
		directMap[uid] = n
		teamMap[uid] = teamFn(uid)
	}

	return &hierarchyEffectiveStats{team: teamMap, direct: directMap, excluded: excluded, excludedIM: excludedIM}, nil
}

func (s *HierarchyService) tryHierarchyEffectiveStats(ctx context.Context, orgID primitive.ObjectID) *hierarchyEffectiveStats {
	st, err := s.loadHierarchyEffectiveStats(ctx, orgID)
	if err != nil {
		log.ZWarn(ctx, "层级有效成员统计失败，沿用库内 team_size/direct_downline_count", err, "orgID", orgID.Hex())
		return nil
	}
	return st
}
