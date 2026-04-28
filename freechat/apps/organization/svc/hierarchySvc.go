// Copyright © 2023 OpenIM open source community. All rights reserved.

package svc

import (
	"context"
	"errors"
	"time"

	"github.com/openimsdk/chat/freechat/apps/organization/dto"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// HierarchyService handles business logic for user hierarchy relationships
type HierarchyService struct {
	db *mongo.Database
}

// NewHierarchyService creates a new hierarchy service instance
func NewHierarchyService(db *mongo.Database) *HierarchyService {
	return &HierarchyService{
		db: db,
	}
}

// attachForbiddenImNin 在层级列表查询中排除 forbidden_account / 超管封禁中的 IM 账号（与 search_panel、effectiveStats 口径一致）。
func (s *HierarchyService) attachForbiddenImNin(filter bson.M, forbidden map[string]struct{}) {
	if len(forbidden) == 0 {
		return
	}
	ids := make([]string, 0, len(forbidden))
	for id := range forbidden {
		if id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return
	}
	filter["im_server_user_id"] = bson.M{"$nin": ids}
}

func mergeStringNin(filter bson.M, field string, ids []string) {
	if len(ids) == 0 {
		return
	}
	seen := make(map[string]struct{}, len(ids))
	merged := make([]string, 0, len(ids))

	add := func(v string) {
		if v == "" {
			return
		}
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		merged = append(merged, v)
	}

	if cond, ok := filter[field].(bson.M); ok {
		switch old := cond["$nin"].(type) {
		case []string:
			for _, v := range old {
				add(v)
			}
		case []interface{}:
			for _, v := range old {
				if s, ok := v.(string); ok {
					add(s)
				}
			}
		case primitive.A:
			for _, v := range old {
				if s, ok := v.(string); ok {
					add(s)
				}
			}
		}
		for _, v := range ids {
			add(v)
		}
		cond["$nin"] = merged
		filter[field] = cond
		return
	}

	for _, v := range ids {
		add(v)
	}
	filter[field] = bson.M{"$nin": merged}
}

func (s *HierarchyService) applyHierarchyVisibilityFilter(filter bson.M, st *hierarchyEffectiveStats, forbidden map[string]struct{}) {
	filter["status"] = bson.M{"$ne": chat.OrganizationUserDisableStatus}
	if st != nil {
		mergeStringNin(filter, "im_server_user_id", st.excludedImServerUserIDs())
		return
	}
	s.attachForbiddenImNin(filter, forbidden)
}

// GetHierarchyTree returns a tree structure of users based on their hierarchy relationships
func (s *HierarchyService) GetHierarchyTree(ctx context.Context, organizationID primitive.ObjectID, rootUserID string, maxDepth int) (*dto.HierarchyTreeResp, error) {
	var rootUser *chat.OrganizationUser
	var err error

	// If no rootUserID specified, use the organization's virtual root node
	if rootUserID == "" {
		// Get or create the organization root node
		rootUser, err = s.getOrCreateOrgRootNode(ctx, organizationID)
		if err != nil {
			log.ZError(ctx, "Failed to get or create organization root node", err,
				"organizationID", organizationID.Hex())
			return nil, err
		}
	} else {
		// Find the specified root user
		rootUser, err = s.getUserByID(ctx, organizationID, rootUserID)
		if err != nil {
			return nil, err
		}
	}

	// Convert to tree node
	account, nickname, faceURL := s.getUserInfo(ctx, rootUser)
	rootNode := &dto.HierarchyTreeNode{
		UserID:              rootUser.UserId,
		Account:             account,
		Nickname:            nickname,
		FaceURL:             faceURL,
		Level:               rootUser.Level,
		TeamSize:            rootUser.TeamSize,
		DirectDownlineCount: rootUser.DirectDownlineCount,
		Children:            []*dto.HierarchyTreeNode{},
		HasMoreChildren:     false,
		UserType:            string(rootUser.UserType),
	}

	st := s.tryHierarchyEffectiveStats(ctx, organizationID)

	// If maxDepth is 0 or positive, build the tree recursively
	if maxDepth != 0 {
		forbiddenIm := s.mergeForbiddenImServerUserIDs(ctx)
		err = s.buildTreeRecursively(ctx, organizationID, rootNode, maxDepth, 1, forbiddenIm, st)
		if err != nil {
			return nil, err
		}
	}

	if st != nil {
		st.applyTreeRecursive(rootNode)
	}

	return &dto.HierarchyTreeResp{
		Root: rootNode,
	}, nil
}

// GetHierarchyTreeRootSummary 仅返回组织虚拟根节点摘要（与完整 tree 中 root 字段结构一致），
// 不递归子节点、不做全量有效人数重算（team_size/direct_downline_count 直接来自 organization_user 文档）。
func (s *HierarchyService) GetHierarchyTreeRootSummary(ctx context.Context, organizationID primitive.ObjectID) (*dto.HierarchyTreeResp, error) {
	rootUser, err := s.getOrCreateOrgRootNode(ctx, organizationID)
	if err != nil {
		return nil, err
	}

	account, nickname, faceURL := s.getUserInfo(ctx, rootUser)

	st := s.tryHierarchyEffectiveStats(ctx, organizationID)
	collection := s.db.Collection("organization_user")
	childFilter := bson.M{
		"organization_id": organizationID,
		"level":           1,
	}
	s.applyHierarchyVisibilityFilter(childFilter, st, s.mergeForbiddenImServerUserIDs(ctx))
	childCount, err := collection.CountDocuments(ctx, childFilter)
	if err != nil {
		return nil, err
	}

	rootNode := &dto.HierarchyTreeNode{
		UserID:              rootUser.UserId,
		Account:             account,
		Nickname:            nickname,
		FaceURL:             faceURL,
		Level:               rootUser.Level,
		TeamSize:            rootUser.TeamSize,
		DirectDownlineCount: rootUser.DirectDownlineCount,
		Children:            nil,
		HasMoreChildren:     childCount > 0,
		UserType:            string(rootUser.UserType),
	}
	if st != nil {
		st.applyTreeNode(rootNode)
		childCount = int64(st.directDownlineCountFor(rootUser.UserId))
		rootNode.HasMoreChildren = childCount > 0
	}

	return &dto.HierarchyTreeResp{Root: rootNode}, nil
}

// buildTreeRecursively builds the hierarchy tree recursively
func (s *HierarchyService) buildTreeRecursively(ctx context.Context, organizationID primitive.ObjectID, node *dto.HierarchyTreeNode, maxDepth, currentDepth int, forbiddenIm map[string]struct{}, st *hierarchyEffectiveStats) error {
	// 检查是否是组织根节点（检查UserType字段和OrgRootNode前缀）
	isOrgRootNode := node.UserType == string(chat.OrganizationUserTypeOrganization) ||
		(len(node.UserID) >= len(OrgRootNodePrefix) && node.UserID[:len(OrgRootNodePrefix)] == OrgRootNodePrefix)

	// 如果是组织根节点，查找所有level=1的用户（无上级的用户）
	filter := bson.M{
		"organization_id": organizationID,
	}

	if isOrgRootNode {
		// 对于组织根节点，查找所有level=1的用户
		filter["level"] = 1
	} else {
		// 对于普通用户节点，查找所有level1_parent=node.UserID的用户
		filter["level1_parent"] = node.UserID
	}
	s.applyHierarchyVisibilityFilter(filter, st, forbiddenIm)

	// 如果已经达到最大深度，只检查是否还有更多子节点
	if maxDepth > 0 && currentDepth >= maxDepth {
		collection := s.db.Collection("organization_user")
		count, err := collection.CountDocuments(ctx, filter)
		if err != nil {
			return err
		}

		node.HasMoreChildren = count > 0
		return nil
	}

	// 限制每个节点最多100个子节点，以保证性能
	var children []*chat.OrganizationUser
	collection := s.db.Collection("organization_user")
	cursor, err := collection.Find(ctx, filter, &options.FindOptions{
		Limit: toPtr(int64(100)),
		Sort:  bson.D{{Key: "created_at", Value: 1}}, // 按创建时间升序排序（越早注册的在上方）
	})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	if err := cursor.All(ctx, &children); err != nil {
		return err
	}

	// 如果子节点数量达到了限制值，设置hasMoreChildren标志
	node.HasMoreChildren = len(children) >= 100

	// 处理每个子节点
	for _, child := range children {
		if st != nil && st.isExcludedUserID(child.UserId) {
			continue
		}
		account, nickname, faceURL := s.getUserInfo(ctx, child)
		childNode := &dto.HierarchyTreeNode{
			UserID:              child.UserId,
			Account:             account,
			Nickname:            nickname,
			FaceURL:             faceURL,
			Level:               child.Level,
			TeamSize:            child.TeamSize,
			DirectDownlineCount: child.DirectDownlineCount,
			Children:            []*dto.HierarchyTreeNode{},
			HasMoreChildren:     false,
			UserType:            string(child.UserType),
		}

		// 递归构建子节点的子树
		err = s.buildTreeRecursively(ctx, organizationID, childNode, maxDepth, currentDepth+1, forbiddenIm, st)
		if err != nil {
			return err
		}

		// 将子节点添加到父节点
		node.Children = append(node.Children, childNode)
	}

	return nil
}

// GetHierarchyChildren returns the direct children of a user in the hierarchy
func (s *HierarchyService) GetHierarchyChildren(ctx context.Context, organizationID primitive.ObjectID, parentUserID string, pagination *paginationUtils.DepPagination) (*dto.HierarchyChildrenResp, error) {
	// 检查是否是组织根节点
	isOrgRootNode := false
	if parentUserID != "" {
		if len(parentUserID) >= len(OrgRootNodePrefix) && parentUserID[:len(OrgRootNodePrefix)] == OrgRootNodePrefix {
			isOrgRootNode = true
		} else {
			// 查询父节点，检查是否是组织节点
			parent, err := s.getUserByID(ctx, organizationID, parentUserID)
			if err == nil && parent.UserType == chat.OrganizationUserTypeOrganization {
				isOrgRootNode = true
			}
		}
	}

	// 构建基础过滤条件
	filter := bson.M{
		"organization_id": organizationID,
	}

	if isOrgRootNode {
		// 对于组织根节点，查询level=1的用户
		filter["level"] = 1
	} else {
		// 对于普通用户，查询直接下级
		filter["level1_parent"] = parentUserID
	}
	st := s.tryHierarchyEffectiveStats(ctx, organizationID)
	forbiddenIm := s.mergeForbiddenImServerUserIDs(ctx)
	s.applyHierarchyVisibilityFilter(filter, st, forbiddenIm)

	collection := s.db.Collection("organization_user")

	// 公共管道：先按组织/层级过滤，再关联 attribute 拿账号/昵称/头像
	commonPipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: filter}},
		bson.D{{Key: "$lookup", Value: bson.M{
			"from":         "attribute",
			"localField":   "user_id",
			"foreignField": "user_id",
			"as":           "attr",
		}}},
		bson.D{{Key: "$unwind", Value: bson.M{
			"path":                       "$attr",
			"preserveNullAndEmptyArrays": true,
		}}},
	}

	// 1. 统计总数
	countPipeline := append(commonPipeline,
		bson.D{{Key: "$count", Value: "total"}},
	)

	var total int64
	countCursor, err := collection.Aggregate(ctx, countPipeline)
	if err != nil {
		return nil, err
	}
	var countResult []struct {
		Total int64 `bson:"total"`
	}
	if err = countCursor.All(ctx, &countResult); err != nil {
		return nil, err
	}
	if len(countResult) > 0 {
		total = countResult[0].Total
	}

	// 若无子节点，直接返回
	if total == 0 {
		return &dto.HierarchyChildrenResp{
			Children: []dto.UserHierarchyInfo{},
			Total:    0,
		}, nil
	}

	// 2. 查询当前页数据
	showNumber := pagination.GetShowNumber()
	pageNumber := pagination.GetPageNumber()

	// 注意：旧实现中，当前接口在很多地方是“前端自行分页”，当 pageSize 为空或 0 时，
	// 后端会返回全部子节点，然后前端再做本地分页。为了保持兼容：
	// - 当 showNumber <= 0 时，不在管道中添加 $skip/$limit，只按 created_at 排序返回全部结果；
	// - 只有当前端明确传入 pageSize>0 时，才真正启用后端分页。

	dataPipeline := append(commonPipeline,
		bson.D{{Key: "$sort", Value: bson.D{{Key: "created_at", Value: 1}}}}, // 按创建时间升序
	)

	if showNumber > 0 {
		skip := int64((pageNumber - 1) * showNumber)
		limit := int64(showNumber)
		if limit < 1 {
			limit = 1
		}
		dataPipeline = append(dataPipeline,
			bson.D{{Key: "$skip", Value: skip}},
			bson.D{{Key: "$limit", Value: limit}},
		)
	}

	dataPipeline = append(dataPipeline,
		bson.D{{Key: "$project", Value: bson.M{
			"user_id":               1,
			"im_server_user_id":     1,
			"level":                 1,
			"invitation_code":       1,
			"team_size":             1,
			"direct_downline_count": 1,
			"ancestor_path":         1,
			"created_at":            1,
			"user_type":             1,
			"account":               "$attr.account",
			"nickname":              "$attr.nickname",
			"face_url":              "$attr.face_url",
		}}},
	)

	cursor, err := collection.Aggregate(ctx, dataPipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var docs []struct {
		UserID              string    `bson:"user_id"`
		ImServerUserId      string    `bson:"im_server_user_id"`
		Level               int       `bson:"level"`
		InvitationCode      string    `bson:"invitation_code"`
		TeamSize            int       `bson:"team_size"`
		DirectDownlineCount int       `bson:"direct_downline_count"`
		AncestorPath        []string  `bson:"ancestor_path"`
		CreatedAt           time.Time `bson:"created_at"`
		UserType            string    `bson:"user_type"`
		Account             string    `bson:"account"`
		Nickname            string    `bson:"nickname"`
		FaceURL             string    `bson:"face_url"`
	}
	if err = cursor.All(ctx, &docs); err != nil {
		return nil, err
	}

	// 3. 对 attribute 中昵称为空的用户，从 user 表批量补查昵称（与 getUserInfo 逻辑一致）
	userCollection := s.db.Collection("user")
	needNicknameImIDs := make([]string, 0, len(docs))
	for _, d := range docs {
		if d.Nickname == "" && d.ImServerUserId != "" {
			needNicknameImIDs = append(needNicknameImIDs, d.ImServerUserId)
		}
	}
	imIDToNickname := make(map[string]string)
	if len(needNicknameImIDs) > 0 {
		userCursor, qErr := userCollection.Find(ctx, bson.M{"user_id": bson.M{"$in": needNicknameImIDs}})
		if qErr == nil {
			defer userCursor.Close(ctx)
			var userList []struct {
				UserID   string `bson:"user_id"`
				Nickname string `bson:"nickname"`
			}
			if qErr = userCursor.All(ctx, &userList); qErr == nil {
				for _, u := range userList {
					if u.Nickname != "" {
						imIDToNickname[u.UserID] = u.Nickname
					}
				}
			}
		}
	}

	// 4. 组装响应：优先使用 attribute 昵称，为空时用 user 表昵称，再空才用 account/用户+id 兜底
	result := &dto.HierarchyChildrenResp{
		Children: make([]dto.UserHierarchyInfo, 0, len(docs)),
		Total:    total,
	}
	for _, d := range docs {
		account := d.Account
		nickname := d.Nickname
		if nickname == "" && d.ImServerUserId != "" {
			if n, ok := imIDToNickname[d.ImServerUserId]; ok && n != "" {
				nickname = n
			}
		}
		if nickname == "" {
			if account != "" {
				nickname = account
			} else if len(d.UserID) > 6 {
				nickname = "用户" + d.UserID[len(d.UserID)-6:]
			} else {
				nickname = d.UserID
			}
		}

		result.Children = append(result.Children, dto.UserHierarchyInfo{
			UserID:              d.UserID,
			Account:             account,
			Nickname:            nickname,
			FaceURL:             d.FaceURL,
			Level:               d.Level,
			InvitationCode:      d.InvitationCode,
			TeamSize:            d.TeamSize,
			DirectDownlineCount: d.DirectDownlineCount,
			AncestorPath:        d.AncestorPath,
			CreatedAt:           d.CreatedAt,
			UserType:            d.UserType,
		})
	}

	if st != nil {
		st.applyUserSlice(result.Children)
		result.Total = int64(st.directDownlineCountFor(parentUserID))
	}

	return result, nil
}

// GetHierarchyDetail returns detailed information about a user in the hierarchy
func (s *HierarchyService) GetHierarchyDetail(ctx context.Context, organizationID primitive.ObjectID, userID string) (*dto.HierarchyDetailResp, error) {
	// 查找用户
	user, err := s.getUserByID(ctx, organizationID, userID)
	if err != nil {
		return nil, err
	}

	// 创建响应对象，使用新的getUserInfo函数获取分离的账号、昵称和头像
	account, nickname, faceURL := s.getUserInfo(ctx, user)

	// 创建用户信息对象
	userHierarchyInfo := dto.UserHierarchyInfo{
		UserID:              user.UserId,
		Account:             account,  // 设置账号
		Nickname:            nickname, // 设置昵称
		FaceURL:             faceURL,
		Level:               user.Level,
		InvitationCode:      user.InvitationCode,
		TeamSize:            user.TeamSize,
		DirectDownlineCount: user.DirectDownlineCount,
		AncestorPath:        user.AncestorPath,
		CreatedAt:           user.CreatedAt,
		UserType:            string(user.UserType),
		AncestorInfoList:    []dto.AncestorInfo{},
	}

	// 获取所有祖先节点的信息
	// 1. 先检查ancestor_path是否完整（应该包含所有上级直到顶层）
	// 针对ancestor_path可能只包含直接上级和根节点的问题，重新构建完整的上级路径

	// 完整的上级路径应当等于用户级别 - 1，如用户级别为3，应有2个上级
	ancestorInfoList := make([]dto.AncestorInfo, 0)

	// 使用递归方式查找所有上级，从直接父级开始向上追溯
	if user.Level1Parent != "" {
		// 创建一个追踪已访问节点的映射，防止循环依赖
		visitedNodes := make(map[string]bool)

		// 获取完整的祖先路径
		err = s.buildAncestorListRecursively(ctx, organizationID, user.Level1Parent, user.Level-1, &ancestorInfoList, visitedNodes)
		if err != nil {
			log.ZWarn(ctx, "Error building ancestor list", err, "user_id", user.UserId)
			// 错误不中断流程，继续尝试使用ancestor_path
		}
	}

	// 2. 回退方案：如果上面的方法无法获取完整路径，则使用ancestor_path
	if len(ancestorInfoList) == 0 && len(user.AncestorPath) > 0 {
		// 从上向下收集祖先信息（最顶层祖先可能在前，也可能在后）
		for i, ancestorID := range user.AncestorPath {
			// 查询祖先信息
			ancestor, err := s.getUserByID(ctx, organizationID, ancestorID)
			if err == nil {
				// 计算实际层级 - 祖先路径的索引与层级的关系
				// 当前用户的层级 - (祖先路径长度 - 当前祖先索引)
				// 例如：用户层级为5，共3个祖先，第一个祖先(index=0)的层级 = 5 - (3 - 0) = 2
				actualLevel := user.Level - (len(user.AncestorPath) - i)

				// 获取祖先的账号和昵称
				ancestorAccount, ancestorNickname, _ := s.getUserInfo(ctx, ancestor)

				// 添加到祖先信息列表
				ancestorInfoList = append(ancestorInfoList, dto.AncestorInfo{
					UserID:   ancestorID,
					Account:  ancestorAccount,
					Nickname: ancestorNickname,
					Level:    actualLevel,
				})
			}
		}
	}

	// 确保祖先列表是按照层级顺序的（从顶层到当前用户的直接父级）
	// 对祖先列表按照层级从小到大排序（从顶级节点开始）
	if len(ancestorInfoList) > 1 {
		// 实现简单的冒泡排序
		for i := 0; i < len(ancestorInfoList)-1; i++ {
			for j := i + 1; j < len(ancestorInfoList); j++ {
				if ancestorInfoList[i].Level > ancestorInfoList[j].Level {
					// 交换位置，确保层级小的在前面
					ancestorInfoList[i], ancestorInfoList[j] = ancestorInfoList[j], ancestorInfoList[i]
				}
			}
		}
	}

	// 修复层级值，确保最小层级至少为1
	// 如果发现有负数或0层级，整体调整所有层级值
	hasInvalidLevel := false
	minLevel := 999
	for _, ancestor := range ancestorInfoList {
		if ancestor.Level <= 0 {
			hasInvalidLevel = true
		}
		if ancestor.Level < minLevel {
			minLevel = ancestor.Level
		}
	}

	// 如果有非正数层级，调整所有层级值
	if hasInvalidLevel && len(ancestorInfoList) > 0 {
		// 计算需要增加的偏移量
		offset := 1 - minLevel // 确保最小层级变为1
		for i := range ancestorInfoList {
			ancestorInfoList[i].Level += offset
		}
	}

	// 将祖先信息添加到用户信息中
	userHierarchyInfo.AncestorInfoList = ancestorInfoList

	result := &dto.HierarchyDetailResp{
		User:           userHierarchyInfo,
		DirectChildren: []dto.UserHierarchyInfo{},
		TotalChildren:  user.TeamSize,
	}

	// 查找父节点信息（level1, level2, level3）
	// 如果当前节点是level=1的用户，其上级应当是组织根节点
	if user.Level == 1 {
		// 获取组织根节点
		orgRoot, err := s.getOrCreateOrgRootNode(ctx, organizationID)
		if err == nil {
			account, nickname, faceURL := s.getUserInfo(ctx, orgRoot)
			parentInfo := &dto.UserHierarchyInfo{
				UserID:              orgRoot.UserId,
				Account:             account,
				Nickname:            nickname,
				FaceURL:             faceURL,
				Level:               orgRoot.Level,
				InvitationCode:      orgRoot.InvitationCode,
				TeamSize:            orgRoot.TeamSize,
				DirectDownlineCount: orgRoot.DirectDownlineCount,
				AncestorPath:        orgRoot.AncestorPath,
				CreatedAt:           orgRoot.CreatedAt,
				UserType:            string(orgRoot.UserType),
			}
			result.Level1Parent = parentInfo
		}
	} else if user.Level1Parent != "" {
		// 正常查询上级
		parent, err := s.getUserByID(ctx, organizationID, user.Level1Parent)
		if err == nil {
			account, nickname, faceURL := s.getUserInfo(ctx, parent)
			parentInfo := &dto.UserHierarchyInfo{
				UserID:              parent.UserId,
				Account:             account,
				Nickname:            nickname,
				FaceURL:             faceURL,
				Level:               parent.Level,
				InvitationCode:      parent.InvitationCode,
				TeamSize:            parent.TeamSize,
				DirectDownlineCount: parent.DirectDownlineCount,
				AncestorPath:        parent.AncestorPath,
				CreatedAt:           parent.CreatedAt,
				UserType:            string(parent.UserType),
			}
			result.Level1Parent = parentInfo
		}
	}

	if user.Level2Parent != "" {
		parent, err := s.getUserByID(ctx, organizationID, user.Level2Parent)
		if err == nil {
			account, nickname, faceURL := s.getUserInfo(ctx, parent)
			parentInfo := &dto.UserHierarchyInfo{
				UserID:              parent.UserId,
				Account:             account,
				Nickname:            nickname,
				FaceURL:             faceURL,
				Level:               parent.Level,
				InvitationCode:      parent.InvitationCode,
				TeamSize:            parent.TeamSize,
				DirectDownlineCount: parent.DirectDownlineCount,
				AncestorPath:        parent.AncestorPath,
				CreatedAt:           parent.CreatedAt,
				UserType:            string(parent.UserType),
			}
			result.Level2Parent = parentInfo
		}
	}

	if user.Level3Parent != "" {
		parent, err := s.getUserByID(ctx, organizationID, user.Level3Parent)
		if err == nil {
			account, nickname, faceURL := s.getUserInfo(ctx, parent)
			parentInfo := &dto.UserHierarchyInfo{
				UserID:              parent.UserId,
				Account:             account,
				Nickname:            nickname,
				FaceURL:             faceURL,
				Level:               parent.Level,
				InvitationCode:      parent.InvitationCode,
				TeamSize:            parent.TeamSize,
				DirectDownlineCount: parent.DirectDownlineCount,
				AncestorPath:        parent.AncestorPath,
				CreatedAt:           parent.CreatedAt,
				UserType:            string(parent.UserType),
			}
			result.Level3Parent = parentInfo
		}
	}

	// 查找直接下级
	// 如果是组织节点，查找level=1的用户
	filter := bson.M{
		"organization_id": organizationID,
	}

	if user.UserType == chat.OrganizationUserTypeOrganization {
		filter["level"] = 1
	} else {
		filter["level1_parent"] = userID
	}
	st := s.tryHierarchyEffectiveStats(ctx, organizationID)
	s.applyHierarchyVisibilityFilter(filter, st, s.mergeForbiddenImServerUserIDs(ctx))

	// 限制返回10个直接下级用户
	collection := s.db.Collection("organization_user")
	cursor, err := collection.Find(ctx, filter, &options.FindOptions{
		Limit: toPtr(int64(10)),
		Sort:  bson.D{{Key: "created_at", Value: 1}}, // 按创建时间升序排序（越早注册的在上方）
	})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var children []*chat.OrganizationUser
	if err := cursor.All(ctx, &children); err != nil {
		return nil, err
	}

	// 添加子节点到结果
	for _, child := range children {
		account, nickname, faceURL := s.getUserInfo(ctx, child)
		result.DirectChildren = append(result.DirectChildren, dto.UserHierarchyInfo{
			UserID:              child.UserId,
			Account:             account,
			Nickname:            nickname,
			FaceURL:             faceURL,
			Level:               child.Level,
			InvitationCode:      child.InvitationCode,
			TeamSize:            child.TeamSize,
			DirectDownlineCount: child.DirectDownlineCount,
			AncestorPath:        child.AncestorPath,
			CreatedAt:           child.CreatedAt,
			UserType:            string(child.UserType),
		})
	}

	if st != nil {
		st.applyUser(&result.User)
		st.applyUserPtr(result.Level1Parent)
		st.applyUserPtr(result.Level2Parent)
		st.applyUserPtr(result.Level3Parent)
		st.applyUserSlice(result.DirectChildren)
		result.TotalChildren = st.totalTeamSizeFor(result.User.UserID)
	}

	return result, nil
}

// SearchHierarchy searches for users in the hierarchy based on criteria
func (s *HierarchyService) SearchHierarchy(ctx context.Context, organizationID primitive.ObjectID, req *dto.SearchHierarchyReq, pagination *paginationUtils.DepPagination) (*dto.SearchHierarchyResp, error) {
	// 构建搜索过滤条件
	filter := bson.M{
		"organization_id": organizationID,
	}

	// 默认排除组织虚拟节点，除非明确指定搜索所有类型
	if req.IncludeOrgNodes {
		// 不过滤节点类型，可以搜索到组织虚拟节点
	} else {
		// 默认只搜索普通用户节点
		filter["user_type"] = bson.M{
			"$ne": chat.OrganizationUserTypeOrganization,
		}
	}

	// 添加关键词搜索
	if req.Keyword != "" {
		filter["$or"] = []bson.M{
			{"user_id": bson.M{"$regex": req.Keyword, "$options": "i"}},
			{"nickname": bson.M{"$regex": req.Keyword, "$options": "i"}},
			{"invitation_code": bson.M{"$regex": req.Keyword, "$options": "i"}},
		}
	}

	// 添加层级过滤
	if req.Level > 0 {
		filter["level"] = req.Level
	}

	// 添加上级节点过滤
	if req.AncestorID != "" {
		// 如果是组织根节点ID，则搜索所有level=1的用户
		if len(req.AncestorID) >= len(OrgRootNodePrefix) && req.AncestorID[:len(OrgRootNodePrefix)] == OrgRootNodePrefix {
			filter["level"] = 1
		} else {
			// 检查指定的上级是否是组织节点
			ancestor, err := s.getUserByID(ctx, organizationID, req.AncestorID)
			if err == nil && ancestor.UserType == chat.OrganizationUserTypeOrganization {
				filter["level"] = 1
			} else {
				// 普通用户上级，使用ancestor_path
				filter["ancestor_path"] = req.AncestorID
			}
		}
	}

	// 获取总数
	collection := s.db.Collection("organization_user")
	total, err := collection.CountDocuments(ctx, filter)
	if err != nil {
		return nil, err
	}

	// 准备分页
	skip := int64((pagination.GetPageNumber() - 1) * pagination.GetShowNumber())
	limit := int64(pagination.GetShowNumber())

	// 准备排序选项
	sortField := "created_at" // 默认排序字段
	if req.SortByField != "" {
		// 只允许特定字段进行排序
		switch req.SortByField {
		case "created_at", "level", "team_size", "direct_downline_count":
			sortField = req.SortByField
		}
	}

	sortOrder := 1 // 默认升序（按创建时间从早到晚）
	if req.SortOrder == "desc" {
		sortOrder = -1
	}

	// 查询用户并应用分页和排序
	cursor, err := collection.Find(ctx, filter, &options.FindOptions{
		Skip:  &skip,
		Limit: &limit,
		Sort:  bson.D{{Key: sortField, Value: sortOrder}},
	})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var users []*chat.OrganizationUser
	if err := cursor.All(ctx, &users); err != nil {
		return nil, err
	}

	// 转换为响应格式
	result := &dto.SearchHierarchyResp{
		Users: make([]dto.UserHierarchyInfo, 0, len(users)),
		Total: total,
	}

	// 优化：预先收集所有用户ID，以便批量查询attribute和user表
	var userIDs []string
	var imServerUserIDs []string
	userIDToIndex := make(map[string]int)

	// 第一步：收集所有ID和建立映射
	for i, user := range users {
		userIDs = append(userIDs, user.UserId)
		if user.ImServerUserId != "" {
			imServerUserIDs = append(imServerUserIDs, user.ImServerUserId)
		}
		userIDToIndex[user.UserId] = i

		// 创建默认结果项
		result.Users = append(result.Users, dto.UserHierarchyInfo{
			UserID:              user.UserId,
			Account:             "",
			Nickname:            "",
			FaceURL:             "",
			Level:               user.Level,
			InvitationCode:      user.InvitationCode,
			TeamSize:            user.TeamSize,
			DirectDownlineCount: user.DirectDownlineCount,
			AncestorPath:        user.AncestorPath,
			CreatedAt:           user.CreatedAt,
			UserType:            string(user.UserType),
		})
	}

	// 第二步：批量查询attribute表获取账号和昵称
	if len(userIDs) > 0 {
		attributeCollection := s.db.Collection("attribute")
		cursor, err := attributeCollection.Find(ctx, bson.M{"user_id": bson.M{"$in": userIDs}})
		if err == nil {
			defer cursor.Close(ctx)

			var attributes []struct {
				UserID   string `bson:"user_id"`
				Account  string `bson:"account"`
				Nickname string `bson:"nickname"`
				FaceURL  string `bson:"face_url"`
			}

			if err := cursor.All(ctx, &attributes); err == nil {
				for _, attr := range attributes {
					if idx, ok := userIDToIndex[attr.UserID]; ok {
						result.Users[idx].Account = attr.Account
						result.Users[idx].Nickname = attr.Nickname
						if attr.FaceURL != "" {
							result.Users[idx].FaceURL = attr.FaceURL
						}
					}
				}
			}
		}
	}

	// 第三步：对于nickname仍为空的用户，从user表批量查询
	// 优化: 只查询真正需要的用户，而不是所有有ImServerUserId的用户
	if len(imServerUserIDs) > 0 {
		// 构建需要查询的ImServerUserId列表和映射关系
		var neededImServerIDs []string
		imServerIDToUserID := make(map[string]string)
		imServerIDToIdx := make(map[string]int)

		// 只为nickname为空的用户构建查询
		for i, user := range users {
			if user.ImServerUserId != "" && result.Users[i].Nickname == "" {
				neededImServerIDs = append(neededImServerIDs, user.ImServerUserId)
				imServerIDToUserID[user.ImServerUserId] = user.UserId
				imServerIDToIdx[user.ImServerUserId] = i
			}
		}

		// 只在有需要查询的ID时执行查询
		if len(neededImServerIDs) > 0 {
			userCollection := s.db.Collection("user")
			cursor, err := userCollection.Find(ctx, bson.M{"user_id": bson.M{"$in": neededImServerIDs}})
			if err == nil {
				defer cursor.Close(ctx)

				var userInfos []struct {
					UserID   string `bson:"user_id"` // 这是ImServerUserId
					Nickname string `bson:"nickname"`
					FaceURL  string `bson:"face_url"`
				}

				if err := cursor.All(ctx, &userInfos); err == nil {
					// 直接使用索引映射更新结果，减少查找操作
					for _, info := range userInfos {
						if idx, ok := imServerIDToIdx[info.UserID]; ok {
							if info.Nickname != "" {
								result.Users[idx].Nickname = info.Nickname
							}
							if info.FaceURL != "" && result.Users[idx].FaceURL == "" {
								result.Users[idx].FaceURL = info.FaceURL
							}
						}
					}
				}
			}
		}
	}

	// 第四步：对于特殊情况（组织节点）批量处理
	// 优化：收集需要查询的组织ID和索引映射，进行批量查询
	var orgNodes []struct {
		Index int
		OrgID primitive.ObjectID
	}

	for i, user := range users {
		if user.UserType == chat.OrganizationUserTypeOrganization ||
			(len(user.UserId) >= len(OrgRootNodePrefix) && user.UserId[:len(OrgRootNodePrefix)] == OrgRootNodePrefix) {
			orgNodes = append(orgNodes, struct {
				Index int
				OrgID primitive.ObjectID
			}{i, user.OrganizationId})
		}
	}

	// 只有当有组织节点时才执行查询
	if len(orgNodes) > 0 {
		// 收集所有需要查询的组织ID
		var orgIDs []primitive.ObjectID
		orgIDToIndexes := make(map[string][]int)

		for _, node := range orgNodes {
			orgIDs = append(orgIDs, node.OrgID)
			key := node.OrgID.Hex()
			orgIDToIndexes[key] = append(orgIDToIndexes[key], node.Index)
		}

		// 批量查询组织信息
		if len(orgIDs) > 0 {
			organizationCollection := s.db.Collection("organization")
			cursor, err := organizationCollection.Find(ctx, bson.M{"_id": bson.M{"$in": orgIDs}})

			if err == nil {
				defer cursor.Close(ctx)

				var orgs []struct {
					ID      primitive.ObjectID `bson:"_id"`
					Name    string             `bson:"name"`
					LogoURL string             `bson:"logo_url"`
				}

				if err := cursor.All(ctx, &orgs); err == nil {
					// 使用查询结果更新所有匹配的组织节点
					for _, org := range orgs {
						if indexes, ok := orgIDToIndexes[org.ID.Hex()]; ok {
							for _, idx := range indexes {
								if org.Name != "" {
									result.Users[idx].Nickname = org.Name + "（组织根节点）"
								}
								if org.LogoURL != "" {
									result.Users[idx].FaceURL = org.LogoURL
								}
							}
						}
					}
				}
			}

			// 为未找到组织信息的节点设置默认值
			for _, node := range orgNodes {
				if result.Users[node.Index].Nickname == "" ||
					result.Users[node.Index].Nickname == "用户"+result.Users[node.Index].UserID[len(result.Users[node.Index].UserID)-6:] {
					result.Users[node.Index].Nickname = OrgRootNodeNickname
				}
			}
		}
	}

	// 最后，确保所有用户都有nickname，如果仍为空，则填入默认值
	for i := range result.Users {
		if result.Users[i].Nickname == "" {
			// 使用与getUserInfo函数一致的回退策略
			if result.Users[i].Account != "" {
				result.Users[i].Nickname = result.Users[i].Account
			} else {
				result.Users[i].Nickname = "用户" + result.Users[i].UserID[len(result.Users[i].UserID)-6:]
			}
		}
	}

	if st := s.tryHierarchyEffectiveStats(ctx, organizationID); st != nil {
		st.applyUserSlice(result.Users)
	}

	return result, nil
}

// RepairHierarchy is deprecated and now just returns a report without performing any operations
// 此函数已废弃，仅返回空报告但不执行任何操作
// 前端已经不再调用此接口，而是使用 getHierarchyTree 获取最新数据
func (s *HierarchyService) RepairHierarchy(ctx context.Context, organizationID primitive.ObjectID, operationID string) (*dto.HierarchyRepairReport, error) {
	startTime := time.Now()

	// 仅创建并返回空报告，不执行实际修复逻辑
	endTime := time.Now()
	report := &dto.HierarchyRepairReport{
		StartTime:         startTime.Format(time.RFC3339),
		EndTime:           endTime.Format(time.RFC3339),
		OrganizationID:    organizationID,
		OperationID:       operationID,
		Message:           "此API已被废弃，请使用getHierarchyTree获取最新数据",
		TotalUsers:        0,
		ProcessedUsers:    0,
		FixedUsers:        0,
		ErrorCount:        0,
		DurationInSeconds: 0,
	}

	return report, nil
}

// Helper function to get a user by ID
func (s *HierarchyService) getUserByID(ctx context.Context, organizationID primitive.ObjectID, userID string) (*chat.OrganizationUser, error) {
	filter := bson.M{
		"organization_id": organizationID,
		"user_id":         userID,
	}

	var user chat.OrganizationUser
	collection := s.db.Collection("organization_user")
	err := collection.FindOne(ctx, filter).Decode(&user)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.New("user not found")
		}
		return nil, err
	}

	return &user, nil
}

// Helper function to check if two string slices are equal
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}

// repairSingleUserHierarchy 修复单个用户的层级关系
// 这是一个针对单个用户的优化版 RepairHierarchy 函数
// 专门用于在用户注册后调用修复其层级关系
func (s *HierarchyService) repairSingleUserHierarchy(ctx context.Context, organizationID primitive.ObjectID, filter bson.M) error {
	// 确保过滤器包含组织ID
	if _, ok := filter["organization_id"]; !ok {
		filter["organization_id"] = organizationID
	}

	// 1. 获取用户信息
	collection := s.db.Collection("organization_user")
	var user chat.OrganizationUser
	err := collection.FindOne(ctx, filter).Decode(&user)
	if err != nil {
		return err
	}

	log.ZInfo(ctx, "正在修复用户层级关系",
		"organization_id", organizationID.Hex(),
		"user_id", user.UserId,
		"inviter", user.Inviter,
		"inviter_type", user.InviterType)

	needsUpdate := false
	updates := make(map[string]interface{})

	// 设置用户类型字段（如果未设置）
	if user.UserType == "" {
		updates["user_type"] = chat.OrganizationUserTypeUser
		needsUpdate = true
	}

	// 处理层级关系
	if user.Inviter == "" {
		// 处理无邀请人的用户
		// 设置为级别1，并将组织根节点设为其父节点
		if user.Level != 1 {
			updates["level"] = 1
			needsUpdate = true
		}

		// 尝试获取组织根节点
		orgRootNode, err := s.getOrCreateOrgRootNode(ctx, organizationID)
		if err != nil {
			log.ZWarn(ctx, "获取组织根节点失败", err)
		} else if orgRootNode != nil {
			// 设置父节点为组织根节点
			if user.Level1Parent != orgRootNode.UserId {
				updates["level1_parent"] = orgRootNode.UserId
				needsUpdate = true
			}

			// 设置祖先路径只包含组织根节点
			correctAncestorPath := []string{orgRootNode.UserId}
			if !equalStringSlices(user.AncestorPath, correctAncestorPath) {
				updates["ancestor_path"] = correctAncestorPath
				needsUpdate = true
			}

			// 清除其他父节点引用
			if user.Level2Parent != "" || user.Level3Parent != "" {
				updates["level2_parent"] = ""
				updates["level3_parent"] = ""
				needsUpdate = true
			}
		} else {
			// 组织根节点不存在或获取失败，清除所有父节点引用
			if user.Level1Parent != "" || user.Level2Parent != "" || user.Level3Parent != "" || len(user.AncestorPath) > 0 {
				updates["level1_parent"] = ""
				updates["level2_parent"] = ""
				updates["level3_parent"] = ""
				updates["ancestor_path"] = []string{}
				needsUpdate = true
			}
		}
	} else {
		// 处理有邀请人的用户
		// 查找邀请人
		var parent chat.OrganizationUser
		err := collection.FindOne(ctx, bson.M{"invitation_code": user.Inviter, "organization_id": organizationID}).Decode(&parent)

		if err != nil {
			// 如果找不到邀请人，设置为组织根节点的直接子节点（级别1）
			orgRootNode, err := s.getOrCreateOrgRootNode(ctx, organizationID)
			if err != nil {
				log.ZWarn(ctx, "获取组织根节点失败", err)
			} else if orgRootNode != nil {
				if user.Level != 1 {
					updates["level"] = 1
					needsUpdate = true
				}

				if user.Level1Parent != orgRootNode.UserId {
					updates["level1_parent"] = orgRootNode.UserId
					needsUpdate = true
				}

				correctAncestorPath := []string{orgRootNode.UserId}
				if !equalStringSlices(user.AncestorPath, correctAncestorPath) {
					updates["ancestor_path"] = correctAncestorPath
					needsUpdate = true
				}

				if user.Level2Parent != "" || user.Level3Parent != "" {
					updates["level2_parent"] = ""
					updates["level3_parent"] = ""
					needsUpdate = true
				}
			} else {
				// 如果没有找到组织根节点，则设置为级别1且无父节点
				if user.Level != 1 {
					updates["level"] = 1
					needsUpdate = true
				}

				if user.Level1Parent != "" || user.Level2Parent != "" || user.Level3Parent != "" || len(user.AncestorPath) > 0 {
					updates["level1_parent"] = ""
					updates["level2_parent"] = ""
					updates["level3_parent"] = ""
					updates["ancestor_path"] = []string{}
					needsUpdate = true
				}
			}
		} else {
			// 计算正确的级别和祖先路径
			correctLevel := parent.Level + 1

			// 检查并修复级别
			if user.Level != correctLevel {
				updates["level"] = correctLevel
				needsUpdate = true
			}

			// 构建正确的祖先路径
			correctAncestorPath := append([]string{parent.UserId}, parent.AncestorPath...)

			// 检查祖先路径是否正确
			if !equalStringSlices(user.AncestorPath, correctAncestorPath) {
				updates["ancestor_path"] = correctAncestorPath
				needsUpdate = true
			}

			// 检查并修复Level1Parent
			if user.Level1Parent != parent.UserId {
				updates["level1_parent"] = parent.UserId
				needsUpdate = true
			}

			// 检查并修复Level2Parent
			correctLevel2Parent := ""
			if len(correctAncestorPath) > 1 {
				correctLevel2Parent = correctAncestorPath[1]
			}

			if user.Level2Parent != correctLevel2Parent {
				updates["level2_parent"] = correctLevel2Parent
				needsUpdate = true
			}

			// 检查并修复Level3Parent
			correctLevel3Parent := ""
			if len(correctAncestorPath) > 2 {
				correctLevel3Parent = correctAncestorPath[2]
			}

			if user.Level3Parent != correctLevel3Parent {
				updates["level3_parent"] = correctLevel3Parent
				needsUpdate = true
			}

			// 更新父节点的 DirectDownlineCount 和 TeamSize
			if parent.UserId != "" {
				_, err = collection.UpdateOne(
					ctx,
					bson.M{"user_id": parent.UserId, "organization_id": organizationID},
					bson.M{
						"$inc": bson.M{
							"direct_downline_count": 1,
							"team_size":             1,
						},
					},
				)
				if err != nil {
					log.ZWarn(ctx, "更新父节点DirectDownlineCount和TeamSize失败", err)
				}

				// 更新所有更高层级祖先节点的 TeamSize
				if len(parent.AncestorPath) > 0 {
					_, err = collection.UpdateMany(
						ctx,
						bson.M{
							"user_id":         bson.M{"$in": parent.AncestorPath},
							"organization_id": organizationID,
						},
						bson.M{
							"$inc": bson.M{
								"team_size": 1,
							},
						},
					)
					if err != nil {
						log.ZWarn(ctx, "更新祖先节点TeamSize失败", err)
					}
				}
			}
		}
	}

	// 如果需要更新，执行更新操作
	if needsUpdate {
		// 更新用户
		_, err := collection.UpdateOne(
			ctx,
			bson.M{"user_id": user.UserId, "organization_id": organizationID},
			bson.M{"$set": updates},
		)

		if err != nil {
			log.ZError(ctx, "修复用户层级关系失败", err,
				"user_id", user.UserId)
			return err
		}

		log.ZInfo(ctx, "成功修复用户层级关系",
			"user_id", user.UserId,
			"updates", updates)
	} else {
		log.ZInfo(ctx, "用户层级关系无需修复",
			"user_id", user.UserId)
	}

	return nil
}

// Helper function to convert int64 to pointer
func toPtr(i int64) *int64 {
	return &i
}

// Helper function to get user display info
// 返回用户显示名称 (账号(昵称)) 和头像URL
// getUserInfo 获取用户完整信息，分离返回账号、昵称和头像URL
func (s *HierarchyService) getUserInfo(ctx context.Context, user *chat.OrganizationUser) (account string, nickname string, faceURL string) {
	// 检查是否是组织虚拟根节点
	if user.UserType == chat.OrganizationUserTypeOrganization {
		// 找到组织信息
		organizationCollection := s.db.Collection("organization")
		var org struct {
			Name    string `bson:"name"`
			LogoURL string `bson:"logo_url"`
		}

		err := organizationCollection.FindOne(ctx, bson.M{"_id": user.OrganizationId}).Decode(&org)
		if err == nil && org.Name != "" {
			nickname = org.Name + "（组织根节点）"
			if org.LogoURL != "" {
				faceURL = org.LogoURL
			}
			return "", nickname, faceURL
		}

		// 如果找不到组织信息，使用默认的组织根节点名称
		return "", OrgRootNodeNickname, ""
	}

	// 处理普通用户节点
	// 默认头像URL
	defaultFaceURL := ""

	// 1. 从attribute集合获取用户账号和昵称信息
	attrFilter := bson.M{"user_id": user.UserId}
	attributeCollection := s.db.Collection("attribute")

	var attribute struct {
		Account  string `bson:"account"`
		Nickname string `bson:"nickname"`
		FaceURL  string `bson:"face_url"`
		Email    string `bson:"email"`
	}

	err := attributeCollection.FindOne(ctx, attrFilter).Decode(&attribute)
	if err == nil {
		// 成功从attribute获取到用户信息
		account = attribute.Account
		nickname = attribute.Nickname

		// 如果有头像URL，使用它
		if attribute.FaceURL != "" {
			faceURL = attribute.FaceURL
		}
	}

	// 2. 如果nickname为空，尝试从user集合获取用户昵称
	if nickname == "" {
		// 检查ImServerUserId是否有效
		if user.ImServerUserId == "" {
			log.ZWarn(ctx, "ImServerUserId为空，无法从user表查询信息", nil, "user_id", user.UserId, "organization_id", user.OrganizationId.Hex())
		} else {
			userFilter := bson.M{"user_id": user.ImServerUserId}
			userCollection := s.db.Collection("user")

			var userInfo struct {
				Nickname string `bson:"nickname"`
				FaceURL  string `bson:"face_url"`
			}

			err := userCollection.FindOne(ctx, userFilter).Decode(&userInfo)
			if err != nil {
				log.ZWarn(ctx, "从user表查询用户信息失败", err, "user_id", user.UserId, "im_server_user_id", user.ImServerUserId)
			} else if userInfo.Nickname != "" {
				// 从user表获取到了昵称
				nickname = userInfo.Nickname
				log.ZInfo(ctx, "成功从user表获取用户昵称", "user_id", user.UserId, "im_server_user_id", user.ImServerUserId, "nickname", nickname)

				// 如果有头像URL且之前没设置，使用它
				if userInfo.FaceURL != "" && faceURL == "" {
					faceURL = userInfo.FaceURL
				}
			} else {
				log.ZWarn(ctx, "user表中nickname为空", nil, "user_id", user.UserId, "im_server_user_id", user.ImServerUserId)
			}
		}
	}

	// 如果没有获取到头像URL，使用默认值
	if faceURL == "" {
		faceURL = defaultFaceURL
	}

	return account, nickname, faceURL
}

// buildAncestorListRecursively 递归构建祖先列表
// parentID: 当前要处理的父节点ID
// parentLevel: 当前父节点的层级
// ancestorList: 收集到的祖先列表（引用传递，会被修改）
// visitedNodes: 已访问的节点ID，用于防止循环引用
func (s *HierarchyService) buildAncestorListRecursively(ctx context.Context, organizationID primitive.ObjectID, parentID string, parentLevel int, ancestorList *[]dto.AncestorInfo, visitedNodes map[string]bool) error {
	// 检查节点是否已访问，防止循环引用
	if visitedNodes[parentID] {
		return nil
	}
	visitedNodes[parentID] = true

	// 获取当前父节点
	parent, err := s.getUserByID(ctx, organizationID, parentID)
	if err != nil {
		return err
	}

	// 获取父节点的账号和昵称
	account, nickname, _ := s.getUserInfo(ctx, parent)

	// 添加到祖先列表
	*ancestorList = append(*ancestorList, dto.AncestorInfo{
		UserID:   parentID,
		Account:  account,
		Nickname: nickname,
		Level:    parentLevel, // 使用传入的层级值
	})

	// 继续向上追溯
	if parent.Level1Parent != "" && !visitedNodes[parent.Level1Parent] {
		return s.buildAncestorListRecursively(ctx, organizationID, parent.Level1Parent, parentLevel-1, ancestorList, visitedNodes)
	}

	return nil
}

func (s *HierarchyService) getUserDisplayInfo(ctx context.Context, user *chat.OrganizationUser) (displayName string, faceURL string) {
	// 使用新的getUserInfo函数获取分离的账号、昵称和头像信息
	account, nickname, faceUrl := s.getUserInfo(ctx, user)

	// 默认值（如果查询失败，使用ID后6位生成）
	defaultDisplayName := "用户" + user.UserId[len(user.UserId)-6:]

	// 保持原有的显示名称生成逻辑
	if account != "" {
		if nickname != "" && nickname != account {
			// 同时有账号和昵称，且不相同
			displayName = account + "(" + nickname + ")"
		} else {
			// 只有账号或昵称与账号相同
			displayName = account
		}
	} else if nickname != "" {
		// 只有昵称
		displayName = nickname
	} else {
		// 账号昵称都没有，使用默认值
		displayName = defaultDisplayName
	}

	return displayName, faceUrl
}
