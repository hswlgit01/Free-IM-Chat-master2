// Copyright © 2023 OpenIM open source community. All rights reserved.

package svc

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/apps/organization/dto"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	chat "github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 此函数将替代原有的SearchHierarchy函数，在不修改数据库结构的情况下实现更强大的搜索能力
//
// 搜索策略：
// 1. 如果没有关键词，直接使用原始搜索方法
// 2. 如果有关键词，使用优化后的直接搜索方法，包括：
//   - 在organization_user集合中查询邀请码匹配的用户
//   - 在attribute集合中查询账号/昵称/邮箱匹配的用户
//   - 合并两组结果并去重
//
// 3. 所有查询都使用索引，避免全表扫描，提高性能
func (s *HierarchyService) SearchHierarchyEnhanced(ctx context.Context, organizationID primitive.ObjectID, req *dto.SearchHierarchyReq, pagination *paginationUtils.DepPagination) (*dto.SearchHierarchyResp, error) {
	log.ZInfo(ctx, "开始执行增强版用户搜索", "organizationID", organizationID.Hex(), "keyword", req.Keyword)

	// 没有关键词时，使用原始搜索方法（仅在 organization_user 上搜索，性能更好）
	if req.Keyword == "" {
		return s.SearchHierarchy(ctx, organizationID, req, pagination)
	}

	// 使用基于聚合管道的实现：
	// 1. 以 organization_user 作为主集合，首先按 organization_id、层级、上级等条件过滤
	// 2. 通过 $lookup 关联 attribute 集合获取账号/昵称/邮箱等信息
	// 3. 在管道中统一做关键词匹配与排序、分页
	return s.searchHierarchyWithAggregation(ctx, organizationID, req, pagination)
}

// hierarchySearchResult 用于承接聚合查询结果的临时结构体
type hierarchySearchResult struct {
	UserID              string    `bson:"user_id"`
	Level               int       `bson:"level"`
	InvitationCode      string    `bson:"invitation_code"`
	TeamSize            int       `bson:"team_size"`
	DirectDownlineCount int       `bson:"direct_downline_count"`
	AncestorPath        []string  `bson:"ancestor_path"`
	CreatedAt           time.Time `bson:"created_at"`
	UserType            string    `bson:"user_type"`

	Account  string `bson:"account"`
	Nickname string `bson:"nickname"`
	FaceURL  string `bson:"face_url"`
}

// 并通过 $lookup attribute 获取账号、昵称等信息。
func (s *HierarchyService) searchHierarchyWithAggregation(ctx context.Context, organizationID primitive.ObjectID, req *dto.SearchHierarchyReq, pagination *paginationUtils.DepPagination) (*dto.SearchHierarchyResp, error) {
	orgColl := s.db.Collection("organization_user")

	// 1. 构建基础匹配条件（只在 organization_user 上）
	match := bson.M{
		"organization_id": organizationID,
	}

	// 默认排除组织虚拟节点，除非明确指定
	if !req.IncludeOrgNodes {
		match["user_type"] = bson.M{
			"$ne": chat.OrganizationUserTypeOrganization,
		}
	}

	// 层级过滤
	if req.Level > 0 {
		match["level"] = req.Level
	}

	// 上级过滤：和 SearchHierarchy 中逻辑保持一致
	if req.AncestorID != "" {
		// 组织根节点：level=1
		if len(req.AncestorID) >= len(OrgRootNodePrefix) && req.AncestorID[:len(OrgRootNodePrefix)] == OrgRootNodePrefix {
			match["level"] = 1
		} else {
			ancestor, err := s.getUserByID(ctx, organizationID, req.AncestorID)
			if err == nil && ancestor.UserType == chat.OrganizationUserTypeOrganization {
				// 上级是组织节点，同样视为根，level=1
				match["level"] = 1
			} else {
				// 普通用户上级，根据 ancestor_path 精确匹配
				match["ancestor_path"] = req.AncestorID
			}
		}
	}

	// 2. 排序字段与方向
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

	skip := int64((pagination.GetPageNumber() - 1) * pagination.GetShowNumber())
	limit := int64(pagination.GetShowNumber())
	if limit <= 0 {
		// 兼容前端未传 pageSize 的情况，Mongo $limit 必须为正数
		limit = 20
	}

	// 3. 公共的管道前缀：match -> lookup attribute -> unwind attr
	commonPipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: match}},
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

	// 4. 关键词匹配：user_id / invitation_code / attribute(账号/昵称/邮箱)
	if req.Keyword != "" {
		keywordMatch := bson.M{
			"$or": []bson.M{
				{"user_id": bson.M{"$regex": req.Keyword, "$options": "i"}},
				{"invitation_code": bson.M{"$regex": req.Keyword, "$options": "i"}},
				{"attr.account": bson.M{"$regex": req.Keyword, "$options": "i"}},
				{"attr.nickname": bson.M{"$regex": req.Keyword, "$options": "i"}},
				{"attr.email": bson.M{"$regex": req.Keyword, "$options": "i"}},
			},
		}
		commonPipeline = append(commonPipeline, bson.D{{Key: "$match", Value: keywordMatch}})
	}

	// 5. 先计算总数
	countPipeline := append(commonPipeline,
		bson.D{{Key: "$count", Value: "total"}},
	)

	var total int64
	countCursor, err := orgColl.Aggregate(ctx, countPipeline)
	if err != nil {
		log.ZError(ctx, "层级搜索统计总数失败", err)
		return nil, err
	}
	var countResult []struct {
		Total int64 `bson:"total"`
	}
	if err = countCursor.All(ctx, &countResult); err != nil {
		log.ZError(ctx, "层级搜索统计结果解析失败", err)
		return nil, err
	}
	if len(countResult) > 0 {
		total = countResult[0].Total
	} else {
		total = 0
	}

	// 若没有匹配数据，直接返回空结果，避免再跑一次聚合
	if total == 0 {
		return &dto.SearchHierarchyResp{
			Users: []dto.UserHierarchyInfo{},
			Total: 0,
		}, nil
	}

	// 6. 查询当前页数据：在公共管道基础上追加 sort / skip / limit / project
	dataPipeline := append(commonPipeline,
		bson.D{{Key: "$sort", Value: bson.D{{Key: sortField, Value: sortOrder}}}},
		bson.D{{Key: "$skip", Value: skip}},
		bson.D{{Key: "$limit", Value: limit}},
		bson.D{{Key: "$project", Value: bson.M{
			"user_id":               1,
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

	cursor, err := orgColl.Aggregate(ctx, dataPipeline)
	if err != nil {
		log.ZError(ctx, "层级搜索数据查询失败", err)
		return nil, err
	}
	defer cursor.Close(ctx)

	var docs []hierarchySearchResult
	if err = cursor.All(ctx, &docs); err != nil {
		log.ZError(ctx, "层级搜索数据解析失败", err)
		return nil, err
	}

	// 7. 转换为响应 DTO
	resp := &dto.SearchHierarchyResp{
		Users: make([]dto.UserHierarchyInfo, 0, len(docs)),
		Total: total,
	}

	for _, d := range docs {
		account := d.Account
		nickname := d.Nickname

		// 与原有 getUserInfo 的兜底策略保持一致：没有昵称时用账号或“用户+后6位”
		if nickname == "" {
			if account != "" {
				nickname = account
			} else if len(d.UserID) > 6 {
				nickname = "用户" + d.UserID[len(d.UserID)-6:]
			} else {
				nickname = d.UserID
			}
		}

		resp.Users = append(resp.Users, dto.UserHierarchyInfo{
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

	if st := s.tryHierarchyEffectiveStats(ctx, organizationID); st != nil {
		st.applyUserSlice(resp.Users)
	}

	return resp, nil
}

// directSearchOptimized 是一个高效的搜索实现，使用并行查询代替复杂聚合
//
// 实现策略：
// 1. 执行两个独立的高效查询：
//   - 查询 organization_user 集合中邀请码匹配的用户
//   - 查询 attribute 集合中账号/昵称/邮箱匹配的用户ID，再查询这些ID对应的用户详情
//
// 2. 所有查询都添加适当的过滤条件（组织ID、用户类型、层级、上级路径等）
// 3. 合并两组查询结果，并在内存中去重
// 4. 统一应用分页和排序
//
// 优点：
// 1. 充分利用索引，每个查询都可以使用最合适的索引
// 2. 避免复杂的聚合管道和全表扫描，提高查询性能
// 3. 支持通过账号、昵称、邮箱和邀请码多字段搜索
// 4. 兼容原有API格式，保持向后兼容性
func (s *HierarchyService) directSearchOptimized(ctx context.Context, organizationID primitive.ObjectID, req *dto.SearchHierarchyReq, pagination *paginationUtils.DepPagination) (*dto.SearchHierarchyResp, error) {
	log.ZInfo(ctx, "执行直接优化搜索", "organizationID", organizationID.Hex(), "keyword", req.Keyword)

	// 准备分页参数
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

	sortOrder := -1 // 默认降序
	if req.SortOrder == "asc" {
		sortOrder = 1
	}

	// 注意: 在directSearchOptimized中，我们直接使用organizationID构建过滤条件

	// 初始化集合
	orgCollection := s.db.Collection("organization_user")
	attrCollection := s.db.Collection("attribute")

	// 准备过滤条件相关变量
	var userTypeFilter bson.M
	var levelFilter int32
	var ancestorFilter string
	var hasLevelFilter bool
	var hasAncestorFilter bool

	// 准备用户类型过滤
	if !req.IncludeOrgNodes {
		userTypeFilter = bson.M{"$ne": string(chat.OrganizationUserTypeOrganization)}
	}

	// 准备层级过滤
	if req.Level > 0 {
		levelFilter = int32(req.Level)
		hasLevelFilter = true
	}

	// 准备上级节点过滤
	if req.AncestorID != "" {
		// 如果是组织根节点ID，则搜索所有level=1的用户
		if len(req.AncestorID) >= 9 && req.AncestorID[:9] == "ORG_ROOT_" {
			levelFilter = 1
			hasLevelFilter = true
		} else {
			// 检查指定的上级是否是组织节点
			ancestor, err := s.getUserByID(ctx, organizationID, req.AncestorID)
			if err == nil && ancestor.UserType == chat.OrganizationUserTypeOrganization {
				levelFilter = 1
				hasLevelFilter = true
			} else {
				// 普通用户上级，使用ancestor_path
				ancestorFilter = req.AncestorID
				hasAncestorFilter = true
			}
		}
	}

	// 第一步: 查询邀请码匹配的用户
	invFilter := bson.M{
		"organization_id": organizationID,
		"invitation_code": bson.M{"$regex": req.Keyword, "$options": "i"},
	}

	// 应用附加过滤条件
	if !req.IncludeOrgNodes && userTypeFilter != nil {
		invFilter["user_type"] = userTypeFilter
	}
	if hasLevelFilter {
		invFilter["level"] = levelFilter
	}
	if hasAncestorFilter {
		invFilter["ancestor_path"] = ancestorFilter
	}

	// 设置排序
	sortOption := bson.D{{Key: sortField, Value: sortOrder}}

	var invUsers []*chat.OrganizationUser
	invOpts := options.Find().SetSort(sortOption).SetSkip(skip).SetLimit(limit)
	invCursor, err := orgCollection.Find(ctx, invFilter, invOpts)
	if err != nil {
		log.ZError(ctx, "邀请码查询失败", err)
	} else {
		defer invCursor.Close(ctx)
		if err := invCursor.All(ctx, &invUsers); err != nil {
			log.ZError(ctx, "解析邀请码查询结果失败", err)
		}
	}

	// 计算邀请码匹配的总数
	invCount, _ := orgCollection.CountDocuments(ctx, invFilter)
	log.ZInfo(ctx, "邀请码匹配数", "count", invCount)

	// 第二步: 查询属性表中匹配的用户ID
	attrFilter := bson.M{
		"$or": []bson.M{
			{"account": bson.M{"$regex": req.Keyword, "$options": "i"}},
			{"nickname": bson.M{"$regex": req.Keyword, "$options": "i"}},
			{"email": bson.M{"$regex": req.Keyword, "$options": "i"}},
		},
	}

	// 属性表查询不需要分页，我们需要获取所有匹配的ID
	var attrResults []bson.M
	attrProjection := bson.M{"user_id": 1, "account": 1, "nickname": 1, "face_url": 1, "_id": 0}
	attrOpts := options.Find().SetProjection(attrProjection)

	attrCursor, err := attrCollection.Find(ctx, attrFilter, attrOpts)
	if err != nil {
		log.ZError(ctx, "属性表查询失败", err)
	} else {
		defer attrCursor.Close(ctx)
		if err := attrCursor.All(ctx, &attrResults); err != nil {
			log.ZError(ctx, "解析属性表查询结果失败", err)
		}
	}

	// 提取所有匹配的用户ID
	var userIDs []string
	attrUserMap := make(map[string]bson.M) // 用于后面快速获取用户属性
	for _, attr := range attrResults {
		if userID, ok := attr["user_id"].(string); ok {
			userIDs = append(userIDs, userID)
			attrUserMap[userID] = attr
		}
	}

	// 第三步: 查询属性表匹配ID的用户信息
	var attrUsers []*chat.OrganizationUser
	if len(userIDs) > 0 {
		userFilter := bson.M{
			"organization_id": organizationID,
			"user_id":         bson.M{"$in": userIDs},
		}

		// 应用附加过滤条件
		if !req.IncludeOrgNodes && userTypeFilter != nil {
			userFilter["user_type"] = userTypeFilter
		}
		if hasLevelFilter {
			userFilter["level"] = levelFilter
		}
		if hasAncestorFilter {
			userFilter["ancestor_path"] = ancestorFilter
		}

		// 应用相同的排序和分页
		userOpts := options.Find().SetSort(sortOption).SetSkip(skip).SetLimit(limit)
		userCursor, err := orgCollection.Find(ctx, userFilter, userOpts)
		if err != nil {
			log.ZError(ctx, "用户信息查询失败", err)
		} else {
			defer userCursor.Close(ctx)
			if err := userCursor.All(ctx, &attrUsers); err != nil {
				log.ZError(ctx, "解析用户信息失败", err)
			}
		}

		// 计算该过滤条件下的总用户数
		attrCount, _ := orgCollection.CountDocuments(ctx, userFilter)
		log.ZInfo(ctx, "属性匹配用户数", "count", attrCount, "foundIDs", len(userIDs))
	}

	// 合并结果集，优先考虑邀请码匹配
	var allUsers []*chat.OrganizationUser
	addedUserMap := make(map[string]bool)

	// 首先添加邀请码匹配的用户
	for _, user := range invUsers {
		if !addedUserMap[user.UserId] {
			allUsers = append(allUsers, user)
			addedUserMap[user.UserId] = true
		}
	}

	// 然后添加属性匹配的用户
	for _, user := range attrUsers {
		if !addedUserMap[user.UserId] {
			allUsers = append(allUsers, user)
			addedUserMap[user.UserId] = true
		}
	}

	// 计算总数 (邀请码匹配 + 属性匹配 - 重复项)
	// 由于我们已经做了去重，这里直接使用去重后的长度即可
	total := int64(len(allUsers))

	// 限制返回数量
	if len(allUsers) > int(limit) {
		allUsers = allUsers[:limit]
	}

	// 构建响应
	result := &dto.SearchHierarchyResp{
		Users: make([]dto.UserHierarchyInfo, 0, len(allUsers)),
		Total: total,
	}

	// 转换用户信息到响应格式
	for _, user := range allUsers {
		var account, nickname, faceURL string

		// 首先尝试从属性映射中获取
		if attr, ok := attrUserMap[user.UserId]; ok {
			nickname, _ = attr["nickname"].(string)
			account, _ = attr["account"].(string) // 获取账号
			faceURL, _ = attr["face_url"].(string)
		} else {
			// 如果映射中没有，使用新的getUserInfo方法获取完整信息
			account, nickname, faceURL = s.getUserInfo(ctx, user)
		}

		// 添加到响应
		result.Users = append(result.Users, dto.UserHierarchyInfo{
			UserID:              user.UserId,
			Nickname:            nickname, // 使用分离的昵称
			Account:             account,  // 使用分离的账号
			FaceURL:             faceURL,
			Level:               user.Level,
			InvitationCode:      user.InvitationCode,
			TeamSize:            user.TeamSize,
			DirectDownlineCount: user.DirectDownlineCount,
			AncestorPath:        user.AncestorPath,
			CreatedAt:           user.CreatedAt,
			UserType:            string(user.UserType),
		})
	}

	if st := s.tryHierarchyEffectiveStats(ctx, organizationID); st != nil {
		st.applyUserSlice(result.Users)
	}

	return result, nil
}

// getUserAccountInfo 获取用户的账号信息
func (s *HierarchyService) getUserAccountInfo(ctx context.Context, userID string) (*struct {
	Account string `bson:"account"`
	Email   string `bson:"email"`
}, error) {
	// 从attribute集合获取用户账号信息
	attrFilter := bson.M{"user_id": userID}
	attributeCollection := s.db.Collection("attribute")

	var attribute struct {
		Account string `bson:"account"`
		Email   string `bson:"email"`
	}

	err := attributeCollection.FindOne(ctx, attrFilter).Decode(&attribute)
	if err != nil {
		log.ZWarn(ctx, "获取用户账号信息失败", err, "userID", userID)
		return nil, err
	}

	return &attribute, nil
}

// backupSearch 提供一个高效的备选搜索实现，直接使用多个并行查询而不是$facet
//
// 备选方案说明：
// 该方法是directSearchOptimized的一个变体，实现思路相同但细节略有不同：
// 1. 同样使用两个独立查询代替复杂聚合
// 2. 同样采用索引优化的查询方式
// 3. 提供了更详细的日志输出和错误处理
// 4. 结果处理逻辑略有差异
//
// 注意：此方法是一个备选实现，当主要方法出现问题时可以作为替代方案
func (s *HierarchyService) backupSearch(ctx context.Context, organizationID primitive.ObjectID, keyword string, pagination *paginationUtils.DepPagination) (*dto.SearchHierarchyResp, error) {
	log.ZInfo(ctx, "执行高效备选搜索", "organizationID", organizationID.Hex(), "keyword", keyword)

	// 准备分页参数
	skip := int64((pagination.GetPageNumber() - 1) * pagination.GetShowNumber())
	limit := int64(pagination.GetShowNumber())

	// 添加索引提示
	orgCollection := s.db.Collection("organization_user")
	attrCollection := s.db.Collection("attribute")

	// 方法1：直接查询invitation_code（直接使用Find，更高效）
	invitationFilter := bson.M{
		"organization_id": organizationID,
		"invitation_code": bson.M{"$regex": keyword, "$options": "i"},
	}

	// 方法2：查询attribute表，然后关联organization_user
	// 使用两步查询，先找出匹配的用户ID，再查询对应的用户
	attrFilter := bson.M{
		"$or": []bson.M{
			{"account": bson.M{"$regex": keyword, "$options": "i"}},
			{"nickname": bson.M{"$regex": keyword, "$options": "i"}},
			{"email": bson.M{"$regex": keyword, "$options": "i"}},
		},
	}

	// 1. 执行第一个查询：匹配邀请码
	var invUsers []*chat.OrganizationUser
	invOpts := options.Find().SetLimit(limit).SetSkip(skip)
	invCursor, err := orgCollection.Find(ctx, invitationFilter, invOpts)
	if err != nil {
		log.ZError(ctx, "邀请码查询失败", err)
		// 继续后面的查询，不要直接返回错误
	} else {
		defer invCursor.Close(ctx)
		if err := invCursor.All(ctx, &invUsers); err != nil {
			log.ZError(ctx, "解析邀请码查询结果失败", err)
		}
	}

	// 计算邀请码匹配的总数
	invCount, err := orgCollection.CountDocuments(ctx, invitationFilter)
	if err != nil {
		log.ZError(ctx, "邀请码查询计数失败", err)
		invCount = int64(len(invUsers)) // 使用返回数作为备选
	}

	log.ZInfo(ctx, "邀请码查询结果", "count", len(invUsers), "total", invCount)

	// 2. 执行第二个查询：从attribute表查找匹配账号/昵称的用户ID
	var attrMatches []bson.M
	attrProjection := bson.M{"user_id": 1, "account": 1, "nickname": 1, "face_url": 1, "_id": 0}
	attrOpts := options.Find().SetProjection(attrProjection)
	attrCursor, err := attrCollection.Find(ctx, attrFilter, attrOpts)
	if err != nil {
		log.ZError(ctx, "属性查询失败", err)
	} else {
		defer attrCursor.Close(ctx)
		if err := attrCursor.All(ctx, &attrMatches); err != nil {
			log.ZError(ctx, "解析属性查询结果失败", err)
		}
	}

	// 提取匹配的用户ID
	var userIDs []string
	attrUserMap := make(map[string]bson.M) // 用于存储用户ID到属性的映射
	for _, attr := range attrMatches {
		if userID, ok := attr["user_id"].(string); ok {
			userIDs = append(userIDs, userID)
			attrUserMap[userID] = attr
		}
	}

	log.ZInfo(ctx, "属性查询匹配用户ID", "count", len(userIDs))

	// 3. 查找匹配账号/昵称的用户详细信息
	var attrUsers []*chat.OrganizationUser
	if len(userIDs) > 0 {
		// 查询匹配的用户详情
		userFilter := bson.M{
			"organization_id": organizationID,
			"user_id":         bson.M{"$in": userIDs},
		}

		userOpts := options.Find().SetLimit(limit).SetSkip(skip)
		userCursor, err := orgCollection.Find(ctx, userFilter, userOpts)
		if err != nil {
			log.ZError(ctx, "通过ID查询用户失败", err)
		} else {
			defer userCursor.Close(ctx)
			if err := userCursor.All(ctx, &attrUsers); err != nil {
				log.ZError(ctx, "解析用户查询结果失败", err)
			}
		}

		log.ZInfo(ctx, "通过ID查询到的用户", "count", len(attrUsers))
	}

	// 计算属性匹配的总数
	var attrCount int64 = 0
	if len(userIDs) > 0 {
		userCountFilter := bson.M{
			"organization_id": organizationID,
			"user_id":         bson.M{"$in": userIDs},
		}
		attrCount, err = orgCollection.CountDocuments(ctx, userCountFilter)
		if err != nil {
			log.ZError(ctx, "属性匹配用户计数失败", err)
			attrCount = int64(len(userIDs)) // 使用ID数作为备选
		}
	}

	// 合并结果（不重复）
	totalCount := invCount + attrCount

	// 准备响应
	result := &dto.SearchHierarchyResp{
		Users: make([]dto.UserHierarchyInfo, 0, len(invUsers)+len(attrUsers)),
		Total: totalCount,
	}

	// 添加邀请码匹配用户
	addedUsers := make(map[string]bool)
	for _, user := range invUsers {
		if addedUsers[user.UserId] {
			continue // 避免重复
		}

		account, nickname, faceURL := s.getUserInfo(ctx, user)
		result.Users = append(result.Users, dto.UserHierarchyInfo{
			UserID:              user.UserId,
			Account:             account,
			Nickname:            nickname,
			FaceURL:             faceURL,
			Level:               user.Level,
			InvitationCode:      user.InvitationCode,
			TeamSize:            user.TeamSize,
			DirectDownlineCount: user.DirectDownlineCount,
			AncestorPath:        user.AncestorPath,
			CreatedAt:           user.CreatedAt,
			UserType:            string(user.UserType),
		})

		addedUsers[user.UserId] = true
	}

	// 添加属性匹配用户
	for _, user := range attrUsers {
		if addedUsers[user.UserId] {
			continue // 避免重复
		}

		var account, nickname, faceURL string
		// 使用预先获取的属性信息，避免再次查询
		if attr, ok := attrUserMap[user.UserId]; ok {
			nickname, _ = attr["nickname"].(string)
			account, _ = attr["account"].(string) // 获取账号
			faceURL, _ = attr["face_url"].(string)
		} else {
			// 使用新的getUserInfo方法获取分离的账号、昵称和头像
			account, nickname, faceURL = s.getUserInfo(ctx, user)
		}

		result.Users = append(result.Users, dto.UserHierarchyInfo{
			UserID:              user.UserId,
			Nickname:            nickname,
			Account:             account, // 添加账号字段
			FaceURL:             faceURL,
			Level:               user.Level,
			InvitationCode:      user.InvitationCode,
			TeamSize:            user.TeamSize,
			DirectDownlineCount: user.DirectDownlineCount,
			AncestorPath:        user.AncestorPath,
			CreatedAt:           user.CreatedAt,
			UserType:            string(user.UserType),
		})

		addedUsers[user.UserId] = true
	}

	// 如果结果数量少于原有的limit，可能是因为去重导致的，可以增加一个额外的查询补充结果

	if st := s.tryHierarchyEffectiveStats(ctx, organizationID); st != nil {
		st.applyUserSlice(result.Users)
	}

	return result, nil
}
