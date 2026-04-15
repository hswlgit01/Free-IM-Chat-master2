// Copyright © 2023 OpenIM open source community. All rights reserved.

package svc

import (
	"context"
	"fmt"
	"time"

	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// 虚拟根节点相关常量
const (
	// 用于标识虚拟根节点的前缀
	OrgRootNodePrefix = "ORG_ROOT_"

	// 根节点的层级值
	OrgRootNodeLevel = 0

	// 根节点的昵称
	OrgRootNodeNickname = "组织根节点"
)

// 获取组织虚拟根节点ID
func getOrgRootNodeID(organizationID primitive.ObjectID) string {
	return fmt.Sprintf("%s%s", OrgRootNodePrefix, organizationID.Hex())
}

// getOrCreateOrgRootNode 获取或创建组织虚拟根节点
// 如果根节点不存在，则创建一个新的
func (s *HierarchyService) getOrCreateOrgRootNode(ctx context.Context, organizationID primitive.ObjectID) (*chat.OrganizationUser, error) {
	// 1. 尝试获取现有的根节点
	rootNodeID := getOrgRootNodeID(organizationID)

	filter := bson.M{
		"organization_id": organizationID,
		"user_id":         rootNodeID,
		"user_type":       chat.OrganizationUserTypeOrganization,
	}

	var rootNode chat.OrganizationUser
	collection := s.db.Collection("organization_user")
	err := collection.FindOne(ctx, filter).Decode(&rootNode)

	// 2. 如果找到了根节点，直接返回
	if err == nil {
		return &rootNode, nil
	}

	// 3. 如果是其他错误（非文档不存在），返回错误
	if err != mongo.ErrNoDocuments {
		log.ZWarn(ctx, "Error finding organization root node", err,
			"organizationID", organizationID.Hex(),
			"rootNodeID", rootNodeID)
		return nil, err
	}

	// 4. 如果根节点不存在，创建一个新的
	now := time.Now()
	newRootNode := chat.OrganizationUser{
		OrganizationId:      organizationID,
		UserId:              rootNodeID,
		UserType:            chat.OrganizationUserTypeOrganization,
		Role:                chat.OrganizationUserSuperAdminRole,
		Status:              chat.OrganizationUserEnableStatus,
		ImServerUserId:      "", // 虚拟根节点没有对应的IM用户
		InvitationCode:      "", // 使用组织邀请码而不是单独的邀请码
		RegisterType:        chat.OrganizationUserRegisterTypeBackend,
		Inviter:             "",
		InviterType:         "",
		AncestorPath:        []string{},
		Level:               OrgRootNodeLevel,
		Level1Parent:        "",
		Level2Parent:        "",
		Level3Parent:        "",
		TeamSize:            0, // 这个值会在迁移脚本中更新
		DirectDownlineCount: 0, // 这个值会在迁移脚本中更新
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	// 5. 将根节点保存到数据库
	_, err = collection.InsertOne(ctx, newRootNode)
	if err != nil {
		log.ZWarn(ctx, "Error creating organization root node", err,
			"organizationID", organizationID.Hex())
		return nil, err
	}

	// 6. 记录创建成功的日志
	log.ZInfo(ctx, "Created new organization root node",
		"organizationID", organizationID.Hex(),
		"rootNodeID", rootNodeID)

	return &newRootNode, nil
}

// findAllOrgRootNodes 查找所有组织的虚拟根节点
func (s *HierarchyService) findAllOrgRootNodes(ctx context.Context) ([]*chat.OrganizationUser, error) {
	filter := bson.M{
		"user_type": chat.OrganizationUserTypeOrganization,
	}

	var rootNodes []*chat.OrganizationUser
	collection := s.db.Collection("organization_user")
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		log.ZWarn(ctx, "Error finding all organization root nodes", err)
		return nil, err
	}
	defer cursor.Close(ctx)

	if err := cursor.All(ctx, &rootNodes); err != nil {
		log.ZWarn(ctx, "Error decoding all organization root nodes", err)
		return nil, err
	}

	return rootNodes, nil
}

// findOrgUsersByOrganizationID 查找组织中的所有普通用户
func (s *HierarchyService) findOrgUsersByOrganizationID(ctx context.Context, organizationID primitive.ObjectID) ([]*chat.OrganizationUser, error) {
	filter := bson.M{
		"organization_id": organizationID,
		"user_type": bson.M{
			"$ne": chat.OrganizationUserTypeOrganization, // 排除组织虚拟根节点
		},
	}

	var orgUsers []*chat.OrganizationUser
	collection := s.db.Collection("organization_user")
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		log.ZWarn(ctx, "Error finding organization users", err,
			"organizationID", organizationID.Hex())
		return nil, err
	}
	defer cursor.Close(ctx)

	if err := cursor.All(ctx, &orgUsers); err != nil {
		log.ZWarn(ctx, "Error decoding organization users", err,
			"organizationID", organizationID.Hex())
		return nil, err
	}

	return orgUsers, nil
}
