// Copyright © 2023 OpenIM open source community. All rights reserved.

package svc

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MigrateToVirtualRootNode 迁移组织用户层级结构到虚拟根节点模式
// 该函数执行以下步骤：
// 1. 为组织创建虚拟根节点（如果不存在）
// 2. 更新所有顶级用户（无上级或邀请者）的父节点为根节点
// 3. 重建整个组织的层级关系
// 4. 更新所有节点的团队大小和直接下级数量
func MigrateToVirtualRootNode(ctx context.Context, organizationID primitive.ObjectID) error {
	// 记录迁移开始
	log.ZInfo(ctx, "Starting migration to virtual root node model",
		"organization_id", organizationID.Hex())

	startTime := time.Now()
	db := plugin.MongoCli().GetDB()
	collection := db.Collection("organization_user")

	// 步骤1: 获取或创建组织虚拟根节点
	hierarchySvc := NewHierarchyService(db)
	rootNode, err := hierarchySvc.getOrCreateOrgRootNode(ctx, organizationID)
	if err != nil {
		log.ZError(ctx, "Failed to get or create organization root node", err,
			"organization_id", organizationID.Hex())
		return err
	}

	log.ZInfo(ctx, "Organization root node ready",
		"organization_id", organizationID.Hex(),
		"root_node_id", rootNode.UserId)

	// 步骤2: 将所有user_type字段设置为USER（对于非根节点用户）
	userTypeFilter := bson.M{
		"organization_id": organizationID,
		"user_type":       bson.M{"$exists": false},
		"user_id":         bson.M{"$ne": rootNode.UserId},
	}

	userTypeUpdate := bson.M{
		"$set": bson.M{
			"user_type": chat.OrganizationUserTypeUser,
		},
	}

	userTypeResult, err := collection.UpdateMany(ctx, userTypeFilter, userTypeUpdate)
	if err != nil {
		log.ZError(ctx, "Failed to update user_type field", err)
		// 继续执行，不要中断整个迁移流程
	} else {
		log.ZInfo(ctx, "Updated user_type field",
			"modified_count", userTypeResult.ModifiedCount)
	}

	// 步骤3: 找出所有顶级用户（level=1或没有上级的用户）并更新其上级为根节点
	topLevelFilter := bson.M{
		"organization_id": organizationID,
		"user_type":       chat.OrganizationUserTypeUser,
		"$or": []bson.M{
			{"level": 1},
			{"level1_parent": ""},
			{"level1_parent": bson.M{"$exists": false}},
		},
	}

	// 获取所有顶级用户
	topLevelUsers, err := findUsers(ctx, collection, topLevelFilter)
	if err != nil {
		log.ZError(ctx, "Failed to find top level users", err)
		return err
	}

	log.ZInfo(ctx, "Found top level users",
		"count", len(topLevelUsers))

	// 批量更新顶级用户的上级为根节点
	const batchSize = 100
	batch := make([]mongo.WriteModel, 0, batchSize)
	usersProcessed := 0

	for _, user := range topLevelUsers {
		// 为顶级用户设置根节点为上级
		update := bson.M{
			"$set": bson.M{
				"level":         1,
				"ancestor_path": []string{rootNode.UserId},
				"level1_parent": rootNode.UserId,
				"level2_parent": "",
				"level3_parent": "",
			},
		}

		batch = append(batch, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"user_id": user.UserID, "organization_id": organizationID}).
			SetUpdate(update))

		// 执行批量更新
		if len(batch) >= batchSize {
			if len(batch) > 0 {
				_, err := collection.BulkWrite(ctx, batch, options.BulkWrite())
				if err != nil {
					log.ZError(ctx, "Failed to execute batch update for top level users", err)
					return err
				}
			}

			usersProcessed += len(batch)
			batch = batch[:0] // 清空批次
		}
	}

	// 执行剩余的批量更新
	if len(batch) > 0 {
		_, err := collection.BulkWrite(ctx, batch, options.BulkWrite())
		if err != nil {
			log.ZError(ctx, "Failed to execute final batch update for top level users", err)
			return err
		}
		usersProcessed += len(batch)
	}

	log.ZInfo(ctx, "Updated top level users",
		"count", usersProcessed)

	// 步骤4: 迁移现有用户的层级关系
	// 使用现有的迁移功能，但跳过已经处理的顶级用户
	err = MigrateHierarchyData(ctx, organizationID)
	if err != nil {
		log.ZError(ctx, "Failed to migrate hierarchy relationships", err)
		return err
	}

	// 步骤5: 更新根节点的团队规模和直接下级数量
	// 计算根节点的直接下级数量（level=1的用户）
	directDownlineCount, err := collection.CountDocuments(
		ctx,
		bson.M{
			"organization_id": organizationID,
			"level":           1,
			"user_id":         bson.M{"$ne": rootNode.UserId},
		},
	)
	if err != nil {
		log.ZWarn(ctx, "Failed to count root node direct downlines", err)
	}

	// 计算根节点的团队规模（即组织中除了根节点外的所有用户）
	teamSize, err := collection.CountDocuments(
		ctx,
		bson.M{
			"organization_id": organizationID,
			"user_type":       chat.OrganizationUserTypeUser,
		},
	)
	if err != nil {
		log.ZWarn(ctx, "Failed to count root node team size", err)
	}

	// 更新根节点的计数
	rootNodeUpdate := bson.M{
		"$set": bson.M{
			"direct_downline_count": directDownlineCount,
			"team_size":             teamSize,
		},
	}

	_, err = collection.UpdateOne(
		ctx,
		bson.M{"user_id": rootNode.UserId, "organization_id": organizationID},
		rootNodeUpdate,
	)
	if err != nil {
		log.ZWarn(ctx, "Failed to update root node counts", err)
	}

	elapsedTime := time.Since(startTime)
	log.ZInfo(ctx, "Completed migration to virtual root node model",
		"organization_id", organizationID.Hex(),
		"duration_seconds", elapsedTime.Seconds(),
		"root_node_team_size", teamSize,
		"root_node_direct_downlines", directDownlineCount)

	return nil
}

// 查找用户的辅助函数
func findUsers(ctx context.Context, collection *mongo.Collection, filter interface{}) ([]struct {
	UserID string `bson:"user_id"`
}, error) {
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var users []struct {
		UserID string `bson:"user_id"`
	}

	if err := cursor.All(ctx, &users); err != nil {
		return nil, err
	}

	return users, nil
}
