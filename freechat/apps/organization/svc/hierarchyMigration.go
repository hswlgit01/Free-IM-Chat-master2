// Copyright © 2023 OpenIM open source community. All rights reserved.

package svc

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MigrateHierarchyData migrates existing users to populate hierarchy fields
// This function processes all users regardless of status
func MigrateHierarchyData(ctx context.Context, organizationID primitive.ObjectID) error {
	// Log start of migration
	log.ZInfo(ctx, "Starting hierarchy data migration",
		"organization_id", organizationID.Hex())

	startTime := time.Now()
	db := plugin.MongoCli().GetDB()
	collection := db.Collection("organization_user")

	// Find all users in this organization
	filter := bson.M{
		"organization_id": organizationID,
	}

	// Find users that have inviter but missing hierarchy data
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		log.ZError(ctx, "Failed to fetch users for hierarchy migration", err)
		return err
	}
	defer cursor.Close(ctx)

	// Process users batch by batch
	const batchSize = 100
	batch := make([]mongo.WriteModel, 0, batchSize)
	usersProcessed := 0
	type UserInfo struct {
		UserID              string
		Inviter             string
		InviterType         string
		InvitationCode      string
		Level               int
		AncestorPath        []string
		Level1Parent        string
		Level2Parent        string
		Level3Parent        string
		DirectDownlineCount int64
		TeamSize            int64
	}
	usersByID := make(map[string]UserInfo)

	// First pass: collect all users and their invitation codes
	for cursor.Next(ctx) {
		var user struct {
			UserID              string   `bson:"user_id"`
			Inviter             string   `bson:"inviter"`
			InviterType         string   `bson:"inviter_type"`
			InvitationCode      string   `bson:"invitation_code"`
			Level               int      `bson:"level"`
			AncestorPath        []string `bson:"ancestor_path"`
			Level1Parent        string   `bson:"level1_parent"`
			Level2Parent        string   `bson:"level2_parent"`
			Level3Parent        string   `bson:"level3_parent"`
			DirectDownlineCount int64    `bson:"direct_downline_count"`
			TeamSize            int64    `bson:"team_size"`
		}

		if err := cursor.Decode(&user); err != nil {
			log.ZWarn(ctx, "Failed to decode user during hierarchy migration", err)
			continue
		}

		usersByID[user.UserID] = UserInfo{
			UserID:              user.UserID,
			Inviter:             user.Inviter,
			InviterType:         user.InviterType,
			InvitationCode:      user.InvitationCode,
			Level:               user.Level,
			AncestorPath:        user.AncestorPath,
			Level1Parent:        user.Level1Parent,
			Level2Parent:        user.Level2Parent,
			Level3Parent:        user.Level3Parent,
			DirectDownlineCount: user.DirectDownlineCount,
			TeamSize:            user.TeamSize,
		}
	}

	if err := cursor.Err(); err != nil {
		log.ZError(ctx, "Error during cursor iteration in hierarchy migration", err)
		return err
	}

	// Second pass: build the hierarchy (top-down approach)
	processedUsers := make(map[string]bool)

	// 迭代处理直到没有新的用户被处理
	for {
		newProcessed := false

		for userID, user := range usersByID {
			// 跳过已处理的用户
			if processedUsers[userID] {
				continue
			}

			// 处理特殊情况: 根本没有邀请者 或 邀请者类型为组织
			if user.Inviter == "" || user.InviterType == "org" {
				// 将这些用户设置为顶级用户 (level 1)
				if user.Level != 1 || len(user.AncestorPath) != 0 ||
					user.Level1Parent != "" || user.Level2Parent != "" || user.Level3Parent != "" {

					// 准备更新
					update := bson.M{
						"$set": bson.M{
							"level":         1,
							"ancestor_path": []string{},
							"level1_parent": "",
							"level2_parent": "",
							"level3_parent": "",
						},
					}

					batch = append(batch, mongo.NewUpdateOneModel().
						SetFilter(bson.M{"user_id": userID, "organization_id": organizationID}).
						SetUpdate(update))
				}
				processedUsers[userID] = true
				newProcessed = true
				continue
			}

			// 对于普通用户，找到邀请者
			// 确认邀请者类型为"orgUser"
			if user.InviterType == "orgUser" {
				// 根据邀请者ID查找父用户
				parentID := user.Inviter

				// 检查父用户是否已经处理过
				parent, exists := usersByID[parentID]
				if !exists {
					// 如果找不到父用户，设为顶级用户
					if user.Level != 1 || len(user.AncestorPath) != 0 ||
						user.Level1Parent != "" || user.Level2Parent != "" || user.Level3Parent != "" {

						update := bson.M{
							"$set": bson.M{
								"level":         1,
								"ancestor_path": []string{},
								"level1_parent": "",
								"level2_parent": "",
								"level3_parent": "",
							},
						}

						batch = append(batch, mongo.NewUpdateOneModel().
							SetFilter(bson.M{"user_id": userID, "organization_id": organizationID}).
							SetUpdate(update))
					}
					processedUsers[userID] = true
					newProcessed = true
					continue
				}

				// 如果父用户未处理，则跳过当前用户，等待父用户处理完毕
				if !processedUsers[parentID] {
					continue
				}

				// 父用户已处理，使用其信息构建当前用户的层级关系
				level := parent.Level + 1
				ancestorPath := append([]string{parentID}, parent.AncestorPath...)

				// 获取一级、二级、三级父级
				level1Parent := parentID
				level2Parent := ""
				level3Parent := ""

				if len(parent.AncestorPath) > 0 {
					level2Parent = parent.AncestorPath[0]

					if len(parent.AncestorPath) > 1 {
						level3Parent = parent.AncestorPath[1]
					}
				}

				// 检查是否需要更新
				if user.Level != level ||
					!equalStringSlices(user.AncestorPath, ancestorPath) ||
					user.Level1Parent != level1Parent ||
					user.Level2Parent != level2Parent ||
					user.Level3Parent != level3Parent {

					// 准备更新
					update := bson.M{
						"$set": bson.M{
							"level":         level,
							"ancestor_path": ancestorPath,
							"level1_parent": level1Parent,
							"level2_parent": level2Parent,
							"level3_parent": level3Parent,
						},
					}

					batch = append(batch, mongo.NewUpdateOneModel().
						SetFilter(bson.M{"user_id": userID, "organization_id": organizationID}).
						SetUpdate(update))
				}

				processedUsers[userID] = true
				newProcessed = true

				// 执行批量更新（如果达到批处理大小）
				if len(batch) >= batchSize {
					if len(batch) > 0 {
						_, err := collection.BulkWrite(ctx, batch, options.BulkWrite())
						if err != nil {
							log.ZError(ctx, "Failed to execute batch update during hierarchy migration", err)
							return err
						}
					}

					usersProcessed += len(batch)
					batch = batch[:0] // 清空批次
				}
			}
		}

		// 如果这一轮没有新处理的用户，跳出循环
		if !newProcessed {
			break
		}
	}

	// 执行剩余的批量更新
	if len(batch) > 0 {
		_, err := collection.BulkWrite(ctx, batch, options.BulkWrite())
		if err != nil {
			log.ZError(ctx, "Failed to execute final batch update during hierarchy migration", err)
			return err
		}
		usersProcessed += len(batch)
	}

	// 现在更新所有用户的团队规模和直接下级数量
	cursor, err = collection.Find(ctx, filter)
	if err != nil {
		log.ZError(ctx, "Failed to fetch users for team size update", err)
		return err
	}
	defer cursor.Close(ctx)

	batch = batch[:0] // 清空批次
	for cursor.Next(ctx) {
		var user struct {
			UserID string `bson:"user_id"`
		}

		if err := cursor.Decode(&user); err != nil {
			log.ZWarn(ctx, "Failed to decode user during team size update", err)
			continue
		}

		// 计算直接下级数量
		directDownlineCount, err := collection.CountDocuments(
			ctx,
			bson.M{
				"organization_id": organizationID,
				"level1_parent":   user.UserID,
			},
		)
		if err != nil {
			log.ZWarn(ctx, "Failed to count direct downlines", err)
			continue
		}

		// 计算总团队规模 - 全新递归计算策略
		// 统计所有直接或间接下级 - 递归计算

		// 1. 首先计算直接下级数量（这个是准确的）
		// 已经在前面计算过了，直接使用

		// 2. 获取所有直接下级
		cursor2, err2 := collection.Find(
			ctx,
			bson.M{
				"organization_id": organizationID,
				"level1_parent":   user.UserID,
			},
		)

		// 3. 递归计算所有直接下级的团队规模
		var indirectDownlines int64 = 0
		if err2 == nil {
			defer cursor2.Close(ctx)
			for cursor2.Next(ctx) {
				var childUser struct {
					UserID   string `bson:"user_id"`
					TeamSize int64  `bson:"team_size"`
				}

				if err := cursor2.Decode(&childUser); err == nil && childUser.UserID != "" {
					// 添加每个子用户的团队规模
					indirectDownlines += childUser.TeamSize
				}
			}
		}

		// 4. 总团队规模 = 直接下级数量 + 所有直接下级的团队规模总和
		teamSize := directDownlineCount + indirectDownlines

		// 更新计数
		update := bson.M{
			"$set": bson.M{
				"direct_downline_count": directDownlineCount,
				"team_size":             teamSize,
			},
		}

		batch = append(batch, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"user_id": user.UserID, "organization_id": organizationID}).
			SetUpdate(update))

		// 执行批量更新（如果达到批处理大小）
		if len(batch) >= batchSize {
			if len(batch) > 0 {
				_, err := collection.BulkWrite(ctx, batch, options.BulkWrite())
				if err != nil {
					log.ZError(ctx, "Failed to execute batch update during team size update", err)
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
			log.ZError(ctx, "Failed to execute final batch update during team size update", err)
			return err
		}
		usersProcessed += len(batch)
	}

	// 在invitation_code上创建索引，提高性能
	_, err = collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "invitation_code", Value: 1},
		},
		Options: options.Index().SetBackground(true),
	})
	if err != nil {
		log.ZWarn(ctx, "Failed to create index on invitation_code", err)
	}

	elapsedTime := time.Since(startTime)
	log.ZInfo(ctx, "Completed hierarchy data migration",
		"organization_id", organizationID.Hex(),
		"users_processed", usersProcessed,
		"duration_seconds", elapsedTime.Seconds())

	return nil
}

// 不再需要重复定义 equalStringSlices 函数
// 直接使用 hierarchySvc.go 中定义的函数
