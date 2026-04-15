// Copyright © 2023 OpenIM open source community. All rights reserved.

package svc

import (
	"context"
	"errors"

	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// GetUserForTeamInfo 获取用户的团队信息
func (s *HierarchyService) GetUserForTeamInfo(ctx context.Context, organizationID primitive.ObjectID, userID string) (*chat.OrganizationUser, error) {
	collection := s.db.Collection("organization_user")

	// 查询条件：组织ID和用户ID
	filter := bson.M{
		"organization_id": organizationID,
		"user_id":         userID,
	}

	// 查询用户记录
	var user chat.OrganizationUser
	err := collection.FindOne(ctx, filter).Decode(&user)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			log.ZWarn(ctx, "用户不存在或不属于该组织",
				errors.New("user not found"),
				"userID", userID,
				"organizationID", organizationID.Hex())
			return nil, errors.New("用户不存在或不属于该组织")
		}
		log.ZError(ctx, "查询用户信息失败", err,
			"userID", userID,
			"organizationID", organizationID.Hex())
		return nil, err
	}

	return &user, nil
}
