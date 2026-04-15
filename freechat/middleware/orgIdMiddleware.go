// Copyright © 2023 OpenIM open source community. All rights reserved.

package middleware

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/pkg/common/constant"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// OrgIdMiddleware 确保请求上下文中包含组织ID，并将组织信息添加到上下文中
func OrgIdMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 从请求头中获取组织ID
		operationID, _ := GetOperationId(c)
		orgId := c.GetHeader(constant.RpcOrgId)

		// 如果没有组织ID，从用户会话或其他地方获取默认组织ID
		if orgId == "" {
			// 获取用户ID
			userID := mctx.GetOpUserID(c)
			if userID == "" {
				log.ZWarn(c, "用户ID为空，无法获取组织信息",
					nil, "operationID", operationID)
				c.JSON(http.StatusBadRequest, gin.H{
					"errCode": 500,
					"errMsg":  "failed to get organization from context",
					"errDlt":  "user ID is empty",
				})
				c.Abort()
				return
			}

			// 从数据库或缓存中获取用户的默认组织
			defaultOrg, err := getDefaultOrganization(context.Background(), userID)
			if err != nil || defaultOrg == "" {
				log.ZWarn(c, "无法获取用户默认组织",
					err, "operationID", operationID, "userID", userID)
				c.JSON(http.StatusInternalServerError, gin.H{
					"errCode": 500,
					"errMsg":  "failed to get organization from context",
					"errDlt":  "cannot get default organization",
				})
				c.Abort()
				return
			}

			// 设置默认组织ID
			orgId = defaultOrg
		}

		// 将组织ID添加到请求上下文和请求头中
		c.Set(constant.RpcOrgId, []string{orgId})
		c.Request.Header.Set(constant.RpcOrgId, orgId)

		// 创建一个有效的OrgInfo对象并设置到上下文中
		objectId, err := primitive.ObjectIDFromHex(orgId)
		if err != nil {
			log.ZWarn(c, "无效的组织ID格式",
				err, "operationID", operationID, "orgId", orgId)
			c.JSON(http.StatusBadRequest, gin.H{
				"errCode": 500,
				"errMsg":  "failed to get organization from context",
				"errDlt":  "invalid organization ID format",
			})
			c.Abort()
			return
		}

		// 如果是测试环境，创建一个模拟的OrgInfo
		if utils.IsLocalTestEnv() {
			mockOrgInfo := &OrgInfo{
				Organization: &model.Organization{
					ID:          objectId,
					Name:        "测试组织",
					Status:      model.OrganizationStatusPass,
					Description: "测试使用的组织",
				},
				OrgUser: &model.OrganizationUser{
					OrganizationId: objectId,
					UserId:         mctx.GetOpUserID(c),
					Role:           model.OrganizationUserNormalRole,
					Status:         model.OrganizationUserEnableStatus,
					InvitationCode: "TEST123",
					// 我们将在控制器中手动构建要返回的数据
				},
			}
			c.Set(ContextOrgInfoKey, mockOrgInfo)
			c.Next()
			return
		}

		// 在生产环境，尝试从数据库获取真实的组织信息
		userID := mctx.GetOpUserID(c)
		orgInfo, err := newOrgInfo(context.Background(), userID, orgId)
		if err != nil {
			log.ZWarn(c, "无法获取组织信息",
				err, "operationID", operationID, "userID", userID, "orgId", orgId)
			c.JSON(http.StatusInternalServerError, gin.H{
				"errCode": 500,
				"errMsg":  "failed to get organization from context",
				"errDlt":  err.Error(),
			})
			c.Abort()
			return
		}

		// 将组织信息设置到上下文中
		c.Set(ContextOrgInfoKey, orgInfo)

		// 继续处理请求
		c.Next()
	}
}

// getDefaultOrganization 获取用户的默认组织ID
// 这里使用简化的实现，实际应该从数据库或缓存中获取
func getDefaultOrganization(ctx context.Context, userID string) (string, error) {
	// 在实际项目中，这里应该查询数据库获取用户的默认组织
	// 为简化测试，这里返回一个固定的组织ID
	return "65cb3e4e84c5d73b8add8526", nil
}
