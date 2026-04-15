package middleware

import (
	"context"
	"errors"
	"fmt"
	orgCache "github.com/openimsdk/chat/freechat/apps/organization/cache"
	"slices"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	// ContextOrgInfoKey 用于在 gin.Context 中存储组织信息的 key
	ContextOrgInfoKey = "orgInfo"
)

type OrgInfo struct {
	*model.Organization
	OrgUser *model.OrganizationUser
}

func newOrgInfo(sessionCtx context.Context, userID string, orgId string) (*OrgInfo, error) {
	// 查询用户的组织ID
	db := plugin.MongoCli().GetDB()
	organizationUserDao := model.NewOrganizationUserDao(db)

	objectId, err := primitive.ObjectIDFromHex(orgId)
	if err != nil {
		return nil, err
	}
	orgUser, err := organizationUserDao.GetByUserIdAndOrgId(sessionCtx, userID, objectId)
	if err != nil {
		return nil, freeErrors.NotFoundErrWithResource("user id : " + userID)
	}

	if orgUser.Status == model.OrganizationUserDisableStatus {
		return nil, freeErrors.ForbiddenErr(fmt.Sprintf("Access forbidden! user is disable, user id: %s", orgUser.UserId))
	}

	organization, err := orgCache.NewOrgCacheRedis(plugin.RedisCli(), db).GetByIdAndStatus(sessionCtx, orgUser.OrganizationId, model.OrganizationStatusPass)
	if err != nil {
		return nil, freeErrors.ForbiddenErr(fmt.Sprintf("Access forbidden! Failed to query user organization, organization id: %s", orgUser.OrganizationId))
	}

	return &OrgInfo{
		Organization: organization,
		OrgUser:      orgUser,
	}, nil

}

// CheckOrganization 验证组织用户是否有权限访问某个接口
func CheckOrganization(allowRole ...model.OrganizationUserRole) gin.HandlerFunc {
	return func(c *gin.Context) {
		userID, _, err := mctx.Check(c)
		orgId, _ := mctx.GetOrgId(c)
		if err != nil {
			apiresp.GinError(c, err)
			c.Abort()
			return
		}

		orgInfo, err := newOrgInfo(context.Background(), userID, orgId)
		if err != nil {
			apiresp.GinError(c, err)
			c.Abort()
			return
		}

		// 将组织信息存储到上下文中
		c.Set(ContextOrgInfoKey, orgInfo)

		if len(allowRole) <= 0 {
			allowRole = []model.OrganizationUserRole{model.OrganizationUserBackendAdminRole, model.OrganizationUserSuperAdminRole}
		}
		if !slices.Contains(allowRole, orgInfo.OrgUser.Role) {
			apiresp.GinError(c, freeErrors.ForbiddenErr(fmt.Sprintf("Access forbidden! user id: %s, user role: %s", orgInfo.OrgUser.UserId, orgInfo.OrgUser.Role)))
			c.Abort()
			return
		}

		c.Next()
	}
}

// GetOrgInfoFromCtx 从 gin.Context 中获取组织信息
func GetOrgInfoFromCtx(c *gin.Context) (*OrgInfo, error) {
	value, exists := c.Get(ContextOrgInfoKey)
	if !exists {
		return nil, errors.New("failed to get organization from context")
	}
	organizationInfo, ok := value.(*OrgInfo)
	if !ok {
		log.ZError(c.Request.Context(), "Failed to get organization from context", nil, "key", ContextOrgInfoKey, "value", value)
		return nil, errors.New("failed to get organization from context")
	}
	return organizationInfo, nil
}
