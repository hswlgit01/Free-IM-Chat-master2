package model

import (
	"context"
	"regexp"
	"strings"
	"time"

	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/chat/tools/db/pagination"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type OrganizationUserRole string

const (
	OrganizationUserSuperAdminRole   OrganizationUserRole = "SuperAdmin"
	OrganizationUserBackendAdminRole OrganizationUserRole = "BackendAdmin"
	OrganizationUserGroupManagerRole OrganizationUserRole = "GroupManager"
	OrganizationUserTermManagerRole  OrganizationUserRole = "TermManager"
	OrganizationUserNormalRole       OrganizationUserRole = "Normal"
)

var AllOrganizationUserRole = []OrganizationUserRole{
	OrganizationUserSuperAdminRole,
	OrganizationUserBackendAdminRole,
	OrganizationUserGroupManagerRole,
	OrganizationUserTermManagerRole,
	OrganizationUserNormalRole,
}

// IsOrgWebElevatedRole 组织管理台可设置的「管理员 / 团队长」等非普通成员角色（与权限表 role 一致）
func IsOrgWebElevatedRole(r OrganizationUserRole) bool {
	return r == OrganizationUserGroupManagerRole || r == OrganizationUserTermManagerRole
}

type OrganizationUserStatus string

const (
	OrganizationUserDisableStatus OrganizationUserStatus = "Disable"
	OrganizationUserEnableStatus  OrganizationUserStatus = "Enable"
)

type OrganizationUserRegisterType string

const (
	OrganizationUserRegisterTypeH5      OrganizationUserRegisterType = "h5"
	OrganizationUserRegisterTypeBackend OrganizationUserRegisterType = "backend"
	OrganizationUserRegisterTypeWeb     OrganizationUserRegisterType = "web"
)

type OrganizationUserInviterType string

const (
	OrganizationUserInviterTypeOrg     OrganizationUserInviterType = "org"
	OrganizationUserInviterTypeOrgUser OrganizationUserInviterType = "orgUser"
)

const OrgUserInvitationCodeLength = 8

type OrganizationUser struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrganizationId primitive.ObjectID     `bson:"organization_id" json:"organization_id"`
	ThirdUserId    string                 `bson:"third_user_id" json:"third_user_id"`
	UserId         string                 `bson:"user_id" json:"user_id"`
	Role           OrganizationUserRole   `bson:"role" json:"role"`
	Status         OrganizationUserStatus `bson:"status" json:"status"`

	ImServerUserId string                       `bson:"im_server_user_id" json:"im_server_user_id"`
	InvitationCode string                       `bson:"invitation_code" json:"invitation_code"`
	RegisterType   OrganizationUserRegisterType `bson:"register_type" json:"register_type"`

	Inviter               string                      `bson:"inviter" json:"inviter"`
	InviterType           OrganizationUserInviterType `bson:"inviter_type" json:"inviter_type"`
	InviterImServerUserId string                      `bson:"inviter_im_server_user_id" json:"inviter_im_server_user_id"`

	Tags      []primitive.ObjectID `bson:"tags,omitempty" json:"tags,omitempty"` // 用户标签ID数组
	Points    int64                `bson:"points" json:"points"`
	CreatedAt time.Time            `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time            `bson:"updated_at" json:"updated_at"`
}

// OrganizationUserWithLoginRecord 扩展结构体，用于聚合查询返回（包含登录记录信息）
type OrganizationUserWithLoginRecord struct {
	OrganizationUser `bson:",inline"`
	LoginRecord      *chatModel.UserLoginRecord `bson:"login_record,omitempty" json:"login_record,omitempty"`
}

// OrganizationUserWithUser 简化的扩展结构体，只包含用户基本信息
type OrganizationUserWithUser struct {
	OrganizationUser `bson:",inline"`
	User             *openImModel.User `bson:"user" json:"user,omitempty"`
}

func (OrganizationUser) TableName() string {
	return constant.CollectionOrganizationUser
}

func CreateOrganizationUserIndex(db *mongo.Database) error {
	m := &OrganizationUser{}

	coll := db.Collection(m.TableName())
	// 索引合并说明：
	// - 不设单独 organization_id：多数复合索引以 organization_id 为首字段，前缀即可支持「仅按组织」过滤。
	// - 保留 user_id / third_user_id / im_server_user_id 单列：存在不按 organization_id 查的场景（前缀无法替代）。
	// - 去掉 {org, role, status}：与 {org, role, status, created_at} 前缀重复，后者已覆盖前三列等值查询。
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "user_id", Value: 1},
			},
		},
		// 超管用户列表：attribute $lookup 子管道按 user_id + role 过滤
		{
			Keys: bson.D{
				{Key: "user_id", Value: 1},
				{Key: "role", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "third_user_id", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "im_server_user_id", Value: 1},
			},
		},
		// 复合索引优化 - 针对默认角色查询场景（最高优先级）
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "role", Value: 1}, // 角色过滤提前，因为是高频查询
				{Key: "status", Value: 1},
				{Key: "created_at", Value: -1}, // 降序，用于排序
			},
		},
		// 备用索引：纯组织+状态查询
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "status", Value: 1},
				{Key: "created_at", Value: -1},
			},
		},
		// 标签查询优化索引
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "tags", Value: 1},
			},
		},
		// 用户ID筛选优化索引
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "user_id", Value: 1},
			},
		},
		// ImServerUserId筛选优化索引
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "im_server_user_id", Value: 1},
			},
		},
		// 红包/转账高并发校验：组织 + 角色 + 状态 + IM用户ID（覆盖投影查询）
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "role", Value: 1},
				{Key: "status", Value: 1},
				{Key: "im_server_user_id", Value: 1},
			},
		},
		// 时间范围查询优化索引
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "created_at", Value: 1},
			},
		},
		// 邀请码查询索引
		{
			Keys: bson.D{
				{Key: "invitation_code", Value: 1},
			},
		},
		// 层级管理：组织根下 level=1 用户列表 + 按注册时间排序
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "level", Value: 1},
				{Key: "created_at", Value: 1},
			},
		},
		// 层级管理：某用户直接下级（level1_parent）+ 按注册时间排序
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "level1_parent", Value: 1},
				{Key: "created_at", Value: 1},
			},
		},
		// 层级搜索：组织 + 用户类型 + 时间排序
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "user_type", Value: 1},
				{Key: "created_at", Value: 1},
			},
		},
		// 组织内邀请码（层级搜索前缀匹配）
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
				{Key: "invitation_code", Value: 1},
			},
		},
	})
	return err
}

type OrganizationUserDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewOrganizationUserDao(db *mongo.Database) *OrganizationUserDao {
	return &OrganizationUserDao{
		DB:         db,
		Collection: db.Collection(OrganizationUser{}.TableName()),
	}
}

func (o *OrganizationUserDao) ExistByThirdUserIdAndOrganizationId(ctx context.Context, ThirdUserId string, organizationId primitive.ObjectID) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"third_user_id": ThirdUserId, "organization_id": organizationId})
}

func (o *OrganizationUserDao) GetByThirdUserIdAndOrganizationId(ctx context.Context, ThirdUserId string, organizationId primitive.ObjectID) (*OrganizationUser, error) {
	return mongoutil.FindOne[*OrganizationUser](ctx, o.Collection, bson.M{"third_user_id": ThirdUserId, "organization_id": organizationId})
}

func (o *OrganizationUserDao) Create(ctx context.Context, obj *OrganizationUser, enabled ...bool) error {
	isEnabled := true
	if len(enabled) > 0 {
		isEnabled = enabled[0]
	}
	if isEnabled {
		for i := 0; i < 20; i++ {
			invitationCode := utils.RandomNumString(OrgUserInvitationCodeLength)
			_, err := o.GetByInvitationCode(ctx, invitationCode)
			if err == nil {
				continue
			} else if dbutil.IsDBNotFound(err) {
				obj.InvitationCode = invitationCode
				break
			} else {
				return err
			}
		}
	}
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*OrganizationUser{obj})
}

func (o *OrganizationUserDao) GetByUserId(ctx context.Context, userId string) (*OrganizationUser, error) {
	return mongoutil.FindOne[*OrganizationUser](ctx, o.Collection, bson.M{"user_id": userId})
}

// GetByImServerUserId 通过 OpenIM 服务的用户ID查找组织用户
// 这个方法用于支持客户端传入 OpenIM ID 的场景
func (o *OrganizationUserDao) GetByImServerUserId(ctx context.Context, imServerUserId string) (*OrganizationUser, error) {
	return mongoutil.FindOne[*OrganizationUser](ctx, o.Collection, bson.M{"im_server_user_id": imServerUserId})
}

func (o *OrganizationUserDao) GetByInvitationCode(ctx context.Context, invitationCode string) (*OrganizationUser, error) {
	return mongoutil.FindOne[*OrganizationUser](ctx, o.Collection, bson.M{"invitation_code": invitationCode})
}

func (o *OrganizationUserDao) GetByUserIdAndOrgId(ctx context.Context, userId string, organizationId primitive.ObjectID) (*OrganizationUser, error) {
	return mongoutil.FindOne[*OrganizationUser](ctx, o.Collection, bson.M{"user_id": userId, "organization_id": organizationId})
}

// ListByOrgIdAndUserIDs 批量校验用户是否属于指定组织（索引：organization_id + user_id）
func (o *OrganizationUserDao) ListByOrgIdAndUserIDs(ctx context.Context, organizationId primitive.ObjectID, userIDs []string) ([]*OrganizationUser, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	return mongoutil.Find[*OrganizationUser](ctx, o.Collection, bson.M{
		"organization_id": organizationId,
		"user_id":         bson.M{"$in": userIDs},
	})
}

func (o *OrganizationUserDao) Select(ctx context.Context, userId string, organizationId primitive.ObjectID, roles []OrganizationUserRole) ([]*OrganizationUser, error) {
	filter := bson.M{}

	if userId != "" {
		filter["user_id"] = userId
	}

	if organizationId != primitive.NilObjectID {
		filter["organization_id"] = organizationId
	}

	if len(roles) != 0 {
		filter["role"] = bson.M{"$in": roles}
	}

	findOptions := options.Find()
	findOptions.SetSort(bson.D{{Key: "created_at", Value: -1}})

	data, err := mongoutil.Find[*OrganizationUser](ctx, o.Collection, filter, findOptions)
	if err != nil {
		return nil, err
	}

	return data, nil
}
func (o *OrganizationUserDao) GetByUserIdAndOrgID(ctx context.Context, userId string, orgId string) (*OrganizationUser, error) {
	// 将字符串形式的组织ID转换为ObjectID
	organizationObjectID, err := primitive.ObjectIDFromHex(orgId)
	if err != nil {
		log.ZError(ctx, "无效的组织ID格式", err, "org_id", orgId)
		return nil, err
	}

	return mongoutil.FindOne[*OrganizationUser](ctx, o.Collection, bson.M{"user_id": userId, "organization_id": organizationObjectID})
}

// GetByImServerUserIdAndOrgID 根据 IM 用户 ID 和组织 ID 查询组织用户（用于压测/客户端传 im_server_user_id 的场景）
func (o *OrganizationUserDao) GetByImServerUserIdAndOrgID(ctx context.Context, imServerUserId string, orgId string) (*OrganizationUser, error) {
	organizationObjectID, err := primitive.ObjectIDFromHex(orgId)
	if err != nil {
		log.ZError(ctx, "无效的组织ID格式", err, "org_id", orgId)
		return nil, err
	}
	return mongoutil.FindOne[*OrganizationUser](ctx, o.Collection, bson.M{"im_server_user_id": imServerUserId, "organization_id": organizationObjectID})
}

func (o *OrganizationUserDao) GetByUserIMServerUserId(ctx context.Context, userId string) (*OrganizationUser, error) {
	return mongoutil.FindOne[*OrganizationUser](ctx, o.Collection, bson.M{"im_server_user_id": userId})
}

// GetByIMServerUserIdsAndOrgId 根据IM服务器用户ID列表和组织ID批量查询用户
func (o *OrganizationUserDao) GetByIMServerUserIdsAndOrgId(ctx context.Context, imServerUserIds []string, organizationId primitive.ObjectID) ([]*OrganizationUser, error) {
	if len(imServerUserIds) == 0 {
		return []*OrganizationUser{}, nil
	}

	return mongoutil.Find[*OrganizationUser](ctx, o.Collection, bson.M{
		"im_server_user_id": bson.M{"$in": imServerUserIds},
		"organization_id":   organizationId,
	})
}

// GetByIMServerUserIds 根据IM服务器用户ID列表批量查询用户
func (o *OrganizationUserDao) GetByIMServerUserIds(ctx context.Context, imServerUserIds []string) ([]*OrganizationUser, error) {
	if len(imServerUserIds) == 0 {
		return []*OrganizationUser{}, nil
	}

	return mongoutil.Find[*OrganizationUser](ctx, o.Collection, bson.M{
		"im_server_user_id": bson.M{"$in": imServerUserIds},
	})
}

type UpdateInfoByIdField struct {
	Role   OrganizationUserRole
	Status OrganizationUserStatus
}

func (o *OrganizationUserDao) UpdateInfoById(ctx context.Context, id primitive.ObjectID, updateField UpdateInfoByIdField) error {
	data := map[string]any{
		"role":       updateField.Role,
		"status":     updateField.Status,
		"updated_at": time.Now().UTC(),
	}
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"_id": id}, bson.M{"$set": data}, false)
}

// AddPointsByImServerUserId 根据 IM 服务器用户ID增加积分
func (o *OrganizationUserDao) AddPointsByImServerUserId(ctx context.Context, imServerUserId string, orgId primitive.ObjectID, points int64) error {
	orgUser, err := o.GetByUserIMServerUserId(ctx, imServerUserId)
	if err != nil {
		return err
	}

	filter := bson.M{
		"im_server_user_id": imServerUserId,
		"organization_id":   orgId,
	}

	if orgUser.Points == 0 {
		return mongoutil.UpdateOne(ctx, o.Collection, filter, bson.M{
			"$set": bson.M{
				"updated_at": time.Now().UTC(),
				"points":     points,
			},
		}, false)
	}
	update := bson.M{
		"$inc": bson.M{"points": points},
		"$set": bson.M{"updated_at": time.Now().UTC()},
	}

	return mongoutil.UpdateOne(ctx, o.Collection, filter, update, false)
}

// UpdateUserTags 更新用户标签
func (o *OrganizationUserDao) UpdateUserTags(ctx context.Context, userId string, orgId primitive.ObjectID, tags []primitive.ObjectID) error {
	updateField := bson.M{
		"$set": bson.M{
			"tags":       tags,
			"updated_at": time.Now().UTC(),
		},
	}
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"im_server_user_id": userId, "organization_id": orgId}, updateField, false)
}

func (o *OrganizationUserDao) SelectJoinUserWithTags(ctx context.Context, organizationId primitive.ObjectID, keyword string, userIds []string, notInImUserIds []string, excludeUserIds []string, roles []OrganizationUserRole,
	status []OrganizationUserStatus, tagIds []primitive.ObjectID, canSendFreeMsg *int32, startTime, endTime *time.Time, page *paginationUtils.DepPagination) (int64, []*OrganizationUserWithUser, error) {

	// 判断是否需要用户表过滤（昵称或消息权限）
	needUserFilter := keyword != "" || canSendFreeMsg != nil

	if needUserFilter {
		// 有昵称过滤，使用连表查询
		result, err := o.selectWithJoinQuery(ctx, organizationId, keyword, userIds, notInImUserIds, excludeUserIds, roles, status, tagIds, canSendFreeMsg, startTime, endTime, page)
		if err != nil {
			return 0, nil, err
		}
		total, err := o.selectCountWithJoinQuery(ctx, organizationId, keyword, userIds, notInImUserIds, excludeUserIds, roles, status, tagIds, canSendFreeMsg, startTime, endTime)
		if err != nil {
			return 0, nil, err
		}
		return total, result, nil
	}
	// 没有昵称过滤，分开查询
	return o.selectWithSeparateQuery(ctx, organizationId, userIds, notInImUserIds, excludeUserIds, roles, status, tagIds, startTime, endTime, page)
}

// mergeOrgUserMainUserIDFilter 同时应用主账号 user_id 白名单（如 login_ip 预筛选）与超管封禁 excludeUserIds。
// 禁止分两次赋值 user_id：后赋值的 $nin 会覆盖先赋值的 $in，导致 IP 等条件失效、返回几乎全组织用户。
func mergeOrgUserMainUserIDFilter(dst bson.M, userIds []string, excludeUserIds []string) {
	cond := bson.M{}
	if len(userIds) > 0 {
		cond["$in"] = userIds
	}
	if len(excludeUserIds) > 0 {
		cond["$nin"] = excludeUserIds
	}
	if len(cond) > 0 {
		dst["user_id"] = cond
	}
}

// selectWithJoinQuery 连表查询方式（用于有昵称过滤的场景）
func (o *OrganizationUserDao) selectWithJoinQuery(ctx context.Context, organizationId primitive.ObjectID, keyword string, userIds []string, notInImUserIds []string, excludeUserIds []string, roles []OrganizationUserRole,
	status []OrganizationUserStatus, tagIds []primitive.ObjectID, canSendFreeMsg *int32, startTime, endTime *time.Time, page *paginationUtils.DepPagination) ([]*OrganizationUserWithUser, error) {

	userModel := openImModel.User{}

	// 构建基础匹配条件
	baseMatchStage := bson.M{
		"organization_id": organizationId,
	}

	if len(roles) > 0 {
		baseMatchStage["role"] = bson.M{"$in": roles}
	}

	if len(status) > 0 {
		baseMatchStage["status"] = bson.M{"$in": status}
	}

	if startTime != nil || endTime != nil {
		timeFilter := bson.M{}
		if startTime != nil {
			timeFilter["$gte"] = *startTime
		}
		if endTime != nil {
			timeFilter["$lte"] = *endTime
		}
		baseMatchStage["created_at"] = timeFilter
	}

	if len(tagIds) > 0 {
		baseMatchStage["tags"] = bson.M{"$all": tagIds}
	}

	mergeOrgUserMainUserIDFilter(baseMatchStage, userIds, excludeUserIds)

	if len(notInImUserIds) > 0 {
		baseMatchStage["im_server_user_id"] = bson.M{"$nin": notInImUserIds}
	}

	// 构建aggregation pipeline
	var pipeline []bson.M

	// 基础过滤
	pipeline = append(pipeline, bson.M{"$match": baseMatchStage})

	// 如果 keyword 存在（昵称左匹配），先在 user 表用索引定位匹配的 user_id
	// 再用 im_server_user_id 在组织用户表上做 $in 过滤，避免把模糊正则留到 $lookup 之后。
	//
	// 此分支下不会再对 joined user 做 nickname regex 过滤，因此可以把分页提前到 $lookup 之前。
	if keyword != "" {
		matchingUserIDs, err := o.findUserIDsByNicknamePrefix(ctx, keyword, canSendFreeMsg)
		if err != nil {
			return nil, err
		}
		if len(matchingUserIDs) == 0 {
			return []*OrganizationUserWithUser{}, nil
		}
		pipeline = append(pipeline, bson.M{
			"$match": bson.M{
				"im_server_user_id": bson.M{"$in": matchingUserIDs},
			},
		})
	}

	// 排序
	pipeline = append(pipeline, bson.M{"$sort": bson.M{"created_at": -1}})

	// 只有在 keyword 分支（已提前过滤）时，才安全把分页提前到 $lookup 之前。
	if keyword != "" && page != nil {
		offset := (page.Page - 1) * page.PageSize
		pipeline = append(pipeline,
			bson.M{"$skip": offset},
			bson.M{"$limit": page.PageSize},
		)
	}

	// 用户表关联（仅用于返回 user 字段）
	pipeline = append(pipeline, bson.M{
		"$lookup": bson.M{
			"from":         userModel.TableName(),
			"localField":   "im_server_user_id",
			"foreignField": "user_id",
			"as":           "user",
		},
	})

	pipeline = append(pipeline, bson.M{
		"$unwind": bson.M{
			"path":                       "$user",
			"preserveNullAndEmptyArrays": false,
		},
	})

	// 非 keyword 分支：仍需要在 joined user 上过滤 can_send_free_msg 等条件。
	if keyword == "" {
		userFilters := buildUserFilters(keyword, canSendFreeMsg)
		if len(userFilters) > 0 {
			pipeline = append(pipeline, bson.M{"$match": userFilters})
		}
		if page != nil {
			offset := (page.Page - 1) * page.PageSize
			pipeline = append(pipeline,
				bson.M{"$skip": offset},
				bson.M{"$limit": page.PageSize},
			)
		}
	}

	// 执行查询
	data, err := mongoutil.Aggregate[*OrganizationUserWithUser](ctx, o.Collection, pipeline)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// findUserIDsByNicknamePrefix 用昵称左前缀（^keyword）在 user 表定位匹配的 user_id。
// 如需过滤 can_send_free_msg，同样按原有语义（0=或缺失/为空）附加到 user 表查询条件中。
func (o *OrganizationUserDao) findUserIDsByNicknamePrefix(ctx context.Context, keyword string, canSendFreeMsg *int32) ([]string, error) {
	prefixRegex := "^" + regexp.QuoteMeta(keyword)

	userFilter := bson.M{
		"nickname": bson.M{
			"$regex":   prefixRegex,
			"$options": "i",
		},
	}

	if canSendFreeMsg != nil {
		if *canSendFreeMsg == 0 {
			// 兼容字段缺失/空值：与原 buildUserFilters 保持一致
			canSendFreeMsgConditions := []bson.M{
				{"can_send_free_msg": 0},
				{"can_send_free_msg": bson.M{"$exists": false}},
				{"can_send_free_msg": nil},
			}
			userFilter = bson.M{
				"$and": []bson.M{
					userFilter,
					{"$or": canSendFreeMsgConditions},
				},
			}
		} else {
			userFilter["can_send_free_msg"] = *canSendFreeMsg
		}
	}

	userModel := openImModel.User{}
	cursor, err := o.DB.Collection(userModel.TableName()).
		Find(ctx, userFilter, options.Find().SetProjection(bson.M{"user_id": 1, "_id": 0}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	type idOnly struct {
		UserID string `bson:"user_id"`
	}
	var rows []idOnly
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, err
	}

	userIDs := make([]string, 0, len(rows))
	for _, r := range rows {
		userIDs = append(userIDs, r.UserID)
	}
	return userIDs, nil
}

func (o *OrganizationUserDao) selectCountWithJoinQuery(ctx context.Context, organizationId primitive.ObjectID, keyword string, userIds []string, notInImUserIds []string, excludeUserIds []string, roles []OrganizationUserRole,
	status []OrganizationUserStatus, tagIds []primitive.ObjectID, canSendFreeMsg *int32, startTime, endTime *time.Time) (int64, error) {
	userDao := openImModel.NewUserDao(o.DB)

	// 构建基础匹配条件
	filter := bson.M{
		"organization_id": organizationId,
	}

	// 组织用户表筛选条件：im_server_user_id（允许同时叠加 $nin 和 $in）
	imUserCond := bson.M{}
	if len(notInImUserIds) > 0 {
		imUserCond["$nin"] = notInImUserIds
	}

	// keyword 分支：用昵称左前缀在 user 表定位 user_id，然后直接 count。
	if keyword != "" {
		imUserIDs, err := o.findUserIDsByNicknamePrefix(ctx, keyword, canSendFreeMsg)
		if err != nil {
			return 0, err
		}
		if len(imUserIDs) == 0 {
			return 0, nil
		}
		imUserCond["$in"] = imUserIDs
	} else if canSendFreeMsg != nil {
		// 仅 canSendFreeMsg 过滤：保持原有逻辑（can_send_free_msg 语义、索引命中）
		userFilters := bson.M{}
		if *canSendFreeMsg == 0 {
			userFilters["$or"] = []bson.M{
				{"can_send_free_msg": 0},
				{"can_send_free_msg": bson.M{"$exists": false}},
				{"can_send_free_msg": nil},
			}
		} else {
			userFilters["can_send_free_msg"] = *canSendFreeMsg
		}

		users, err := userDao.FindFilter(ctx, userFilters)
		if err != nil {
			return 0, err
		}
		imUserIDs := make([]string, 0, len(users))
		for _, u := range users {
			imUserIDs = append(imUserIDs, u.UserID)
		}
		if len(imUserIDs) == 0 {
			return 0, nil
		}
		imUserCond["$in"] = imUserIDs
	}
	if len(imUserCond) > 0 {
		filter["im_server_user_id"] = imUserCond
	}

	if len(roles) > 0 {
		filter["role"] = bson.M{"$in": roles}
	}

	if len(status) > 0 {
		filter["status"] = bson.M{"$in": status}
	}

	if startTime != nil || endTime != nil {
		timeFilter := bson.M{}
		if startTime != nil {
			timeFilter["$gte"] = *startTime
		}
		if endTime != nil {
			timeFilter["$lte"] = *endTime
		}
		filter["created_at"] = timeFilter
	}

	if len(tagIds) > 0 {
		filter["tags"] = bson.M{"$all": tagIds}
	}

	mergeOrgUserMainUserIDFilter(filter, userIds, excludeUserIds)

	// 计算总数
	return mongoutil.Count(ctx, o.Collection, filter)

}

// selectWithSeparateQuery 分离查询方式（用于没有昵称过滤的场景）
func (o *OrganizationUserDao) selectWithSeparateQuery(ctx context.Context, organizationId primitive.ObjectID, userIds []string, notInImUserIds []string, excludeUserIds []string, roles []OrganizationUserRole,
	status []OrganizationUserStatus, tagIds []primitive.ObjectID, startTime, endTime *time.Time, page *paginationUtils.DepPagination) (int64, []*OrganizationUserWithUser, error) {

	// 构建查询条件
	filter := bson.M{
		"organization_id": organizationId,
	}

	if len(roles) > 0 {
		filter["role"] = bson.M{"$in": roles}
	}

	if len(status) > 0 {
		filter["status"] = bson.M{"$in": status}
	}

	if startTime != nil || endTime != nil {
		timeFilter := bson.M{}
		if startTime != nil {
			timeFilter["$gte"] = *startTime
		}
		if endTime != nil {
			timeFilter["$lte"] = *endTime
		}
		filter["created_at"] = timeFilter
	}

	if len(tagIds) > 0 {
		filter["tags"] = bson.M{"$all": tagIds}
	}

	mergeOrgUserMainUserIDFilter(filter, userIds, excludeUserIds)

	if len(notInImUserIds) > 0 {
		filter["im_server_user_id"] = bson.M{"$nin": notInImUserIds}
	}

	// 1. 先查询总数
	count, err := o.Collection.CountDocuments(ctx, filter)
	if err != nil {
		return 0, nil, err
	}
	total := count

	// 2. 查询组织用户数据
	findOptions := options.Find()
	findOptions.SetSort(bson.D{{Key: "created_at", Value: -1}})

	if page != nil {
		offset := (page.Page - 1) * page.PageSize
		findOptions.SetSkip(int64(offset))
		findOptions.SetLimit(int64(page.PageSize))
	}

	cursor, err := o.Collection.Find(ctx, filter, findOptions)
	if err != nil {
		return 0, nil, err
	}
	defer cursor.Close(ctx)

	var orgUsers []*OrganizationUser
	if err = cursor.All(ctx, &orgUsers); err != nil {
		return 0, nil, err
	}

	if len(orgUsers) == 0 {
		return total, []*OrganizationUserWithUser{}, nil
	}

	// 3. 转换为结果格式（用户数据将在service层查询和组装）
	result := make([]*OrganizationUserWithUser, len(orgUsers))
	for i, orgUser := range orgUsers {
		result[i] = &OrganizationUserWithUser{
			OrganizationUser: *orgUser,
			User:             nil, // 用户数据在service层处理
		}
	}

	return total, result, nil
}

// SearchOrgUsersForSuperAdmin 超管「全部用户」列表：以 organization_user 为起点取 user_id、created_at，
// $lookup attribute 取 account；$lookup super_admin_forbidden 排除封禁主表记录；
// keyword 非空时对 user_id、account 做不区分大小写的子串匹配；同一 user_id 多组织时保留 created_at 最新的一条。
func (o *OrganizationUserDao) SearchOrgUsersForSuperAdmin(ctx context.Context, keyword string, p pagination.Pagination) (int64, []*chatModel.SuperAdminUserListItem, error) {
	kw := strings.TrimSpace(keyword)
	attrColl := chatModel.Attribute{}.TableName()

	pipeline := []bson.M{
		{"$lookup": bson.M{
			"from": constant.CollectionSuperAdminForbidden,
			"let":  bson.M{"uid": "$user_id"},
			"pipeline": []bson.M{
				{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$user_id", "$$uid"}}}},
				{"$limit": 1},
				{"$project": bson.M{"_id": 1}},
			},
			"as": "_sa_forbidden",
		}},
		{"$match": bson.M{"_sa_forbidden": bson.M{"$size": 0}}},
		{"$lookup": bson.M{
			"from": attrColl,
			"let":  bson.M{"uid": "$user_id"},
			"pipeline": []bson.M{
				{"$match": bson.M{"$expr": bson.M{"$eq": bson.A{"$user_id", "$$uid"}}}},
				{"$limit": 1},
				{"$project": bson.M{"account": 1}},
			},
			"as": "_attr",
		}},
		{"$unwind": bson.M{"path": "$_attr", "preserveNullAndEmptyArrays": true}},
		{"$addFields": bson.M{
			"account": bson.M{"$ifNull": bson.A{"$_attr.account", ""}},
		}},
	}

	if kw != "" {
		safe := regexp.QuoteMeta(kw)
		pipeline = append(pipeline, bson.M{"$match": bson.M{
			"$or": []bson.M{
				{"user_id": bson.M{"$regex": safe, "$options": "i"}},
				{"account": bson.M{"$regex": safe, "$options": "i"}},
			},
		}})
	}

	pipeline = append(pipeline,
		bson.M{"$sort": bson.M{"created_at": -1}},
		bson.M{"$group": bson.M{
			"_id":         "$user_id",
			"create_time": bson.M{"$first": "$created_at"},
			"account":     bson.M{"$first": "$account"},
		}},
		bson.M{"$project": bson.M{
			"_id":         0,
			"user_id":     "$_id",
			"create_time": 1,
			"account":     1,
		}},
		bson.M{"$sort": bson.M{"create_time": -1}},
	)

	countPipeline := make([]bson.M, len(pipeline))
	copy(countPipeline, pipeline)
	countPipeline = append(countPipeline, bson.M{"$count": "total"})

	countCursor, err := o.Collection.Aggregate(ctx, countPipeline)
	if err != nil {
		log.ZError(ctx, "count super admin org user list error", err)
		return 0, nil, err
	}
	defer countCursor.Close(ctx)

	var countResult []bson.M
	if err = countCursor.All(ctx, &countResult); err != nil {
		log.ZError(ctx, "count super admin org user list decode error", err)
		return 0, nil, err
	}

	var total int64
	if len(countResult) > 0 {
		if totalVal, exists := countResult[0]["total"]; exists {
			switch v := totalVal.(type) {
			case int32:
				total = int64(v)
			case int64:
				total = v
			case int:
				total = int64(v)
			}
		}
	}

	queryPipeline := make([]bson.M, len(pipeline))
	copy(queryPipeline, pipeline)
	if p != nil && p.GetPageNumber() > 0 && p.GetShowNumber() > 0 {
		skip := (p.GetPageNumber() - 1) * p.GetShowNumber()
		queryPipeline = append(queryPipeline,
			bson.M{"$skip": skip},
			bson.M{"$limit": p.GetShowNumber()},
		)
	}

	cursor, err := o.Collection.Aggregate(ctx, queryPipeline)
	if err != nil {
		log.ZError(ctx, "find super admin org user list error", err)
		return 0, nil, err
	}
	defer cursor.Close(ctx)

	var list []*chatModel.SuperAdminUserListItem
	if err = cursor.All(ctx, &list); err != nil {
		log.ZError(ctx, "decode super admin org user list error", err)
		return 0, nil, err
	}

	return total, list, nil
}

// buildUserFilters 构建user表相关的过滤条件（提取为独立函数，避免重复代码）
func buildUserFilters(keyword string, canSendFreeMsg *int32) bson.M {
	userFilters := bson.M{}

	if keyword != "" {
		// 左匹配：^keyword + 正则转义，避免退化成“左右模糊”导致扫描
		leftPrefixRegex := "^" + regexp.QuoteMeta(keyword)
		userFilters["$or"] = []bson.M{
			{"user.nickname": bson.M{"$regex": leftPrefixRegex, "$options": "i"}},
		}
	}

	if canSendFreeMsg != nil {
		if *canSendFreeMsg == 0 {
			canSendFreeMsgConditions := []bson.M{
				{"user.can_send_free_msg": 0},
				{"user.can_send_free_msg": bson.M{"$exists": false}},
				{"user.can_send_free_msg": nil},
			}

			if keyword != "" {
				userFilters = bson.M{
					"$and": []bson.M{
						{"$or": userFilters["$or"].([]bson.M)},
						{"$or": canSendFreeMsgConditions},
					},
				}
			} else {
				userFilters["$or"] = canSendFreeMsgConditions
			}
		} else {
			userFilters["user.can_send_free_msg"] = *canSendFreeMsg
		}
	}

	return userFilters
}

func (o *OrganizationUserDao) SelectJoinUser(ctx context.Context, organizationId primitive.ObjectID, keyword string, userIds []string, notInImUserIds []string, roles []OrganizationUserRole,
	status []OrganizationUserStatus) ([]*OrganizationUser, error) {
	// 聚合查询
	userModel := openImModel.User{}
	pipeline := []bson.M{
		{
			"$lookup": bson.M{
				"from":         userModel.TableName(),
				"localField":   "im_server_user_id",
				"foreignField": "user_id",
				"as":           "user",
			},
		},
	}

	// 构建过滤条件
	filter := bson.M{}
	if organizationId != primitive.NilObjectID {
		filter["organization_id"] = organizationId
	}

	if len(userIds) > 0 {
		filter["user_id"] = bson.M{"$in": userIds}
	}

	if len(notInImUserIds) > 0 {
		filter["user.user_id"] = bson.M{"$nin": notInImUserIds}
	}

	if len(roles) > 0 {
		filter["role"] = bson.M{"$in": roles}
	}

	if len(status) > 0 {
		filter["status"] = bson.M{"$in": status}
	}

	if keyword != "" {
		filter["$or"] = []bson.M{
			{"user.nickname": bson.M{"$regex": keyword, "$options": "i"}},
			//{"third_user_id": bson.M{"$regex": keyword, "$options": "i"}},
			{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
		}
	}

	findPipeline := make([]bson.M, 0)

	if len(filter) > 0 {
		findPipeline = append(pipeline, bson.M{"$match": filter})
	}

	findPipeline = append(findPipeline, bson.M{"$sort": bson.M{"created_at": 1}})

	// 执行聚合查询获取数据
	data, err := mongoutil.Aggregate[*OrganizationUser](ctx, o.Collection, findPipeline)
	return data, err
}

func (o *OrganizationUserDao) CountByOrgIdAndStatus(ctx context.Context, organizationId primitive.ObjectID, notInImUserIds []string, roles []OrganizationUserRole) (int64, error) {
	// 构建过滤条件
	filter := bson.M{}
	if organizationId != primitive.NilObjectID {
		filter["organization_id"] = organizationId
	}

	if len(notInImUserIds) > 0 {
		filter["im_server_user_id"] = bson.M{"$nin": notInImUserIds}
	}

	if len(roles) > 0 {
		filter["role"] = bson.M{"$in": roles}
	}

	return mongoutil.Count(ctx, o.Collection, filter)
}

func (o *OrganizationUserDao) CountVerifiedByOrgId(ctx context.Context, organizationId primitive.ObjectID, notInImUserIds []string, roles []OrganizationUserRole) (int64, error) {
	match := bson.M{
		"organization_id": organizationId,
	}

	if len(notInImUserIds) > 0 {
		match["im_server_user_id"] = bson.M{"$nin": notInImUserIds}
	}

	if len(roles) > 0 {
		match["role"] = bson.M{"$in": roles}
	}

	pipeline := []bson.M{
		{"$match": match},
		{"$lookup": bson.M{
			"from":         "attribute",
			"localField":   "user_id",
			"foreignField": "user_id",
			"as":           "attribute",
		}},
		{"$unwind": bson.M{
			"path":                       "$attribute",
			"preserveNullAndEmptyArrays": false,
		}},
		{"$match": bson.M{
			"attribute.is_real_name_verified": true,
		}},
		{"$group": bson.M{
			"_id": "$user_id",
		}},
		{"$count": "count"},
	}

	type countResult struct {
		Count int64 `bson:"count"`
	}

	result, err := mongoutil.Aggregate[*countResult](ctx, o.Collection, pipeline)
	if err != nil {
		return 0, err
	}
	if len(result) == 0 || result[0] == nil {
		return 0, nil
	}
	return result[0].Count, nil
}

// GetFirstSuperAdminByOrgId 获取指定组织的第一个超级管理员（按创建时间正序）
func (o *OrganizationUserDao) GetFirstSuperAdminByOrgId(ctx context.Context, organizationId primitive.ObjectID) (*OrganizationUser, error) {
	filter := bson.M{
		"organization_id": organizationId,
		"role":            OrganizationUserSuperAdminRole,
	}

	findOptions := options.FindOne().SetSort(bson.M{"created_at": 1}) // 按创建时间正序排列

	var orgUser OrganizationUser
	err := o.Collection.FindOne(ctx, filter, findOptions).Decode(&orgUser)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, errs.NewCodeError(freeErrors.UserNotFoundCode, "未找到组织超级管理员")
		}
		return nil, err
	}

	return &orgUser, nil
}

func (o *OrganizationUserDao) GetByCreateTime(ctx context.Context, organizationId string,
	startTime *time.Time, endTime *time.Time, pagination *paginationUtils.DepPagination) (int64, []*openImModel.User, error) {
	orgId, err := primitive.ObjectIDFromHex(organizationId)
	if err != nil {
		return 0, nil, err
	}
	filter := bson.M{
		"organization_id": orgId,
	}
	if startTime != nil && endTime != nil {
		filter["created_at"] = bson.M{
			"$gte": startTime,
			"$lt":  endTime,
		}
	} else if startTime != nil {
		filter["created_at"] = bson.M{
			"$gte": startTime,
		}
	} else if endTime != nil {
		filter["created_at"] = bson.M{
			"$lt": endTime,
		}
	}
	opts := make([]*options.FindOptions, 0)
	if nil != pagination {
		opts = append(opts, pagination.ToOptions())
	}
	list, err := mongoutil.Find[*OrganizationUser](ctx, o.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}
	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	var userIds = make([]string, len(list))
	for i, item := range list {
		userIds[i] = item.ImServerUserId
	}
	userDao := openImModel.NewUserDao(o.DB)
	_, users, err := userDao.Select(ctx, userIds, nil)
	if err != nil {
		return 0, nil, err
	}
	return total, users, nil
}

type ChangeOrgUser struct {
	ID             primitive.ObjectID `bson:"_id" json:"id"`
	ImServerUserId string             `bson:"im_server_user_id" json:"im_server_user_id"`
	OrgId          primitive.ObjectID `bson:"org_id" json:"org_id"`
	CreatedAt      time.Time          `bson:"created_at" json:"created_at"`
}
type ChangeOrgUserDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewChangeOrgUserDao(db *mongo.Database) *ChangeOrgUserDao {
	return &ChangeOrgUserDao{
		DB:         db,
		Collection: db.Collection(ChangeOrgUser{}.TableName()),
	}
}
func (ChangeOrgUser) TableName() string {
	return constant.CollectionChangeOrgRecord
}

// UpsertTodayLoginRecord 插入或更新今日登录记录
// 如果用户今天已有切换记录，则更新创建时间；否则插入新记录
func (o *ChangeOrgUserDao) UpsertTodayLoginRecord(ctx context.Context, imServerUserId string, orgId primitive.ObjectID) error {
	now := time.Now()
	// 获取今天的开始和结束时间
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	todayEnd := todayStart.AddDate(0, 0, 1)

	filter := bson.M{
		"im_server_user_id": imServerUserId,
		"org_id":            orgId,
		"created_at": bson.M{
			"$gte": todayStart,
			"$lt":  todayEnd,
		},
	}

	update := bson.M{
		"$set": bson.M{
			"created_at": now,
		},
		"$setOnInsert": bson.M{
			"im_server_user_id": imServerUserId,
			"org_id":            orgId,
		},
	}

	opts := options.UpdateOptions{
		Upsert: &[]bool{true}[0],
	}

	_, err := o.Collection.UpdateOne(ctx, filter, update, &opts)
	return err
}

// GetIMServerUserIdsByOrgIdAndRole 根据组织ID和角色查询用户的imServerUserId列表
func (o *OrganizationUserDao) GetIMServerUserIdsByOrgIdAndRole(ctx context.Context, orgId primitive.ObjectID, role OrganizationUserRole) ([]string, error) {
	filter := bson.M{
		"organization_id":   orgId,
		"role":              role,
		"status":            OrganizationUserEnableStatus, // 只查询启用状态的用户
		"im_server_user_id": bson.M{"$ne": ""},            // 排除空的imServerUserId
	}

	// 只查询imServerUserId字段
	projection := bson.M{"im_server_user_id": 1}

	var results []bson.M
	cursor, err := o.Collection.Find(ctx, filter, options.Find().SetProjection(projection))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	var imServerUserIds []string
	for _, result := range results {
		if id, ok := result["im_server_user_id"].(string); ok && id != "" {
			imServerUserIds = append(imServerUserIds, id)
		}
	}

	return imServerUserIds, nil
}
