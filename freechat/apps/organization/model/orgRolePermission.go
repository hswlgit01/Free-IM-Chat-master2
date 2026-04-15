package model

import (
	"context"
	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type PermissionCode string

const (
	PermissionCodeModifyNickname     PermissionCode = "modify_nickname"     // 允许修改昵称
	PermissionCodeSendFile           PermissionCode = "send_file"           // 允许发送文件
	PermissionCodeSendBusinessCard   PermissionCode = "send_business_card"  // 允许发送名片
	PermissionCodeCreateGroup        PermissionCode = "create_group"        // 允许建群
	PermissionCodeAddFriend          PermissionCode = "add_friend"          // 允许加好友
	PermissionCodeSendRedPacket      PermissionCode = "send_red_packet"     // 允许发送红包
	PermissionCodeTransfer           PermissionCode = "transfer"            // 允许转账
	PermissionCodeCheckin            PermissionCode = "checkin"             // 允许签到
	PermissionCodeLottery            PermissionCode = "lottery"             // 允许抽奖
	PermissionCodeLivestream         PermissionCode = "livestream"          // 允许开启直播
	PermissionCodeLoginRecord        PermissionCode = "login_record"        // 允许查看登录记录
	PermissionCodeFreePrivateChat    PermissionCode = "free_private_chat"   // 可无视好友关系直接私聊
	PermissionCodeOfficialProtection PermissionCode = "official_protection" // 官方账号保护（不能被发起音视频、踢出群组、禁言）
)

// IsValidPermissionCode 管理端可保存的权限码（已废弃的 basic 不再接受）
func IsValidPermissionCode(c PermissionCode) bool {
	switch c {
	case PermissionCodeModifyNickname,
		PermissionCodeSendFile,
		PermissionCodeSendBusinessCard,
		PermissionCodeCreateGroup,
		PermissionCodeAddFriend,
		PermissionCodeSendRedPacket,
		PermissionCodeTransfer,
		PermissionCodeCheckin,
		PermissionCodeLottery,
		PermissionCodeLivestream,
		PermissionCodeLoginRecord,
		PermissionCodeFreePrivateChat,
		PermissionCodeOfficialProtection:
		return true
	default:
		return false
	}
}

type OrganizationRolePermission struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrgId primitive.ObjectID   `bson:"org_id" json:"org_id"`
	Role  OrganizationUserRole `bson:"role" json:"role"`

	PermissionCode PermissionCode `bson:"permission_code" json:"permission_code"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (OrganizationRolePermission) TableName() string {
	return constant.CollectionOrganizationRolePermission
}

func CreateOrganizationRolePermissionIndex(db *mongo.Database) error {
	m := &OrganizationRolePermission{}

	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
			},
		},
	})
	return err
}

type OrganizationRolePermissionDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewOrganizationRolePermissionDao(db *mongo.Database) *OrganizationRolePermissionDao {
	return &OrganizationRolePermissionDao{
		DB:         db,
		Collection: db.Collection(OrganizationRolePermission{}.TableName()),
	}
}

func (o *OrganizationRolePermissionDao) Create(ctx context.Context, obj *OrganizationRolePermission) error {
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*OrganizationRolePermission{obj})
}

func (o *OrganizationRolePermissionDao) CreateDefaultRolePermission(ctx context.Context, orgId primitive.ObjectID) error {
	insertPermissions := make([]*OrganizationRolePermission, 0)

	// 默认普通用户权限
	insertPermissions = append(insertPermissions, &OrganizationRolePermission{
		OrgId:          orgId,
		Role:           OrganizationUserNormalRole,
		PermissionCode: PermissionCodeCheckin,
	})

	// 默认群组管理员权限（发送文件/名片/建群/加好友需在后台单独勾选，此处不默认授予）
	insertPermissions = append(insertPermissions, &OrganizationRolePermission{
		OrgId:          orgId,
		Role:           OrganizationUserGroupManagerRole,
		PermissionCode: PermissionCodeCheckin,
	})

	// 默认团队长权限（与管理员相同起点，具体能力在管理台勾选）
	insertPermissions = append(insertPermissions, &OrganizationRolePermission{
		OrgId:          orgId,
		Role:           OrganizationUserTermManagerRole,
		PermissionCode: PermissionCodeCheckin,
	})

	// 默认后台管理员权限
	insertPermissions = append(insertPermissions, &OrganizationRolePermission{
		OrgId:          orgId,
		Role:           OrganizationUserBackendAdminRole,
		PermissionCode: PermissionCodeModifyNickname,
	})

	// 默认超级管理员权限
	insertPermissions = append(insertPermissions, &OrganizationRolePermission{
		OrgId:          orgId,
		Role:           OrganizationUserSuperAdminRole,
		PermissionCode: PermissionCodeModifyNickname,
	})

	for _, permission := range insertPermissions {
		permission.CreatedAt = time.Now().UTC()
		permission.UpdatedAt = time.Now().UTC()
	}

	return mongoutil.InsertMany(ctx, o.Collection, insertPermissions)
}

func (o *OrganizationRolePermissionDao) GetByOrgIdAndRole(ctx context.Context, orgId primitive.ObjectID, role OrganizationUserRole) ([]*OrganizationRolePermission, error) {
	return mongoutil.Find[*OrganizationRolePermission](ctx, o.Collection, bson.M{"org_id": orgId, "role": role})
}

func (o *OrganizationRolePermissionDao) DeleteByOrgIdAndRole(ctx context.Context, orgId primitive.ObjectID, role OrganizationUserRole) error {
	filter := bson.M{}
	filter["org_id"] = orgId
	filter["role"] = role
	return mongoutil.DeleteMany(ctx, o.Collection, filter)
}

func (o *OrganizationRolePermissionDao) ExistPermission(ctx context.Context, orgId primitive.ObjectID, role OrganizationUserRole, code PermissionCode) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"org_id": orgId, "role": role, "permission_code": code})
}
