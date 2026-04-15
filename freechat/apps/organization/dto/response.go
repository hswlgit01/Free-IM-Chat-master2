package dto

import (
	"context"
	"time"

	orgCache "github.com/openimsdk/chat/freechat/apps/organization/cache"
	chatCache "github.com/openimsdk/chat/freechat/third/chat/cache"

	"github.com/openimsdk/chat/freechat/apps/organization/model"
	userDto "github.com/openimsdk/chat/freechat/apps/user/dto"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	"github.com/openimsdk/chat/freechat/plugin"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/constant"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type SimpleOrganizationResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	Name           string `bson:"name" json:"name"`
	Type           string `bson:"type" json:"type"`
	Email          string `bson:"email" json:"email"`
	Phone          string `bson:"phone" json:"phone"`
	Description    string `bson:"description" json:"description"`
	Contacts       string `bson:"contacts" json:"contacts"`
	InvitationCode string `bson:"invitation_code" json:"invitation_code"`
	CreatorId      string `bson:"creator_id" json:"creator_id"`
	Status         string `bson:"status" json:"status"`
	Logo           string `bson:"logo" json:"logo"`

	//GroupTotal int64 `json:"group_total"`
	//UserTotal   int  `json:"user_total"`

	// 签到规则说明（富文本HTML）
	CheckinRuleDescription string `json:"checkin_rule_description,omitempty"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func NewSimpleOrganizationResp(org *model.Organization) (*SimpleOrganizationResp, error) {
	return &SimpleOrganizationResp{
		ID:                     org.ID,
		Name:                   org.Name,
		Type:                   string(org.Type),
		Email:                  org.Email,
		Phone:                  org.Phone,
		Description:            org.Description,
		Contacts:               org.Contacts,
		InvitationCode:         org.InvitationCode,
		CreatorId:              org.CreatorId,
		Status:                 string(org.Status),
		CreatedAt:              org.CreatedAt,
		UpdatedAt:              org.UpdatedAt,
		Logo:                   org.Logo,
		CheckinRuleDescription: org.CheckinRuleDescription, // 签到规则说明
	}, nil
}

type OrganizationResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	Name           string `bson:"name" json:"name"`
	Type           string `bson:"type" json:"type"`
	Email          string `bson:"email" json:"email"`
	Phone          string `bson:"phone" json:"phone"`
	Description    string `bson:"description" json:"description"`
	Contacts       string `bson:"contacts" json:"contacts"`
	InvitationCode string `bson:"invitation_code" json:"invitation_code"`
	OwnerId        string `bson:"owner_id" json:"owner_id"`
	Status         string `bson:"status" json:"status"`
	Logo           string `bson:"logo" json:"logo"`

	AccountPrefix string `json:"account_prefix"`

	GroupTotal int64 `json:"group_total"`

	WalletExist bool `json:"wallet_exist"`
	UserTotal   int  `json:"user_total"`

	// 新增：创建者信息
	CreatorInfo *userDto.AttributeResp `json:"creator_info,omitempty"`

	// 签到规则说明（富文本HTML）
	CheckinRuleDescription string `json:"checkin_rule_description,omitempty"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func NewOrganizationResp(org *model.Organization, operationID string) (*OrganizationResp, error) {
	mongoCli := plugin.MongoCli()
	groupDao := openImModel.NewGroupDao(mongoCli.GetDB())
	orgUserDao := model.NewOrganizationUserDao(mongoCli.GetDB())

	total, err := groupDao.CountGroupsByOrgID(context.TODO(), org.ID.Hex())
	if err != nil {
		return nil, errs.NewCodeError(freeErrors.ErrSystem, "Failed to query groups")
	}

	walletInfoDao := walletModel.NewWalletInfoDao(mongoCli.GetDB())
	walletExist, err := walletInfoDao.ExistByOwnerIdAndOwnerType(context.TODO(), org.ID.Hex(), walletModel.WalletInfoOwnerTypeOrganization)
	if err != nil {
		return nil, err
	}

	imApiCaller := plugin.ImApiCaller()
	// 在服务内部获取管理员Token
	openImCtx := context.WithValue(context.Background(), constantpb.OperationID, operationID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(openImCtx)
	if err != nil {
		log.ZError(context.TODO(), "获取IM管理员token失败", err, "operation_id", operationID)
		return nil, err
	}
	openImCtx = context.WithValue(openImCtx, constant.CtxApiToken, adminToken)

	forbiddenAccountDao := chatModel.NewForbiddenAccountDao(mongoCli.GetDB())

	notInImUserIds, err := forbiddenAccountDao.FindAllIDs(context.TODO())
	if err != nil {
		return nil, err
	}

	userTotal, err := orgUserDao.CountByOrgIdAndStatus(context.TODO(), org.ID, notInImUserIds, []model.OrganizationUserRole{
		model.OrganizationUserNormalRole,
		model.OrganizationUserGroupManagerRole,
		model.OrganizationUserTermManagerRole,
	})
	if err != nil {
		return nil, err
	}

	attributeCache := chatCache.NewAttributeCacheRedis(plugin.RedisCli(), mongoCli.GetDB())

	orgCreatorUser, err := attributeCache.Take(context.TODO(), org.CreatorId)
	if err != nil {
		return nil, err
	}
	accountPrefix := orgCreatorUser.Account + "_"

	// 构造创建者信息
	creatorInfo := userDto.NewAttributeResp(orgCreatorUser)

	return &OrganizationResp{
		ID:                     org.ID,
		Name:                   org.Name,
		Type:                   string(org.Type),
		Email:                  org.Email,
		Phone:                  org.Phone,
		Description:            org.Description,
		Contacts:               org.Contacts,
		InvitationCode:         org.InvitationCode,
		OwnerId:                org.CreatorId,
		Status:                 string(org.Status),
		CreatedAt:              org.CreatedAt,
		UpdatedAt:              org.UpdatedAt,
		GroupTotal:             total,
		WalletExist:            walletExist,
		UserTotal:              int(userTotal),
		Logo:                   org.Logo,
		AccountPrefix:          accountPrefix,
		CreatorInfo:            creatorInfo,                // 新增：创建者信息
		CheckinRuleDescription: org.CheckinRuleDescription, // 签到规则说明
	}, nil
}

type OrgUserResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrganizationId primitive.ObjectID           `bson:"organization_id" json:"organization_id"`
	ThirdUserId    string                       `bson:"third_user_id" json:"third_user_id"`
	UserId         string                       `bson:"user_id" json:"user_id"`
	Role           model.OrganizationUserRole   `bson:"role" json:"role"`
	Status         model.OrganizationUserStatus `bson:"status" json:"status"`
	ImServerUserID string                       `bson:"im_server_user_id" json:"im_server_user_id"`

	// Attribute 来自 Mongo attribute 集合；其中 Account 对应字段 account（登录账号展示以此为准）
	Attribute *userDto.AttributeResp `json:"attribute"`
	User      *userDto.UserResp      `json:"user"`
	// LastLoginRecordIp / Region / DeviceID / Platform 来自 Mongo user_login_record 各 user_id 最新一条的 ip 等字段（Region 为服务端对 ip 的解析结果）
	LastLoginRecordIp       string `json:"last_login_record_ip"`
	LastLoginRecordIpRegion string `json:"last_login_record_ip_region"`
	// RegisterIp 来自 registers 集合（用户注册时记录的 ip）
	RegisterIp string `json:"register_ip"`

	DeviceID string         `json:"device_id"` // 设备ID
	Platform string         `json:"platform"`  // 平台信息
	Tags     []*UserTagResp `json:"tags"`      // 用户标签详情列表（仅在需要时填充）

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`

	// 钱包余额信息
	WalletBalances []*WalletBalanceInfo `json:"wallet_balances,omitempty"`
	// 补偿金余额信息
	CompensationBalance string `json:"compensation_balance,omitempty"` // 补偿金余额，单个值
}

// NewOrgUserRespWithBatchData 使用批量查询数据创建用户响应（真正的批量优化版本）
func NewOrgUserRespWithBatchData(
	obj *model.OrganizationUserWithUser,
	tagMap map[string]*UserTagResp,
	attributeMap map[string]*chatModel.Attribute,
	loginRecordCache *chatCache.UserLoginRecordCache,
	registerIP string,
) (*OrgUserResp, error) {
	// 用户昵称/头像等必须来自 OpenIM user 表；无记录时返回空 UserResp（避免 JSON 中 user 为 null 导致前端报错）
	var userResp *userDto.UserResp
	if obj.User != nil {
		userResp = userDto.NewUserResp(obj.User)
	} else {
		userResp = &userDto.UserResp{UserID: obj.ImServerUserId}
	}

	// 从批量查询的数据中获取 Attribute（Mongo attribute 表，账号取其中 account）
	var attributeResp *userDto.AttributeResp
	if attr, exists := attributeMap[obj.UserId]; exists && attr != nil {
		attributeResp = userDto.NewAttributeResp(attr)
	}

	// 标签处理优化：预分配准确容量，减少slice扩容
	var tags []*UserTagResp
	if len(obj.Tags) > 0 {
		// 预计算实际匹配的标签数量（避免append导致的内存重分配）
		validTagCount := 0
		tagStrs := make([]string, len(obj.Tags)) // 预分配字符串数组

		for i, tagId := range obj.Tags {
			tagStrs[i] = tagId.Hex()
			if _, exists := tagMap[tagStrs[i]]; exists {
				validTagCount++
			}
		}

		// 使用准确容量分配内存
		if validTagCount > 0 {
			tags = make([]*UserTagResp, 0, validTagCount)
			for _, tagStr := range tagStrs {
				if tagResp, exists := tagMap[tagStr]; exists {
					tags = append(tags, tagResp)
				}
			}
		}
	}

	// 最近登录信息：由调用方传入基于 user_login_record 表最新一条组装的结构（IP 与库表 ip 一致）
	var lastLoginRecordIp, lastLoginRecordIpRegion, deviceID, platform string

	if loginRecordCache != nil {
		lastLoginRecordIp = loginRecordCache.IP
		deviceID = loginRecordCache.DeviceID
		platform = loginRecordCache.Platform
		lastLoginRecordIpRegion = loginRecordCache.Region
	}

	// 这里只创建用户的基本信息，钱包余额会在GetOrgUserWithFilters方法中添加
	return &OrgUserResp{
		ID:                      obj.ID,
		OrganizationId:          obj.OrganizationId,
		ThirdUserId:             obj.ThirdUserId,
		UserId:                  obj.UserId,
		Role:                    obj.Role,
		Status:                  obj.Status,
		ImServerUserID:          obj.ImServerUserId,
		Attribute:               attributeResp,
		User:                    userResp,
		LastLoginRecordIp:       lastLoginRecordIp,
		LastLoginRecordIpRegion: lastLoginRecordIpRegion,
		RegisterIp:              registerIP,
		DeviceID:                deviceID,
		Platform:                platform,
		Tags:                    tags,
		CreatedAt:               obj.CreatedAt,
		UpdatedAt:               obj.UpdatedAt,
	}, nil
}

type OrgUserWithOrgResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrganizationId primitive.ObjectID           `bson:"organization_id" json:"organization_id"`
	ThirdUserId    string                       `bson:"third_user_id" json:"third_user_id"`
	UserId         string                       `bson:"user_id" json:"user_id"`
	Role           model.OrganizationUserRole   `bson:"role" json:"role"`
	Status         model.OrganizationUserStatus `bson:"status" json:"status"`
	Organization   *SimpleOrganizationResp      `json:"organization"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func NewOrgUserWithOrgResp(ctx context.Context, db *mongo.Database, obj *model.OrganizationUser) (*OrgUserWithOrgResp, error) {
	organization, err := orgCache.NewOrgCacheRedis(plugin.RedisCli(), db).GetByIdAndStatus(ctx, obj.OrganizationId, model.OrganizationStatusPass)
	if err != nil {
		return nil, err
	}

	simpleOrganizationResp, err := NewSimpleOrganizationResp(organization)
	if err != nil {
		return nil, err
	}

	return &OrgUserWithOrgResp{
		ID:             obj.ID,
		OrganizationId: obj.OrganizationId,
		ThirdUserId:    obj.ThirdUserId,
		UserId:         obj.UserId,
		Role:           obj.Role,
		Status:         obj.Status,
		CreatedAt:      obj.CreatedAt,
		UpdatedAt:      obj.UpdatedAt,
		Organization:   simpleOrganizationResp,
	}, nil
}

type OrgRolePermissionResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrgId primitive.ObjectID         `bson:"org_id" json:"org_id"`
	Role  model.OrganizationUserRole `bson:"role" json:"role"`

	PermissionCode model.PermissionCode `bson:"permission_code" json:"permission_code"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func NewOrgRolePermissionResp(ctx context.Context, obj *model.OrganizationRolePermission) *OrgRolePermissionResp {
	return &OrgRolePermissionResp{
		ID:             obj.ID,
		OrgId:          obj.OrgId,
		Role:           obj.Role,
		PermissionCode: obj.PermissionCode,

		CreatedAt: obj.CreatedAt,
		UpdatedAt: obj.UpdatedAt,
	}
}

// 通知账户相关的DTO

// CreateNotificationAccountReq 创建通知账户请求
type CreateNotificationAccountReq struct {
	Nickname string `json:"nickname" binding:"required"`
	FaceURL  string `json:"face_url"`
}

// UpdateNotificationAccountReq 更新通知账户请求
type UpdateNotificationAccountReq struct {
	UserID   string `json:"user_id" binding:"required"`
	Nickname string `json:"nickname"`
	FaceURL  string `json:"face_url"`
}

// SearchNotificationAccountReq 搜索通知账户请求
type SearchNotificationAccountReq struct {
	Keyword    string                         `json:"keyword"`
	Pagination *paginationUtils.DepPagination `json:"pagination"`
}

// NotificationAccountResp 通知账户响应
type NotificationAccountResp struct {
	UserID    string `json:"user_id"`
	Nickname  string `json:"nickname"`
	FaceURL   string `json:"face_url"`
	OrgId     string `json:"org_id"`
	CreatedAt int64  `json:"created_at"`
}

// SearchNotificationAccountResp 搜索通知账户响应
type SearchNotificationAccountResp struct {
	Total uint32                     `json:"total"`
	List  []*NotificationAccountResp `json:"list"`
}

// SendBannerNotificationReq 发送图文通知请求
type SendBannerNotificationReq struct {
	SenderID  string      `json:"sender_id" binding:"required"` // 通知账户ID
	SendToAll bool        `json:"send_to_all"`                  // 是否发送给组织全部用户
	RecvIDs   []string    `json:"recv_ids"`                     // 接收用户ID列表（send_to_all=true时可为空）
	Elem      interface{} `json:"elem"`                         // 图文通知元素，前端传递什么就发送什么
	Ex        string      `json:"ex"`                           // 扩展字段
}

// WalletBalanceInfo 钱包余额信息结构
type WalletBalanceInfo struct {
	CurrencyId   string `json:"currency_id"`       // 币种ID
	CurrencyName string `json:"currency_name"`     // 币种名称
	Balance      string `json:"available_balance"` // 可用余额
}
