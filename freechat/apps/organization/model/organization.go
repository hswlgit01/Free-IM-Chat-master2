package model

import (
	"context"
	"encoding/json"
	"time"

	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type OrganizationType string

const (
	OrganizationTypeEnterprise OrganizationType = "enterprise"
)

type OrganizationStatus string

const (
	OrganizationStatusPass   OrganizationStatus = "pass"
	OrganizationStatusReject OrganizationStatus = "reject"
	OrganizationStatusWait   OrganizationStatus = "wait"
)

const OrgInvitationCodeLength = 6

type Organization struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	Name           string             `bson:"name" json:"name"`
	Type           OrganizationType   `bson:"type" json:"type"`
	Email          string             `bson:"email" json:"email"`
	Phone          string             `bson:"phone" json:"phone"`
	Description    string             `bson:"description" json:"description"`
	Contacts       string             `bson:"contacts" json:"contacts"`
	InvitationCode string             `bson:"invitation_code" json:"invitation_code"`
	CreatorId      string             `bson:"creator_id" json:"creator_id"`
	Status         OrganizationStatus `bson:"status" json:"status"`
	Logo           string             `bson:"logo" json:"logo"`
	AesKeyBase64   string             `bson:"aesKeyBase64" json:"-"`

	// 签到规则说明（富文本HTML）
	CheckinRuleDescription string `bson:"checkin_rule_description,omitempty" json:"checkin_rule_description,omitempty"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (Organization) TableName() string {
	return constant.CollectionOrganization
}

func CreateOrganizationIndex(db *mongo.Database) error {
	m := &Organization{}

	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
				{Key: "name", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "name", Value: 1},
			},
		},
	})
	return err
}

type OrganizationDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewOrganizationDao(db *mongo.Database) *OrganizationDao {
	return &OrganizationDao{
		DB:         db,
		Collection: db.Collection(Organization{}.TableName()),
	}
}

func (o *OrganizationDao) GetByNameAndStatus(ctx context.Context, name string, status OrganizationStatus) (*Organization, error) {
	return mongoutil.FindOne[*Organization](ctx, o.Collection, bson.M{"name": name, "status": status})
}

func (o *OrganizationDao) ExistByNameAndStatus(ctx context.Context, name string, status OrganizationStatus) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"name": name, "status": status})
}

func (o *OrganizationDao) ExistByCreatorId(ctx context.Context, creatorId string) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"creator_id": creatorId})
}

func (o *OrganizationDao) GetByIdAndStatus(ctx context.Context, id primitive.ObjectID, status OrganizationStatus) (*Organization, error) {
	return mongoutil.FindOne[*Organization](ctx, o.Collection, bson.M{"_id": id, "status": status})
}

func (o *OrganizationDao) GetById(ctx context.Context, id primitive.ObjectID) (*Organization, error) {
	return mongoutil.FindOne[*Organization](ctx, o.Collection, bson.M{"_id": id})
}

func (o *OrganizationDao) ExistById(ctx context.Context, id primitive.ObjectID) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"_id": id})
}

func (o *OrganizationDao) GetByInvitationCode(ctx context.Context, invitationCode string) (*Organization, error) {
	return mongoutil.FindOne[*Organization](ctx, o.Collection, bson.M{"invitation_code": invitationCode})
}

func (o *OrganizationDao) Create(ctx context.Context, obj *Organization) error {
	for i := 0; i < 20; i++ {
		invitationCode := utils.RandomAlphaString(1) + utils.RandomNumString(OrgInvitationCodeLength-1)
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
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*Organization{obj})
}

type OrgUpdateInfoFieldParam struct {
	Email          string
	Phone          string
	Description    string
	Contacts       string
	InvitationCode string
	Logo           string
}

func (o *OrganizationDao) UpdateInfo(ctx context.Context, id primitive.ObjectID, param OrgUpdateInfoFieldParam) error {
	updateField := bson.M{"$set": bson.M{
		"email":           param.Email,
		"phone":           param.Phone,
		"description":     param.Description,
		"contacts":        param.Contacts,
		"invitation_code": param.InvitationCode,
		"updated_at":      time.Now().UTC(),
		"logo":            param.Logo,
	}}
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"_id": id}, updateField, false)
}

// GetByEmailAndStatus 根据邮箱查询组织
func (o *OrganizationDao) GetByEmailAndStatus(ctx context.Context, email string, status OrganizationStatus) (*Organization, error) {
	return mongoutil.FindOne[*Organization](ctx, o.Collection, bson.M{"email": email, "status": status})
}

// GetByPhoneAndStatus 根据手机号查询组织
func (o *OrganizationDao) GetByPhoneAndStatus(ctx context.Context, phone string, status OrganizationStatus) (*Organization, error) {
	return mongoutil.FindOne[*Organization](ctx, o.Collection, bson.M{"phone": phone, "status": status})
}

// GetByCreatorIdAndStatus 根据创建者ID查询组织
func (o *OrganizationDao) GetByCreatorIdAndStatus(ctx context.Context, creatorId string, status OrganizationStatus) (*Organization, error) {
	return mongoutil.FindOne[*Organization](ctx, o.Collection, bson.M{"creatorId": creatorId, "status": status})
}

type OrganizationJoinAll struct {
	*Organization `bson:",inline"`
	//Checkin          map[string]interface{} `bson:"checkin"`
	//User             map[string]interface{} `bson:"user"`
	//OrganizationUser map[string]interface{} `bson:"organization_user"`
	Attribute map[string]interface{} `bson:"attribute"`
}

func (o *OrganizationDao) SelectJoinAll(ctx context.Context, keyword string,
	startTime time.Time, endTime time.Time, page *paginationUtils.DepPagination) (int64, []*OrganizationJoinAll, error) {

	// 构建 Organization 表的查询条件
	orgFilter := bson.M{}

	// 添加时间过滤条件
	tsTime := bson.M{}
	if !startTime.IsZero() {
		tsTime["$gte"] = startTime
	}
	if !endTime.IsZero() {
		tsTime["$lte"] = endTime
	}
	if len(tsTime) > 0 {
		orgFilter["created_at"] = tsTime
	}

	// 处理关键字搜索 - 只在组织表内搜索
	if keyword != "" {
		orgFilter["$or"] = []bson.M{
			{"name": bson.M{"$regex": keyword, "$options": "i"}},
			{"creator_id": bson.M{"$regex": keyword, "$options": "i"}},
		}
	}

	// 第一次查询：获取总数
	total, err := mongoutil.Count(ctx, o.Collection, orgFilter)
	if err != nil {
		return 0, nil, err
	}

	if total == 0 {
		return 0, []*OrganizationJoinAll{}, nil
	}

	// 第二次查询：获取分页数据
	opts := make([]*options.FindOptions, 0)
	opts = append(opts, options.Find().SetSort(bson.M{"created_at": -1}))

	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	// 查询组织数据
	organizations, err := mongoutil.Find[*Organization](ctx, o.Collection, orgFilter, opts...)
	if err != nil {
		return 0, nil, err
	}

	if len(organizations) == 0 {
		return total, []*OrganizationJoinAll{}, nil
	}

	// 直接返回组织数据，不进行任何连表查询和数据拼接
	result := make([]*OrganizationJoinAll, 0, len(organizations))
	for _, org := range organizations {
		joinedOrg := &OrganizationJoinAll{
			Organization: org,
			Attribute:    make(map[string]interface{}), // 空的 Attribute 字段
		}
		result = append(result, joinedOrg)
	}

	return total, result, nil
}

// MarshalJSON 自定义 Organization 的 JSON 序列化
func (o Organization) MarshalJSON() ([]byte, error) {
	type Alias Organization
	return json.Marshal(&struct {
		ID string `json:"id"`
		*Alias
	}{
		ID:    o.ID.Hex(),
		Alias: (*Alias)(&o),
	})
}

// UnmarshalJSON 自定义 Organization 的 JSON 反序列化
func (o *Organization) UnmarshalJSON(data []byte) error {
	type Alias Organization
	aux := &struct {
		ID string `json:"id"`
		*Alias
	}{
		Alias: (*Alias)(o),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.ID != "" {
		objectID, err := primitive.ObjectIDFromHex(aux.ID)
		if err != nil {
			return err
		}
		o.ID = objectID
	}
	return nil
}
