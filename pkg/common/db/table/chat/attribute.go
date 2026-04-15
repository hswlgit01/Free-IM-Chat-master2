package chat

import (
	"context"
	"time"

	"github.com/openimsdk/chat/tools/db/pagination"
)

type Attribute struct {
	UserID           string    `bson:"user_id"`
	Account          string    `bson:"account"`
	PhoneNumber      string    `bson:"phone_number"`
	AreaCode         string    `bson:"area_code"`
	Email            string    `bson:"email"`
	Nickname         string    `bson:"nickname"`
	FaceURL          string    `bson:"face_url"`
	Gender           int32     `bson:"gender"`
	CreateTime       time.Time `bson:"create_time"`
	ChangeTime       time.Time `bson:"change_time"`
	BirthTime        time.Time `bson:"birth_time"`
	Level            int32     `bson:"level"`
	AllowVibration   int32     `bson:"allow_vibration"`
	AllowBeep        int32     `bson:"allow_beep"`
	AllowAddFriend   int32     `bson:"allow_add_friend"`
	GlobalRecvMsgOpt int32     `bson:"global_recv_msg_opt"`
	RegisterType     int32     `bson:"register_type"`
	// 实名认证字段
	IsRealNameVerified bool      `bson:"is_real_name_verified"` // 是否已实名认证
	RealName           string    `bson:"real_name"`             // 真实姓名
	VerifiedTime       time.Time `bson:"verified_time"`         // 认证通过时间
}

// AttributeWithOrgUser 包含连表查询结果的结构体
type AttributeWithOrgUser struct {
	UserID           string    `bson:"user_id" json:"user_chat_id"`
	Account          string    `bson:"account" json:"account"`
	PhoneNumber      string    `bson:"phone_number" json:"phone_number"`
	AreaCode         string    `bson:"area_code" json:"area_code"`
	Email            string    `bson:"email" json:"email"`
	Nickname         string    `bson:"nickname" json:"nickname"`
	FaceURL          string    `bson:"face_url" json:"face_url"`
	Gender           int32     `bson:"gender" json:"gender"`
	CreateTime       time.Time `bson:"create_time" json:"create_time"`
	ChangeTime       time.Time `bson:"change_time" json:"change_time"`
	BirthTime        time.Time `bson:"birth_time" json:"birth"`
	Level            int32     `bson:"level" json:"level"`
	AllowVibration   int32     `bson:"allow_vibration" json:"allow_vibration"`
	AllowBeep        int32     `bson:"allow_beep" json:"allow_beep"`
	AllowAddFriend   int32     `bson:"allow_add_friend" json:"allow_add_friend"`
	GlobalRecvMsgOpt int32     `bson:"global_recv_msg_opt" json:"global_recv_msg_opt"`
	RegisterType     int32     `bson:"register_type" json:"register_type"`
	ImServerUserId   string    `bson:"im_server_user_id" json:"user_id"`
	ThirdUserId      string    `bson:"third_user_id" json:"third_user_id"`
	InvitationCode   string    `bson:"invitation_code" json:"invitation_code"`
	Inviter          string    `bson:"inviter" json:"inviter"`
	InviterType      string    `bson:"inviter_type" json:"inviter_type"`
	// 实名认证字段
	IsRealNameVerified bool      `bson:"is_real_name_verified" json:"is_real_name_verified"` // 是否已实名认证
	RealName           string    `bson:"real_name" json:"real_name"`                         // 真实姓名
	VerifiedTime       time.Time `bson:"verified_time" json:"verified_time"`                 // 认证通过时间
}

// ToAttribute 转换为普通的Attribute结构体
func (a *AttributeWithOrgUser) ToAttribute() *Attribute {
	return &Attribute{
		UserID:           a.UserID,
		Account:          a.Account,
		PhoneNumber:      a.PhoneNumber,
		AreaCode:         a.AreaCode,
		Email:            a.Email,
		Nickname:         a.Nickname,
		FaceURL:          a.FaceURL,
		Gender:           a.Gender,
		CreateTime:       a.CreateTime,
		ChangeTime:       a.ChangeTime,
		BirthTime:        a.BirthTime,
		Level:            a.Level,
		AllowVibration:   a.AllowVibration,
		AllowBeep:        a.AllowBeep,
		AllowAddFriend:   a.AllowAddFriend,
		GlobalRecvMsgOpt: a.GlobalRecvMsgOpt,
		RegisterType:     a.RegisterType,
	}
}

func (Attribute) TableName() string {
	return "attributes"
}

type AttributeInterface interface {
	// NewTx(tx any) AttributeInterface
	Create(ctx context.Context, attribute ...*Attribute) error
	Update(ctx context.Context, userID string, data map[string]any) error
	Find(ctx context.Context, userIds []string) ([]*Attribute, error)
	FindAccount(ctx context.Context, accounts []string) ([]*Attribute, error)
	Search(ctx context.Context, keyword string, genders []int32, pagination pagination.Pagination) (int64, []*Attribute, error)
	TakePhone(ctx context.Context, areaCode string, phoneNumber string) (*Attribute, error)
	TakeEmail(ctx context.Context, email string) (*Attribute, error)
	TakeAccount(ctx context.Context, account string) (*Attribute, error)
	Take(ctx context.Context, userID string) (*Attribute, error)
	SearchNormalUser(ctx context.Context, keyword string, forbiddenID []string, gender int32, pagination pagination.Pagination, org_id string) (int64, []*AttributeWithOrgUser, error)
	SearchUser(ctx context.Context, keyword string, userIDs []string, genders []int32, pagination pagination.Pagination) (int64, []*Attribute, error)
	Delete(ctx context.Context, userIDs []string) error
}
