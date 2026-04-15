package dto

import (
	"time"

	"github.com/openimsdk/chat/freechat/third/chat/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type AttributeResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	UserID           string    `bson:"user_id" json:"user_id"`
	Account          string    `bson:"account" json:"account"`
	PhoneNumber      string    `bson:"phone_number" json:"phone_number"`
	AreaCode         string    `bson:"area_code" json:"area_code"`
	Email            string    `bson:"email" json:"email"`
	Nickname         string    `bson:"nickname" json:"nickname"`
	FaceURL          string    `bson:"face_url" json:"face_url"`
	Gender           int32     `bson:"gender" json:"gender"`
	CreateTime       time.Time `bson:"create_time" json:"create_time"`
	ChangeTime       time.Time `bson:"change_time" json:"change_time"`
	BirthTime        time.Time `bson:"birth_time" json:"birth_time"`
	Level            int32     `bson:"level" json:"level"`
	AllowVibration   int32     `bson:"allow_vibration" json:"allow_vibration"`
	AllowBeep        int32     `bson:"allow_beep" json:"allow_beep"`
	AllowAddFriend   int32     `bson:"allow_add_friend" json:"allow_add_friend"`
	GlobalRecvMsgOpt int32     `bson:"global_recv_msg_opt" json:"global_recv_msg_opt"`
	RegisterType     int32     `bson:"register_type" json:"register_type"`
	// 实名认证字段
	IsRealNameVerified bool      `bson:"is_real_name_verified" json:"isRealNameVerified"` // 是否已实名认证
	RealName           string    `bson:"real_name" json:"realName"`                       // 真实姓名
	VerifiedTime       time.Time `bson:"verified_time" json:"verifiedTime"`               // 认证通过时间
}

func NewAttributeResp(obj *model.Attribute) *AttributeResp {
	return &AttributeResp{
		ID:                 obj.ID,
		UserID:             obj.UserID,
		Nickname:           obj.Nickname,
		FaceURL:            obj.FaceURL,
		GlobalRecvMsgOpt:   obj.GlobalRecvMsgOpt,
		CreateTime:         obj.CreateTime,
		ChangeTime:         obj.ChangeTime,
		BirthTime:          obj.BirthTime,
		Level:              obj.Level,
		AllowVibration:     obj.AllowVibration,
		AllowBeep:          obj.AllowBeep,
		AllowAddFriend:     obj.AllowAddFriend,
		RegisterType:       obj.RegisterType,
		Gender:             obj.Gender,
		AreaCode:           obj.AreaCode,
		PhoneNumber:        obj.PhoneNumber,
		Account:            obj.Account,
		Email:              obj.Email,
		IsRealNameVerified: obj.IsRealNameVerified,
		RealName:           obj.RealName,
		VerifiedTime:       obj.VerifiedTime,
	}
}

type UserResp struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	UserID         string             `bson:"user_id" json:"user_id"`
	Nickname       string             `bson:"nickname" json:"nickname"`
	FaceURL        string             `bson:"face_url" json:"face_url"`
	Ex             string             `bson:"ex" json:"ex"`
	AppMangerLevel int32              `bson:"app_manger_level" json:"app_manger_level"`
	CanSendFreeMsg int32              `bson:"can_send_free_msg" json:"can_send_free_msg"`

	GlobalRecvMsgOpt int32     `bson:"global_recv_msg_opt" json:"global_recv_msg_opt"`
	CreateTime       time.Time `bson:"create_time" json:"create_time"`
}

// NewUserResp 优化版本：减少内存分配，内联字段赋值
func NewUserResp(obj *openImModel.User) *UserResp {
	if obj == nil {
		return nil
	}

	// 直接返回结构体字面量，减少中间变量
	return &UserResp{
		ID:               obj.ID,
		UserID:           obj.UserID,
		Nickname:         obj.Nickname,
		FaceURL:          obj.FaceURL,
		GlobalRecvMsgOpt: obj.GlobalRecvMsgOpt,
		CreateTime:       obj.CreateTime,
		Ex:               obj.Ex,
		AppMangerLevel:   obj.AppMangerLevel,
		CanSendFreeMsg:   obj.CanSendFreeMsg,
	}
}

type GetLoginRecordResp struct {
	UserID    string    `json:"user_id"`    // 用户ID
	LoginTime time.Time `json:"login_time"` // 登录时间
	IP        string    `json:"ip"`         // 登录IP
	DeviceID  string    `json:"device_id"`  // 设备ID
	Platform  string    `json:"platform"`   // 平台
	Region    string    `json:"region"`     // 地区信息
}
