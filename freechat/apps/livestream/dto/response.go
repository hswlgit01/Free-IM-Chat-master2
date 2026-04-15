package dto

import (
	"github.com/openimsdk/chat/freechat/apps/livestream/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type LsStatisticsJoinUserResp struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	RoomName  string             `bson:"room_name" json:"room_name"`   // 房间名称
	CreatorId string             `bson:"creator_id" json:"creator_id"` // 创建人id
	Cover     string             `bson:"cover" json:"cover"`
	Detail    string             `bson:"detail" json:"detail"`
	Nickname  string             `bson:"nickname" json:"nickname"`

	TotalRaiseHands int `bson:"total_raise_hands" json:"total_raise_hands"` // 举手总数
	TotalUsers      int `bson:"total_users" json:"total_users"`             // 进入直播的总人数
	MaxOnlineUsers  int `bson:"max_online_users" json:"max_online_users"`   // 最多在线人数
	TotalOnStage    int `bson:"total_on_stage" json:"total_on_stage"`       // 上台总数

	RecordFile []string `bson:"record_file" json:"record_file"`

	StopTime  time.Time                        `bson:"stop_time" json:"stop_time"`   // 直播结束时间
	StartTime time.Time                        `bson:"start_time" json:"start_time"` // 直播开始时间
	Status    model.LivestreamStatisticsStatus `bson:"status" json:"status"`         // 直播状态

	CreatedAt time.Time `bson:"created_at" json:"created_at"` // 创建时间
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"` // 更新时间

	User interface{} `bson:"user" json:"user"`
}

func NewLsStatisticsJoinUserResp(obj *model.LivestreamStatisticsJoinUser) *LsStatisticsJoinUserResp {
	return &LsStatisticsJoinUserResp{
		ID:        obj.ID,
		RoomName:  obj.RoomName,
		CreatorId: obj.CreatorId,
		Cover:     obj.Cover,

		TotalRaiseHands: obj.TotalRaiseHands,
		TotalUsers:      obj.TotalUsers,
		MaxOnlineUsers:  obj.MaxOnlineUsers,
		TotalOnStage:    obj.TotalOnStage,
		StopTime:        obj.StopTime,
		StartTime:       obj.StartTime,
		CreatedAt:       obj.CreatedAt,
		UpdatedAt:       obj.UpdatedAt,
		Status:          obj.Status,
		Detail:          obj.Detail,
		Nickname:        obj.Nickname,
		RecordFile:      obj.RecordFile,

		User: obj.User,
	}
}

type DetailLsStatisticsResp struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	RoomName  string             `bson:"room_name" json:"room_name"`   // 房间名称
	CreatorId string             `bson:"creator_id" json:"creator_id"` // 创建人id

	Cover    string `bson:"cover" json:"cover"`
	Detail   string `bson:"detail" json:"detail"`
	Nickname string `bson:"nickname" json:"nickname"`

	TotalRaiseHands int `bson:"total_raise_hands" json:"total_raise_hands"` // 举手总数
	TotalUsers      int `bson:"total_users" json:"total_users"`             // 进入直播的总人数
	MaxOnlineUsers  int `bson:"max_online_users" json:"max_online_users"`   // 最多在线人数
	TotalOnStage    int `bson:"total_on_stage" json:"total_on_stage"`       // 上台总数

	RecordFile []string `bson:"record_file" json:"record_file"`

	StopTime  time.Time                        `bson:"stop_time" json:"stop_time"`   // 直播结束时间
	StartTime time.Time                        `bson:"start_time" json:"start_time"` // 直播开始时间
	Status    model.LivestreamStatisticsStatus `bson:"status" json:"status"`         // 直播状态

	CreatedAt time.Time `bson:"created_at" json:"created_at"` // 创建时间
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"` // 更新时间
}

func NewDetailLsStatisticsResp(obj *model.LivestreamStatistics) *DetailLsStatisticsResp {
	return &DetailLsStatisticsResp{
		ID:        obj.ID,
		RoomName:  obj.RoomName,
		CreatorId: obj.CreatorId,

		TotalRaiseHands: obj.TotalRaiseHands,
		TotalUsers:      obj.TotalUsers,
		MaxOnlineUsers:  obj.MaxOnlineUsers,
		TotalOnStage:    obj.TotalOnStage,
		StopTime:        obj.StopTime,
		StartTime:       obj.StartTime,
		CreatedAt:       obj.CreatedAt,
		UpdatedAt:       obj.UpdatedAt,
		Status:          obj.Status,
		Detail:          obj.Detail,
		Nickname:        obj.Nickname,
		Cover:           obj.Cover,
		RecordFile:      obj.RecordFile,
	}
}
