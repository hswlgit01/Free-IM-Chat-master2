package svc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/openimsdk/chat/freechat/apps/livestream/dto"
	"github.com/openimsdk/chat/freechat/apps/livestream/model"
	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"k8s.io/utils/pointer"
	"slices"
	"strings"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
)

/*
	元数据相关
*/

// RoomMetadata 房间元数据结构
type RoomMetadata struct {
	CreatorIdentity    string             `json:"creator_identity"`    // 创建者身份标识
	OrgId              primitive.ObjectID `json:"org_id"`              // 房间所属组织id
	EnableChat         bool               `json:"enable_chat"`         // 是否启用聊天
	AllowParticipation bool               `json:"allow_participation"` // 是否允许参与
	BlockedIdentities  []string           `json:"blocked_identities"`  // 被屏蔽的用户列表

	Nickname  string    `json:"nickname" ` // 房间名称
	Detail    string    `json:"detail" `   // 房间简介
	Cover     string    `json:"cover"`     // 房间封面
	CreatedAt time.Time `json:"create_at"` // 创建时间

	EgressId string `json:"egress_id"` // 录屏任务ID

	TotalRaiseHands int `json:"total_raise_hands"` // 举手总数
	TotalUsers      int `json:"total_users"`       // 进入直播的总人数
	MaxOnlineUsers  int `json:"max_online_users"`  // 最多在线人数
	TotalOnStage    int `json:"total_on_stage"`    // 上台总数
}

type ParticipantRoleName string

const (
	ParticipantRoleOwnerName     ParticipantRoleName = "owner"
	ParticipantRoleAdminName     ParticipantRoleName = "admin"
	ParticipantRolePublisherName ParticipantRoleName = "publisher"
	ParticipantRoleUserName      ParticipantRoleName = "user"
)

var ParticipantRoleMap = map[ParticipantRoleName]*Role{
	ParticipantRoleOwnerName: {Name: ParticipantRoleOwnerName,
		RolePermission: &livekit.ParticipantPermission{CanPublish: true, CanSubscribe: true, CanPublishData: true}, Parent: ""},
	ParticipantRoleAdminName: {Name: ParticipantRoleAdminName,
		RolePermission: &livekit.ParticipantPermission{CanPublish: true, CanSubscribe: true, CanPublishData: true}, Parent: ParticipantRoleOwnerName},
	ParticipantRolePublisherName: {Name: ParticipantRolePublisherName,
		RolePermission: &livekit.ParticipantPermission{CanPublish: true, CanSubscribe: true, CanPublishData: true}, Parent: ParticipantRoleAdminName},
	ParticipantRoleUserName: {Name: ParticipantRoleUserName,
		RolePermission: &livekit.ParticipantPermission{CanPublish: false, CanSubscribe: true, CanPublishData: true}, Parent: ParticipantRolePublisherName},
}

type Role struct {
	Name           ParticipantRoleName            `json:"name"`
	RolePermission *livekit.ParticipantPermission `json:"role_permission"`
	Parent         ParticipantRoleName            `json:"parent"`
}

// GetSubRoles 获取某个角色的所有子角色
func (p *Role) GetSubRoles(roleName ParticipantRoleName) []ParticipantRoleName {
	var subRoles []ParticipantRoleName
	visited := map[ParticipantRoleName]bool{}
	queue := []ParticipantRoleName{roleName}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for name, role := range ParticipantRoleMap {
			if role.Parent == current && !visited[name] {
				visited[name] = true
				subRoles = append(subRoles, name)
				queue = append(queue, name)
			}
		}
	}
	return subRoles
}

// ParticipantMetadata 参与者元数据结构
type ParticipantMetadata struct {
	Role     *Role                 `json:"role"`      // 角色
	SubRoles []ParticipantRoleName `json:"sub_roles"` // 下辖管理的所有子角色

	Nickname string `json:"nickname"` // 用户名称
	FaceURL  string `json:"faceURL"`  // 用户头像

	HandRaised     bool `json:"hand_raised"`      // 是否举手
	InvitedToStage bool `json:"invited_to_stage"` // 是否被邀请上台
	//TODO 此字段生成随机数使用，用于区分两条websocket消息（举手同意 与 下麦元数据内容一致，移动端判定位重复推送，拒绝第二条消息处理），增加随机数。
	Nonce string `json:"nonce"`
}

func NewParticipantMetadata(roleName ParticipantRoleName) *ParticipantMetadata {
	p := &ParticipantMetadata{}
	p.UpdateRole(roleName)
	return p
}

func NewParticipantMetadataFromJson(jsonStr string) (*ParticipantMetadata, error) {
	var metadata ParticipantMetadata
	if err := json.Unmarshal([]byte(jsonStr), &metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}

// UpdateRole 更新元数据中的角色
func (p *ParticipantMetadata) UpdateRole(roleName ParticipantRoleName) *ParticipantMetadata {
	p.Role = ParticipantRoleMap[roleName]
	p.SubRoles = p.Role.GetSubRoles(roleName)
	return p
}

func (p *ParticipantMetadata) HasRole(roleNames ...ParticipantRoleName) bool {
	return slices.Contains(roleNames, p.Role.Name)
}

/*
	svc相关
*/

// LivestreamService 处理直播流相关的业务逻辑
type LivestreamService struct {
	roomService    *lksdk.RoomServiceClient // 房间服务客户端
	ingressService *lksdk.IngressClient     // 入口点服务客户端
	egressService  *lksdk.EgressClient      // 录屏服务客户端

	apiKey    string // API 密钥
	apiSecret string // API 密钥对应的密文
	wsUrl     string // WebSocket URL
}

// NewLivestreamService 创建一个新的LiveKitStreamService实例
func NewLivestreamService() (*LivestreamService, error) {
	// 从配置获取LiveKit配置
	config := plugin.ChatCfg()

	livestreamDao := model.NewLivestreamUrlDao(plugin.RedisCli())
	url, err := livestreamDao.AutomaticallySearchUrl(context.Background(), plugin.ChatCfg().ChatRpcConfig.LiveKit.BackupUrls)
	if err != nil {
		return nil, err
	}

	// 创建HTTP URL
	httpUrl := strings.Replace(url, "wss://", "https://", 1)
	httpUrl = strings.Replace(httpUrl, "ws://", "http://", 1)

	// 创建房间服务和入口点服务客户端
	roomService := lksdk.NewRoomServiceClient(
		httpUrl,
		config.ChatRpcConfig.LiveKit.Key,
		config.ChatRpcConfig.LiveKit.Secret,
	)

	ingressService := lksdk.NewIngressClient(
		httpUrl,
		config.ChatRpcConfig.LiveKit.Key,
		config.ChatRpcConfig.LiveKit.Secret,
	)

	egressService := lksdk.NewEgressClient(
		httpUrl,
		config.ChatRpcConfig.LiveKit.Key,
		config.ChatRpcConfig.LiveKit.Secret,
	)

	return &LivestreamService{
		roomService:    roomService,
		ingressService: ingressService,
		egressService:  egressService,
		apiKey:         config.ChatRpcConfig.LiveKit.Key,
		apiSecret:      config.ChatRpcConfig.LiveKit.Secret,
		wsUrl:          url,
	}, nil
}

type CreateRoomMetadataParams struct {
	EnableChat         bool   `json:"enable_chat"`                 // 是否启用聊天
	AllowParticipation bool   `json:"allow_participation"`         // 是否允许参与
	Nickname           string `json:"nickname" binding:"required"` // 房间别名,用于展示
	Detail             string `json:"detail"`                      // 房间简介
	Cover              string `json:"cover"`                       // 房间封面
}

// ConnectionDetails 连接详情结构
type ConnectionDetails struct {
	Token string `json:"token"`  // LiveKit 令牌
	WsUrl string `json:"ws_url"` // WebSocket URL
}

// CreateStreamParams 创建直播流的参数
type CreateStreamParams struct {
	Metadata CreateRoomMetadataParams `json:"metadata" binding:"required"` // 房间元数据
}

// CreateStreamResponse 创建直播流的响应
type CreateStreamResponse struct {
	ConnectionDetails ConnectionDetails `json:"connection_details"` // 连接详情
	RoomName          string            `json:"room_name"`
}

// WebCreateStream 创建直播流（通过用户设备摄像头和麦克风）
func (s *LivestreamService) WebCreateStream(ctx context.Context, orgUser *orgModel.OrganizationUser, params CreateStreamParams) (*CreateStreamResponse, error) {
	userDao := openImModel.NewUserDao(plugin.MongoCli().GetDB())
	orgRolePermissionDao := orgModel.NewOrganizationRolePermissionDao(plugin.MongoCli().GetDB())

	// 校验是否有权限创建房间
	hasPermission, err := orgRolePermissionDao.ExistPermission(ctx, orgUser.OrganizationId, orgUser.Role, orgModel.PermissionCodeLivestream)
	if err != nil {
		return nil, err
	}
	if !hasPermission {
		return nil, freeErrors.ApiErr("no permission")
	}

	// 1. 生成房间名称
	roomName := utils.GenerateRoomID()

	// 2. 创建房间
	roomMetadata := &RoomMetadata{
		CreatorIdentity:    orgUser.ImServerUserId,
		EnableChat:         params.Metadata.EnableChat,
		AllowParticipation: params.Metadata.AllowParticipation,
		BlockedIdentities:  []string{},
		Nickname:           params.Metadata.Nickname,
		Cover:              params.Metadata.Cover,
		Detail:             params.Metadata.Detail,
		OrgId:              orgUser.OrganizationId,
		CreatedAt:          time.Now().UTC(),
	}
	metadata, err := json.Marshal(roomMetadata)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	_, err = s.roomService.CreateRoom(ctx, &livekit.CreateRoomRequest{
		Name:         roomName,
		Metadata:     string(metadata),
		EmptyTimeout: 60,
	})
	if err != nil {
		return nil, freeErrors.LiveStreamSystemErr(err)
	}

	selfInfo, err := userDao.Take(ctx, orgUser.ImServerUserId)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.UserNotFoundErr
		}
		return nil, freeErrors.SystemErr(errs.New("user Take: " + err.Error()))
	}

	participantMetadata := NewParticipantMetadata(ParticipantRoleOwnerName)
	participantMetadata.Nickname = selfInfo.Nickname
	participantMetadata.FaceURL = selfInfo.FaceURL

	// 3. 生成访问令牌（可发布、可订阅）,创建者默认为owner最高权限
	token, err := s.InternalGenerateTokenWithMetadata(orgUser.ImServerUserId, roomName, participantMetadata)
	if err != nil {
		return nil, freeErrors.LiveStreamSystemErr(err)
	}

	dao := model.NewLivestreamStatisticsDao(plugin.MongoCli().GetDB())
	err = dao.Create(context.Background(), &model.LivestreamStatistics{
		RoomName:        roomName,
		OrgId:           orgUser.OrganizationId,
		CreatorId:       roomMetadata.CreatorIdentity,
		TotalUsers:      roomMetadata.TotalUsers,
		TotalOnStage:    roomMetadata.TotalOnStage,
		TotalRaiseHands: roomMetadata.TotalRaiseHands,
		MaxOnlineUsers:  roomMetadata.MaxOnlineUsers,
		StartTime:       roomMetadata.CreatedAt,
		Cover:           roomMetadata.Cover,
		Detail:          roomMetadata.Detail,
		Nickname:        roomMetadata.Nickname,
		Status:          model.LivestreamStatisticsStatusStart,
	})
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	return &CreateStreamResponse{
		RoomName: roomName,
		ConnectionDetails: ConnectionDetails{
			WsUrl: s.wsUrl,
			Token: token,
		},
	}, nil
}

// StopStreamParams 主播停止当前房间直播流
type StopStreamParams struct {
	RoomName string `json:"room_name" binding:"required"` // 房间名称
}

// WebStopStream 主播停止当前房间直播流
func (s *LivestreamService) WebStopStream(ctx context.Context, orgUser *orgModel.OrganizationUser, params StopStreamParams) error {
	db := plugin.MongoCli().GetDB()

	// 检查房间是否存在,验证权限
	rooms, err := s.roomService.ListRooms(ctx, &livekit.ListRoomsRequest{
		Names: []string{params.RoomName},
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(err)
	}

	if len(rooms.Rooms) == 0 {
		return freeErrors.LiveStreamRoomNotFoundErr(params.RoomName)
	}

	var roomMetadata RoomMetadata
	if err := json.Unmarshal([]byte(rooms.Rooms[0].Metadata), &roomMetadata); err != nil {
		return freeErrors.SystemErr(err)
	}

	selfParticipant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: orgUser.ImServerUserId,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant selfParticipant: " + err.Error()))
	}
	selfParticipantMetadata, err := NewParticipantMetadataFromJson(selfParticipant.Metadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}
	if !selfParticipantMetadata.HasRole(ParticipantRoleOwnerName) {
		return freeErrors.LiveStreamRoomExecutePermissionErr
	}

	// 2. 删除房间并添加统计数据
	_, err = s.roomService.DeleteRoom(ctx, &livekit.DeleteRoomRequest{
		Room: params.RoomName,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("DeleteRoom: " + err.Error()))
	}

	dao := model.NewLivestreamStatisticsDao(db)
	err = dao.UpdateByRoomName(context.Background(), params.RoomName, &model.LivestreamStatistics{
		//RoomName:        params.RoomName,
		CreatorId:       roomMetadata.CreatorIdentity,
		TotalUsers:      roomMetadata.TotalUsers,
		TotalOnStage:    roomMetadata.TotalOnStage,
		TotalRaiseHands: roomMetadata.TotalRaiseHands,
		MaxOnlineUsers:  roomMetadata.MaxOnlineUsers,
		StopTime:        time.Now().UTC(),
		StartTime:       roomMetadata.CreatedAt,
		Status:          model.LivestreamStatisticsStatusStop,
	})
	return err
}

// JoinStreamParams 加入直播流的参数
type JoinStreamParams struct {
	RoomName string `json:"room_name" binding:"required"` // 房间名称
}

// JoinStreamResponse 加入直播流的响应
type JoinStreamResponse struct {
	ConnectionDetails ConnectionDetails `json:"connection_details"` // 连接详情
}

// WebJoinStream 观众加入直播流
func (s *LivestreamService) WebJoinStream(ctx context.Context, orgUser *orgModel.OrganizationUser, params JoinStreamParams) (*JoinStreamResponse, error) {
	livestreamDao := model.NewLivestreamStatisticsDao(plugin.MongoCli().GetDB())
	// 1. 检查房间是否存在
	rooms, err := s.roomService.ListRooms(ctx, &livekit.ListRoomsRequest{
		Names: []string{params.RoomName},
	})
	if err != nil {
		return nil, freeErrors.LiveStreamSystemErr(err)
	}

	if len(rooms.Rooms) == 0 {
		_ = livestreamDao.UpdateStatusByRoomName(ctx, params.RoomName, model.LivestreamStatisticsStatusStop)
		return nil, freeErrors.LiveStreamRoomNotFoundErr(params.RoomName)
	}

	// 2. 检查用户是否可以加入组织房间
	room := rooms.Rooms[0]
	var roomMetadata RoomMetadata
	if err := json.Unmarshal([]byte(room.Metadata), &roomMetadata); err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	for _, blockedID := range roomMetadata.BlockedIdentities {
		if blockedID == orgUser.ImServerUserId {
			return nil, freeErrors.LiveStreamParticipantBlockedErr(blockedID)
		}
	}

	if roomMetadata.OrgId != orgUser.OrganizationId {
		return nil, freeErrors.LiveStreamRoomNotFoundErr(params.RoomName)
	}

	// 3. 检查用户是否已存在
	_, err = s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: orgUser.ImServerUserId,
	})
	if err == nil { // 没有错误表示找到了参与者
		return nil, freeErrors.LiveStreamSystemErr(errs.New("参与者已存在"))
	}

	userDao := openImModel.NewUserDao(plugin.MongoCli().GetDB())
	selfInfo, err := userDao.Take(ctx, orgUser.ImServerUserId)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.UserNotFoundErr
		}
		return nil, freeErrors.SystemErr(errs.New("user Take: " + err.Error()))
	}
	participantMetadata := NewParticipantMetadata(ParticipantRoleUserName)
	participantMetadata.Nickname = selfInfo.Nickname
	participantMetadata.FaceURL = selfInfo.FaceURL

	// 4. 生成访问令牌（不可发布，可订阅，可发送数据）
	token, err := s.InternalGenerateTokenWithMetadata(orgUser.ImServerUserId, params.RoomName, participantMetadata)
	if err != nil {
		return nil, freeErrors.LiveStreamSystemErr(err)
	}

	// 5. 修改房间统计数据
	if int(room.NumParticipants)+1 > roomMetadata.MaxOnlineUsers {
		roomMetadata.MaxOnlineUsers = int(room.NumParticipants)
	}
	roomMetadata.TotalUsers += 1

	roomMetadataBytes, err := json.Marshal(roomMetadata)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	_, err = s.roomService.UpdateRoomMetadata(ctx, &livekit.UpdateRoomMetadataRequest{
		Room:     params.RoomName,
		Metadata: string(roomMetadataBytes),
	})
	if err != nil {
		return nil, freeErrors.LiveStreamSystemErr(err)
	}

	dao := model.NewLivestreamStatisticsDao(plugin.MongoCli().GetDB())
	err = dao.IncTotalUsersByRoomName(context.Background(), params.RoomName, 1)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	return &JoinStreamResponse{
		ConnectionDetails: ConnectionDetails{
			WsUrl: s.wsUrl,
			Token: token,
		},
	}, nil
}

// InviteToStageParams 邀请观众上台的参数
type InviteToStageParams struct {
	RoomName string `json:"room_name" binding:"required"`
	Identity string `json:"identity" binding:"required"` // 用户标识
}

// WebInviteToStage 邀请观众上台
func (s *LivestreamService) WebInviteToStage(ctx context.Context, hostIdentity string, params InviteToStageParams) error {
	// 1. 获取自身,目标参与者信息
	selfParticipant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: hostIdentity,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant selfParticipant: " + err.Error()))
	}

	participant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: params.Identity,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant: " + err.Error()))
	}

	// 2. 验证双方权限
	selfParticipantMetadata, err := NewParticipantMetadataFromJson(selfParticipant.Metadata)
	if err != nil {
		return err
	}
	if !selfParticipantMetadata.HasRole(ParticipantRoleOwnerName, ParticipantRoleAdminName) {
		return freeErrors.LiveStreamRoomExecutePermissionErr
	}

	participantMetadata, err := NewParticipantMetadataFromJson(participant.Metadata)
	if err != nil {
		return err
	}
	if !participantMetadata.HasRole(ParticipantRoleUserName) {
		return freeErrors.LiveStreamRoomParticipantPermissionErr
	}

	// 3. 修改元数据
	participantMetadata.InvitedToStage = true

	// 序列化元数据
	metadataBytes, err := json.Marshal(participantMetadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}

	// 4. 更新参与者
	log.ZInfo(ctx, "邀请观众上台", "房间", params.RoomName, "主播", hostIdentity, "观众", params.Identity)
	_, err = s.roomService.UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{
		Room:     params.RoomName,
		Identity: params.Identity,
		Metadata: string(metadataBytes),
		// 注意：此处不直接授予权限，只设置invited标记，等用户举手后才授予发布权限
	})
	return freeErrors.LiveStreamSystemErr(err)
}

// RemoveFromStageParams 将参与者从舞台移除的参数
type RemoveFromStageParams struct {
	RoomName string `json:"room_name" binding:"required"`
	Identity string `json:"identity"` // 用户标识
}

// WebRemoveFromStage 将参与者从舞台移除
func (s *LivestreamService) WebRemoveFromStage(ctx context.Context, requestorIdentity string, params RemoveFromStageParams) error {
	// 如果未提供目标身份，使用请求者身份（自行下台）
	targetIdentity := params.Identity
	if params.Identity == "" {
		targetIdentity = requestorIdentity
	}

	// 1. 获取自身,目标参与者信息
	participant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: targetIdentity,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant: " + err.Error()))
	}

	participantMetadata, err := NewParticipantMetadataFromJson(participant.Metadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}

	// 2. 验证双方权限
	if targetIdentity != requestorIdentity {
		selfParticipant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
			Room:     params.RoomName,
			Identity: requestorIdentity,
		})
		if err != nil {
			return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant selfParticipant: " + err.Error()))
		}

		selfParticipantMetadata, err := NewParticipantMetadataFromJson(selfParticipant.Metadata)
		if err != nil {
			return freeErrors.SystemErr(err)
		}
		if !selfParticipantMetadata.HasRole(ParticipantRoleOwnerName, ParticipantRoleAdminName) {
			return freeErrors.LiveStreamRoomExecutePermissionErr
		}

		if !participantMetadata.HasRole(ParticipantRolePublisherName, ParticipantRoleUserName) {
			return freeErrors.LiveStreamRoomParticipantPermissionErr
		}
	}

	// 3. 重置元数据和权限
	participantMetadata.HandRaised = false
	participantMetadata.InvitedToStage = false
	participantMetadata.UpdateRole(ParticipantRoleUserName)
	participantMetadata.Nonce = uuid.New().String()

	// 序列化元数据
	metadataBytes, err := json.Marshal(participantMetadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}

	// 4. 更新参与者，撤销发布权限
	log.ZInfo(ctx, "将参与者从舞台移除", "房间", params.RoomName, "操作者", requestorIdentity, "目标", targetIdentity)

	_, err = s.roomService.UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{
		Room:       params.RoomName,
		Identity:   targetIdentity,
		Metadata:   string(metadataBytes),
		Permission: participantMetadata.Role.RolePermission,
	})
	return freeErrors.LiveStreamSystemErr(err)
}

// RaiseHandParams 观众举手参数
type RaiseHandParams struct {
	RoomName string `json:"room_name" binding:"required"`
}

// WebRaiseHand 观众举手（请求上台）
func (s *LivestreamService) WebRaiseHand(ctx context.Context, identity string, params RaiseHandParams) error {
	// 校验房间是否存在
	rooms, err := s.roomService.ListRooms(ctx, &livekit.ListRoomsRequest{
		Names: []string{params.RoomName},
	})
	if err != nil {
		return err
	}
	if len(rooms.Rooms) <= 0 {
		return freeErrors.LiveStreamRoomNotFoundErr(params.RoomName)
	}

	// 获取参与者信息
	participant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: identity,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant: " + err.Error()))
	}

	// 更新元数据和权限
	participantMetadata, err := NewParticipantMetadataFromJson(participant.Metadata)
	if err != nil {
		return freeErrors.LiveStreamSystemErr(err)
	}

	if !participantMetadata.HasRole(ParticipantRoleUserName) {
		return freeErrors.LiveStreamRoomParticipantPermissionErr
	}

	participantMetadata.HandRaised = true

	// 准备权限对象
	permission := participant.Permission

	var roomMetadata RoomMetadata
	if err := json.Unmarshal([]byte(rooms.Rooms[0].Metadata), &roomMetadata); err != nil {
		return freeErrors.SystemErr(err)
	}

	// 如果已被邀请且举手，自动授予发布权限
	if participantMetadata.InvitedToStage {
		log.ZInfo(ctx, "参与者接受邀请并举手，授予发布权限", "房间", params.RoomName, "参与者", identity)
		participantMetadata.UpdateRole(ParticipantRolePublisherName)
		participantMetadata.InvitedToStage = false
		participantMetadata.HandRaised = false
		permission = participantMetadata.Role.RolePermission
		// 记录邀请上台的统计数据
		roomMetadata.TotalOnStage += 1
	} else {
		// 记录普通举手的统计数据
		log.ZInfo(ctx, "普通观众举手", "房间", params.RoomName, "观众", identity)
		roomMetadata.TotalRaiseHands += 1
	}

	// 序列化元数据
	metadataBytes, err := json.Marshal(participantMetadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}

	// 更新参与者
	_, err = s.roomService.UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{
		Room:       params.RoomName,
		Identity:   identity,
		Metadata:   string(metadataBytes),
		Permission: permission,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(err)
	}

	// 更新统计数据
	roomMetadataBytes, err := json.Marshal(roomMetadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}
	_, err = s.roomService.UpdateRoomMetadata(ctx, &livekit.UpdateRoomMetadataRequest{
		Room:     params.RoomName,
		Metadata: string(roomMetadataBytes),
	})
	return freeErrors.LiveStreamSystemErr(err)
}

// ApproveHandRaiseParams 主播批准观众的举手请求的参数
type ApproveHandRaiseParams struct {
	RoomName string `json:"room_name" binding:"required"`
	Identity string `json:"identity" binding:"required"` // 用户标识
}

// WebApproveHandRaise 主播批准观众的举手请求
func (s *LivestreamService) WebApproveHandRaise(ctx context.Context, hostIdentity string, params ApproveHandRaiseParams) error {
	// 1. 验证房间和主播权限
	rooms, err := s.roomService.ListRooms(ctx, &livekit.ListRoomsRequest{
		Names: []string{params.RoomName},
	})
	if err != nil {
		return err
	}
	if len(rooms.Rooms) <= 0 {
		return freeErrors.LiveStreamRoomNotFoundErr(params.RoomName)
	}

	selfParticipant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: hostIdentity,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant selfParticipant: " + err.Error()))
	}

	selfParticipantMetadata, err := NewParticipantMetadataFromJson(selfParticipant.Metadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}
	if !selfParticipantMetadata.HasRole(ParticipantRoleOwnerName, ParticipantRoleAdminName) {
		return freeErrors.LiveStreamRoomExecutePermissionErr
	}

	// 2. 获取目标参与者信息
	participant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: params.Identity,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant: " + err.Error()))
	}

	// 3. 更新元数据和权限
	participantMetadata, err := NewParticipantMetadataFromJson(participant.Metadata)
	if err != nil {
		return err
	}
	if !participantMetadata.HasRole(ParticipantRoleUserName) {
		return freeErrors.LiveStreamRoomParticipantPermissionErr
	}

	// 清除手势标志，因为请求已处理
	participantMetadata.HandRaised = false
	participantMetadata.InvitedToStage = false
	participantMetadata.UpdateRole(ParticipantRolePublisherName)

	// 序列化元数据
	metadataBytes, err := json.Marshal(participantMetadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}

	log.ZInfo(ctx, "主播批准举手请求", "房间", params.RoomName, "主播", hostIdentity, "被批准者", params.Identity)

	// 更新参与者
	_, err = s.roomService.UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{
		Room:       params.RoomName,
		Identity:   params.Identity,
		Metadata:   string(metadataBytes),
		Permission: participantMetadata.Role.RolePermission,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(err)
	}

	// 记录上台统计数据
	var roomMetadata RoomMetadata
	if err := json.Unmarshal([]byte(rooms.Rooms[0].Metadata), &roomMetadata); err != nil {
		return freeErrors.SystemErr(err)
	}
	roomMetadata.TotalOnStage += 1
	roomMetadataBytes, err := json.Marshal(roomMetadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}
	_, err = s.roomService.UpdateRoomMetadata(ctx, &livekit.UpdateRoomMetadataRequest{
		Room:     params.RoomName,
		Metadata: string(roomMetadataBytes),
	})
	return freeErrors.LiveStreamSystemErr(err)
}

// BlockViewerParams 主播屏蔽观众的参数
type BlockViewerParams struct {
	RoomName string `json:"room_name" binding:"required"`
	Identity string `json:"identity" binding:"required"` // 用户标识
}

// WebBlockViewer 主播屏蔽观众
func (s *LivestreamService) WebBlockViewer(ctx context.Context, hostIdentity string, params BlockViewerParams) error {
	// 1. 验证房间和主播权限
	rooms, err := s.roomService.ListRooms(ctx, &livekit.ListRoomsRequest{
		Names: []string{params.RoomName},
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(err)
	}

	if len(rooms.Rooms) == 0 {
		return freeErrors.LiveStreamRoomNotFoundErr(params.RoomName)
	}

	room := rooms.Rooms[0]
	var roomMetadata RoomMetadata
	if err := json.Unmarshal([]byte(room.Metadata), &roomMetadata); err != nil {
		return freeErrors.SystemErr(err)
	}

	selfParticipant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: hostIdentity,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant selfParticipant: " + err.Error()))
	}

	selfParticipantMetadata, err := NewParticipantMetadataFromJson(selfParticipant.Metadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}
	if !selfParticipantMetadata.HasRole(ParticipantRoleOwnerName, ParticipantRoleAdminName) {
		return freeErrors.LiveStreamRoomExecutePermissionErr
	}

	// 2. 验证目标参与者权限
	participant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: params.Identity,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant: " + err.Error()))
	}

	participantMetadata, err := NewParticipantMetadataFromJson(participant.Metadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}
	if !participantMetadata.HasRole(ParticipantRolePublisherName, ParticipantRoleUserName) {
		return freeErrors.LiveStreamRoomParticipantPermissionErr
	}

	// 3. 将用户添加到屏蔽列表
	// 检查是否已经在屏蔽列表中
	isBlocked := false
	for _, id := range roomMetadata.BlockedIdentities {
		if id == params.Identity {
			isBlocked = true
			break
		}
	}

	// 如果未被屏蔽，添加到屏蔽列表
	if !isBlocked {
		roomMetadata.BlockedIdentities = append(roomMetadata.BlockedIdentities, params.Identity)

		// 更新房间元数据
		metadataBytes, err := json.Marshal(roomMetadata)
		if err != nil {
			return freeErrors.SystemErr(err)
		}

		_, err = s.roomService.UpdateRoomMetadata(ctx, &livekit.UpdateRoomMetadataRequest{
			Room:     params.RoomName,
			Metadata: string(metadataBytes),
		})
		if err != nil {
			return freeErrors.LiveStreamSystemErr(err)
		}
	}

	// 3. 从房间中移除该参与者
	log.ZInfo(ctx, "主播屏蔽观众", "房间", params.RoomName, "主播", hostIdentity, "被屏蔽者", params.Identity)

	if _, err = s.roomService.RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: params.Identity,
	}); err != nil {
		// 忽略已经离开的情况
		log.ZWarn(ctx, "移除参与者失败（可能已离开）", nil, "err", err, "房间", params.RoomName, "参与者", params.Identity)
	}

	return nil
}

// SetAdminParams 设置房管的参数
type SetAdminParams struct {
	RoomName string `json:"room_name" binding:"required"`
	Identity string `json:"identity" binding:"required"` // 用户标识
}

// WebSetAdminRole 设置房管
func (s *LivestreamService) WebSetAdminRole(ctx context.Context, hostIdentity string, params SetAdminParams) error {
	// 获取自身信息 验证自身权限
	selfParticipant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: hostIdentity,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant selfParticipant: " + err.Error()))
	}

	selfParticipantMetadata, err := NewParticipantMetadataFromJson(selfParticipant.Metadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}
	if !selfParticipantMetadata.HasRole(ParticipantRoleOwnerName) {
		return freeErrors.LiveStreamRoomExecutePermissionErr
	}

	// 验证目标参与者权限
	participant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: params.Identity,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant: " + err.Error()))
	}

	participantMetadata, err := NewParticipantMetadataFromJson(participant.Metadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}
	if !participantMetadata.HasRole(ParticipantRolePublisherName, ParticipantRoleUserName) {
		return freeErrors.LiveStreamRoomParticipantPermissionErr
	}

	participantMetadata.UpdateRole(ParticipantRoleAdminName)
	// 序列化元数据
	metadataBytes, err := json.Marshal(participantMetadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}

	log.ZInfo(ctx, "设置房管", "房间", params.RoomName, "主播", hostIdentity, "被批准者", params.Identity)

	// 5. 更新参与者
	_, err = s.roomService.UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{
		Room:       params.RoomName,
		Identity:   params.Identity,
		Metadata:   string(metadataBytes),
		Permission: participantMetadata.Role.RolePermission,
	})
	return freeErrors.LiveStreamSystemErr(err)
}

// RevokeAdminRoleParams 取消设置房管的参数
type RevokeAdminRoleParams struct {
	RoomName string `json:"room_name" binding:"required"`
	Identity string `json:"identity" binding:"required"` // 用户标识
}

// WebRevokeAdminRole 取消设置房管
func (s *LivestreamService) WebRevokeAdminRole(ctx context.Context, hostIdentity string, params RevokeAdminRoleParams) error {
	participant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     params.RoomName,
		Identity: params.Identity,
	})
	if err != nil {
		return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant: " + err.Error()))
	}

	participantMetadata, err := NewParticipantMetadataFromJson(participant.Metadata)
	if err != nil {
		return err
	}

	// 如果不是自己下台房管,需要验证操作者的权限
	if hostIdentity != params.Identity {
		selfParticipant, err := s.roomService.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
			Room:     params.RoomName,
			Identity: hostIdentity,
		})
		if err != nil {
			return freeErrors.LiveStreamSystemErr(errors.New("GetParticipant selfParticipant: " + err.Error()))
		}
		selfParticipantMetadata, err := NewParticipantMetadataFromJson(selfParticipant.Metadata)
		if err != nil {
			return err
		}
		if !selfParticipantMetadata.HasRole(ParticipantRoleOwnerName) {
			return freeErrors.LiveStreamRoomExecutePermissionErr
		}
	}

	participantMetadata.UpdateRole(ParticipantRoleUserName)
	// 序列化元数据
	metadataBytes, err := json.Marshal(participantMetadata)
	if err != nil {
		return freeErrors.SystemErr(err)
	}

	log.ZInfo(ctx, "取消房管", "房间", params.RoomName, "执行人", hostIdentity, "被批准者", params.Identity)

	// 5. 更新参与者
	_, err = s.roomService.UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{
		Room:       params.RoomName,
		Identity:   params.Identity,
		Metadata:   string(metadataBytes),
		Permission: participantMetadata.Role.RolePermission,
	})
	return freeErrors.LiveStreamSystemErr(err)
}

/*
	辅助函数
*/

// InternalGenerateTokenWithMetadata 生成访问令牌携带metadata
func (s *LivestreamService) InternalGenerateTokenWithMetadata(identity, room string, metaData *ParticipantMetadata) (string, error) {
	at := auth.NewAccessToken(s.apiKey, s.apiSecret)

	permission := metaData.Role.RolePermission

	grant := &auth.VideoGrant{
		RoomJoin:     true,
		Room:         room,
		CanPublish:   &permission.CanPublish,   // 使用指针
		CanSubscribe: &permission.CanSubscribe, // 使用指针
	}

	// 特殊处理数据发布权限
	if permission.CanPublishData {
		grant.CanPublishData = &permission.CanPublishData // 使用指针
	}

	metadataBytes, err := json.Marshal(metaData)
	if err != nil {
		return "", err
	}

	at.AddGrant(grant).
		SetIdentity(identity).
		SetMetadata(string(metadataBytes)).
		SetValidFor(time.Hour * 24) // 令牌有效期1小时

	return at.ToJWT()
}

type WebStartRecordingReq struct {
	RoomName string `json:"room_name" binding:"required"`
}

type WebStartRecordingResp struct {
	RoomName string `json:"room_name" binding:"required"`
	EgressId string `json:"egress_id"`
	FilePath string `json:"file_path"`
}

func (s *LivestreamService) WebStartRecording(ctx context.Context, params *WebStartRecordingReq) (*WebStartRecordingResp, error) {
	chatRpcConfig := plugin.ChatCfg().ChatRpcConfig
	lsDao := model.NewLivestreamStatisticsDao(plugin.MongoCli().GetDB())

	// todo 校验房间组织权限
	roomObj, err := lsDao.GetByRoomName(ctx, params.RoomName)
	if err != nil {
		return nil, err
	}

	// 校验房间是否有效
	rooms, err := s.roomService.ListRooms(context.Background(), &livekit.ListRoomsRequest{
		Names: []string{params.RoomName},
	})
	if err != nil {
		return nil, err
	}

	if len(rooms.Rooms) <= 0 {
		return nil, freeErrors.LiveStreamRoomNotFoundErr(params.RoomName)
	}

	room := rooms.Rooms[0]
	if room.NumParticipants <= 0 {
		return nil, freeErrors.ApiErr("room has no active participants")
	}

	// 校验是否有视频流
	participants, err := s.roomService.ListParticipants(context.Background(), &livekit.ListParticipantsRequest{
		Room: params.RoomName,
	})
	if err != nil {
		return nil, err
	}

	hasTrack := false
	for _, p := range participants.Participants {
		for _, t := range p.Tracks {
			if (t.Type == livekit.TrackType_VIDEO || t.Type == livekit.TrackType_AUDIO) && !t.Muted {
				hasTrack = true
				break
			}
		}
		if hasTrack {
			break
		}
	}
	if !hasTrack {
		return nil, freeErrors.ApiErr("room has no active video tracks")
	}

	activeEgress, err := s.egressService.ListEgress(context.Background(), &livekit.ListEgressRequest{
		RoomName: params.RoomName,
		Active:   true,
	})
	if err != nil {
		return nil, err
	}

	if len(activeEgress.Items) > 0 {
		return nil, freeErrors.ApiErr("room already has active egress")
	}

	filePath := fmt.Sprintf("s3-output/%s.mp4", params.RoomName)

	req := &livekit.RoomCompositeEgressRequest{
		RoomName:  params.RoomName,
		Layout:    "speaker",
		AudioOnly: false,
		FileOutputs: []*livekit.EncodedFileOutput{
			//{
			//	FileType: livekit.EncodedFileType_MP4,
			//	Filepath: fmt.Sprintf("/home/egress/output/%s.mp4", params.RoomName),
			//},
			{
				FileType: livekit.EncodedFileType_MP4,
				Filepath: filePath, // ✅ S3 上的路径
				Output: &livekit.EncodedFileOutput_S3{
					S3: &livekit.S3Upload{
						Secret:    chatRpcConfig.LiveKitRecord.Aws.Secret,
						AccessKey: chatRpcConfig.LiveKitRecord.Aws.AccessKey,
						Bucket:    chatRpcConfig.LiveKitRecord.Aws.Bucket,
						Region:    chatRpcConfig.LiveKitRecord.Aws.Region,
					},
				},
			},
		},
		Options: &livekit.RoomCompositeEgressRequest_Preset{
			Preset: livekit.EncodingOptionsPreset_H264_720P_30,
		},
	}

	comRes, err := s.egressService.StartRoomCompositeEgress(context.Background(), req)
	if err != nil {
		return nil, freeErrors.LiveStreamSystemErr(err)
	}

	var roomMetadata RoomMetadata
	if err := json.Unmarshal([]byte(room.Metadata), &roomMetadata); err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	roomMetadata.EgressId = comRes.EgressId
	roomMetadataBytes, err := json.Marshal(roomMetadata)
	if err != nil {
		return nil, freeErrors.SystemErr(err)
	}

	_, err = s.roomService.UpdateRoomMetadata(ctx, &livekit.UpdateRoomMetadataRequest{
		Room:     params.RoomName,
		Metadata: string(roomMetadataBytes),
	})
	if err != nil {
		return nil, freeErrors.LiveStreamSystemErr(err)
	}

	roomObj.RecordFile = append(roomObj.RecordFile, filePath)
	err = lsDao.UpdateRecordFileByRoomName(ctx, roomObj.RoomName, roomObj.RecordFile)
	if err != nil {
		return nil, err
	}

	return &WebStartRecordingResp{
		RoomName: params.RoomName,
		EgressId: comRes.EgressId,
		FilePath: filePath,
	}, nil
}

type WebStopRecordingReq struct {
	RoomName string `json:"room_name" binding:"required"`
}

type WebStopRecordingResp struct {
	StoppedEgress []*livekit.EgressInfo `json:"stopped_egress"`
}

func (s *LivestreamService) WebStopRecording(ctx context.Context, params *WebStopRecordingReq) (*WebStopRecordingResp, error) {
	activeEgress, err := s.egressService.ListEgress(context.Background(), &livekit.ListEgressRequest{
		RoomName: params.RoomName,
		Active:   true,
	})
	if err != nil {
		return nil, err
	}

	resp := &WebStopRecordingResp{
		StoppedEgress: []*livekit.EgressInfo{},
	}
	for _, egress := range activeEgress.Items {
		e, err := s.egressService.StopEgress(context.Background(), &livekit.StopEgressRequest{
			EgressId: egress.EgressId,
		})
		if err != nil {
			return nil, err
		}
		resp.StoppedEgress = append(resp.StoppedEgress, e)
	}

	return resp, nil
}

type WebListRecordingReq struct {
	RoomName string `json:"room_name" binding:"required"`
	Active   *bool  `json:"active"`
}

func (s *LivestreamService) WebListRecording(ctx context.Context, params *WebListRecordingReq) (*livekit.ListEgressResponse, error) {
	if params.Active == nil {
		params.Active = pointer.Bool(true)
	}

	activeEgress, err := s.egressService.ListEgress(context.Background(), &livekit.ListEgressRequest{
		RoomName: params.RoomName,
		Active:   *params.Active,
	})
	if err != nil {
		return nil, err
	}

	return activeEgress, nil
}

type CmsDetailRecordFileUrlReq struct {
	RoomName   string `json:"room_name" binding:"required"`
	RecordFile string `json:"record_file" binding:"required"`
}

type CmsDetailRecordFileUrlResp struct {
	RoomName    string `json:"room_name" `
	RecordFile  string `json:"record_file" `
	DownloadUrl string `json:"download_url" `
}

func (s *LivestreamService) CmsDetailRecordFileUrl(ctx context.Context, params *CmsDetailRecordFileUrlReq) (*CmsDetailRecordFileUrlResp, error) {
	chatRpcCfg := plugin.ChatCfg().ChatRpcConfig
	accessKey := chatRpcCfg.LiveKitRecord.Aws.AccessKey
	secretKey := chatRpcCfg.LiveKitRecord.Aws.Secret
	region := chatRpcCfg.LiveKitRecord.Aws.Region
	bucket := chatRpcCfg.LiveKitRecord.Aws.Bucket
	key := params.RecordFile

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, freeErrors.ApiErr("load config failed: " + err.Error())
	}

	client := s3.NewFromConfig(cfg)
	presignClient := s3.NewPresignClient(client)

	presignResult, err := presignClient.PresignGetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		return nil, freeErrors.ApiErr("generate presigned URL failed: " + err.Error())
	}

	return &CmsDetailRecordFileUrlResp{
		RoomName:    params.RoomName,
		RecordFile:  params.RecordFile,
		DownloadUrl: presignResult.URL,
	}, nil
}

// LivestreamStatisticsSvc 直播统计服务
type LivestreamStatisticsSvc struct{}

func NewLivestreamStatisticsService() *LivestreamStatisticsSvc {
	return &LivestreamStatisticsSvc{}
}

func (w *LivestreamStatisticsSvc) WebDetailStatisticsSvc(roomName string) (*dto.DetailLsStatisticsResp, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	dao := model.NewLivestreamStatisticsDao(db)
	statistics, err := dao.GetByRoomName(context.TODO(), roomName)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, freeErrors.NotFoundErr
		}
		return nil, err
	}
	return dto.NewDetailLsStatisticsResp(statistics), nil
}

func (w *LivestreamStatisticsSvc) WebListStatisticsSvc(orgId primitive.ObjectID, keyword string, status model.LivestreamStatisticsStatus, startCreatedTime, endCreatedTime time.Time,
	page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.LsStatisticsJoinUserResp], error) {
	db := plugin.MongoCli().GetDB()

	dao := model.NewLivestreamStatisticsDao(db)

	total, result, err := dao.SelectJoinUser(context.TODO(), "", keyword, orgId, status, startCreatedTime, endCreatedTime, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.LsStatisticsJoinUserResp]{
		Total: total,
		List:  []*dto.LsStatisticsJoinUserResp{},
	}
	for _, obj := range result {
		resp.List = append(resp.List, dto.NewLsStatisticsJoinUserResp(obj))
	}

	return resp, nil
}
