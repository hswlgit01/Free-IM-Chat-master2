package imapi

import (
	"context"
	"sync"
	"time"

	"github.com/openimsdk/protocol/msg"

	"github.com/openimsdk/tools/log"

	"github.com/openimsdk/chat/freechat/utils"
	constantpb "github.com/openimsdk/chat/pkg/constant"
	"github.com/openimsdk/chat/pkg/eerrs"
	"github.com/openimsdk/protocol/auth"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/group"
	"github.com/openimsdk/protocol/relation"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/protocol/user"
)

type CallerInterface interface {
	ImAdminTokenWithDefaultAdmin(ctx context.Context) (string, error)
	ImportFriend(ctx context.Context, ownerUserID string, friendUserID []string) error
	GetUserToken(ctx context.Context, userID string, platform int32) (string, error)
	GetAdminTokenCache(ctx context.Context, userID string) (string, error)
	InviteToGroup(ctx context.Context, userID string, groupIDs []string) error
	UpdateUserInfo(ctx context.Context, userID string, nickName string, faceURL string) error
	ForceOffLine(ctx context.Context, userID string) error
	ForceOffLines(ctx context.Context, data any) error
	RegisterUser(ctx context.Context, users []*sdkws.UserInfo) error
	FindGroupInfo(ctx context.Context, groupIDs []string) ([]*sdkws.GroupInfo, error)
	UserRegisterCount(ctx context.Context, start int64, end int64) (map[string]int64, int64, error)
	FriendUserIDs(ctx context.Context, userID string) ([]string, error)
	AccountCheckSingle(ctx context.Context, userID string) (bool, error)
	GetGroupMemberUserIDs(ctx context.Context, groupID string) ([]string, error)
	SendMsg(ctx context.Context, msgData any) (*msg.SendMsgResp, error)
	BatchSendMsg(ctx context.Context, msgData any) error
	CreateGroup(ctx context.Context, req group.CreateGroupReq) (*group.CreateGroupResp, error)
	DismissGroup(ctx context.Context, req group.DismissGroupReq) (*group.DismissGroupResp, error)
	MuteGroup(ctx context.Context, req group.MuteGroupReq) (*group.MuteGroupResp, error)
	TransferGroup(ctx context.Context, req group.TransferGroupOwnerReq) (*group.TransferGroupOwnerResp, error)
	CreateGroupOperationLog(ctx context.Context, req any) (*any, error)
	CancelMuteGroup(ctx context.Context, req group.CancelMuteGroupReq) (*group.CancelMuteGroupResp, error)
	GetGroupMemberList(ctx context.Context, req group.GetGroupMemberListReq) (*group.GetGroupMemberListResp, error)
	SetGroupInfo(ctx context.Context, req group.SetGroupInfoReq) (*group.SetGroupInfoResp, error)
	SetGroupMemberInfo(ctx context.Context, req group.SetGroupMemberInfoReq) (*group.SetGroupMemberInfoResp, error)
	MuteGroupMember(ctx context.Context, req group.MuteGroupMemberReq) (*group.MuteGroupMemberResp, error)
	KickGroupMember(ctx context.Context, req group.KickGroupMemberReq) (*group.KickGroupMemberResp, error)
	CancelMuteGroupMember(ctx context.Context, req group.CancelMuteGroupMemberReq) (*group.CancelMuteGroupMemberResp, error)

	RegisterOrgUser(ctx context.Context, users []*OrgUserInfo) error
	UpdateOrgUserInfo(ctx context.Context, userID string, orgId, orgRole string) error
	UpdateUserCanSendFreeMsg(ctx context.Context, userID string, canSendFreeMsg int32) error
	AddNotificationAccount(ctx context.Context, userID, nickname, faceURL, orgId string) error
	UpdateNotificationAccount(ctx context.Context, userID, nickname, faceURL string) error
	AddFriend(ctx context.Context, req relation.ApplyToAddFriendReq) error
}

type authToken struct {
	token   string
	timeout time.Time
}

type Caller struct {
	imApi           string
	imSecret        string
	defaultIMUserID string
	tokenCache      map[string]*authToken
	lock            sync.RWMutex
}

func New(imApi string, imSecret string, defaultIMUserID string) CallerInterface {
	return &Caller{
		imApi:           imApi,
		imSecret:        imSecret,
		defaultIMUserID: defaultIMUserID,
		tokenCache:      make(map[string]*authToken),
		lock:            sync.RWMutex{},
	}
}

func (c *Caller) ImportFriend(ctx context.Context, ownerUserID string, friendUserIDs []string) error {
	if len(friendUserIDs) == 0 {
		return nil
	}
	_, err := importFriend.Call(ctx, c.imApi, &relation.ImportFriendReq{
		OwnerUserID:   ownerUserID,
		FriendUserIDs: friendUserIDs,
	})
	return err
}

type GetAdminTokenReq struct {
	Secret           string `json:"secret"`
	UserID           string `json:"userID"`
	RsaEncryptSecret string `json:"rsaEncryptSecret"`
}

func (c *Caller) ImAdminTokenWithDefaultAdmin(ctx context.Context) (string, error) {
	return c.GetAdminTokenCache(ctx, c.defaultIMUserID)
}

func (c *Caller) GetAdminTokenCache(ctx context.Context, userID string) (string, error) {
	c.lock.RLock()
	t, ok := c.tokenCache[userID]
	c.lock.RUnlock()
	if !ok || t.timeout.Before(time.Now()) {
		c.lock.Lock()
		defer c.lock.Unlock()
		t, ok = c.tokenCache[userID]
		if !ok || t.timeout.Before(time.Now()) {

			// 生成RSA密钥对
			publicKeyPEM, privateKeyPEM, err := utils.GenerateRSAKeyPair()
			if err != nil {
				log.ZError(ctx, "generate rsa key pair", err)
				return "", err
			}

			getAdminReq := GetAdminTokenReq{
				Secret:           c.imSecret,
				UserID:           userID,
				RsaEncryptSecret: publicKeyPEM,
			}
			token, err := c.getAdminTokenServer(ctx, getAdminReq)
			if err != nil {
				log.ZError(ctx, "get im admin token", err, "userID", userID)
				return "", err
			}

			// 服务端返回的token是用公钥加密的，需要用私钥解密
			decryptedToken, err := utils.RSADecrypt(privateKeyPEM, token)
			if err != nil {
				log.ZError(ctx, "decrypt token with rsa private key", err)
				return "", err
			}

			actualToken := string(decryptedToken)
			log.ZDebug(ctx, "get im admin token", "userID", userID)
			t = &authToken{token: actualToken, timeout: time.Now().Add(time.Minute * 5)}
			c.tokenCache[userID] = t
		}
	}
	return t.token, nil
}

func (c *Caller) getAdminTokenServer(ctx context.Context, getAdminReq any) (string, error) {
	resp, err := getAdminToken.Call(ctx, c.imApi, &getAdminReq)
	if err != nil {
		return "", err
	}
	return resp.Token, nil
}

func (c *Caller) GetUserToken(ctx context.Context, userID string, platformID int32) (string, error) {
	resp, err := getuserToken.Call(ctx, c.imApi, &auth.GetUserTokenReq{
		PlatformID: platformID,
		UserID:     userID,
	})
	if err != nil {
		return "", err
	}
	return resp.Token, nil
}

func (c *Caller) InviteToGroup(ctx context.Context, userID string, groupIDs []string) error {
	for _, groupID := range groupIDs {
		_, _ = inviteToGroup.Call(ctx, c.imApi, &group.InviteUserToGroupReq{
			GroupID:        groupID,
			Reason:         "",
			InvitedUserIDs: []string{userID},
		})
	}
	return nil
}

func (c *Caller) UpdateUserInfo(ctx context.Context, userID string, nickName string, faceURL string) error {
	_, err := updateUserInfo.Call(ctx, c.imApi, &user.UpdateUserInfoReq{UserInfo: &sdkws.UserInfo{
		UserID:   userID,
		Nickname: nickName,
		FaceURL:  faceURL,
	}})
	return err
}

func (c *Caller) RegisterUser(ctx context.Context, users []*sdkws.UserInfo) error {
	_, err := registerUser.Call(ctx, c.imApi, &user.UserRegisterReq{
		Users: users,
	})
	return err
}

func (c *Caller) ForceOffLine(ctx context.Context, userID string) error {
	for id := range constantpb.PlatformID2Name {
		// 跳过H5相关平台，避免参数错误
		if id == constantpb.H5PlatformID || id == constantpb.H5WebPlatformID {
			continue
		}
		_, _ = forceOffLine.Call(ctx, c.imApi, &auth.ForceLogoutReq{
			PlatformID: int32(id),
			UserID:     userID,
		})
	}
	return nil
}
func (c *Caller) ForceOffLines(ctx context.Context, data any) error {
	_, err := forceOffLines.Call(ctx, c.imApi, &data)
	if err != nil {
		return err
	}
	return nil
}

func (c *Caller) FindGroupInfo(ctx context.Context, groupIDs []string) ([]*sdkws.GroupInfo, error) {
	_ctx, _ := context.WithTimeout(ctx, 5*time.Minute)
	resp, err := getGroupsInfo.Call(_ctx, c.imApi, &group.GetGroupsInfoReq{
		GroupIDs: groupIDs,
	})
	if err != nil {
		return nil, err
	}
	return resp.GroupInfos, nil
}

func (c *Caller) UserRegisterCount(ctx context.Context, start int64, end int64) (map[string]int64, int64, error) {
	resp, err := registerUserCount.Call(ctx, c.imApi, &user.UserRegisterCountReq{
		Start: start,
		End:   end,
	})
	if err != nil {
		return nil, 0, err
	}
	return resp.Count, resp.Total, nil
}

func (c *Caller) FriendUserIDs(ctx context.Context, userID string) ([]string, error) {
	resp, err := friendUserIDs.Call(ctx, c.imApi, &relation.GetFriendIDsReq{UserID: userID})
	if err != nil {
		return nil, err
	}
	return resp.FriendIDs, nil
}

// return true when isUserNotExist.
func (c *Caller) AccountCheckSingle(ctx context.Context, userID string) (bool, error) {
	resp, err := accountCheck.Call(ctx, c.imApi, &user.AccountCheckReq{CheckUserIDs: []string{userID}})
	if err != nil {
		return false, err
	}
	if resp.Results[0].AccountStatus == constant.Registered {
		return false, eerrs.ErrAccountAlreadyRegister.Wrap()
	}
	return true, nil
}

// GetGroupMembers 获取群组成员列表
func (c *Caller) GetGroupMemberUserIDs(ctx context.Context, groupID string) ([]string, error) {
	resp, err := getGroupMemberUserIDs.Call(ctx, c.imApi, &group.GetGroupMemberUserIDsReq{
		GroupID: groupID,
	})
	if err != nil {
		return nil, err
	}
	return resp.UserIDs, nil
}

func (c *Caller) SendMsg(ctx context.Context, msgData any) (*msg.SendMsgResp, error) {
	resp, err := sendMsg.Call(ctx, c.imApi, &msgData)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (c *Caller) BatchSendMsg(ctx context.Context, msgData any) error {
	_, err := batchSendMsg.Call(ctx, c.imApi, &msgData)
	if err != nil {
		return err
	}
	return err
}

func (c *Caller) CreateGroup(ctx context.Context, req group.CreateGroupReq) (*group.CreateGroupResp, error) {
	resp, err := createGroup.Call(ctx, c.imApi, &req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (c *Caller) DismissGroup(ctx context.Context, req group.DismissGroupReq) (*group.DismissGroupResp, error) {
	resp, err := dismissGroup.Call(ctx, c.imApi, &req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (c *Caller) MuteGroup(ctx context.Context, req group.MuteGroupReq) (*group.MuteGroupResp, error) {
	resp, err := muteGroup.Call(ctx, c.imApi, &req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (c *Caller) CancelMuteGroup(ctx context.Context, req group.CancelMuteGroupReq) (*group.CancelMuteGroupResp, error) {
	resp, err := cancelMuteGroup.Call(ctx, c.imApi, &req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (c *Caller) GetGroupMemberList(ctx context.Context, req group.GetGroupMemberListReq) (*group.GetGroupMemberListResp, error) {
	resp, err := getGroupMemberList.Call(ctx, c.imApi, &req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (c *Caller) SetGroupInfo(ctx context.Context, req group.SetGroupInfoReq) (*group.SetGroupInfoResp, error) {
	resp, err := setGroupInfo.Call(ctx, c.imApi, &req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (c *Caller) SetGroupMemberInfo(ctx context.Context, req group.SetGroupMemberInfoReq) (*group.SetGroupMemberInfoResp, error) {
	resp, err := setGroupMemberInfo.Call(ctx, c.imApi, &req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (c *Caller) TransferGroup(ctx context.Context, req group.TransferGroupOwnerReq) (*group.TransferGroupOwnerResp, error) {
	resp, err := transferGroup.Call(ctx, c.imApi, &req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (c *Caller) CreateGroupOperationLog(ctx context.Context, req any) (*any, error) {
	resp, err := createGroupOperationLog.Call(ctx, c.imApi, &req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (c *Caller) MuteGroupMember(ctx context.Context, req group.MuteGroupMemberReq) (*group.MuteGroupMemberResp, error) {
	resp, err := muteGroupMember.Call(ctx, c.imApi, &req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (c *Caller) CancelMuteGroupMember(ctx context.Context, req group.CancelMuteGroupMemberReq) (*group.CancelMuteGroupMemberResp, error) {
	resp, err := cancelMuteGroupMember.Call(ctx, c.imApi, &req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (c *Caller) KickGroupMember(ctx context.Context, req group.KickGroupMemberReq) (*group.KickGroupMemberResp, error) {
	resp, err := kickGroupMember.Call(ctx, c.imApi, &req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

type OrgUserInfo struct {
	UserID           string `protobuf:"bytes,1,opt,name=userID,proto3" json:"userID"`
	Nickname         string `protobuf:"bytes,2,opt,name=nickname,proto3" json:"nickname"`
	FaceURL          string `protobuf:"bytes,3,opt,name=faceURL,proto3" json:"faceURL"`
	Ex               string `protobuf:"bytes,4,opt,name=ex,proto3" json:"ex"`
	CreateTime       int64  `protobuf:"varint,5,opt,name=createTime,proto3" json:"createTime"`
	AppMangerLevel   int32  `protobuf:"varint,6,opt,name=appMangerLevel,proto3" json:"appMangerLevel"`
	GlobalRecvMsgOpt int32  `protobuf:"varint,7,opt,name=globalRecvMsgOpt,proto3" json:"globalRecvMsgOpt"`
	CanSendFreeMsg   int32  `protobuf:"varint,8,opt,name=canSendFreeMsg,proto3" json:"canSendFreeMsg"` // 消息自由发送权限：0=需好友验证，1=可跳过验证

	OrgId   string `protobuf:"bytes,9,opt,name=orgId,proto3" json:"orgId"`      // 组织ID
	OrgRole string `protobuf:"bytes,10,opt,name=orgRole,proto3" json:"orgRole"` // 四种枚举 - "SuperAdmin", "BackendAdmin", "GroupManager", "Normal"
}

type RegisterOrgUserReq struct {
	Users []*OrgUserInfo `protobuf:"bytes,1,rep,name=users,proto3" json:"users"`
}

func (c *Caller) RegisterOrgUser(ctx context.Context, users []*OrgUserInfo) error {
	_, err := registerOrgUser.Call(ctx, c.imApi, &RegisterOrgUserReq{
		Users: users,
	})
	return err
}

type UpdateOrgUserInfoReq struct {
	UserInfo *OrgUserInfo `protobuf:"bytes,1,opt,name=userInfo,proto3" json:"userInfo"`
}

func (c *Caller) UpdateOrgUserInfo(ctx context.Context, userID string, orgId, orgRole string) error {
	_, err := updateOrgUserInfo.Call(ctx, c.imApi, &UpdateOrgUserInfoReq{UserInfo: &OrgUserInfo{
		UserID:  userID,
		OrgId:   orgId,
		OrgRole: orgRole,
	}})
	return err
}

func (c *Caller) UpdateUserCanSendFreeMsg(ctx context.Context, userID string, canSendFreeMsg int32) error {
	_, err := updateOrgUserInfo.Call(ctx, c.imApi, &UpdateOrgUserInfoReq{UserInfo: &OrgUserInfo{
		UserID:         userID,
		CanSendFreeMsg: canSendFreeMsg,
	}})
	return err
}

func (c *Caller) GetUserInfo(ctx context.Context, userID string, orgId, orgRole string) error {
	_, err := updateOrgUserInfo.Call(ctx, c.imApi, &UpdateOrgUserInfoReq{UserInfo: &OrgUserInfo{
		UserID:  userID,
		OrgId:   orgId,
		OrgRole: orgRole,
	}})
	return err
}

type AddNotificationAccountReq struct {
	UserID         string `protobuf:"bytes,1,opt,name=userID,proto3" json:"userID,omitempty"`
	NickName       string `protobuf:"bytes,2,opt,name=nickName,proto3" json:"nickName,omitempty"`
	FaceURL        string `protobuf:"bytes,3,opt,name=faceURL,proto3" json:"faceURL,omitempty"`
	AppMangerLevel int32  `protobuf:"varint,4,opt,name=appMangerLevel,proto3" json:"appMangerLevel,omitempty"`
	OrgId          string `protobuf:"bytes,5,opt,name=orgId,proto3" json:"orgId,omitempty"` // 组织ID
}

func (c *Caller) AddNotificationAccount(ctx context.Context, userID, nickname, faceURL, orgId string) error {
	_, err := addNotificationAccount.Call(ctx, c.imApi, &AddNotificationAccountReq{
		AppMangerLevel: 3,
		NickName:       nickname,
		UserID:         userID,
		FaceURL:        faceURL,
		OrgId:          orgId,
	})
	return err
}

func (c *Caller) UpdateNotificationAccount(ctx context.Context, userID, nickname, faceURL string) error {
	_, err := updateNotificationAccount.Call(ctx, c.imApi, &user.UpdateNotificationAccountInfoReq{
		UserID:   userID,
		NickName: nickname,
		FaceURL:  faceURL,
	})
	return err
}

func (c *Caller) AddFriend(ctx context.Context, req relation.ApplyToAddFriendReq) error {
	_, err := addFriend.Call(ctx, c.imApi, &req)
	return err
}
