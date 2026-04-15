package svc

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/openimsdk/chat/freechat/apps/identity/dto"
	OrgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	systemStatistics "github.com/openimsdk/chat/freechat/apps/systemStatistics"
	"github.com/openimsdk/chat/freechat/plugin"
	chatCache "github.com/openimsdk/chat/freechat/third/chat/cache"
	freechatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	chatModel "github.com/openimsdk/chat/pkg/common/db/model/chat"
	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/mongo"
)

type IdentitySvc struct{}

func NewIdentitySvc() *IdentitySvc {
	return &IdentitySvc{}
}

// ValidateIDCard 验证身份证号格式
func (s *IdentitySvc) ValidateIDCard(idCard string) error {
	if len(idCard) != 18 {
		return errors.New("身份证号必须为18位")
	}

	// 验证格式：前17位数字，最后一位数字或X
	pattern := regexp.MustCompile(`^\d{17}[\dXx]$`)
	if !pattern.MatchString(idCard) {
		return errors.New("身份证号格式不正确")
	}

	return nil
}

// SubmitIdentity 提交身份认证
func (s *IdentitySvc) SubmitIdentity(ctx context.Context, userID string, req *dto.SubmitIdentityReq) (*dto.SubmitIdentityResp, error) {
	// 1. 验证身份证号
	if err := s.ValidateIDCard(req.IDCardNumber); err != nil {
		return nil, errs.Wrap(err)
	}

	// 2. 检查是否已有认证记录
	db := plugin.MongoCli().GetDB()
	identityDB, err := chatModel.NewIdentityVerification(db)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	existing, err := identityDB.Take(ctx, userID)

	now := time.Now()

	if err != nil {
		// 如果不存在，创建新记录
		if errors.Is(err, mongo.ErrNoDocuments) {
			identity := &chat.IdentityVerification{
				UserID:       userID,
				Status:       1, // 审核中
				RealName:     req.RealName,
				IDCardNumber: req.IDCardNumber,
				IDCardFront:  req.IDCardFront,
				IDCardBack:   req.IDCardBack,
				ApplyTime:    now,
				CreateTime:   now,
				UpdateTime:   now,
			}

			if err := identityDB.Create(ctx, identity); err != nil {
				return nil, errs.Wrap(err)
			}

			return &dto.SubmitIdentityResp{Status: 1}, nil
		}
		return nil, errs.Wrap(err)
	}

	// 3. 如果已存在，检查状态
	// 只有状态为0（待认证）或3（已拒绝）时才能重新提交
	if existing.Status == 1 {
		return nil, errors.New("已有认证申请正在审核中，请勿重复提交")
	}
	if existing.Status == 2 {
		return nil, errors.New("您已通过实名认证，无需重复提交")
	}

	// 4. 更新认证信息
	updateData := map[string]any{
		"status":         int32(1), // 审核中
		"real_name":      req.RealName,
		"id_card_number": req.IDCardNumber,
		"id_card_front":  req.IDCardFront,
		"id_card_back":   req.IDCardBack,
		"apply_time":     now,
		"reject_reason":  "", // 清空拒绝原因
	}

	if err := identityDB.Update(ctx, userID, updateData); err != nil {
		return nil, errs.Wrap(err)
	}

	return &dto.SubmitIdentityResp{Status: 1}, nil
}

// GetIdentityInfo 获取身份认证信息
func (s *IdentitySvc) GetIdentityInfo(ctx context.Context, userID string) (*dto.GetIdentityInfoResp, error) {
	db := plugin.MongoCli().GetDB()
	identityDB, err := chatModel.NewIdentityVerification(db)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	identity, err := identityDB.Take(ctx, userID)

	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// 不存在记录，返回待认证状态
			return &dto.GetIdentityInfoResp{Status: 0}, nil
		}
		return nil, errs.Wrap(err)
	}

	// 处理时间字段（零值时返回0）
	var applyTime, verifyTime int64
	if !identity.ApplyTime.IsZero() {
		applyTime = identity.ApplyTime.Unix()
	}
	if !identity.VerifyTime.IsZero() {
		verifyTime = identity.VerifyTime.Unix()
	}

	resp := &dto.GetIdentityInfoResp{
		Status:       identity.Status,
		RealName:     identity.RealName,
		IDCardNumber: identity.IDCardNumber,
		IDCardFront:  identity.IDCardFront,
		IDCardBack:   identity.IDCardBack,
		RejectReason: identity.RejectReason,
		ApplyTime:    applyTime,
		VerifyTime:   verifyTime,
	}

	return resp, nil
}

// AdminGetIdentityList 管理员获取认证列表
func (s *IdentitySvc) AdminGetIdentityList(ctx context.Context, req *dto.AdminGetIdentityListReq, orgID string) (*dto.AdminGetIdentityListResp, error) {
	db := plugin.MongoCli().GetDB()
	identityDB, err := chatModel.NewIdentityVerification(db)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	// 分页查询认证记录（req 实现了 Pagination 接口）
	// 传递所有查询参数，包括排序和时间范围过滤
	total, identities, err := identityDB.FindByStatusAndOrg(
		ctx,
		req.Status,
		req.Keyword,
		orgID,
		req,
		req.OrderKey,
		req.OrderDirection,
		req.StartTime,
		req.EndTime,
		req.VerifyStartTime,
		req.VerifyEndTime,
	)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	list, err := s.buildAdminIdentityItems(ctx, db, identities)
	if err != nil {
		return nil, err
	}

	return &dto.AdminGetIdentityListResp{
		Total: total,
		List:  list,
	}, nil
}

// AdminGetPendingIdentityList 当前组织下「审核中」实名申请列表（status=1），默认每页 100、单页上限 500。
func (s *IdentitySvc) AdminGetPendingIdentityList(ctx context.Context, req *dto.AdminGetPendingIdentityListReq, orgID string) (*dto.AdminGetIdentityListResp, error) {
	pn := req.PageNumber
	sn := req.ShowNumber
	if pn < 1 {
		pn = 1
	}
	if sn < 1 {
		sn = 100
	}
	if sn > 500 {
		sn = 500
	}
	status := int32(1)
	listReq := &dto.AdminGetIdentityListReq{
		Status:          &status,
		Keyword:         req.Keyword,
		PageNumber:      pn,
		ShowNumber:      sn,
		OrderKey:        req.OrderKey,
		OrderDirection:  req.OrderDirection,
		StartTime:       req.StartTime,
		EndTime:         req.EndTime,
		VerifyStartTime: 0,
		VerifyEndTime:   0,
	}
	return s.AdminGetIdentityList(ctx, listReq, orgID)
}

// AdminApproveBatch 批量通过；单条失败不中断，在 failed 中返回原因。
func (s *IdentitySvc) AdminApproveBatch(ctx context.Context, userIDs []string, adminID string) *dto.AdminApproveBatchResp {
	out := &dto.AdminApproveBatchResp{Failed: []dto.AdminApproveFailure{}}
	for _, uid := range userIDs {
		uid = strings.TrimSpace(uid)
		if uid == "" {
			continue
		}
		if err := s.AdminApprove(ctx, uid, adminID); err != nil {
			out.Failed = append(out.Failed, dto.AdminApproveFailure{UserID: uid, ErrMsg: err.Error()})
		} else {
			out.Success++
		}
	}
	return out
}

// buildAdminIdentityItems 将 identity_verifications 记录组装为管理员列表项（仅做关联查询与字段映射，不依赖 FindByStatusAndOrg）。
func (s *IdentitySvc) buildAdminIdentityItems(ctx context.Context, db *mongo.Database, identities []*chat.IdentityVerification) ([]*dto.AdminIdentityItem, error) {
	if len(identities) == 0 {
		return []*dto.AdminIdentityItem{}, nil
	}

	// 获取申请用户信息和审核管理员信息
	userIDs := make([]string, 0, len(identities)*2)
	for _, identity := range identities {
		userIDs = append(userIDs, identity.UserID)
		if identity.VerifyAdmin != "" {
			userIDs = append(userIDs, identity.VerifyAdmin)
		}
	}

	userIDMap := make(map[string]bool)
	uniqueUserIDs := make([]string, 0, len(userIDs))
	for _, id := range userIDs {
		if !userIDMap[id] {
			userIDMap[id] = true
			uniqueUserIDs = append(uniqueUserIDs, id)
		}
	}

	var attributes []*freechatModel.Attribute
	var err error
	if len(uniqueUserIDs) > 0 {
		attributeDB := freechatModel.NewAttributeDao(db)
		attributes, err = attributeDB.Find(ctx, uniqueUserIDs)
		if err != nil {
			return nil, errs.Wrap(err)
		}
	}

	userMap := make(map[string]*freechatModel.Attribute)
	for _, attr := range attributes {
		userMap[attr.UserID] = attr
	}

	orgUserDao := OrgModel.NewOrganizationUserDao(db)
	imUserDao := openImModel.NewUserDao(db)
	imUserMap := make(map[string]*openImModel.User)

	for _, userID := range uniqueUserIDs {
		orgUser, err := orgUserDao.GetByUserId(ctx, userID)
		if err == nil && orgUser != nil {
			imUser, err := imUserDao.Take(ctx, orgUser.ImServerUserId)
			if err == nil && imUser != nil {
				imUserMap[userID] = imUser
			}
		}
	}

	list := make([]*dto.AdminIdentityItem, 0, len(identities))
	for _, identity := range identities {
		var applyTime, verifyTime int64
		if !identity.ApplyTime.IsZero() {
			applyTime = identity.ApplyTime.UnixMilli()
		}
		if !identity.VerifyTime.IsZero() {
			verifyTime = identity.VerifyTime.UnixMilli()
		}

		item := &dto.AdminIdentityItem{
			UserID:       identity.UserID,
			RealName:     identity.RealName,
			IDCardNumber: identity.IDCardNumber,
			IDCardFront:  identity.IDCardFront,
			IDCardBack:   identity.IDCardBack,
			Status:       identity.Status,
			ApplyTime:    applyTime,
			VerifyTime:   verifyTime,
			VerifyAdmin:  identity.VerifyAdmin,
			RejectReason: identity.RejectReason,
		}

		if imUser, ok := imUserMap[identity.UserID]; ok {
			item.Nickname = imUser.Nickname
			item.FaceURL = imUser.FaceURL
		}
		if attr, ok := userMap[identity.UserID]; ok {
			item.Account = attr.Account
			if item.Nickname == "" {
				item.Nickname = attr.Nickname
			}
			if item.FaceURL == "" {
				item.FaceURL = attr.FaceURL
			}
		}

		if identity.VerifyAdmin != "" {
			if adminIMUser, ok := imUserMap[identity.VerifyAdmin]; ok {
				item.VerifyAdminName = adminIMUser.Nickname
			} else if adminAttr, ok := userMap[identity.VerifyAdmin]; ok {
				item.VerifyAdminName = adminAttr.Nickname
			}
		}

		list = append(list, item)
	}

	return list, nil
}

// AdminGetIdentityDetailByKeyword 按 keyword 查询当前组织一条实名详情：organization_user 归属校验 + identity_verifications.Take + 组装字段；不调用 FindByStatusAndOrg。
// keyword 支持 chat 侧 user_id，或 attribute.account（账号）；均须属于当前 org。
func (s *IdentitySvc) AdminGetIdentityDetailByKeyword(ctx context.Context, keyword string, orgID string) (*dto.AdminGetIdentityListResp, error) {
	log.ZInfo(ctx, "AdminGetIdentityDetailByKeyword request", "keyword", keyword, "orgID", orgID)

	empty := &dto.AdminGetIdentityListResp{Total: 0, List: []*dto.AdminIdentityItem{}}

	keyword = strings.TrimSpace(keyword)
	db := plugin.MongoCli().GetDB()
	identityDB, err := chatModel.NewIdentityVerification(db)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	chatUserID, err := s.resolveKeywordToOrgChatUserID(ctx, db, keyword, orgID)
	if err != nil {
		log.ZError(ctx, "管理端实名详情：解析关键词或校验组织失败", err, "关键词", keyword, "组织ID", orgID)
		return nil, err
	}
	if chatUserID == "" {
		logAdminGetIdentityDetailResponse(ctx, keyword, orgID, empty)
		return empty, nil
	}

	identity, err := identityDB.Take(ctx, chatUserID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			logAdminGetIdentityDetailResponse(ctx, keyword, orgID, empty)
			return empty, nil
		}
		log.ZError(ctx, "管理端实名详情：查询实名认证表失败", err, "关键词", keyword, "组织ID", orgID, "聊天用户ID", chatUserID)
		return nil, errs.Wrap(err)
	}

	list, err := s.buildAdminIdentityItems(ctx, db, []*chat.IdentityVerification{identity})
	if err != nil {
		log.ZError(ctx, "管理端实名详情：组装返回数据失败", err, "关键词", keyword, "组织ID", orgID)
		return nil, err
	}

	resp := &dto.AdminGetIdentityListResp{Total: int64(len(list)), List: list}
	logAdminGetIdentityDetailResponse(ctx, keyword, orgID, resp)
	return resp, nil
}

// resolveKeywordToOrgChatUserID 先按 chat user_id 查 organization_user；若无则按 attribute.account 解析 user_id；必须属于 orgID 对应组织。
func (s *IdentitySvc) resolveKeywordToOrgChatUserID(ctx context.Context, db *mongo.Database, keyword, orgID string) (string, error) {
	orgUserDao := OrgModel.NewOrganizationUserDao(db)
	attributeDao := freechatModel.NewAttributeDao(db)

	_, err := orgUserDao.GetByUserIdAndOrgID(ctx, keyword, orgID)
	if err == nil {
		return keyword, nil
	}
	if !dbutil.IsDBNotFound(err) {
		return "", errs.Wrap(err)
	}

	attr, err := attributeDao.TakeAccount(ctx, keyword)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return "", nil
		}
		return "", errs.Wrap(err)
	}

	_, err = orgUserDao.GetByUserIdAndOrgID(ctx, attr.UserID, orgID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return "", nil
		}
		return "", errs.Wrap(err)
	}
	return attr.UserID, nil
}

// logAdminGetIdentityDetailResponse 输出与 list 相同的业务字段；身份证号脱敏，证件照 URL 只打长度避免日志过长或泄露 token。
func logAdminGetIdentityDetailResponse(ctx context.Context, keyword, orgID string, resp *dto.AdminGetIdentityListResp) {
	if resp == nil {
		log.ZInfo(ctx, "管理端实名详情响应", "关键词", keyword, "组织ID", orgID, "总数", int64(0), "条数", 0)
		return
	}
	log.ZInfo(ctx, "管理端实名详情响应汇总", "关键词", keyword, "组织ID", orgID, "总数", resp.Total, "条数", len(resp.List))
	for i, it := range resp.List {
		log.ZInfo(ctx, "管理端实名详情响应条目",
			"序号", i,
			"用户ID", it.UserID,
			"账号", it.Account,
			"昵称", it.Nickname,
			"头像URL长度", len(it.FaceURL),
			"真实姓名", it.RealName,
			"身份证号脱敏", maskIDCardForLog(it.IDCardNumber),
			"身份证正面URL长度", len(it.IDCardFront),
			"身份证反面URL长度", len(it.IDCardBack),
			"状态", it.Status,
			"申请时间", it.ApplyTime,
			"审核时间", it.VerifyTime,
			"审核管理员ID", it.VerifyAdmin,
			"审核管理员昵称", it.VerifyAdminName,
			"拒绝原因", it.RejectReason,
		)
	}
}

func maskIDCardForLog(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if len(s) <= 4 {
		return "****"
	}
	return "****" + s[len(s)-4:]
}

// AdminApprove 管理员审核通过
func (s *IdentitySvc) AdminApprove(ctx context.Context, userID string, adminID string) error {
	db := plugin.MongoCli().GetDB()
	identityDB, err := chatModel.NewIdentityVerification(db)
	if err != nil {
		return errs.Wrap(err)
	}

	// 1. 获取认证信息
	identity, err := identityDB.Take(ctx, userID)
	if err != nil {
		return errs.Wrap(err)
	}

	// 2. 检查状态
	if identity.Status != 1 {
		return errors.New("只能审核状态为审核中的申请")
	}

	// 3. 更新认证表
	if err := identityDB.Approve(ctx, userID, adminID); err != nil {
		return errs.Wrap(err)
	}

	// 4. 更新用户属性表
	now := time.Now()
	updateData := map[string]any{
		"is_real_name_verified": true,
		"real_name":             identity.RealName,
		"verified_time":         now,
	}

	attributeDB := freechatModel.NewAttributeDao(db)
	if err := attributeDB.Update(ctx, userID, updateData); err != nil {
		return errs.Wrap(err)
	}

	// 5. 清除Redis缓存，确保下次查询获取最新数据
	attributeCache := chatCache.NewAttributeCacheRedis(plugin.RedisCli(), db)
	if err := attributeCache.DelCache(ctx, userID); err != nil {
		// 缓存删除失败不应该阻塞整个流程，记录日志即可
		log.ZWarn(ctx, "删除用户属性缓存失败", err, "userID", userID)
	}

	if ou, e := OrgModel.NewOrganizationUserDao(db).GetByUserId(ctx, userID); e == nil && ou != nil {
		systemStatistics.NotifySalesDailyStatsChangedAsync(ou.OrganizationId)
	}

	return nil
}

// AdminReject 管理员审核拒绝
func (s *IdentitySvc) AdminReject(ctx context.Context, userID string, adminID string, reason string) error {
	db := plugin.MongoCli().GetDB()
	identityDB, err := chatModel.NewIdentityVerification(db)
	if err != nil {
		return errs.Wrap(err)
	}

	// 1. 获取认证信息
	identity, err := identityDB.Take(ctx, userID)
	if err != nil {
		return errs.Wrap(err)
	}

	// 2. 检查状态
	if identity.Status != 1 {
		return errors.New("只能审核状态为审核中的申请")
	}

	// 3. 更新认证表
	if err := identityDB.Reject(ctx, userID, adminID, reason); err != nil {
		return errs.Wrap(err)
	}

	return nil
}

// AdminCancelVerification 管理员取消用户实名认证
func (s *IdentitySvc) AdminCancelVerification(ctx context.Context, userID string, adminID string) error {
	db := plugin.MongoCli().GetDB()

	// 1. 检查用户是否已实名认证
	attributeDB := freechatModel.NewAttributeDao(db)

	attribute, err := attributeDB.Take(ctx, userID)
	if err != nil {
		return errs.Wrap(err)
	}

	if !attribute.IsRealNameVerified {
		return errors.New("该用户尚未实名认证")
	}

	// 2. 更新用户属性表，取消实名认证
	updateData := map[string]any{
		"is_real_name_verified": false,
		"real_name":             "",
		"verified_time":         time.Time{}, // 设置为零值
	}

	if err := attributeDB.Update(ctx, userID, updateData); err != nil {
		return errs.Wrap(err)
	}

	// 3. 清除Redis缓存，确保下次查询获取最新数据
	attributeCache := chatCache.NewAttributeCacheRedis(plugin.RedisCli(), db)
	if err := attributeCache.DelCache(ctx, userID); err != nil {
		// 缓存删除失败不应该阻塞整个流程，记录日志即可
		log.ZWarn(ctx, "删除用户属性缓存失败", err, "userID", userID)
	}

	// 4. 更新或删除认证记录（将状态改为待认证）
	identityDB, err := chatModel.NewIdentityVerification(db)
	if err != nil {
		return errs.Wrap(err)
	}

	// 检查是否存在认证记录
	identity, err := identityDB.Take(ctx, userID)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return errs.Wrap(err)
	}

	if identity != nil {
		// 存在记录，更新为待认证状态
		identityUpdateData := map[string]any{
			"status":        int32(0), // 待认证
			"verify_admin":  "",
			"verify_time":   time.Time{},
			"reject_reason": "管理员取消认证",
		}
		if err := identityDB.Update(ctx, userID, identityUpdateData); err != nil {
			return errs.Wrap(err)
		}
	}

	if ou, e := OrgModel.NewOrganizationUserDao(db).GetByUserId(ctx, userID); e == nil && ou != nil {
		systemStatistics.NotifySalesDailyStatsChangedAsync(ou.OrganizationId)
	}

	return nil
}
