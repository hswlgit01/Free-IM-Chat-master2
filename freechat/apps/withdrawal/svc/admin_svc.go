package svc

import (
	"context"

	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	withdrawalDto "github.com/openimsdk/chat/freechat/apps/withdrawal/dto"
	withdrawalModel "github.com/openimsdk/chat/freechat/apps/withdrawal/model"
	"github.com/openimsdk/chat/freechat/plugin"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/errs"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type AdminWithdrawalSvc struct{}

func NewAdminWithdrawalSvc() *AdminWithdrawalSvc {
	return &AdminWithdrawalSvc{}
}

// GetWithdrawalList 获取提现列表
func (s *AdminWithdrawalSvc) GetWithdrawalList(ctx context.Context, organizationID string, req *withdrawalDto.AdminGetWithdrawalListReq) (*withdrawalDto.AdminGetWithdrawalListResp, error) {
	pagination := chat.Pagination{
		Page:     req.Page,
		PageSize: req.PageSize,
	}

	// 第一次搜索: 按订单号或用户ID搜索
	records, total, err := withdrawalModel.GetWithdrawalRecordDao().FindByOrganization(ctx, organizationID, req.Status, req.Keyword, pagination)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	// 如果有关键词但没有搜索结果,尝试按用户账号搜索
	if req.Keyword != "" && total == 0 {
		db := plugin.MongoCli().GetDB()
		attributeDao := chatModel.NewAttributeDao(db)

		// 尝试查找匹配账号的用户
		attr, err := attributeDao.TakeAccount(ctx, req.Keyword)
		if err == nil && attr != nil {
			// 用找到的用户ID再次搜索
			records, total, err = withdrawalModel.GetWithdrawalRecordDao().FindByOrganization(ctx, organizationID, req.Status, attr.UserID, pagination)
			if err != nil {
				return nil, errs.Wrap(err)
			}
		}
	}

	// 收集所有用户ID
	userIDs := make([]string, 0, len(records))
	for _, record := range records {
		if record.UserID != "" {
			userIDs = append(userIDs, record.UserID)
		}
	}

	// 批量获取用户信息(从Attribute表)
	userMap := make(map[string]*chatModel.Attribute)
	if len(userIDs) > 0 {
		db := plugin.MongoCli().GetDB()
		attributeDao := chatModel.NewAttributeDao(db)
		attributes, err := attributeDao.Find(ctx, userIDs)
		if err == nil {
			for _, attr := range attributes {
				userMap[attr.UserID] = attr
			}
		}
	}

	// 构建响应
	respRecords := make([]*withdrawalDto.WithdrawalRecordResp, 0, len(records))
	for _, record := range records {
		resp := toWithdrawalRecordResp(record)
		// 添加用户信息
		if attr, ok := userMap[record.UserID]; ok {
			resp.UserID = attr.UserID
			resp.UserAccount = attr.Account
			resp.Nickname = attr.Nickname
		} else {
			resp.UserID = record.UserID
			resp.UserAccount = ""
			resp.Nickname = ""
		}
		respRecords = append(respRecords, resp)
	}

	return &withdrawalDto.AdminGetWithdrawalListResp{
		Total:   total,
		Records: respRecords,
	}, nil
}

// GetWithdrawalDetail 获取提现详情
func (s *AdminWithdrawalSvc) GetWithdrawalDetail(ctx context.Context, organizationID, recordID string) (*withdrawalDto.WithdrawalRecordResp, error) {
	objectID, err := primitive.ObjectIDFromHex(recordID)
	if err != nil {
		return nil, freeErrors.ParameterInvalidErr.WrapMsg("invalid record ID")
	}

	record, err := withdrawalModel.GetWithdrawalRecordDao().FindByID(ctx, objectID)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	if record.OrganizationID != organizationID {
		return nil, freeErrors.ForbiddenErr("not authorized to view this withdrawal record")
	}

	resp := toWithdrawalRecordResp(record)

	// 获取用户信息(从Attribute表)
	if record.UserID != "" {
		db := plugin.MongoCli().GetDB()
		attributeDao := chatModel.NewAttributeDao(db)
		attr, err := attributeDao.Take(ctx, record.UserID)
		if err == nil {
			resp.UserID = attr.UserID
			resp.UserAccount = attr.Account
			resp.Nickname = attr.Nickname
		} else {
			resp.UserID = record.UserID
			resp.UserAccount = ""
			resp.Nickname = ""
		}
	}

	return resp, nil
}

// ApproveWithdrawal 审批通过提现
func (s *AdminWithdrawalSvc) ApproveWithdrawal(ctx context.Context, organizationID, adminID string, req *withdrawalDto.AdminApproveWithdrawalReq) error {
	objectID, err := primitive.ObjectIDFromHex(req.ID)
	if err != nil {
		return freeErrors.ParameterInvalidErr.WrapMsg("invalid record ID")
	}

	record, err := withdrawalModel.GetWithdrawalRecordDao().FindByID(ctx, objectID)
	if err != nil {
		return errs.Wrap(err)
	}

	if record.OrganizationID != organizationID {
		return freeErrors.ForbiddenErr("not authorized to approve this withdrawal")
	}

	if record.Status != chat.WithdrawalStatusPending {
		return errs.NewCodeError(freeErrors.ErrForbidden, "can only approve pending withdrawal")
	}

	extra := map[string]any{
		"approver_id": adminID,
	}

	return withdrawalModel.GetWithdrawalRecordDao().UpdateStatus(ctx, objectID, chat.WithdrawalStatusApproved, extra)
}

// RejectWithdrawal 审批拒绝提现
func (s *AdminWithdrawalSvc) RejectWithdrawal(ctx context.Context, organizationID, adminID string, req *withdrawalDto.AdminRejectWithdrawalReq) error {
	objectID, err := primitive.ObjectIDFromHex(req.ID)
	if err != nil {
		return freeErrors.ParameterInvalidErr.WrapMsg("invalid record ID")
	}

	record, err := withdrawalModel.GetWithdrawalRecordDao().FindByID(ctx, objectID)
	if err != nil {
		return errs.Wrap(err)
	}

	if record.OrganizationID != organizationID {
		return freeErrors.ForbiddenErr("not authorized to reject this withdrawal")
	}

	if record.Status != chat.WithdrawalStatusPending {
		return errs.NewCodeError(freeErrors.ErrForbidden, "can only reject pending withdrawal")
	}

	extra := map[string]any{
		"approver_id":   adminID,
		"reject_reason": req.Reason,
	}

	if err := withdrawalModel.GetWithdrawalRecordDao().UpdateStatus(ctx, objectID, chat.WithdrawalStatusRejected, extra); err != nil {
		return errs.Wrap(err)
	}

	// 退还金额
	db := plugin.MongoCli().GetDB()
	walletInfoDao := walletModel.NewWalletInfoDao(db)
	walletBalanceDao := walletModel.NewWalletBalanceDao(db)

	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, record.UserID, walletModel.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		return errs.Wrap(err)
	}

	// 使用提现记录中保存的货币ID退还金额
	refundAmount := decimal.NewFromFloat(record.Amount)
	return walletBalanceDao.UpdateAvailableBalance(ctx, walletInfo.ID, record.CurrencyID, refundAmount)
}

// TransferWithdrawal 确认打款
func (s *AdminWithdrawalSvc) TransferWithdrawal(ctx context.Context, organizationID string, req *withdrawalDto.AdminTransferWithdrawalReq) error {
	objectID, err := primitive.ObjectIDFromHex(req.ID)
	if err != nil {
		return freeErrors.ParameterInvalidErr.WrapMsg("invalid record ID")
	}

	record, err := withdrawalModel.GetWithdrawalRecordDao().FindByID(ctx, objectID)
	if err != nil {
		return errs.Wrap(err)
	}

	if record.OrganizationID != organizationID {
		return freeErrors.ForbiddenErr("not authorized to transfer this withdrawal")
	}

	if record.Status != chat.WithdrawalStatusApproved {
		return errs.NewCodeError(freeErrors.ErrForbidden, "can only transfer approved withdrawal")
	}

	return withdrawalModel.GetWithdrawalRecordDao().UpdateStatus(ctx, objectID, chat.WithdrawalStatusTransferring, nil)
}

// CompleteWithdrawal 确认完成
func (s *AdminWithdrawalSvc) CompleteWithdrawal(ctx context.Context, organizationID string, req *withdrawalDto.AdminCompleteWithdrawalReq) error {
	objectID, err := primitive.ObjectIDFromHex(req.ID)
	if err != nil {
		return freeErrors.ParameterInvalidErr.WrapMsg("invalid record ID")
	}

	record, err := withdrawalModel.GetWithdrawalRecordDao().FindByID(ctx, objectID)
	if err != nil {
		return errs.Wrap(err)
	}

	if record.OrganizationID != organizationID {
		return freeErrors.ForbiddenErr("not authorized to complete this withdrawal")
	}

	if record.Status != chat.WithdrawalStatusTransferring {
		return errs.NewCodeError(freeErrors.ErrForbidden, "can only complete transferring withdrawal")
	}

	return withdrawalModel.GetWithdrawalRecordDao().UpdateStatus(ctx, objectID, chat.WithdrawalStatusCompleted, nil)
}

// BatchApprove 批量审批
func (s *AdminWithdrawalSvc) BatchApprove(ctx context.Context, organizationID, adminID string, req *withdrawalDto.AdminBatchApproveReq) error {
	extra := map[string]any{
		"approver_id": adminID,
	}

	for _, idStr := range req.IDs {
		objectID, err := primitive.ObjectIDFromHex(idStr)
		if err != nil {
			continue
		}

		record, err := withdrawalModel.GetWithdrawalRecordDao().FindByID(ctx, objectID)
		if err != nil || record.OrganizationID != organizationID || record.Status != chat.WithdrawalStatusPending {
			continue
		}

		_ = withdrawalModel.GetWithdrawalRecordDao().UpdateStatus(ctx, objectID, chat.WithdrawalStatusApproved, extra)
	}

	return nil
}

// GetWithdrawalRule 获取提现规则
func (s *AdminWithdrawalSvc) GetWithdrawalRule(ctx context.Context, organizationID string) (*withdrawalDto.GetWithdrawalRuleResp, error) {
	rule, err := withdrawalModel.GetWithdrawalRuleDao().FindByOrganizationID(ctx, organizationID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return &withdrawalDto.GetWithdrawalRuleResp{
				IsEnabled:       false,
				MinAmount:       5.0,
				MaxAmount:       50000.0,
				FeeFixed:        5.0,
				FeeRate:         1.0,
				NeedRealName:    true,
				NeedBindAccount: true,
			}, nil
		}
		return nil, errs.Wrap(err)
	}

	return &withdrawalDto.GetWithdrawalRuleResp{
		IsEnabled:       rule.IsEnabled,
		MinAmount:       rule.MinAmount,
		MaxAmount:       rule.MaxAmount,
		FeeFixed:        rule.FeeFixed,
		FeeRate:         rule.FeeRate,
		NeedRealName:    rule.NeedRealName,
		NeedBindAccount: rule.NeedBindAccount,
	}, nil
}

// SaveWithdrawalRule 保存提现规则
func (s *AdminWithdrawalSvc) SaveWithdrawalRule(ctx context.Context, organizationID string, req *withdrawalDto.SaveWithdrawalRuleReq) error {
	// 验证最大金额必须大于最小金额
	if req.MaxAmount <= req.MinAmount {
		return freeErrors.ParameterInvalidErr.WrapMsg("max amount must be greater than min amount")
	}

	rule := &chat.WithdrawalRule{
		OrganizationID:  organizationID,
		IsEnabled:       req.IsEnabled,
		MinAmount:       req.MinAmount,
		MaxAmount:       req.MaxAmount,
		FeeFixed:        req.FeeFixed,
		FeeRate:         req.FeeRate,
		NeedRealName:    req.NeedRealName,
		NeedBindAccount: req.NeedBindAccount,
	}

	return withdrawalModel.GetWithdrawalRuleDao().Upsert(ctx, rule)
}

// toWithdrawalRecordResp 转换为响应对象(辅助函数)
func toWithdrawalRecordResp(record *chat.WithdrawalRecord) *withdrawalDto.WithdrawalRecordResp {
	resp := &withdrawalDto.WithdrawalRecordResp{
		ID:           record.ID.Hex(),
		OrderNo:      record.OrderNo,
		Amount:       record.Amount,
		Fee:          record.Fee,
		ActualAmount: record.ActualAmount,
		Status:       record.Status,
		PaymentType:  record.PaymentType,
		PaymentInfo:  record.PaymentInfo,
		RejectReason: record.RejectReason,
		ApproveTime:  record.ApproveTime,
		TransferTime: record.TransferTime,
		CompleteTime: record.CompleteTime,
		CreatedAt:    record.CreatedAt,
		CurrencyID:   record.CurrencyID.Hex(),
	}

	// 获取币种信息
	db := plugin.MongoCli().GetDB()
	currencyDao := walletModel.NewWalletCurrencyDao(db)

	// 只有在 CurrencyID 不为空时才尝试查询币种
	if !record.CurrencyID.IsZero() {
		currency, err := currencyDao.GetById(context.Background(), record.CurrencyID)
		if err == nil && currency != nil {
			resp.CurrencyName = currency.Name

			// 转换汇率
			exchangeRateDecimal, err := decimal.NewFromString(currency.ExchangeRate.String())
			if err == nil {
				exchangeRate, _ := exchangeRateDecimal.Float64()
				resp.ExchangeRate = exchangeRate

				// 设置币种符号
				if currency.Name == "CNY" {
					resp.CurrencySymbol = "¥"
				} else {
					resp.CurrencySymbol = "$"
				}

				// 计算人民币金额（手续费已经是人民币，不需要换算）
				resp.AmountInCNY = record.Amount * exchangeRate
				resp.ActualAmountInCNY = record.ActualAmount * exchangeRate
			}
		}
	}
	// 如果 CurrencyID 为空或查询失败，字段保持为空（零值）

	return resp
}
