package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	paymentMethodModel "github.com/openimsdk/chat/freechat/apps/paymentMethod/model"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	withdrawalDto "github.com/openimsdk/chat/freechat/apps/withdrawal/dto"
	withdrawalModel "github.com/openimsdk/chat/freechat/apps/withdrawal/model"
	"github.com/openimsdk/chat/freechat/plugin"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	identityModel "github.com/openimsdk/chat/pkg/common/db/model/chat"
	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/errs"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type WithdrawalSvc struct{}

func NewWithdrawalSvc() *WithdrawalSvc {
	return &WithdrawalSvc{}
}

// GetWithdrawalRule 获取提现规则
func (s *WithdrawalSvc) GetWithdrawalRule(ctx context.Context, organizationID string) (*withdrawalDto.GetWithdrawalRuleResp, error) {
	rule, err := withdrawalModel.GetWithdrawalRuleDao().FindByOrganizationID(ctx, organizationID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			// 如果没有规则,返回默认禁用状态
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

// SubmitWithdrawal 提交提现申请
func (s *WithdrawalSvc) SubmitWithdrawal(ctx context.Context, userID, organizationID string, req *withdrawalDto.SubmitWithdrawalReq) (*withdrawalDto.SubmitWithdrawalResp, error) {
	// 1. 获取提现规则
	rule, err := withdrawalModel.GetWithdrawalRuleDao().FindByOrganizationID(ctx, organizationID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return nil, errs.NewCodeError(freeErrors.ErrNotFoundCode, "withdrawal is not enabled for this organization")
		}
		return nil, errs.Wrap(err)
	}

	// 2. 检查提现功能是否启用
	if !rule.IsEnabled {
		return nil, errs.NewCodeError(freeErrors.ErrForbidden, "withdrawal is disabled")
	}

	// 3. 检查是否有未完成的提现申请
	pagination := chat.Pagination{
		Page:     1,
		PageSize: 1,
	}
	records, total, err := withdrawalModel.GetWithdrawalRecordDao().FindByUserID(ctx, userID, pagination)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	// 如果有记录且最新的记录状态为未完成(待审核、已通过、打款中)
	if total > 0 && len(records) > 0 {
		latestRecord := records[0]
		if latestRecord.Status == chat.WithdrawalStatusPending ||
			latestRecord.Status == chat.WithdrawalStatusApproved ||
			latestRecord.Status == chat.WithdrawalStatusTransferring {
			return nil, errs.NewCodeError(freeErrors.ErrForbidden, "you have a pending withdrawal request, please wait for it to complete")
		}
	}

	// 4. 检查金额范围
	if req.Amount < rule.MinAmount {
		return nil, errs.NewCodeError(freeErrors.ErrInvalidAmount, fmt.Sprintf("minimum withdrawal amount is %.2f", rule.MinAmount))
	}
	if req.Amount > rule.MaxAmount {
		return nil, errs.NewCodeError(freeErrors.ErrInvalidAmount, fmt.Sprintf("maximum withdrawal amount is %.2f", rule.MaxAmount))
	}

	// 5. 检查实名认证
	if rule.NeedRealName {
		db := plugin.MongoCli().GetDB()
		identityDB, err := identityModel.NewIdentityVerification(db)
		if err != nil {
			return nil, errs.Wrap(err)
		}
		identityInfo, err := identityDB.Take(ctx, userID)
		if err != nil || identityInfo == nil || identityInfo.Status != 2 {
			return nil, errs.NewCodeError(freeErrors.ErrForbidden, "please complete real-name verification first")
		}
	}

	// 6. 检查收款方式
	if rule.NeedBindAccount {
		paymentMethodID, err := primitive.ObjectIDFromHex(req.PaymentMethodID)
		if err != nil {
			return nil, freeErrors.ParameterInvalidErr.WrapMsg("invalid payment method ID")
		}

		paymentMethod, err := paymentMethodModel.GetPaymentMethodDao().FindByID(ctx, paymentMethodID)
		if err != nil {
			return nil, errs.Wrap(err)
		}

		if paymentMethod.UserID != userID {
			return nil, freeErrors.ForbiddenErr("payment method does not belong to this user")
		}
	}

	// 7. 获取钱包信息和检查余额
	db := plugin.MongoCli().GetDB()
	walletInfoDao := walletModel.NewWalletInfoDao(db)
	walletBalanceDao := walletModel.NewWalletBalanceDao(db)

	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, userID, walletModel.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	var currencyID primitive.ObjectID
	var balanceDecimal decimal.Decimal

	// 如果前端指定了币种ID,则使用指定的币种
	if req.CurrencyID != "" {
		currencyID, err = primitive.ObjectIDFromHex(req.CurrencyID)
		if err != nil {
			return nil, freeErrors.ParameterInvalidErr.WrapMsg("invalid currency ID")
		}

		// 检查指定币种的余额
		walletBalance, err := walletBalanceDao.GetByWalletIdAndCurrencyId(ctx, walletInfo.ID, currencyID)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return nil, errs.NewCodeError(freeErrors.ErrInsufficientBalance, "no balance for this currency")
			}
			return nil, errs.Wrap(err)
		}

		balanceDecimal, err = decimal.NewFromString(walletBalance.AvailableBalance.String())
		if err != nil {
			return nil, errs.Wrap(err)
		}

		requestAmount := decimal.NewFromFloat(req.Amount)
		if balanceDecimal.LessThan(requestAmount) {
			return nil, errs.NewCodeError(freeErrors.ErrInsufficientBalance, "insufficient balance")
		}
	} else {
		// 如果没有指定币种,则自动选择余额最多的币种
		walletBalances, err := walletBalanceDao.FindByWalletId(ctx, walletInfo.ID)
		if err != nil || len(walletBalances) == 0 {
			return nil, errs.NewCodeError(freeErrors.ErrInsufficientBalance, "no balance available")
		}

		// 找余额最大的币种
		requestAmount := decimal.NewFromFloat(req.Amount)
		var maxBalance decimal.Decimal
		found := false

		for _, balance := range walletBalances {
			currentBalance, err := decimal.NewFromString(balance.AvailableBalance.String())
			if err != nil {
				continue
			}

			// 如果余额充足且大于当前最大余额
			if currentBalance.GreaterThanOrEqual(requestAmount) {
				if !found || currentBalance.GreaterThan(maxBalance) {
					maxBalance = currentBalance
					balanceDecimal = currentBalance
					currencyID = balance.CurrencyId
					found = true
				}
			}
		}

		if !found {
			return nil, errs.NewCodeError(freeErrors.ErrInsufficientBalance, "insufficient balance")
		}
	}

	// 8. 验证支付密码
	if !utils.CheckPassword(req.PayPassword, walletInfo.PayPwd) {
		return nil, freeErrors.UserPwdErrErr
	}

	// 9. 获取币种汇率，计算手续费（手续费统一按人民币计算）
	currencyDao := walletModel.NewWalletCurrencyDao(db)
	currency, err := currencyDao.GetById(ctx, currencyID)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	// 将提现金额换算成人民币
	exchangeRateDecimal, err := decimal.NewFromString(currency.ExchangeRate.String())
	if err != nil {
		return nil, errs.Wrap(err)
	}
	exchangeRate, _ := exchangeRateDecimal.Float64()
	amountInCNY := req.Amount * exchangeRate

	// 计算手续费（按人民币金额计算，返回人民币金额）
	feeCNY := s.calculateFee(amountInCNY, rule.FeeRate, rule.FeeFixed)

	// 将手续费换算回原币种（用于从余额中扣除）
	feeInCurrency := feeCNY / exchangeRate

	// 实际到账金额（原币种）
	actualAmount := req.Amount - feeInCurrency

	// 10. 获取收款账户信息
	paymentMethodID, _ := primitive.ObjectIDFromHex(req.PaymentMethodID)
	paymentMethod, _ := paymentMethodModel.GetPaymentMethodDao().FindByID(ctx, paymentMethodID)

	paymentInfoBytes, _ := json.Marshal(map[string]interface{}{
		"type":        paymentMethod.Type,
		"cardNumber":  paymentMethod.CardNumber,
		"bankName":    paymentMethod.BankName,
		"branchName":  paymentMethod.BranchName,
		"accountName": paymentMethod.AccountName,
		"qrCodeUrl":   paymentMethod.QRCodeURL,
	})

	// 11. 创建提现记录
	record := &chat.WithdrawalRecord{
		OrderNo:         s.generateOrderNo(),
		UserID:          userID,
		OrganizationID:  organizationID,
		CurrencyID:      currencyID,
		Amount:          req.Amount,
		Fee:             feeCNY, // 手续费保存为人民币金额
		ActualAmount:    actualAmount,
		Status:          chat.WithdrawalStatusPending,
		PaymentMethodID: req.PaymentMethodID,
		PaymentType:     paymentMethod.Type,
		PaymentInfo:     string(paymentInfoBytes),
	}

	if err := withdrawalModel.GetWithdrawalRecordDao().Create(ctx, record); err != nil {
		return nil, errs.Wrap(err)
	}

	// 12. 扣除余额(冻结金额)
	deductAmount := decimal.NewFromFloat(-req.Amount)
	if err := walletBalanceDao.UpdateAvailableBalance(ctx, walletInfo.ID, currencyID, deductAmount); err != nil {
		return nil, errs.Wrap(err)
	}

	return &withdrawalDto.SubmitWithdrawalResp{
		OrderNo:      record.OrderNo,
		Amount:       record.Amount,
		Fee:          record.Fee,
		ActualAmount: record.ActualAmount,
		Status:       record.Status,
		CreatedAt:    record.CreatedAt,
	}, nil
}

// GetWithdrawalRecordList 获取提现记录列表
func (s *WithdrawalSvc) GetWithdrawalRecordList(ctx context.Context, userID string, req *withdrawalDto.GetWithdrawalRecordListReq) (*withdrawalDto.GetWithdrawalRecordListResp, error) {
	pagination := chat.Pagination{
		Page:     req.Page,
		PageSize: req.PageSize,
	}

	records, total, err := withdrawalModel.GetWithdrawalRecordDao().FindByUserID(ctx, userID, pagination)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	// 获取用户信息(从Attribute表)
	db := plugin.MongoCli().GetDB()
	attributeDao := chatModel.NewAttributeDao(db)
	attr, _ := attributeDao.Take(ctx, userID)

	respRecords := make([]*withdrawalDto.WithdrawalRecordResp, 0, len(records))
	for _, record := range records {
		resp := s.toWithdrawalRecordResp(record)
		// 添加用户信息
		if attr != nil {
			resp.UserID = attr.UserID
			resp.UserAccount = attr.Account
			resp.Nickname = attr.Nickname
		} else {
			resp.UserID = userID
			resp.UserAccount = ""
			resp.Nickname = ""
		}
		respRecords = append(respRecords, resp)
	}

	return &withdrawalDto.GetWithdrawalRecordListResp{
		Total:   total,
		Records: respRecords,
	}, nil
}

// GetWithdrawalDetail 获取提现详情
func (s *WithdrawalSvc) GetWithdrawalDetail(ctx context.Context, userID, orderNo string) (*withdrawalDto.WithdrawalRecordResp, error) {
	record, err := withdrawalModel.GetWithdrawalRecordDao().FindByOrderNo(ctx, orderNo)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	if record.UserID != userID {
		return nil, freeErrors.ForbiddenErr("not authorized to view this withdrawal record")
	}

	resp := s.toWithdrawalRecordResp(record)

	// 获取用户信息(从Attribute表)
	db := plugin.MongoCli().GetDB()
	attributeDao := chatModel.NewAttributeDao(db)
	attr, err := attributeDao.Take(ctx, userID)
	if err == nil {
		resp.UserID = attr.UserID
		resp.UserAccount = attr.Account
		resp.Nickname = attr.Nickname
	} else {
		resp.UserID = userID
		resp.UserAccount = ""
		resp.Nickname = ""
	}

	return resp, nil
}

// CancelWithdrawal 取消提现
func (s *WithdrawalSvc) CancelWithdrawal(ctx context.Context, userID string, req *withdrawalDto.CancelWithdrawalReq) error {
	record, err := withdrawalModel.GetWithdrawalRecordDao().FindByOrderNo(ctx, req.OrderNo)
	if err != nil {
		return errs.Wrap(err)
	}

	if record.UserID != userID {
		return freeErrors.ForbiddenErr("not authorized to cancel this withdrawal")
	}

	if record.Status != chat.WithdrawalStatusPending {
		return errs.NewCodeError(freeErrors.ErrForbidden, "can only cancel pending withdrawal")
	}

	// 更新状态为已取消
	if err := withdrawalModel.GetWithdrawalRecordDao().UpdateStatus(ctx, record.ID, chat.WithdrawalStatusCancelled, nil); err != nil {
		return errs.Wrap(err)
	}

	// 退还金额
	db := plugin.MongoCli().GetDB()
	walletInfoDao := walletModel.NewWalletInfoDao(db)
	walletBalanceDao := walletModel.NewWalletBalanceDao(db)

	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, userID, walletModel.WalletInfoOwnerTypeOrdinary)
	if err != nil {
		return errs.Wrap(err)
	}

	// 使用提现记录中保存的货币ID退还金额
	refundAmount := decimal.NewFromFloat(record.Amount)
	if err := walletBalanceDao.UpdateAvailableBalance(ctx, walletInfo.ID, record.CurrencyID, refundAmount); err != nil {
		return errs.Wrap(err)
	}

	return nil
}

// calculateFee 计算手续费
func (s *WithdrawalSvc) calculateFee(amount, feeRate, feeFixed float64) float64 {
	fee := amount*(feeRate/100) + feeFixed
	return fee
}

// generateOrderNo 生成订单号
func (s *WithdrawalSvc) generateOrderNo() string {
	return fmt.Sprintf("W%d", time.Now().UnixNano()/1000000)
}

// CheckPendingWithdrawal 检查是否有未处理的提现申请
func (s *WithdrawalSvc) CheckPendingWithdrawal(ctx context.Context, userID string) (*withdrawalDto.CheckPendingWithdrawalResp, error) {
	// 查询未处理的提现记录（状态: 0-待审核, 1-已通过, 2-打款中）
	dao := withdrawalModel.GetWithdrawalRecordDao()

	// 使用分页查询最新的一条未处理记录
	pagination := chat.Pagination{
		Page:     1,
		PageSize: 1,
	}

	records, total, err := dao.FindByUserID(ctx, userID, pagination)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	// 检查是否有未处理的提现
	if total > 0 && len(records) > 0 {
		record := records[0]
		// 状态: 0-待审核, 1-已通过, 2-打款中 为未处理状态
		if record.Status == chat.WithdrawalStatusPending ||
			record.Status == chat.WithdrawalStatusApproved ||
			record.Status == chat.WithdrawalStatusTransferring {

			// 获取币种信息
			db := plugin.MongoCli().GetDB()
			currencyDao := walletModel.NewWalletCurrencyDao(db)
			currency, _ := currencyDao.GetById(ctx, record.CurrencyID)

			currencyName := "CNY"
			currencySymbol := "¥"
			if currency != nil {
				currencyName = currency.Name
				if currency.Name == "CNY" {
					currencySymbol = "¥"
				}
			}

			// 获取状态文本
			statusText := s.getStatusText(record.Status)

			return &withdrawalDto.CheckPendingWithdrawalResp{
				HasPending: true,
				PendingWithdrawal: &withdrawalDto.PendingWithdrawalInfo{
					OrderNo:        record.OrderNo,
					CurrencyName:   currencyName,
					CurrencySymbol: currencySymbol,
					Amount:         record.Amount,
					Status:         record.Status,
					StatusText:     statusText,
					CreatedAt:      record.CreatedAt.UnixMilli(),
				},
			}, nil
		}
	}

	return &withdrawalDto.CheckPendingWithdrawalResp{
		HasPending:        false,
		PendingWithdrawal: nil,
	}, nil
}

// getStatusText 获取状态文本
func (s *WithdrawalSvc) getStatusText(status int32) string {
	switch status {
	case chat.WithdrawalStatusPending:
		return "待审核"
	case chat.WithdrawalStatusApproved:
		return "已通过"
	case chat.WithdrawalStatusTransferring:
		return "打款中"
	case chat.WithdrawalStatusCompleted:
		return "已完成"
	case chat.WithdrawalStatusRejected:
		return "已拒绝"
	case chat.WithdrawalStatusCancelled:
		return "已取消"
	default:
		return "未知"
	}
}

// toWithdrawalRecordResp 转换为响应对象
func (s *WithdrawalSvc) toWithdrawalRecordResp(record *chat.WithdrawalRecord) *withdrawalDto.WithdrawalRecordResp {
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
