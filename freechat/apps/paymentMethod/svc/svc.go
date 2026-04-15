package svc

import (
	"context"

	"github.com/openimsdk/chat/freechat/apps/paymentMethod/dto"
	"github.com/openimsdk/chat/freechat/apps/paymentMethod/model"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type PaymentMethodSvc struct{}

func NewPaymentMethodSvc() *PaymentMethodSvc {
	return &PaymentMethodSvc{}
}

// GetPaymentMethods 获取用户的所有支付方式
func (s *PaymentMethodSvc) GetPaymentMethods(ctx context.Context, userID string) ([]*dto.PaymentMethodResp, error) {
	methods, err := model.GetPaymentMethodDao().FindByUserID(ctx, userID)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	resp := make([]*dto.PaymentMethodResp, 0, len(methods))
	for _, method := range methods {
		resp = append(resp, s.toPaymentMethodResp(method))
	}

	return resp, nil
}

// CreatePaymentMethod 创建支付方式
func (s *PaymentMethodSvc) CreatePaymentMethod(ctx context.Context, userID string, req *dto.CreatePaymentMethodReq) (*dto.PaymentMethodResp, error) {
	// 验证请求数据
	if err := s.validateCreateRequest(req); err != nil {
		return nil, err
	}

	// 如果设置为默认,先检查是否已有默认支付方式
	if req.IsDefault {
		// 后续会在DAO层处理,这里只创建
	}

	paymentMethod := &chat.PaymentMethod{
		UserID:      userID,
		Type:        *req.Type, // 解引用指针
		CardNumber:  req.CardNumber,
		BankName:    req.BankName,
		BranchName:  req.BranchName,
		AccountName: req.AccountName,
		QRCodeURL:   req.QRCodeURL,
		IsDefault:   req.IsDefault,
	}

	if err := model.GetPaymentMethodDao().Create(ctx, paymentMethod); err != nil {
		return nil, errs.Wrap(err)
	}

	// 如果需要设置为默认
	if req.IsDefault {
		if err := model.GetPaymentMethodDao().SetDefault(ctx, userID, paymentMethod.ID); err != nil {
			return nil, errs.Wrap(err)
		}
	}

	return s.toPaymentMethodResp(paymentMethod), nil
}

// SetDefaultPaymentMethod 设置默认支付方式
func (s *PaymentMethodSvc) SetDefaultPaymentMethod(ctx context.Context, userID string, id primitive.ObjectID) error {
	// 验证支付方式是否属于该用户
	method, err := model.GetPaymentMethodDao().FindByID(ctx, id)
	if err != nil {
		return errs.Wrap(err)
	}

	if method.UserID != userID {
		return freeErrors.ForbiddenErr("payment method does not belong to this user")
	}

	return model.GetPaymentMethodDao().SetDefault(ctx, userID, id)
}

// DeletePaymentMethod 删除支付方式
func (s *PaymentMethodSvc) DeletePaymentMethod(ctx context.Context, userID string, id primitive.ObjectID) error {
	// 验证支付方式是否属于该用户
	method, err := model.GetPaymentMethodDao().FindByID(ctx, id)
	if err != nil {
		return errs.Wrap(err)
	}

	if method.UserID != userID {
		return freeErrors.ForbiddenErr("payment method does not belong to this user")
	}

	return model.GetPaymentMethodDao().Delete(ctx, id)
}

// validateCreateRequest 验证创建请求
func (s *PaymentMethodSvc) validateCreateRequest(req *dto.CreatePaymentMethodReq) error {
	switch *req.Type { // 解引用指针
	case chat.PaymentMethodTypeBankCard:
		// 银行卡必须有卡号、银行名称、账户名
		if req.CardNumber == "" || req.BankName == "" || req.AccountName == "" {
			return freeErrors.ParameterInvalidErr
		}
	case chat.PaymentMethodTypeWechat, chat.PaymentMethodTypeAlipay:
		// 二维码支付必须有二维码URL和账户名
		if req.QRCodeURL == "" || req.AccountName == "" {
			return freeErrors.ParameterInvalidErr
		}
	default:
		return freeErrors.ParameterInvalidErr
	}

	return nil
}

// toPaymentMethodResp 转换为响应对象
func (s *PaymentMethodSvc) toPaymentMethodResp(method *chat.PaymentMethod) *dto.PaymentMethodResp {
	return &dto.PaymentMethodResp{
		ID:          method.ID.Hex(),
		Type:        method.Type,
		CardNumber:  method.CardNumber,
		BankName:    method.BankName,
		BranchName:  method.BranchName,
		AccountName: method.AccountName,
		QRCodeURL:   method.QRCodeURL,
		IsDefault:   method.IsDefault,
		CreatedAt:   method.CreatedAt,
		UpdatedAt:   method.UpdatedAt,
	}
}
