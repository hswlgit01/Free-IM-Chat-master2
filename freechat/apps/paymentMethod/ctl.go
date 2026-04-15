package paymentMethod

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/paymentMethod/dto"
	"github.com/openimsdk/chat/freechat/apps/paymentMethod/svc"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type PaymentMethodCtl struct{}

func NewPaymentMethodCtl() *PaymentMethodCtl {
	return &PaymentMethodCtl{}
}

// GetPaymentMethods 获取用户的所有支付方式
// @router GET /user/payment-methods
func (p *PaymentMethodCtl) GetPaymentMethods(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	paymentMethodSvc := svc.NewPaymentMethodSvc()
	methods, err := paymentMethodSvc.GetPaymentMethods(c.Request.Context(), opUserID)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, methods)
}

// CreatePaymentMethod 创建支付方式
// @router POST /user/payment-methods
func (p *PaymentMethodCtl) CreatePaymentMethod(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.CreatePaymentMethodReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	paymentMethodSvc := svc.NewPaymentMethodSvc()
	method, err := paymentMethodSvc.CreatePaymentMethod(c.Request.Context(), opUserID, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, method)
}

// SetDefaultPaymentMethod 设置默认支付方式
// @router POST /user/payment-methods/:id/default
func (p *PaymentMethodCtl) SetDefaultPaymentMethod(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.SetDefaultPaymentMethodReq
	if err := c.ShouldBindUri(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	// 转换ID为ObjectID
	objectID, err := primitive.ObjectIDFromHex(req.ID)
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg("invalid payment method ID"))
		return
	}

	paymentMethodSvc := svc.NewPaymentMethodSvc()
	if err := paymentMethodSvc.SetDefaultPaymentMethod(c.Request.Context(), opUserID, objectID); err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, nil)
}

// DeletePaymentMethod 删除支付方式
// @router POST /user/payment-methods/:id/delete
func (p *PaymentMethodCtl) DeletePaymentMethod(c *gin.Context) {
	opUserID, _, err := mctx.Check(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	var req dto.DeletePaymentMethodReq
	if err := c.ShouldBindUri(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg(err.Error()))
		return
	}

	// 转换ID为ObjectID
	objectID, err := primitive.ObjectIDFromHex(req.ID)
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr.WrapMsg("invalid payment method ID"))
		return
	}

	paymentMethodSvc := svc.NewPaymentMethodSvc()
	if err := paymentMethodSvc.DeletePaymentMethod(c.Request.Context(), opUserID, objectID); err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, nil)
}
