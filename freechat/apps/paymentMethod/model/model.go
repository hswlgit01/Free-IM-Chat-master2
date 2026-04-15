package model

import (
	"github.com/openimsdk/chat/freechat/plugin"
	chatModel "github.com/openimsdk/chat/pkg/common/db/model/chat"
	"github.com/openimsdk/chat/pkg/common/db/table/chat"
)

var paymentMethodDao chat.PaymentMethodInterface

// InitPaymentMethodDao 初始化PaymentMethodDAO
func InitPaymentMethodDao() error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	dao, err := chatModel.NewPaymentMethod(db)
	if err != nil {
		return err
	}

	paymentMethodDao = dao
	return nil
}

// GetPaymentMethodDao 获取PaymentMethodDAO实例
func GetPaymentMethodDao() chat.PaymentMethodInterface {
	return paymentMethodDao
}
