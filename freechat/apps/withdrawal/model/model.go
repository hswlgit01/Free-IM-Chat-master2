package model

import (
	"github.com/openimsdk/chat/freechat/plugin"
	chatModel "github.com/openimsdk/chat/pkg/common/db/model/chat"
	"github.com/openimsdk/chat/pkg/common/db/table/chat"
)

var withdrawalRuleDao chat.WithdrawalRuleInterface
var withdrawalRecordDao chat.WithdrawalRecordInterface

// InitWithdrawalDao 初始化提现相关DAO
func InitWithdrawalDao() error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()

	// 初始化提现规则DAO
	ruleDao, err := chatModel.NewWithdrawalRule(db)
	if err != nil {
		return err
	}
	withdrawalRuleDao = ruleDao

	// 初始化提现记录DAO
	recordDao, err := chatModel.NewWithdrawalRecord(db)
	if err != nil {
		return err
	}
	withdrawalRecordDao = recordDao

	return nil
}

// GetWithdrawalRuleDao 获取提现规则DAO实例
func GetWithdrawalRuleDao() chat.WithdrawalRuleInterface {
	return withdrawalRuleDao
}

// GetWithdrawalRecordDao 获取提现记录DAO实例
func GetWithdrawalRecordDao() chat.WithdrawalRecordInterface {
	return withdrawalRecordDao
}
