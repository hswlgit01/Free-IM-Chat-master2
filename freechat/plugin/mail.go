package plugin

import (
	"github.com/openimsdk/chat/pkg/common/constant"
	"github.com/openimsdk/chat/pkg/email"
)

var mail email.Mail

func Mail() email.Mail {
	return mail
}

func InitMail(chatCfg *ChatConfig) {
	mailCfg := chatCfg.ChatRpcConfig.VerifyCode.Mail
	if mailCfg.Use == constant.VerifyMail {
		// 默认支持中心URL
		supportCenterURL := mailCfg.SupportCenterURL
		if mailCfg.SendGrid.APIKey != "" {
			mail = email.NewSendGridMail(mailCfg.SendGrid.APIKey, mailCfg.SenderMail, mailCfg.SendGrid.SenderName, mailCfg.Title, supportCenterURL)
		} else {
			mail = email.NewMail(mailCfg.SMTPAddr, mailCfg.SMTPPort, mailCfg.SenderMail, mailCfg.SenderAuthorizationCode, mailCfg.Title, supportCenterURL)
		}
	}
}
