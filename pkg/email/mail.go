// Copyright © 2023 OpenIM open source community. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package email

import (
	"context"
	"fmt"
	"time"

	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"gopkg.in/gomail.v2"
)

type Mail interface {
	Name() string
	SendMail(ctx context.Context, mail string, verifyCode string) error
}

func NewMail(smtpAddr string, smtpPort int, senderMail, senderAuthorizationCode, title string, supportCenterURL string) Mail {
	dial := gomail.NewDialer(smtpAddr, smtpPort, senderMail, senderAuthorizationCode)
	return &mail{
		title:            title,
		senderMail:       senderMail,
		smtpAddr:         smtpAddr,
		smtpPort:         smtpPort,
		dial:             dial,
		supportCenterURL: supportCenterURL,
	}
}

type mail struct {
	senderMail       string
	title            string
	smtpAddr         string
	smtpPort         int
	dial             *gomail.Dialer
	supportCenterURL string
}

func (m *mail) Name() string {
	return "mail"
}

func (m *mail) SendMail(ctx context.Context, mail string, verifyCode string) error {
	// 创建并配置邮件
	msg := gomail.NewMessage()
	msg.SetHeader(`From`, m.senderMail)
	msg.SetHeader(`To`, []string{mail}...)
	msg.SetHeader(`Subject`, m.title)

	// 当前时间，显示在邮件中
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	htmlBody := fmt.Sprintf(`
	<!DOCTYPE html>
	<html>
	<head>
		<meta charset="UTF-8">
		<meta name="viewport" content="width=device-width, initial-scale=1.0">
		<title>FREECHAT Verification</title>
	</head>
	<body style="margin: 0; padding: 0; font-family: Arial, 'Helvetica Neue', Helvetica, sans-serif;">
		<div style="max-width: 600px; margin: 0 auto; padding: 20px;">
			<div style="text-align: center; margin-bottom: 20px;">
				<h1 style="color: #333; margin: 0;">FREECHAT</h1>
			</div>
			
			<div style="font-family: Arial, sans-serif; text-align: center;">
				<h2>Your <strong style="color:#000;">FREECHAT</strong> verification code</h2>
				<div style="background-color:#f5f5f5; padding:20px; margin-top:10px; border-radius: 5px;">
					<span style="font-size:30px; letter-spacing:10px; font-weight:bold;">%s</span>
				</div>
				<p style="font-size:12px;">%s</p>
				<p>Your one-time verification code expires in 5 minutes.</p>
				<p><strong>Never share this code with anyone.</strong></p>
				<hr style="border:none; border-top:1px solid #ddd;">
				<p style="font-size:12px; color:#777;">If you did not request this code, please contact our 
					<a href="%s" style="color:#2196F3;">Support center</a>
				</p>
			</div>
			
			<div style="text-align: center; margin-top: 30px; color: #999; font-size: 12px;">
				<p>© 2025 FREECHAT. All rights reserved.</p>
			</div>
		</div>
	</body>
	</html>`, verifyCode, currentTime, m.supportCenterURL)

	msg.SetBody("text/html", htmlBody)

	// 尝试发送邮件
	if err := m.dial.DialAndSend(msg); err != nil {
		log.ZError(ctx, "Email sending failed", err, "from", m.senderMail, "to", mail)
		return errs.Wrap(err)
	}
	return nil
}
