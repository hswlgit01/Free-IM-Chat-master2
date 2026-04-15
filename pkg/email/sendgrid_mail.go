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
	"github.com/sendgrid/sendgrid-go"
	sgmail "github.com/sendgrid/sendgrid-go/helpers/mail"
)

// SendGridMail 实现Mail接口
type sendGridMail struct {
	apiKey           string
	senderMail       string
	senderName       string
	title            string
	supportCenterURL string
}

// NewSendGridMail 创建一个新的SendGrid邮件发送器
func NewSendGridMail(apiKey, senderMail, senderName, title string, supportCenterURL string) Mail {
	return &sendGridMail{
		apiKey:           apiKey,
		senderMail:       senderMail,
		senderName:       senderName,
		title:            title,
		supportCenterURL: supportCenterURL,
	}
}

func (s *sendGridMail) Name() string {
	return "sendgrid"
}

func (s *sendGridMail) SendMail(ctx context.Context, mail string, verifyCode string) error {
	from := sgmail.NewEmail(s.senderName, s.senderMail)
	to := sgmail.NewEmail("", mail)
	subject := s.title

	// 当前时间，显示在邮件中
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	// 使用与原始模板相同风格的HTML内容
	htmlContent := fmt.Sprintf(`
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
	</html>`, verifyCode, currentTime, s.supportCenterURL)

	// 简单的纯文本内容作为备选 英文
	plainTextContent := fmt.Sprintf("Your FREECHAT verification code is: %s\nGenerated time: %s\nThis code is valid for 5 minutes. Please do not share it with anyone.", verifyCode, currentTime)

	message := sgmail.NewSingleEmail(from, subject, to, plainTextContent, htmlContent)
	client := sendgrid.NewSendClient(s.apiKey)

	response, err := client.Send(message)
	if err != nil {
		log.ZError(ctx, "Email sending failed", err, "from", s.senderMail, "to", mail)
		return errs.Wrap(err)
	}

	if response.StatusCode >= 400 {
		log.ZError(ctx, "Email sending failed with status code", nil,
			"statusCode", response.StatusCode,
			"from", s.senderMail,
			"to", mail)
		return errs.New(fmt.Sprintf("SendGrid returned error status code: %d", response.StatusCode))
	}

	return nil
}
