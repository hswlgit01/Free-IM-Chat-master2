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

package sms

import (
	"context"
	"strings"

	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	dysmsapi20180501 "github.com/alibabacloud-go/dysmsapi-20180501/v2/client"
	util "github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
)

func NewAli(endpoint, accessKeyId, accessKeySecret, signName, verificationCodeTemplateCode string) (SMS, error) {
	config := &openapi.Config{
		AccessKeyId:     tea.String(accessKeyId),
		AccessKeySecret: tea.String(accessKeySecret),
		Endpoint:        tea.String(endpoint),
	}
	client, err := dysmsapi20180501.NewClient(config)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	return &ali{
		signName:                     signName,
		verificationCodeTemplateCode: verificationCodeTemplateCode,
		client:                       client,
	}, nil
}

type ali struct {
	signName                     string
	verificationCodeTemplateCode string
	client                       *dysmsapi20180501.Client
}

func (a *ali) Name() string {
	return "ali-sms"
}

func (a *ali) SendCode(ctx context.Context, areaCode string, phoneNumber string, verifyCode string) error {
	// 确保区号格式正确（去除+号）
	rawAreaCode := strings.TrimPrefix(areaCode, "+")

	// 国际短信
	sendRequest := &dysmsapi20180501.SendMessageToGlobeRequest{
		To:      tea.String(rawAreaCode + phoneNumber),
		From:    tea.String("freechat"),
		Message: tea.String("Your verification code is " + verifyCode + " and is valid for 5 minutes"),
		//Message: tea.String("VerificationCode"),
	}

	runtime := &util.RuntimeOptions{}
	resp, err := a.client.SendMessageToGlobeWithOptions(sendRequest, runtime)
	if err != nil {
		log.ZError(ctx, "Failed to send international SMS", err,
			"phone", "+"+rawAreaCode+phoneNumber, "code", verifyCode)
		return errs.Wrap(err)
	}

	if *resp.Body.ResponseCode != "OK" {
		log.ZError(ctx, "Aliyun international SMS response error", nil,
			"code", *resp.Body.ResponseCode, "message", *resp.Body.ResponseDescription)
		return errs.New("international sms send failed: " + *resp.Body.ResponseDescription)
	}
	return nil
}
