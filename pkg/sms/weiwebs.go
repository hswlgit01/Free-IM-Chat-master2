package sms

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
)

// NewWeiwebs 创建微薇博通短信平台对接实例
func NewWeiwebs(account, password, signName string) SMS {
	return &weiwebs{
		account:  account,
		password: password,
		signName: signName,
	}
}

type weiwebs struct {
	account  string
	password string
	signName string
}

func (w *weiwebs) Name() string {
	return "weiwebs-sms"
}

// SendCode 发送验证码
// areaCode: 区号，如 "+86"
// phoneNumber: 手机号码
// verifyCode: 验证码
func (w *weiwebs) SendCode(ctx context.Context, areaCode string, phoneNumber string, verifyCode string) error {
	// 从区号中去除+号
	if strings.HasPrefix(areaCode, "+") {
		areaCode = areaCode[1:]
	}

	// 构建短信内容
	//【DEP-CHAT】您的验证码为：965714，如非本人操作，请忽略。
	msg := fmt.Sprintf("【%s】您的验证码为：%s，如非本人操作，请忽略。", w.signName, verifyCode)

	// 构建请求URL
	apiURL := "https://weiwebs.cn/msg/HttpBatchSendSM"

	// 构建参数
	params := url.Values{}
	params.Add("account", w.account)
	params.Add("pswd", w.password)
	params.Add("mobile", areaCode+phoneNumber) // 区号+手机号
	//params.Add("mobile", phoneNumber) // 区号+手机号
	params.Add("msg", msg)
	params.Add("needstatus", "true")

	// 发起HTTP请求
	resp, err := http.Get(apiURL + "?" + params.Encode())
	if err != nil {
		return errs.Wrap(err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errs.Wrap(err)
	}

	// 解析响应
	respText := string(body)
	parts := strings.Split(respText, "\n")
	if len(parts) < 1 {
		return errs.New("invalid response")
	}

	statusParts := strings.Split(parts[0], ",")
	if len(statusParts) < 2 {
		return errs.New("invalid status response")
	}

	// 检查状态码
	statusCode := statusParts[1]
	if statusCode != "0" {
		log.ZError(ctx, "SMS send failed", nil, "code", statusCode, "resp", respText)
		return errs.New("sms send failed with code: " + statusCode)
	}

	log.ZInfo(ctx, "SMS sent successfully", "phone", areaCode+phoneNumber, "code", verifyCode)
	return nil
}
