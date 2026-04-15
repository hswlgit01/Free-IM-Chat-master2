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

package mw

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/openimsdk/chat/freechat/utils"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/pkg/common/constant"
	"github.com/openimsdk/chat/pkg/protocol/admin"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/errs"
)

func New(client admin.AdminClient) *MW {
	return &MW{client: client}
}

type MW struct {
	client admin.AdminClient
}

func (o *MW) parseToken(c *gin.Context) (string, int32, string, error) {
	token := c.GetHeader("token")
	if token == "" {
		return "", 0, "", errors.New("token is empty")
	}
	resp, err := o.client.ParseToken(c, &admin.ParseTokenReq{Token: token})
	if err != nil {
		return "", 0, "", err
	}
	return resp.UserID, resp.UserType, token, nil
}

func (o *MW) parseTokenType(c *gin.Context, userType int32) (string, string, error) {
	userID, t, token, err := o.parseToken(c)
	if err != nil {
		return "", "", err
	}
	if t != userType {
		return "", "", errors.New("token type freeErrors")
	}
	return userID, token, nil
}

func (o *MW) isValidToken(c *gin.Context, userID string, token string) error {
	resp, err := o.client.GetUserToken(c, &admin.GetUserTokenReq{UserID: userID})
	if err != nil {
		return err
	}
	if len(resp.TokensMap) == 0 {
		return errs.ErrTokenExpired.Wrap()
	}
	if v, ok := resp.TokensMap[token]; ok {
		switch v {
		case constantpb.NormalToken:
		case constantpb.KickedToken:
			return errs.ErrTokenExpired.Wrap()
		default:
			return errs.ErrTokenUnknown.Wrap()
		}
	} else {
		return errs.ErrTokenExpired.Wrap()
	}
	return nil
}

func (o *MW) setToken(c *gin.Context, userID string, userType int32) {
	SetToken(c, userID, userType)
}

func (o *MW) CheckToken(c *gin.Context) {
	if utils.IsLocalTestEnv() {
		o.setToken(c, "7922150030", constant.NormalUser)
		return
	}
	userID, userType, token, err := o.parseToken(c)
	if err != nil {
		c.Abort()
		if err.Error() == "token is empty" {
			c.JSON(http.StatusBadRequest, gin.H{
				"errCode": 1001,
				"errMsg":  "ArgsError",
				"errDlt":  "token is empty",
			})
		} else {
			apiresp.GinError(c, err)
		}
		return
	}
	if err := o.isValidToken(c, userID, token); err != nil {
		c.Abort()
		apiresp.GinError(c, err)
		return
	}

	// 验证请求头必须传orgId
	orgId := c.GetHeader(constant.RpcOrgId)
	if orgId == "" {
		c.Abort()
		c.JSON(http.StatusBadRequest, gin.H{
			"errCode": 1001,
			"errMsg":  "ArgsError",
			"errDlt":  "Org-Id header is required and cannot be empty",
		})
		return
	}

	o.setToken(c, userID, userType)
}

func (o *MW) CheckAdmin(c *gin.Context) {
	userID, token, err := o.parseTokenType(c, constant.AdminUser)
	if err != nil {
		c.Abort()
		if err.Error() == "token is empty" {
			c.JSON(http.StatusBadRequest, gin.H{
				"errCode": 1001,
				"errMsg":  "ArgsError",
				"errDlt":  "token is empty",
			})
		} else if err.Error() == "token type freeErrors" {
			c.JSON(http.StatusBadRequest, gin.H{
				"errCode": 1001,
				"errMsg":  "ArgsError",
				"errDlt":  "token type freeErrors",
			})
		} else {
			apiresp.GinError(c, err)
		}
		return
	}
	if err := o.isValidToken(c, userID, token); err != nil {
		c.Abort()
		apiresp.GinError(c, err)
		return
	}
	o.setToken(c, userID, constant.AdminUser)
}

func (o *MW) CheckUser(c *gin.Context) {
	userID, token, err := o.parseTokenType(c, constant.NormalUser)
	if err != nil {
		c.Abort()
		if err.Error() == "token is empty" {
			c.JSON(http.StatusBadRequest, gin.H{
				"errCode": 1001,
				"errMsg":  "ArgsError",
				"errDlt":  "token is empty",
			})
		} else if err.Error() == "token type freeErrors" {
			c.JSON(http.StatusBadRequest, gin.H{
				"errCode": 1001,
				"errMsg":  "ArgsError",
				"errDlt":  "token type freeErrors",
			})
		} else {
			apiresp.GinError(c, err)
		}
		return
	}
	if err := o.isValidToken(c, userID, token); err != nil {
		c.Abort()
		apiresp.GinError(c, err)
		return
	}
	o.setToken(c, userID, constant.NormalUser)
}

func (o *MW) CheckAdminOrNil(c *gin.Context) {
	defer c.Next()
	userID, userType, _, err := o.parseToken(c)
	if err != nil {
		return
	}
	if userType == constant.AdminUser {
		o.setToken(c, userID, constant.AdminUser)
	}
}

func SetToken(c *gin.Context, userID string, userType int32) {
	c.Set(constant.RpcOpUserID, userID)
	c.Set(constant.RpcOpUserType, []string{strconv.Itoa(int(userType))})
	// 初始化自定义头部列表
	customHeaders := []string{constant.RpcOpUserType}
	// 从请求头中获取org-id
	orgId := c.GetHeader(constant.RpcOrgId)
	if orgId != "" {
		c.Set(constant.RpcOrgId, []string{orgId})
		customHeaders = append(customHeaders, constant.RpcOrgId)
	}
	// 设置所有需要传递的自定义头部
	c.Set(constant.RpcCustomHeader, customHeaders)
}
