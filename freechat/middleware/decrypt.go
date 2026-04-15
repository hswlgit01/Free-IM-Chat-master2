package middleware

import (
	"bytes"
	"encoding/json"
	"github.com/openimsdk/chat/freechat/apps/userKeys/dto"
	"github.com/openimsdk/chat/freechat/apps/userKeys/svc"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
)

// DecryptMiddleware AES解密中间件
func DecryptMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 如果不是POST请求，则跳过
		if c.Request.Method != http.MethodPost {
			c.Next()
			return
		}

		// 获取用户ID
		userID := mctx.GetOpUserID(c)
		if userID == "" {
			apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrUnauthorized, freeErrors.ErrorMessages[freeErrors.ErrUnauthorized]))
			c.Abort()
			return
		}

		// 获取来源平台
		platform := c.GetHeader("Source")
		if platform == "" {
			platform = "web" // 默认为web
		}

		// 平台合法性检查
		if platform != "web" && platform != "ios" && platform != "android" && platform != "h5" {
			apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
			c.Abort()
			return
		}

		// 读取原始请求体
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, freeErrors.ErrorMessages[freeErrors.ErrSystem]))
			c.Abort()
			return
		}

		// 关闭原始请求体
		c.Request.Body.Close()

		// 检查是否需要解密
		var encryptedReq dto.EncryptedReq
		if err := json.Unmarshal(body, &encryptedReq); err == nil && encryptedReq.EncryptedData != "" {
			// 需要解密
			userKeysSvc := svc.NewUserKeysSvc()
			decryptedData, err := userKeysSvc.DecryptData(c, userID, platform, encryptedReq.EncryptedData, encryptedReq.UserReqType)
			if err != nil {
				apiresp.GinError(c, err)
				c.Abort()
				return
			}

			// 处理需要RSA验证的请求
			if encryptedReq.NeedRSAVerify {
				var signedData dto.SignedDataReq
				if err := json.Unmarshal(decryptedData, &signedData); err != nil {
					log.ZError(c, "解析签名数据失败", err, "user_id", userID)
					apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
					c.Abort()
					return
				}

				// 验证签名
				valid, err := userKeysSvc.VerifySignature(c, userID, platform, signedData.Data, signedData.Signature)
				if err != nil {
					log.ZError(c, "验证签名失败", err, "user_id", userID)
					apiresp.GinError(c, err)
					c.Abort()
					return
				}

				if !valid {
					log.ZWarn(c, "签名验证失败", nil, "user_id", userID)
					apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrUnauthorized, freeErrors.ErrorMessages[freeErrors.ErrUnauthorized]))
					c.Abort()
					return
				}

				// 签名验证通过，使用原始数据
				decryptedData = signedData.Data
				// 标记签名已验证
				c.Set("signature_verified", true)
			}

			// 替换请求体
			c.Request.Body = io.NopCloser(bytes.NewReader(decryptedData))
			// 更新请求头部的内容长度
			c.Request.ContentLength = int64(len(decryptedData))
			// 在上下文中标记已解密
			c.Set("decrypted", true)
		} else {
			// 不需要解密，恢复原始请求体
			c.Request.Body = io.NopCloser(bytes.NewReader(body))
		}

		c.Next()
	}
}
