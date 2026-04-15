package dto

import (
	"encoding/json"
)

// SetupUserKeysReq 设置用户密钥请求
type SetupUserKeysReq struct {
	UserID       string `json:"user_id,omitempty"`                 // 用户ID，从token获取，不需要客户端传递
	RSAPublicKey string `json:"rsa_public_key" binding:"required"` // RSA公钥
	UserReqType  string `json:"user_req_type"`                     // 用户请求类型 默认是普通用户 organization 组织账户
}

// GetUserAESKeyReq 获取用户AES密钥请求
type GetUserAESKeyReq struct {
	UserID string `json:"user_id,omitempty"` // 用户ID，从token获取，不需要客户端传递
}

// EncryptedReq 加密的交易请求
type EncryptedReq struct {
	EncryptedData string `json:"encrypted_data" binding:"required"` // AES加密后的原始请求数据
	NeedRSAVerify bool   `json:"need_rsa_verify,omitempty"`         // 是否需要RSA验证
	UserReqType   string `json:"user_req_type"`                     // 用户请求类型 默认是普通用户 organization 组织账户
}

// SignedDataReq 带签名的数据请求
type SignedDataReq struct {
	Data      json.RawMessage `json:"data" binding:"required"`      // 原始交易数据
	Signature string          `json:"signature" binding:"required"` // 对原始数据的RSA签名
}
