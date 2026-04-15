package dto

// SetupUserKeysResp 设置用户密钥响应
type SetupUserKeysResp struct {
	EncryptedAESKey string `json:"encrypted_aes_key"` // RSA加密后的AES密钥
}
