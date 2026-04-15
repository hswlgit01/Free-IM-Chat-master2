package utils

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
)

// GenerateAESKey 生成一个新的AES密钥
func GenerateAESKey() (string, error) {
	key := make([]byte, 32) // AES-256
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(key), nil
}

// AESEncrypt 使用AES-256-GCM模式加密
func AESEncrypt(plainText []byte, keyBase64 string) (string, error) {
	// 解码base64密钥
	key, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		return "", errors.New(fmt.Sprintf("base64 decode key error: %v", err))
	}

	// 创建AES加密块
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	// 创建GCM模式
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	// 创建随机nonce
	nonce := make([]byte, aesGCM.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	// 加密
	cipherText := aesGCM.Seal(nonce, nonce, plainText, nil)

	// 返回base64编码的密文
	return base64.StdEncoding.EncodeToString(cipherText), nil
}

// AESDecrypt 使用AES-256-GCM模式解密
func AESDecrypt(ciphertextBase64 string, keyBase64 string) ([]byte, error) {
	// 解码base64密钥
	key, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("base64 decode key error: %v", err))
	}

	// 解码base64密文
	cipherText, err := base64.StdEncoding.DecodeString(ciphertextBase64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("base64 decode cipher text error: %v", err))
	}

	// 创建AES加密块
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// 创建GCM模式
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// 检查密文长度是否足够
	if len(cipherText) < aesGCM.NonceSize() {
		return nil, errors.New("cipher text too short")
	}

	// 从密文中提取nonce
	nonce, cipherText := cipherText[:aesGCM.NonceSize()], cipherText[aesGCM.NonceSize():]

	// 解密
	plainText, err := aesGCM.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return nil, err
	}

	return plainText, nil
}

// RSAEncrypt 使用RSA公钥加密
func RSAEncrypt(plainText []byte, publicKeyPEM string) (string, error) {
	// 解析公钥
	block, _ := pem.Decode([]byte(publicKeyPEM))
	if block == nil || block.Type != "PUBLIC KEY" {
		return "", errors.New("failed to decode PEM block containing public key")
	}

	pubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return "", errors.New(fmt.Sprintf("failed to parse public key: %v", err))
	}

	rsaPublicKey, ok := pubKey.(*rsa.PublicKey)
	if !ok {
		return "", errors.New("not an RSA public key")
	}

	// 加密
	cipherText, err := rsa.EncryptPKCS1v15(rand.Reader, rsaPublicKey, plainText)
	if err != nil {
		return "", err
	}

	// 返回base64编码的密文
	return base64.StdEncoding.EncodeToString(cipherText), nil
}

// RSADecrypt 使用RSA私钥解密
func RSADecrypt(privateKeyPEM string, base64Data string) ([]byte, error) {
	data, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("base64 decode error: %v", err))
	}

	privateKeyBlock, _ := pem.Decode([]byte(privateKeyPEM))
	if privateKeyBlock == nil || privateKeyBlock.Type != "RSA PRIVATE KEY" {
		return nil, errors.New("failed to decode PEM block containing private key")
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(privateKeyBlock.Bytes)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("failed to parse private key: %v", err))
	}

	// 使用私钥解密消息
	return rsa.DecryptPKCS1v15(rand.Reader, privateKey, data)
}

// GenerateRSAKeyPair 生成RSA密钥对
func GenerateRSAKeyPair() (string, string, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}

	// 转换私钥为PKCS1格式
	privateKeyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privateKeyBytes,
	})

	// 转换公钥为PKIX格式
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return "", "", err
	}
	publicKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	})

	return string(publicKeyPEM), string(privateKeyPEM), nil
}

// RSAVerifySignature 使用RSA公钥验证签名
func RSAVerifySignature(data []byte, signatureBase64 string, publicKeyPEM string) (bool, error) {
	// 解码签名
	signature, err := base64.StdEncoding.DecodeString(signatureBase64)
	if err != nil {
		return false, errors.New(fmt.Sprintf("base64 decode signature error: %v", err))
	}

	// 解析公钥
	block, _ := pem.Decode([]byte(publicKeyPEM))
	if block == nil || block.Type != "PUBLIC KEY" {
		return false, errors.New("failed to decode PEM block containing public key")
	}

	pubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return false, errors.New(fmt.Sprintf("failed to parse public key: %v", err))
	}

	rsaPublicKey, ok := pubKey.(*rsa.PublicKey)
	if !ok {
		return false, errors.New("not an RSA public key")
	}

	// 计算数据哈希
	hashed := sha256.Sum256(data)

	// 验证签名
	err = rsa.VerifyPKCS1v15(rsaPublicKey, crypto.SHA256, hashed[:], signature)
	if err != nil {
		return false, nil // 签名验证失败，但不是错误
	}

	return true, nil // 签名验证成功
}
