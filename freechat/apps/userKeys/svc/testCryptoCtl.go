package svc

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"

	"github.com/gin-gonic/gin"
)

// 全局RSA密钥对，仅用于测试
var testPrivateKey *rsa.PrivateKey
var testPublicKeyPEM string
var testAESKey string

// TestCryptoCtl 测试加密控制器
type TestCryptoCtl struct{}

// NewTestCryptoCtl 创建测试加密控制器
func NewTestCryptoCtl() *TestCryptoCtl {
	return &TestCryptoCtl{}
}

// generateRSAKeyPair 生成RSA密钥对
func generateRSAKeyPair() error {
	var err error
	testPrivateKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	// 公钥转PEM格式
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&testPrivateKey.PublicKey)
	if err != nil {
		return err
	}
	publicKeyBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	}
	testPublicKeyPEM = string(pem.EncodeToMemory(publicKeyBlock))

	return nil
}

// rsaDecrypt RSA解密
func rsaDecrypt(ciphertext string) (string, error) {
	ciphertextBytes, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	// 解密
	plaintext, err := rsa.DecryptPKCS1v15(rand.Reader, testPrivateKey, ciphertextBytes)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// rsaSign RSA签名
func rsaSign(data []byte) (string, error) {
	hashed := sha256.Sum256(data)
	signature, err := rsa.SignPKCS1v15(rand.Reader, testPrivateKey, crypto.SHA256, hashed[:])
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(signature), nil
}

// TestSetupKeys 测试设置密钥接口
func (t *TestCryptoCtl) TestSetupKeys(c *gin.Context) {
	// 获取用户ID
	userID := mctx.GetOpUserID(c)
	if userID == "" {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrUnauthorized, freeErrors.ErrorMessages[freeErrors.ErrUnauthorized]))
		return
	}

	// 获取平台类型
	platform := c.Query("platform")
	if platform == "" {
		platform = "web"
	}

	// 生成RSA密钥对
	if err := generateRSAKeyPair(); err != nil {
		log.ZError(c, "生成RSA密钥对失败", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, "生成RSA密钥对失败"))
		return
	}

	// 调用实际的SetupUserKeys功能，将公钥传递给后端
	userKeysSvc := NewUserKeysSvc()
	encryptedAESKey, err := userKeysSvc.SetupUserKeys(c, userID, platform, testPublicKeyPEM, "")
	if err != nil {
		log.ZError(c, "设置用户密钥失败", err, "user_id", userID, "platform", platform)
		apiresp.GinError(c, err)
		return
	}

	// 解密AES密钥
	decryptedAESKey, err := rsaDecrypt(encryptedAESKey)
	if err != nil {
		log.ZError(c, "解密AES密钥失败", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, "解密AES密钥失败"))
		return
	}

	// 保存解密后的AES密钥用于后续测试
	testAESKey = decryptedAESKey

	// 返回简单的成功信息
	apiresp.GinSuccess(c, gin.H{
		"message": "密钥设置成功",
	})
}

// TestNormalTransaction 测试普通加密交易
func (t *TestCryptoCtl) TestNormalTransaction(c *gin.Context) {
	// 获取用户ID
	userID := mctx.GetOpUserID(c)
	if userID == "" {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrUnauthorized, freeErrors.ErrorMessages[freeErrors.ErrUnauthorized]))
		return
	}

	// 获取平台类型
	platform := c.Query("platform")
	if platform == "" {
		platform = "web"
	}

	// 检查目标ID
	targetID := c.Query("target_id")
	if targetID == "" {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, "目标ID不能为空"))
		return
	}

	// 检查金额
	amount := c.Query("amount")
	if amount == "" {
		amount = "0.01"
	}

	// 检查支付密码
	payPassword := c.Query("pay_password")
	if payPassword == "" {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, "支付密码不能为空"))
		return
	}

	// 从数据库获取用户AES密钥
	userKeysSvc := NewUserKeysSvc()
	aesKey, err := userKeysSvc.GetUserAESKey(c, userID, platform)
	if err != nil {
		log.ZError(c, "获取AES密钥失败", err, "user_id", userID, "platform", platform)
		apiresp.GinError(c, err)
		return
	}

	if aesKey == "" {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrUnauthorized, "用户密钥未设置，请先调用设置密钥接口"))
		return
	}

	// 构建交易数据
	transactionData := map[string]interface{}{
		"target_id":        targetID,
		"transaction_type": 0, // P2P红包
		"total_amount":     amount,
		"total_count":      1,
		"greeting":         "测试普通加密交易",
		"pay_password":     payPassword,
	}

	// 转为JSON
	jsonData, err := json.Marshal(transactionData)
	if err != nil {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, "序列化交易数据失败"))
		return
	}

	// 加密交易数据
	encryptedData, err := utils.AESEncrypt(jsonData, aesKey)
	if err != nil {
		log.ZError(c, "加密交易数据失败", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, "加密交易数据失败"))
		return
	}

	// 直接返回接口所需参数
	apiresp.GinSuccess(c, gin.H{
		"encrypted_data": encryptedData,
	})
}

// TestBiometricTransaction 测试生物识别交易
func (t *TestCryptoCtl) TestBiometricTransaction(c *gin.Context) {
	// 获取用户ID
	userID := mctx.GetOpUserID(c)
	if userID == "" {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrUnauthorized, freeErrors.ErrorMessages[freeErrors.ErrUnauthorized]))
		return
	}

	// 获取平台类型
	platform := c.Query("platform")
	if platform == "" {
		platform = "web"
	}

	// 检查目标ID
	targetID := c.Query("target_id")
	if targetID == "" {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, "目标ID不能为空"))
		return
	}

	// 检查金额
	amount := c.Query("amount")
	if amount == "" {
		amount = "0.01"
	}

	// 检查RSA密钥对是否已设置
	if testPrivateKey == nil {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, "请先调用测试设置密钥接口"))
		return
	}

	// 从数据库获取用户AES密钥
	userKeysSvc := NewUserKeysSvc()
	aesKey, err := userKeysSvc.GetUserAESKey(c, userID, platform)
	if err != nil {
		log.ZError(c, "获取AES密钥失败", err, "user_id", userID, "platform", platform)
		apiresp.GinError(c, err)
		return
	}

	if aesKey == "" {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrUnauthorized, "用户密钥未设置，请先调用设置密钥接口"))
		return
	}

	// 构建交易数据（生物识别不需要密码）
	transactionData := map[string]interface{}{
		"target_id":        targetID,
		"transaction_type": 0, // P2P红包
		"total_amount":     amount,
		"total_count":      1,
		"greeting":         "测试生物识别交易",
	}

	// 转为JSON
	jsonData, err := json.Marshal(transactionData)
	if err != nil {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, "序列化交易数据失败"))
		return
	}

	// 使用RSA私钥签名
	signature, err := rsaSign(jsonData)
	if err != nil {
		log.ZError(c, "RSA签名失败", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, "RSA签名失败"))
		return
	}

	// 构建包含原始数据和签名的组合结构
	signedData := struct {
		Data      json.RawMessage `json:"data"`
		Signature string          `json:"signature"`
	}{
		Data:      jsonData,
		Signature: signature,
	}

	// 序列化组合结构
	signedDataJSON, err := json.Marshal(signedData)
	if err != nil {
		log.ZError(c, "序列化签名数据失败", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, "序列化签名数据失败"))
		return
	}

	// 使用AES加密组合结构
	encryptedData, err := utils.AESEncrypt(signedDataJSON, aesKey)
	if err != nil {
		log.ZError(c, "加密组合数据失败", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, "加密组合数据失败"))
		return
	}

	// 直接返回接口所需参数
	apiresp.GinSuccess(c, gin.H{
		"encrypted_data": encryptedData,
	})
}

// TestGetKeys 测试获取当前保存的密钥（仅用于测试）
func (t *TestCryptoCtl) TestGetKeys(c *gin.Context) {
	apiresp.GinSuccess(c, gin.H{
		"aes_key":             testAESKey,
		"rsa_public_key":      testPublicKeyPEM,
		"has_rsa_private_key": testPrivateKey != nil,
	})
}
