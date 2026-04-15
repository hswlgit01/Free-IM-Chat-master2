package utils

import (
	"crypto/rand"
	"github.com/google/uuid"
	"math/big"
	"time"
)

// RandomString 生成指定长度的随机字符串
func RandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			// 如果随机数生成失败，使用时间戳
			result[i] = charset[int(time.Now().UnixNano()%int64(len(charset)))]
			continue
		}
		result[i] = charset[n.Int64()]
	}
	return string(result)
}

func RandomNumString(length int) string {
	const charset = "0123456789"
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			// 如果随机数生成失败，使用时间戳
			result[i] = charset[int(time.Now().UnixNano()%int64(len(charset)))]
			continue
		}
		result[i] = charset[n.Int64()]
	}
	return string(result)
}

func RandomAlphaString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			// 如果随机数生成失败，使用时间戳
			result[i] = charset[int(time.Now().UnixNano()%int64(len(charset)))]
			continue
		}
		result[i] = charset[n.Int64()]
	}
	return string(result)
}

// GenerateRoomID 生成字符串的房间ID
func GenerateRoomID() string {
	return uuid.NewString()
	//return fmt.Sprintf("%s-%s", RandomString(4), RandomString(4))
}
