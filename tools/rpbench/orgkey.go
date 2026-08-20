package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// orgAesKey 从 Mongo 读组织的 aesKeyBase64。
// 这把 key 只用于嵌入式登录换 token，进程内用完即弃，不落盘、不打印。
func orgAesKey(cfg *Config) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		return "", err
	}
	defer cli.Disconnect(ctx)

	var org struct {
		AesKeyBase64 string `bson:"aesKeyBase64"`
	}
	err = cli.Database("openim_v3").Collection("organization").
		FindOne(ctx, bson.M{"status": "pass"}).Decode(&org)
	if err != nil {
		return "", err
	}
	if org.AesKeyBase64 == "" {
		return "", fmt.Errorf("组织的 aesKeyBase64 为空")
	}
	return org.AesKeyBase64, nil
}

func mustOrgAesKey(cfg *Config) string {
	k, err := orgAesKey(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取组织密钥失败: %v\n", err)
		os.Exit(1)
	}
	return k
}
