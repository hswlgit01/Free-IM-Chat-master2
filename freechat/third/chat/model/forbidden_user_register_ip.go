package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ForbiddenUserRegisterIP 被封禁用户关联的注册/登录 IP，用于禁止同 IP 再次注册。
// im_server_user_id 与 forbidden_account.user_id 一致（子账户 IM ID）。
type ForbiddenUserRegisterIP struct {
	ImServerUserID string    `bson:"im_server_user_id" json:"im_server_user_id"`
	IP             string    `bson:"ip" json:"ip"`
	CreateTime     time.Time `bson:"create_time" json:"create_time"`
}

func (ForbiddenUserRegisterIP) TableName() string {
	return "forbidden_user_register_ip"
}

type ForbiddenUserRegisterIPDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewForbiddenUserRegisterIPDao(db *mongo.Database) *ForbiddenUserRegisterIPDao {
	d := &ForbiddenUserRegisterIPDao{
		DB:         db,
		Collection: db.Collection(ForbiddenUserRegisterIP{}.TableName()),
	}
	_, _ = d.Collection.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "im_server_user_id", Value: 1},
				{Key: "ip", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "ip", Value: 1}},
		},
	})
	return d
}

// ReplaceIPsForBannedUser 覆盖该封禁子账户下的 IP 列表（先删后插）。
func (d *ForbiddenUserRegisterIPDao) ReplaceIPsForBannedUser(ctx context.Context, imServerUserID string, ips []string) error {
	if imServerUserID == "" {
		return nil
	}
	_, err := d.Collection.DeleteMany(ctx, bson.M{"im_server_user_id": imServerUserID})
	if err != nil {
		return err
	}
	now := time.Now()
	var docs []*ForbiddenUserRegisterIP
	seen := make(map[string]struct{})
	for _, ip := range ips {
		ip = trimIP(ip)
		if ip == "" {
			continue
		}
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		docs = append(docs, &ForbiddenUserRegisterIP{
			ImServerUserID: imServerUserID,
			IP:             ip,
			CreateTime:     now,
		})
	}
	if len(docs) == 0 {
		return nil
	}
	return mongoutil.InsertMany(ctx, d.Collection, docs)
}

// DeleteByImServerUserIDs 解封时移除关联 IP。
func (d *ForbiddenUserRegisterIPDao) DeleteByImServerUserIDs(ctx context.Context, imServerUserIDs []string) error {
	if len(imServerUserIDs) == 0 {
		return nil
	}
	_, err := d.Collection.DeleteMany(ctx, bson.M{"im_server_user_id": bson.M{"$in": imServerUserIDs}})
	return err
}

// RegisterBlockedByBannedUserIP 若 clientIP 命中某条记录且对应用户仍在 forbidden_account 中，则禁止注册。
func (d *ForbiddenUserRegisterIPDao) RegisterBlockedByBannedUserIP(ctx context.Context, forbidden *ForbiddenAccountDao, clientIP string) (bool, error) {
	clientIP = trimIP(clientIP)
	if clientIP == "" {
		return false, nil
	}
	rows, err := mongoutil.Find[*ForbiddenUserRegisterIP](ctx, d.Collection, bson.M{"ip": clientIP})
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		if row == nil || row.ImServerUserID == "" {
			continue
		}
		banned, err := forbidden.ExistByUserId(ctx, row.ImServerUserID)
		if err != nil {
			return false, err
		}
		if banned {
			return true, nil
		}
	}
	return false, nil
}

func trimIP(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
