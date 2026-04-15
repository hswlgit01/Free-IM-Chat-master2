package svc

import (
	"context"
	"strings"

	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/mongo"
)

func dedupeIPStrings(in []string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func collectIPsForBannedMainUser(ctx context.Context, db *mongo.Database, mainUserID string) []string {
	if mainUserID == "" {
		return nil
	}
	registerDao := chatModel.NewRegisterDao(db)
	loginDao := chatModel.NewUserLoginRecordDao(db)
	var ips []string
	reg, err := registerDao.GetByUserId(ctx, mainUserID)
	if err == nil && reg != nil && strings.TrimSpace(reg.IP) != "" {
		ips = append(ips, reg.IP)
	}
	more, err := loginDao.DistinctIPsByUserID(ctx, mainUserID)
	if err == nil {
		ips = append(ips, more...)
	}
	return dedupeIPStrings(ips)
}

func syncForbiddenRegisterIPsOnBlock(ctx context.Context, db *mongo.Database, mainUserID, imServerUserID string) error {
	ips := collectIPsForBannedMainUser(ctx, db, mainUserID)
	dao := chatModel.NewForbiddenUserRegisterIPDao(db)
	return dao.ReplaceIPsForBannedUser(ctx, imServerUserID, ips)
}

func checkRegisterClientIPNotFromBannedUser(ctx context.Context, db *mongo.Database, clientIP string) error {
	if strings.TrimSpace(clientIP) == "" {
		return nil
	}
	ipDao := chatModel.NewForbiddenUserRegisterIPDao(db)
	forbiddenDao := chatModel.NewForbiddenAccountDao(db)
	blocked, err := ipDao.RegisterBlockedByBannedUserIP(ctx, forbiddenDao, clientIP)
	if err != nil {
		return err
	}
	if blocked {
		return freeErrors.ApiErr("该网络环境曾关联被封禁账号，无法注册")
	}
	return nil
}

func deleteForbiddenRegisterIPsOnUnblock(ctx context.Context, db *mongo.Database, imServerUserIDs []string) {
	dao := chatModel.NewForbiddenUserRegisterIPDao(db)
	if err := dao.DeleteByImServerUserIDs(ctx, imServerUserIDs); err != nil {
		log.ZWarn(ctx, "delete forbidden_user_register_ip on unblock failed", err, "im_server_user_ids", imServerUserIDs)
	}
}
