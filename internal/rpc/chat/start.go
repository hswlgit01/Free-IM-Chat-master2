package chat

import (
	"context"
	"strings"
	"time"

	"github.com/openimsdk/chat/tools/db/redisutil"
	"github.com/redis/go-redis/v9"

	"github.com/openimsdk/chat/pkg/common/constant"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/chat/pkg/common/rtc"
	"github.com/openimsdk/chat/pkg/protocol/admin"
	"github.com/openimsdk/chat/pkg/protocol/chat"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/chat/tools/mw"
	"github.com/openimsdk/tools/discovery"
	"github.com/openimsdk/tools/errs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	chatCache "github.com/openimsdk/chat/freechat/third/chat/cache"
	"github.com/openimsdk/chat/pkg/common/config"
	"github.com/openimsdk/chat/pkg/common/db/database"
	"github.com/openimsdk/chat/pkg/email"
	chatClient "github.com/openimsdk/chat/pkg/rpclient/chat"
	"github.com/openimsdk/chat/pkg/sms"
)

type Config struct {
	RpcConfig     config.Chat
	RedisConfig   config.Redis
	MongodbConfig config.Mongo
	Discovery     config.Discovery
	Share         config.Share
}

func Start(ctx context.Context, config *Config, client discovery.SvcDiscoveryRegistry, server *grpc.Server) error {
	if len(config.Share.ChatAdmin) == 0 {
		return errs.New("share chat admin not configured")
	}
	mgocli, err := mongoutil.NewMongoDB(ctx, config.MongodbConfig.Build())
	if err != nil {
		return err
	}
	var srv chatSvr
	config.RpcConfig.VerifyCode.Phone.Use = strings.ToLower(config.RpcConfig.VerifyCode.Phone.Use)
	config.RpcConfig.VerifyCode.Mail.Use = strings.ToLower(config.RpcConfig.VerifyCode.Mail.Use)
	srv.conf = config.RpcConfig.VerifyCode
	switch config.RpcConfig.VerifyCode.Phone.Use {
	case "ali":
		ali := config.RpcConfig.VerifyCode.Phone.Ali
		srv.SMS, err = sms.NewAli(ali.Endpoint, ali.AccessKeyID, ali.AccessKeySecret, ali.SignName, ali.VerificationCodeTemplateCode)
		if err != nil {
			return err
		}
	case "weiwebs":
		weiwebs := config.RpcConfig.VerifyCode.Phone.Weiwebs
		srv.SMS = sms.NewWeiwebs(weiwebs.Account, weiwebs.Password, weiwebs.SignName)
	case "smart":
		// 智能模式：默认使用阿里云，中国大陆手机号码通过代码内部逻辑自动转为weiwebs
		ali := config.RpcConfig.VerifyCode.Phone.Ali
		srv.SMS, err = sms.NewAli(ali.Endpoint, ali.AccessKeyID, ali.AccessKeySecret, ali.SignName, ali.VerificationCodeTemplateCode)
		if err != nil {
			return err
		}
		// 确保weiwebs配置也是有效的
		if weiwebs := config.RpcConfig.VerifyCode.Phone.Weiwebs; weiwebs.Account == "" || weiwebs.Password == "" {
			return errs.New("weiwebs configuration required in smart mode")
		}
	}
	if mail := config.RpcConfig.VerifyCode.Mail; mail.Use == constant.VerifyMail {
		// 默认支持中心URL
		supportCenterURL := mail.SupportCenterURL
		if mail.SendGrid.APIKey != "" {
			srv.Mail = email.NewSendGridMail(mail.SendGrid.APIKey, mail.SenderMail, mail.SendGrid.SenderName, mail.Title, supportCenterURL)
		} else {
			srv.Mail = email.NewMail(mail.SMTPAddr, mail.SMTPPort, mail.SenderMail, mail.SenderAuthorizationCode, mail.Title, supportCenterURL)
		}
	}
	srv.Database, err = database.NewChatDatabase(mgocli)
	if err != nil {
		return err
	}
	conn, err := client.GetConn(ctx, config.Discovery.RpcService.Admin, grpc.WithTransportCredentials(insecure.NewCredentials()), mw.GrpcClient())
	if err != nil {
		return err
	}
	srv.Admin = chatClient.NewAdminClient(admin.NewAdminClient(conn))
	srv.Code = verifyCode{
		UintTime:   time.Duration(config.RpcConfig.VerifyCode.UintTime) * time.Second,
		MaxCount:   config.RpcConfig.VerifyCode.MaxCount,
		ValidCount: config.RpcConfig.VerifyCode.ValidCount,
		SuperCode:  config.RpcConfig.VerifyCode.SuperCode,
		ValidTime:  time.Duration(config.RpcConfig.VerifyCode.ValidTime) * time.Second,
		Len:        config.RpcConfig.VerifyCode.Len,
	}

	rdb, err := redisutil.NewRedisClient(ctx, config.RedisConfig.Build())
	if err != nil {
		return err
	}
	srv.redisCli = rdb

	// 初始化登录记录缓存
	srv.LoginRecordCache = chatCache.NewLoginRecordCacheRedis(rdb, mgocli.GetDB())

	srv.rpcChatConf = config.RpcConfig
	srv.Livekit = rtc.NewLiveKit(config.RpcConfig.LiveKit.Key, config.RpcConfig.LiveKit.Secret, config.RpcConfig.LiveKit.URL)
	srv.AllowRegister = config.RpcConfig.AllowRegister
	chat.RegisterChatServer(server, &srv)
	return nil
}

type chatSvr struct {
	chat.UnimplementedChatServer
	redisCli         redis.UniversalClient
	rpcChatConf      config.Chat
	conf             config.VerifyCode
	Database         database.ChatDatabaseInterface
	Admin            *chatClient.AdminClient
	SMS              sms.SMS
	Mail             email.Mail
	Code             verifyCode
	Livekit          *rtc.LiveKit
	ChatAdminUserID  string
	AllowRegister    bool
	LoginRecordCache *chatCache.LoginRecordCacheRedis // 新增登录记录缓存字段
}

func (o *chatSvr) WithAdminUser(ctx context.Context) context.Context {
	return mctx.WithAdminUser(ctx, o.ChatAdminUserID)
}

type verifyCode struct {
	UintTime   time.Duration // sec
	MaxCount   int
	ValidCount int
	SuperCode  string
	ValidTime  time.Duration
	Len        int
}
