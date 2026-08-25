package main

import (
	"flag"
	"os"
)

// Config 压测工具的全局配置。所有参数都可以用命令行覆盖，
// 默认值对应测试服 8.148.66.77 的 all-in-one 部署（各服务 network_mode: host）。
type Config struct {
	MongoURI string // Mongo 连接串（含 replicaSet，事务必需）
	ChatAPI  string // freechat chat-api，默认 :10008
	IMAPI    string // openim-api，默认 :10002
	IMSecret string // share.yml 里的 openIM.secret
	IMAdmin  string // share.yml 里的 openIM.adminUserID

	PlanFile string // prepare 产出 / bench 消费的计划文件
	OutDir   string // 压测结果与指标输出目录
}

func defaultConfig() *Config {
	return &Config{
		// directConnection=true：mongo 跑在容器里，副本集自报的成员地址是 localhost:27017，
		// 宿主机连不上。压测工具本身不开事务，直连即可。
		MongoURI: envOr("STRESS_MONGO_URI",
			"mongodb://root:openIM123@127.0.0.1:37017/openim_v3?authSource=admin&directConnection=true"),
		ChatAPI:  envOr("STRESS_CHAT_API", "http://127.0.0.1:10008"),
		IMAPI:    envOr("STRESS_IM_API", "http://127.0.0.1:10002"),
		IMSecret: envOr("STRESS_IM_SECRET", "test"),
		IMAdmin:  envOr("STRESS_IM_ADMIN", "imAdmin"),
		PlanFile: envOr("STRESS_PLAN", "./out/plan.json"),
		OutDir:   envOr("STRESS_OUT", "./out"),
	}
}

// bindCommon 把公共参数挂到某个子命令的 FlagSet 上。
func (c *Config) bindCommon(fs *flag.FlagSet) {
	fs.StringVar(&c.MongoURI, "mongo", c.MongoURI, "MongoDB 连接串")
	fs.StringVar(&c.ChatAPI, "chat", c.ChatAPI, "chat-api 地址")
	fs.StringVar(&c.IMAPI, "im", c.IMAPI, "openim-api 地址")
	fs.StringVar(&c.IMSecret, "im-secret", c.IMSecret, "openIM secret")
	fs.StringVar(&c.IMAdmin, "im-admin", c.IMAdmin, "openIM 管理员 userID")
	fs.StringVar(&c.PlanFile, "plan", c.PlanFile, "压测计划文件路径")
	fs.StringVar(&c.OutDir, "out", c.OutDir, "输出目录")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
