package plugin

import (
	"github.com/openimsdk/chat/pkg/common/config"
)

type ChatConfig struct {
	ApiConfig     config.API
	Discovery     config.Discovery
	Share         config.Share
	MongodbConfig config.Mongo
	RedisConfig   config.Redis
	ChatRpcConfig config.Chat
	RuntimeEnv    string
}

var chatConfig *ChatConfig

func ChatCfg() *ChatConfig {
	return chatConfig
}

func InitChatConfig(chatCfg *ChatConfig) {
	chatConfig = chatCfg
}
