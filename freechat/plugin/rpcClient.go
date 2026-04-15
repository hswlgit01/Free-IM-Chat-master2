package plugin

import (
	"github.com/openimsdk/chat/pkg/protocol/admin"
	chatpb "github.com/openimsdk/chat/pkg/protocol/chat"
)

var (
	chatClient  chatpb.ChatClient
	adminClient admin.AdminClient
)

func ChatClient() chatpb.ChatClient {
	return chatClient
}

func InitChatClient(chatCli chatpb.ChatClient) {
	chatClient = chatCli
}

func AdminClient() admin.AdminClient {
	return adminClient
}

func InitAdminClient(adminCli admin.AdminClient) {
	adminClient = adminCli
}
