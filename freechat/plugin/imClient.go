package plugin

import "github.com/openimsdk/chat/pkg/common/imapi"

var imApiCaller imapi.CallerInterface

func ImApiCaller() imapi.CallerInterface {
	return imApiCaller
}

func InitApiCaller(imApi imapi.CallerInterface) {
	imApiCaller = imApi
}
