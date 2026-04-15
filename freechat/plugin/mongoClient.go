package plugin

import "github.com/openimsdk/chat/tools/db/mongoutil"

var mongoCli *mongoutil.Client

func MongoCli() *mongoutil.Client {
	return mongoCli
}

func InitMongoCli(cli *mongoutil.Client) {
	mongoCli = cli
}
