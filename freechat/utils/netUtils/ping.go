package netUtils

import (
	"fmt"
	"net"
	"time"
)

func PingTCP(host, port string, timeout time.Duration) bool {
	target := fmt.Sprintf("%s:%s", host, port)
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}
