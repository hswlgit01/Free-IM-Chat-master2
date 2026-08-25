package main

import (
	"fmt"
	"os/exec"
	"strings"
	"syscall"
)

const (
	// 低于这个剩余空间就截断容器 stdout 日志
	diskTruncateThresholdGB = 12.0
	// 低于这个剩余空间直接中止压测
	diskAbortThresholdGB = 6.0
)

func diskFreeGB(path string) float64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 1 << 20 // 取不到就当成充足，不误伤
	}
	return float64(st.Bavail) * float64(st.Bsize) / (1024 * 1024 * 1024)
}

// openimServiceLogDir 是 OpenIM 各服务真正写日志的地方。
//
// 坑点：server 的 log.yml 写的是 storageLocation: ./logs/（相对路径），
// 而服务进程的工作目录是二进制所在目录，所以日志落在
// /im-server/_output/bin/platforms/linux/amd64/logs/ ——
// 既不是挂载出来的 /data/logs/server，也就没人看得见、没人清理。
// 叠加 remainLogLevel:6(debug) + rotationTime:24h，一轮压测能写 20GB+，
// 直接把宿主机根分区打满，进而让 mongod 因为分配不出 journal 而 panic。
const openimServiceLogDir = "/im-server/_output/bin/platforms/linux/amd64/logs"

// truncateContainerLogs 把体积最大的几个容器 stdout 日志、以及 OpenIM 那个
// 藏在容器可写层里的服务日志清零。
// 只动日志文件，不碰任何业务数据。
func truncateContainerLogs() {
	// 先清 OpenIM 容器内的服务日志——它才是大头
	out, err := exec.Command("docker", "exec", "freechat-server", "sh", "-c",
		`find `+openimServiceLogDir+` -name 'openim-service-log.*' -exec truncate -s 0 {} \; 2>/dev/null; echo done`).CombinedOutput()
	if err != nil {
		fmt.Printf("[看门狗] 清 OpenIM 服务日志失败: %v %s\n", err, truncate(string(out), 120))
	}
	truncateDockerJSONLogs()
}

func truncateDockerJSONLogs() {
	out, err := exec.Command("sh", "-c",
		`du -a /var/lib/docker/containers --max-depth=2 2>/dev/null | grep -- '-json.log$' | sort -rn | head -4 | cut -f2`).Output()
	if err != nil {
		fmt.Printf("[看门狗] 查找容器日志失败: %v\n", err)
		return
	}
	for _, p := range strings.Fields(string(out)) {
		if !strings.HasSuffix(p, "-json.log") {
			continue // 双保险：只清 json.log
		}
		if err := exec.Command("truncate", "-s", "0", p).Run(); err != nil {
			fmt.Printf("[看门狗] 截断 %s 失败: %v\n", p, err)
		}
	}
}
