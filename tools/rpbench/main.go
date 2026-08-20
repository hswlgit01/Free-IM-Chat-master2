package main

import (
	"fmt"
	"os"
)

const usage = `红包压测工具 — 复现「抢红包打爆 MongoDB / 撑爆 IM 消息队列」

用法：
  rpbench prepare [flags]   造数据：选用户池、建压测群、给发送者充值，产出 plan.json
  rpbench bench   [flags]   打压力：发红包 + N 并发抢 + 并行群消息
  rpbench monitor [flags]   采指标：Mongo 写冲突 / WiredTiger 票据 / Kafka lag / 容器 CPU
  rpbench verify  [flags]   校验一致性：领取记录数、金额合计、超发检查

典型流程（都在被测服务器本机跑）：
  ./rpbench prepare -receivers 5000
  ./rpbench monitor -tag baseline -duration 6m &
  ./rpbench bench   -tag baseline -c 3000 -count 500 -im-rate 200
  ./rpbench verify

公共参数见各子命令 -h。
`

func main() {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		os.Exit(1)
	}
	cfg := defaultConfig()
	var err error
	switch os.Args[1] {
	case "prepare":
		err = runPrepare(cfg, os.Args[2:])
	case "bench":
		err = runBench(cfg, os.Args[2:])
	case "monitor":
		err = runMonitor(cfg, os.Args[2:])
	case "verify":
		err = runVerify(cfg, os.Args[2:])
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	default:
		fmt.Printf("未知子命令 %q\n\n%s", os.Args[1], usage)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "错误: %v\n", err)
		os.Exit(1)
	}
}
