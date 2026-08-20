package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// runMonitor 每隔 interval 采一次样，写成 CSV。
// 采集的四类指标正好对应四个可能的瓶颈：
//   - Mongo writeConflicts：多文档事务写热点文档的冲突重试（红包链路的锅）
//   - WiredTiger 可用票据：Mongo 并发写入槽位耗尽的直接信号
//   - Kafka consumer lag：toRedis / toMongo / toPush 各段的积压
//   - 容器 CPU：谁先烧满
func runMonitor(cfg *Config, args []string) error {
	fs := flag.NewFlagSet("monitor", flag.ExitOnError)
	cfg.bindCommon(fs)
	var (
		interval = fs.Duration("interval", 2*time.Second, "采样间隔")
		duration = fs.Duration("duration", 5*time.Minute, "采样总时长")
		tag      = fs.String("tag", "run", "标签，用于输出文件命名")
		groups   = fs.String("groups", "redis,mongo,push,offlinePush", "要观察的 Kafka 消费组")
	)
	_ = fs.Parse(args)

	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(cfg.OutDir, fmt.Sprintf("metrics-%s-%s.csv", *tag, time.Now().Format("0102-150405")))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	groupList := strings.Split(*groups, ",")
	header := []string{"ts", "write_conflicts", "insert", "update", "query", "command",
		"wt_write_avail", "wt_read_avail", "active_writers", "active_readers"}
	for _, g := range groupList {
		header = append(header, "lag_"+strings.TrimSpace(g))
	}
	header = append(header, "cpu_mongo", "cpu_kafka", "cpu_redis", "cpu_server", "cpu_chat", "disk_free_gb")
	fmt.Fprintln(f, strings.Join(header, ","))

	ctx, cancel := context.WithTimeout(context.Background(), *duration+30*time.Second)
	defer cancel()
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		return fmt.Errorf("连接 Mongo 失败: %w", err)
	}
	defer cli.Disconnect(ctx)

	fmt.Printf("开始采样，间隔 %s，时长 %s → %s\n", *interval, *duration, path)
	deadline := time.Now().Add(*duration)
	tick := time.NewTicker(*interval)
	defer tick.Stop()

	var prev *mongoSample
	for time.Now().Before(deadline) {
		<-tick.C
		row := []string{time.Now().Format("15:04:05")}

		cur, err := sampleMongo(ctx, cli)
		if err != nil {
			fmt.Printf("采样 Mongo 失败: %v\n", err)
			continue
		}
		// 计数器是单调递增的，输出增量才有意义
		if prev != nil {
			row = append(row,
				itoa(cur.writeConflicts-prev.writeConflicts),
				itoa(cur.insert-prev.insert),
				itoa(cur.update-prev.update),
				itoa(cur.query-prev.query),
				itoa(cur.command-prev.command))
		} else {
			row = append(row, "0", "0", "0", "0", "0")
		}
		row = append(row,
			itoa(cur.wtWriteAvail), itoa(cur.wtReadAvail),
			itoa(cur.activeWriters), itoa(cur.activeReaders))
		prev = cur

		for _, g := range groupList {
			row = append(row, itoa(kafkaLag(strings.TrimSpace(g))))
		}
		cpu := dockerCPU()
		for _, name := range []string{"mongo", "kafka", "redis", "freechat-server", "freechat-chat"} {
			row = append(row, cpu[name])
		}

		// 磁盘看门狗。
		// 教训：freechat-server 的 log.yml 是 remainLogLevel=6(debug) + isStdout=true，
		// 而 docker 的 json-file 驱动没配轮转上限，一轮压测就写了 18GB stdout，
		// 直接把根分区打满 → WiredTiger 分配 journal 失败 → mongod panic 退出。
		// 所以压测期间必须盯着剩余空间，逼近阈值就先截断容器日志，再低就中止压测。
		freeGB := diskFreeGB("/")
		row = append(row, fmt.Sprintf("%.1f", freeGB))
		if freeGB < diskTruncateThresholdGB {
			fmt.Printf("[看门狗] 剩余磁盘 %.1fGB，截断容器 stdout 日志\n", freeGB)
			truncateContainerLogs()
		}
		if freeGB < diskAbortThresholdGB {
			fmt.Printf("[看门狗] 剩余磁盘仅 %.1fGB，中止采样以免打爆磁盘\n", freeGB)
			fmt.Fprintln(f, strings.Join(row, ","))
			return fmt.Errorf("磁盘空间不足，压测已中止")
		}

		line := strings.Join(row, ",")
		fmt.Fprintln(f, line)
		f.Sync()
		fmt.Println(line)
	}
	fmt.Printf("采样结束 → %s\n", path)
	return nil
}

type mongoSample struct {
	writeConflicts, insert, update, query, command int64
	wtWriteAvail, wtReadAvail                      int64
	activeWriters, activeReaders                   int64
}

func sampleMongo(ctx context.Context, cli *mongo.Client) (*mongoSample, error) {
	var raw bson.M
	err := cli.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&raw)
	if err != nil {
		return nil, err
	}
	s := &mongoSample{}
	s.writeConflicts = dig(raw, "metrics", "operation", "writeConflicts")
	s.insert = dig(raw, "opcounters", "insert")
	s.update = dig(raw, "opcounters", "update")
	s.query = dig(raw, "opcounters", "query")
	s.command = dig(raw, "opcounters", "command")
	s.wtWriteAvail = dig(raw, "wiredTiger", "concurrentTransactions", "write", "available")
	s.wtReadAvail = dig(raw, "wiredTiger", "concurrentTransactions", "read", "available")
	s.activeWriters = dig(raw, "globalLock", "activeClients", "writers")
	s.activeReaders = dig(raw, "globalLock", "activeClients", "readers")
	return s, nil
}

// dig 按路径取嵌套的数值字段，取不到返回 0（不同 Mongo 版本字段位置有差异）。
func dig(m bson.M, path ...string) int64 {
	var cur any = m
	for _, p := range path {
		mm, ok := cur.(bson.M)
		if !ok {
			return 0
		}
		cur, ok = mm[p]
		if !ok {
			return 0
		}
	}
	switch v := cur.(type) {
	case int32:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	}
	return 0
}

// kafkaLag 调 kafka 容器里的 kafka-consumer-groups.sh 取某个消费组的总 lag。
func kafkaLag(group string) int64 {
	out, err := exec.Command("docker", "exec", "kafka",
		"kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092",
		"--describe", "--group", group).Output()
	if err != nil {
		return -1
	}
	var total int64
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		// GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
		if len(fields) < 6 || fields[0] != group {
			continue
		}
		if n, err := strconv.ParseInt(fields[5], 10, 64); err == nil {
			total += n
		}
	}
	return total
}

// dockerCPU 取各容器当前的 CPU 百分比。
func dockerCPU() map[string]string {
	res := map[string]string{}
	out, err := exec.Command("docker", "stats", "--no-stream", "--format",
		"{{.Name}}\t{{.CPUPerc}}").Output()
	if err != nil {
		return res
	}
	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.Split(strings.TrimSpace(line), "\t")
		if len(parts) != 2 {
			continue
		}
		res[parts[0]] = strings.TrimSuffix(parts[1], "%")
	}
	return res
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

var _ = json.Marshal
