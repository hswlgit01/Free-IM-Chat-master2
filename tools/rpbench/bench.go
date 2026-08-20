package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// grabResult 单次抢红包的结果。
type grabResult struct {
	latency time.Duration
	errCode int    // 0 = 成功；业务错误码原样保留，用来区分「抢完了」和「真失败」
	errMsg  string
}

// benchStats 汇总一轮压测。
type benchStats struct {
	mu        sync.Mutex
	results   []grabResult
	byCode    map[int]int
	startedAt time.Time
	endedAt   time.Time
}

func newBenchStats() *benchStats {
	return &benchStats{byCode: map[int]int{}}
}

func (s *benchStats) add(r grabResult) {
	s.mu.Lock()
	s.results = append(s.results, r)
	s.byCode[r.errCode]++
	s.mu.Unlock()
}

func runBench(cfg *Config, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	cfg.bindCommon(fs)
	var (
		concurrency = fs.Int("c", 2000, "抢红包并发数（同时起多少个 goroutine）")
		packets     = fs.Int("packets", 1, "连续发几个红包")
		count       = fs.Int("count", 200, "每个红包拆成几份")
		amount      = fs.String("amount", "200", "每个红包总金额")
		rpType      = fs.Int("type", 3, "红包类型：2=普通 3=拼手气")
		imRate      = fs.Int("im-rate", 0, "并行的群消息发送速率（条/秒），0=不发")
		imDuration  = fs.Duration("im-duration", 60*time.Second, "群消息压力持续时长")
		warmup      = fs.Duration("warmup", 3*time.Second, "发完红包后等待多久再开抢")
		tag         = fs.String("tag", "run", "本轮压测的标签，用于输出文件命名")
		groupCheck  = fs.Bool("group-check", false, "保留群成员校验（默认跳过，与既有基线可比；开启后才能量到这条路径的开销）")
	)
	_ = fs.Parse(args)

	plan, err := loadPlan(cfg.PlanFile)
	if err != nil {
		return err
	}
	if len(plan.Receivers) < *concurrency {
		fmt.Printf("[警告] 用户池只有 %d 个，小于并发 %d，将循环复用（会产生「已领取」错误码，属正常）\n",
			len(plan.Receivers), *concurrency)
	}

	httpCli := newHTTPClient(*concurrency+256, 60*time.Second)
	chat := newChatClient(cfg, httpCli)
	im := newIMClient(cfg, httpCli)

	// 发送者 token 在 prepare 阶段就换好了，直接复用；
	// 只有计划里没存 token（老版本 plan.json）时才现场登录一次。
	senderToken := plan.SenderToken
	if senderToken == "" {
		fmt.Println("计划里没有 token，正在嵌入式登录...")
		login, err := chat.embedLogin(plan.OrgID, mustOrgAesKey(cfg), plan.SenderThird, "压测发送者")
		if err != nil {
			return fmt.Errorf("嵌入式登录失败（组织缓存命中时服务端密钥会丢，重跑 prepare 即可）: %w", err)
		}
		senderToken = login.ChatToken
	}
	fmt.Printf("发送者 token 就绪 (user_id=%s)\n", plan.SenderUserID)

	// 并行的 IM 群消息压力：红包只是导火索，真正撑爆队列的是红包+正常聊天叠加。
	var imWG sync.WaitGroup
	imStop := make(chan struct{})
	var imSent, imFail int64
	if *imRate > 0 {
		imWG.Add(1)
		go func() {
			defer imWG.Done()
			driveIMTraffic(im, plan, *imRate, imStop, &imSent, &imFail)
		}()
		fmt.Printf("已启动群消息压力：%d 条/秒 → 群 %s\n", *imRate, plan.GroupID)
	}

	allStats := make([]*benchStats, 0, *packets)
	for p := 0; p < *packets; p++ {
		fmt.Printf("\n=== 第 %d/%d 个红包 ===\n", p+1, *packets)
		txID, err := chat.createRedPacket(senderToken, plan.OrgID, plan.GroupID,
			plan.CurrencyID, *amount, *count, *rpType, "")
		if err != nil {
			return fmt.Errorf("创建红包失败: %w", err)
		}
		fmt.Printf("红包已创建 transaction_id=%s 金额=%s 份数=%d\n", txID, *amount, *count)

		time.Sleep(*warmup)

		st := grabStorm(cfg, httpCli, plan, txID, *concurrency, *groupCheck)
		allStats = append(allStats, st)
		printStats(st, *count)
	}

	if *imRate > 0 {
		// 让消息压力再跑一会儿，观察队列消化速度
		remain := *imDuration
		if remain > 0 {
			fmt.Printf("\n红包已抢完，群消息压力继续跑 %s 以观察队列消化...\n", remain)
			time.Sleep(remain)
		}
		close(imStop)
		imWG.Wait()
		fmt.Printf("群消息累计发送 %d 条，失败 %d 条\n",
			atomic.LoadInt64(&imSent), atomic.LoadInt64(&imFail))
	}

	return writeReport(cfg, *tag, allStats)
}

// grabStorm 是核心：C 个 goroutine 在同一时刻放闸，模拟「所有人同时点红包」。
func grabStorm(cfg *Config, httpCli *http.Client, plan *Plan, txID string, concurrency int, groupCheck bool) *benchStats {
	st := newBenchStats()
	url := cfg.ChatAPI + "/third/transaction/receive_stress"
	secret := os.Getenv("STRESS_TEST_SECRET")

	gate := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			recv := plan.Receivers[idx%len(plan.Receivers)]
			body, _ := json.Marshal(map[string]string{
				"transaction_id": txID,
				"receiver_id":    recv.UserID,
				"org_id":         plan.OrgID,
			})
			<-gate // 同时放闸，制造真正的瞬时洪峰
			start := time.Now()
			code, msg := doGrab(httpCli, url, secret, body, groupCheck)
			st.add(grabResult{latency: time.Since(start), errCode: code, errMsg: msg})
		}(i)
	}

	st.startedAt = time.Now()
	close(gate)
	wg.Wait()
	st.endedAt = time.Now()
	return st
}

func doGrab(cli *http.Client, url, secret string, body []byte, groupCheck bool) (int, string) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return -1, err.Error()
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("operationID", newOperationID("grab"))
	if secret != "" {
		req.Header.Set("X-Stress-Test-Secret", secret)
	}
	if groupCheck {
		req.Header.Set("X-Stress-Group-Check", "1")
	}
	resp, err := cli.Do(req)
	if err != nil {
		return -2, err.Error()
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out apiResp
	if err := json.Unmarshal(raw, &out); err != nil {
		return -3, truncate(string(raw), 120)
	}
	return out.ErrCode, out.ErrMsg
}

// driveIMTraffic 以固定速率往压测群发消息，制造 toRedis→toPush 的持续压力。
func driveIMTraffic(im *imClient, plan *Plan, rate int, stop <-chan struct{}, sent, fail *int64) {
	if len(plan.Receivers) == 0 {
		return
	}
	// 用一个令牌桶控速；并发发送者数量按速率给，避免单 goroutine 串行发不满速率。
	workers := rate / 20
	if workers < 4 {
		workers = 4
	}
	if workers > 64 {
		workers = 64
	}
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()
	jobs := make(chan int, rate)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				sender := plan.Receivers[n%len(plan.Receivers)]
				if err := im.sendGroupText(sender.ImID, plan.GroupID,
					fmt.Sprintf("stress msg #%d", n)); err != nil {
					atomic.AddInt64(fail, 1)
				} else {
					atomic.AddInt64(sent, 1)
				}
			}
		}()
	}

	n := 0
	for {
		select {
		case <-stop:
			close(jobs)
			wg.Wait()
			return
		case <-ticker.C:
			n++
			select {
			case jobs <- n:
			default:
				// 发送端已经跟不上目标速率，丢弃并计数，说明 API 侧先饱和了
				atomic.AddInt64(fail, 1)
			}
		}
	}
}

func printStats(st *benchStats, packetCount int) {
	st.mu.Lock()
	defer st.mu.Unlock()

	lats := make([]time.Duration, 0, len(st.results))
	for _, r := range st.results {
		lats = append(lats, r.latency)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	wall := st.endedAt.Sub(st.startedAt)
	success := st.byCode[0]
	fmt.Printf("并发 %d，耗时 %s，吞吐 %.1f req/s\n",
		len(st.results), wall.Truncate(time.Millisecond),
		float64(len(st.results))/wall.Seconds())
	fmt.Printf("成功领取 %d（红包共 %d 份）\n", success, packetCount)
	if len(lats) > 0 {
		fmt.Printf("延迟 p50=%s p90=%s p99=%s max=%s\n",
			pct(lats, 50), pct(lats, 90), pct(lats, 99), lats[len(lats)-1].Truncate(time.Millisecond))
	}
	fmt.Println("错误码分布：")
	codes := make([]int, 0, len(st.byCode))
	for c := range st.byCode {
		codes = append(codes, c)
	}
	sort.Ints(codes)
	for _, c := range codes {
		fmt.Printf("  %6d : %5d  %s\n", c, st.byCode[c], describeCode(c, st.results))
	}
	if success > packetCount {
		fmt.Printf("!! 超发告警：成功数 %d > 红包份数 %d\n", success, packetCount)
	}
}

func describeCode(code int, results []grabResult) string {
	switch code {
	case 0:
		return "成功"
	case -1:
		return "构造请求失败"
	case -2:
		return "连接/超时失败（客户端侧）"
	case -3:
		return "响应非 JSON"
	}
	for _, r := range results {
		if r.errCode == code {
			return r.errMsg
		}
	}
	return ""
}

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := (len(sorted)*p)/100 - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i].Truncate(time.Millisecond)
}

func loadPlan(path string) (*Plan, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取压测计划失败（先跑 prepare）: %w", err)
	}
	p := &Plan{}
	if err := json.Unmarshal(buf, p); err != nil {
		return nil, err
	}
	return p, nil
}

// writeReport 把每轮的原始延迟写成 CSV，方便和改造后的结果做对比。
func writeReport(cfg *Config, tag string, all []*benchStats) error {
	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(cfg.OutDir, fmt.Sprintf("bench-%s-%s.csv", tag, time.Now().Format("0102-150405")))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fmt.Fprintln(f, "packet_index,latency_ms,err_code,err_msg")
	for i, st := range all {
		for _, r := range st.results {
			fmt.Fprintf(f, "%d,%.1f,%d,%q\n", i, float64(r.latency.Microseconds())/1000.0, r.errCode, r.errMsg)
		}
	}
	fmt.Printf("\n原始结果已写入 %s\n", path)
	return nil
}

var _ = rand.Int
