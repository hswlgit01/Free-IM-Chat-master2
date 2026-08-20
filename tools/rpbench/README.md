# 红包压测工具 rpbench

用于复现并量化「抢红包打爆 MongoDB / 撑爆 IM 消息队列」问题，以及验证优化前后的差异。

## 为什么要有它

红包的性能问题只在**瞬时洪峰**下暴露：单个请求跑得飞快，几百人同时抢就雪崩。
没有可重复的压测就没法判断某个改动到底有没有用，所以每一轮优化都必须跑同一套场景做前后对比。

## 构建与部署

压测器必须跑在**被测服务器本机**——测试服只对外开放 80/22，
chat-api(10008) 和 openim-api(10002) 都不可从外网直达。

```bash
# 本机交叉编译
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o rpbench-linux-amd64 .
scp rpbench-linux-amd64 root@<server>:/root/rpbench
```

## 前置条件

chat 服务需要打开压测开关（否则 `/third/transaction/receive_stress` 返回 403）：

```
STRESS_TEST_RECEIVE=1
STRESS_TEST_SECRET=<任意口令>   # 可选，设了就要带 X-Stress-Test-Secret 头
```

测试服上用 `/root/stress-backup/recreate-chat.sh on|off` 一键开关，
该脚本用完全相同的镜像/挂载/网络重建容器，只多加这两个环境变量。

## 用法

```bash
# 1. 造数据：选用户池、建压测群、造发送者并充值
./rpbench prepare -receivers 5000

# 2. 采指标（后台跑，与压测窗口对齐）
setsid nohup ./rpbench monitor -tag base -interval 3s -duration 7m </dev/null >mon.log 2>&1 &

# 3. 打压力
STRESS_TEST_SECRET=xxx ./rpbench bench -tag base -c 300 -count 200 -amount 200

# 4. 一致性校验（性能优化不能把账算错）
./rpbench verify
```

`bench` 关键参数：

| 参数 | 含义 |
|---|---|
| `-c` | 抢红包并发数，所有 goroutine 同一时刻放闸 |
| `-count` | 红包拆成几份 |
| `-amount` | 红包总金额 |
| `-type` | 2=普通红包 3=拼手气 |
| `-im-rate` | 并行的群消息速率（条/秒），用来叠加 IM 主链路压力 |

## prepare 都做了什么

1. 取组织和币种
2. 求「组织用户 ∩ 已开钱包」的交集作为抢包用户池
3. 用**嵌入式登录**造一个固定的压测发送者（`third_user_id=rpbench-sender-001`），
   给它开钱包、充值、清空支付密码、设成本组织里已有 `send_red_packet` 权限的角色
4. 建压测群并补 `group.org_id` 字段（OpenIM 原生建群接口不写这个字段，
   不补的话 `CheckGroupOrganizationRelation` 会判定「群不属于该组织」）
5. 把发送者的 chat token 一起写进 `plan.json`，bench 直接复用

现存的组织用户都没有 `third_user_id`（走的常规注册），换不了 token，
所以发送者必须新造一个，这只影响这一个账号，不动任何真实用户。

## 磁盘看门狗（重要）

OpenIM 的 `log.yml` 用的是 `storageLocation: ./logs/`（**相对路径**），
而服务进程的工作目录是二进制所在目录，所以日志实际落在

```
/im-server/_output/bin/platforms/linux/amd64/logs/openim-service-log.<date>
```

这个路径**没有挂载出来**，在宿主机上看不到，也没人清理。
叠加 `remainLogLevel: 6`(debug) 和 `rotationTime: 24`(一天才轮转一次)，
一轮压测能写 **23.7GB**，直接把根分区打满 →
mongod 分配不出 WiredTiger journal → `__wt_panic` → 进程 abort 退出。

首次压测就是这样把测试环境的 MongoDB 打进了重启循环。

因此 `monitor` 内置了看门狗：
- 剩余空间 < 12GB：截断容器 stdout 日志和上面那个隐藏的服务日志
- 剩余空间 < 6GB：中止采样并报错

## monitor 采集了什么

CSV 每行一个采样点：

| 列 | 说明 |
|---|---|
| `write_conflicts` | Mongo 多文档事务写冲突增量。**为 0 说明不是冲突重试风暴** |
| `insert/update/query/command` | opcounters 增量，用来算写放大 |
| `wt_write_avail` / `wt_read_avail` | WiredTiger 并发票据余量，**掉到个位数 = 写并发被打满** |
| `active_writers/readers` | globalLock 活跃客户端 |
| `lag_*` | Kafka 各消费组积压（toRedis/toMongo/toPush/toOfflinePush） |
| `cpu_*` | 各容器 CPU 百分比（4 核机器满载=400%） |
| `disk_free_gb` | 剩余磁盘，看门狗依据 |

注意：`docker stats --no-stream` 和 `kafka-consumer-groups.sh` 都是秒级慢调用，
实际采样间隔会被拉长到 9~15 秒，CPU 列存在采样错位，**只适合看量级不适合看瞬时值**。

## 已知会遇到的报错

- **`crypto/aes: invalid key size 0`**：服务端组织缓存的缺陷。
  `GetCache` 用 `json.Marshal` 序列化，而 `Organization.AesKeyBase64` 带 `json:"-"`，
  缓存一命中密钥就变成空串。prepare 会自动清一次组织缓存后重试。
- **`E11000 duplicate key ... group_member`**：重复跑 prepare 时成员已在群里，属正常，已忽略。
- **`errCode 10130 系统繁忙`**：并发下的业务拒绝，正是要观测的现象，不是工具的问题。
