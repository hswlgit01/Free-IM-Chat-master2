package livestream

import (
	"context"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/openimsdk/chat/freechat/apps/livestream/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/netUtils"
	"github.com/openimsdk/tools/log"
	"github.com/robfig/cron/v3"
	"strings"
	"time"
)

// LivestreamCronJob 汇率获取定时任务
type LivestreamCronJob struct {
	cron *cron.Cron
}

// NewLivestreamCronJob 创建汇率获取定时任务
func NewLivestreamCronJob() *LivestreamCronJob {
	c := cron.New(cron.WithSeconds())
	return &LivestreamCronJob{
		cron: c,
	}
}

// Start 开始定时任务
func (e *LivestreamCronJob) Start() {
	// 立即执行一次
	ctx := context.Background()

	// 每10分钟执行一次
	//_, err := e.cron.AddFunc("0 */10 * * * *", func() {
	//	e.AutoRemoveLiveRoom(ctx)
	//})

	// 每分钟执行一次
	_, err := e.cron.AddFunc("0 * * * * *", func() {
		e.AutoRemoveLiveRoom(ctx)
	})
	if err != nil {
		log.ZError(ctx, "添加自动清除过期直播间定时任务失败", err)
		return
	}

	e.cron.Start()
	log.ZInfo(ctx, "自动清除过期直播间定时任务已启动")
}

// Stop 停止定时任务
func (e *LivestreamCronJob) Stop() {
	if e.cron != nil {
		e.cron.Stop()
	}
}

func (e *LivestreamCronJob) AutoRemoveLiveRoom(ctx context.Context) {
	config := plugin.ChatCfg()

	livestreamUrlDao := model.NewLivestreamUrlDao(plugin.RedisCli())
	livestreamDao := model.NewLivestreamStatisticsDao(plugin.MongoCli().GetDB())

	url, err := livestreamUrlDao.AutomaticallySearchUrl(context.Background(), plugin.ChatCfg().ChatRpcConfig.LiveKit.BackupUrls)
	if err != nil {
		log.ZError(ctx, "定时任务: 自动获取直播url数据失败", err)
		return
	}

	// 创建HTTP URL
	httpUrl := strings.Replace(url, "wss://", "https://", 1)
	httpUrl = strings.Replace(httpUrl, "ws://", "http://", 1)

	// 创建房间服务和入口点服务客户端
	roomService := lksdk.NewRoomServiceClient(
		httpUrl,
		config.ChatRpcConfig.LiveKit.Key,
		config.ChatRpcConfig.LiveKit.Secret,
	)

	rooms, err := roomService.ListRooms(context.Background(), &livekit.ListRoomsRequest{})
	if err != nil {
		log.ZError(ctx, "定时任务: 获取房间列表失败", err)
		return
	}

	existsRoomsList := make([]string, 0)
	for _, room := range rooms.Rooms {
		existsRoomsList = append(existsRoomsList, room.Name)
	}

	result, err := livestreamDao.UpdateStatusByNotInRoomName(context.Background(), existsRoomsList, model.LivestreamStatisticsStatusStop)
	if err != nil {
		log.ZError(ctx, "定时任务: 自动关闭直播间状态失败", err)
		return
	}

	log.ZInfo(ctx, "自动关闭直播间状态成功", "关闭直播间数量:", result.MatchedCount, "当前直播间数量:", len(existsRoomsList))
}

func (e *LivestreamCronJob) GetLivestreamUrl(ctx context.Context) {

	chatRpcCfg := &plugin.ChatCfg().ChatRpcConfig
	// 没有获取过值,获取值
	for _, url := range plugin.ChatCfg().ChatRpcConfig.LiveKit.BackupUrls {
		_, host, port, err := netUtils.ParseURL(url)
		if err != nil {
			log.ZError(ctx, "解析url失败", err, "url", url)
			continue
		}

		ok := netUtils.PingTCP(host, port, time.Second*2)
		if ok {
			chatRpcCfg.LiveKit.URL = url
			break
		}
	}

	if chatRpcCfg.LiveKit.URL == "" {
		log.ZError(ctx, "没有可用的直播url", nil)
		return
	}

	log.ZInfo(ctx, "直播url数据已更新", "selected url", plugin.ChatCfg().ChatRpcConfig.LiveKit.URL)

}
