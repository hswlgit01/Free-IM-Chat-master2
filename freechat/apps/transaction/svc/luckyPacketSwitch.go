package svc

import (
	"os"
	"strings"
)

// 拼手气红包专用处理路径的开关。
//
// 【背景】processWithReservation 里那段「仅对拼手气红包应用优化流程」的代码
// （幂等检查 → 先确认预留再写库 → 写库带 3 次指数退避重试 → 失败走补偿）
// 因为 isLuckyPacket 的时序缺陷，从来没有真正执行过——判断读的 rtCtx.Transaction
// 要到之后的 ValidateTransactionStatus 才被赋值，所以恒为 false。
//
// 时序修复之后，这段碰钱的代码会第一次真正生效。它本身没有线上运行记录，
// 所以留一个环境变量开关：万一上线后发现异常，改环境变量重启即可退回旧行为，
// 不需要回滚发版。
//
//	LUCKY_PACKET_PATH=off   → 退回修复前的行为（拼手气红包走通用分支）
//	其它值或不设置          → 启用（默认）
//
// 稳定运行一段时间后，这个开关和本文件可以一起删掉。
func luckyPacketPathEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("LUCKY_PACKET_PATH")))
	return v != "off" && v != "0" && v != "false"
}
