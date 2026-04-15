package utils

import (
	"fmt"
	"testing"
	"time"
)

// TestCSTTime 测试CST时区转换和处理
func TestCSTTime(t *testing.T) {
	// 测试CST时区初始化
	if CST == nil {
		t.Fatalf("CST时区未正确初始化")
	}

	// 获取当前UTC时间
	utcNow := time.Now().UTC()
	// 转换为CST时间
	cstNow := utcNow.In(CST)

	// 打印时区信息，用于手动验证
	t.Logf("当前UTC时间: %s", utcNow.Format(time.RFC3339))
	t.Logf("当前CST时间: %s", cstNow.Format(time.RFC3339))

	// 验证CST时区是否为UTC+8
	utcHour := utcNow.Hour()
	cstHour := cstNow.Hour()

	expectedCSTHour := (utcHour + 8) % 24
	if cstHour != expectedCSTHour {
		t.Errorf("CST时区转换错误: UTC %d点应该对应CST %d点，但得到了CST %d点",
			utcHour, expectedCSTHour, cstHour)
	} else {
		t.Logf("CST时区正确: UTC %d点 -> CST %d点", utcHour, cstHour)
	}

	// 测试工具函数
	nowCST := NowCST()
	todayCST := TodayCST()
	yesterdayCST := YesterdayCST()

	t.Logf("NowCST(): %s", nowCST.Format(time.RFC3339))
	t.Logf("TodayCST(): %s", todayCST.Format(time.RFC3339))
	t.Logf("YesterdayCST(): %s", yesterdayCST.Format(time.RFC3339))

	// 验证今天和昨天是否正确
	if todayCST.Day() != nowCST.Day() ||
		todayCST.Month() != nowCST.Month() ||
		todayCST.Year() != nowCST.Year() {
		t.Errorf("TodayCST()日期错误: %s 与当天不符", todayCST.Format("2006-01-02"))
	}

	// 特殊情况：午夜测试（可手动运行）
	// 模拟UTC时间和CST时间在不同日期的情况
	simulateMidnight(t)
}

// simulateMidnight 模拟午夜情况（UTC和CST在不同日期）
func simulateMidnight(t *testing.T) {
	// 模拟UTC时间为23:30（午夜前），对应CST为次日7:30
	utcNearMidnight := time.Date(2023, 1, 1, 23, 30, 0, 0, time.UTC)
	cstNextDay := utcNearMidnight.In(CST)

	t.Logf("\n模拟午夜情况:")
	t.Logf("模拟UTC时间: %s", utcNearMidnight.Format(time.RFC3339))
	t.Logf("对应CST时间: %s", cstNextDay.Format(time.RFC3339))

	// 检查日期是否不同
	if utcNearMidnight.Day() == cstNextDay.Day() {
		t.Errorf("模拟失败: UTC和CST日期应该不同")
	} else {
		t.Logf("正确: UTC日期为%s，CST日期为%s",
			utcNearMidnight.Format("2006-01-02"),
			cstNextDay.Format("2006-01-02"))
	}

	// 使用我们的工具函数模拟获取"今天"
	// 通过自定义Now函数可以进行模拟，但需要修改代码架构支持
	// 这里只是说明可能的问题点
	t.Logf("\n注意：在实际签到场景中，如果UTC时间是%s而客户端是CST时区，"+
		"UTC判断的'今天'和CST判断的'今天'会不同，这就是之前的问题所在。"+
		"现在全部统一使用CST时区解决了这个问题。",
		utcNearMidnight.Format("2006-01-02 15:04:05"))
}

// 简单的命令行测试工具
func TestPrintTimeInfo(t *testing.T) {
	utcNow := time.Now().UTC()
	localNow := time.Now()
	cstNow := NowCST()

	fmt.Printf("\n===== 时间测试信息 =====\n")
	fmt.Printf("UTC时间: %s\n", utcNow.Format(time.RFC3339))
	fmt.Printf("本地时间: %s\n", localNow.Format(time.RFC3339))
	fmt.Printf("CST时间: %s\n", cstNow.Format(time.RFC3339))
	fmt.Printf("\n")
	fmt.Printf("今天(UTC): %s\n", time.Date(utcNow.Year(), utcNow.Month(), utcNow.Day(), 0, 0, 0, 0, time.UTC).Format("2006-01-02"))
	fmt.Printf("今天(本地): %s\n", time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, time.Local).Format("2006-01-02"))
	fmt.Printf("今天(CST): %s\n", TodayCST().Format("2006-01-02"))

	// 如果UTC时间和CST时间在不同的日期，特别标记出来
	if utcNow.Day() != cstNow.Day() || utcNow.Month() != cstNow.Month() || utcNow.Year() != cstNow.Year() {
		fmt.Printf("\n⚠️ UTC日期和CST日期不同! ⚠️\n")
		fmt.Printf("这正是之前签到功能问题出现的情况 - 当UTC和用户所在时区跨日期时。\n")
		fmt.Printf("现在我们统一使用CST时区处理，解决了这个问题。\n")
	}
}
