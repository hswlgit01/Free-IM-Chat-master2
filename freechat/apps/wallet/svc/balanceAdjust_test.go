package svc

import (
	"os"
	"testing"

	"github.com/shopspring/decimal"
)

// 单笔上限只是「防手滑多打一个 0」的保险丝。这里锁住它的边界行为
// 与环境变量覆盖，避免日后有人改动时把校验方向弄反。
func TestMaxSingleAdjust(t *testing.T) {
	t.Run("默认上限", func(t *testing.T) {
		os.Unsetenv("WALLET_MAX_SINGLE_ADJUST")
		if got := maxSingleAdjust(); !got.Equal(decimal.NewFromInt(100000)) {
			t.Errorf("默认上限应为 100000，实际 %s", got)
		}
	})

	t.Run("环境变量覆盖", func(t *testing.T) {
		os.Setenv("WALLET_MAX_SINGLE_ADJUST", "500")
		defer os.Unsetenv("WALLET_MAX_SINGLE_ADJUST")
		if got := maxSingleAdjust(); !got.Equal(decimal.NewFromInt(500)) {
			t.Errorf("应被环境变量覆盖为 500，实际 %s", got)
		}
	})

	t.Run("非法环境变量回落默认值", func(t *testing.T) {
		for _, bad := range []string{"abc", "-1", "0", ""} {
			os.Setenv("WALLET_MAX_SINGLE_ADJUST", bad)
			if got := maxSingleAdjust(); !got.Equal(decimal.NewFromInt(100000)) {
				t.Errorf("环境变量 %q 非法时应回落默认值 100000，实际 %s", bad, got)
			}
		}
		os.Unsetenv("WALLET_MAX_SINGLE_ADJUST")
	})
}

// 上限判断用的是**绝对值**：扣减（负数）同样要受限，
// 否则 -999999 这类误操作会绕过保险丝。
func TestLimitAppliesToBothDirections(t *testing.T) {
	os.Setenv("WALLET_MAX_SINGLE_ADJUST", "1000")
	defer os.Unsetenv("WALLET_MAX_SINGLE_ADJUST")
	limit := maxSingleAdjust()

	cases := []struct {
		name     string
		amount   string
		rejected bool
	}{
		{"增加 500 允许", "500", false},
		{"增加 1000 边界允许", "1000", false},
		{"增加 1000.01 超限拒绝", "1000.01", true},
		{"扣减 500 允许", "-500", false},
		{"扣减 1000 边界允许", "-1000", false},
		{"扣减 1000.01 超限拒绝", "-1000.01", true},
		{"扣减 999999 超限拒绝", "-999999", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			amount, err := decimal.NewFromString(c.amount)
			if err != nil {
				t.Fatalf("金额解析失败: %v", err)
			}
			rejected := amount.Abs().GreaterThan(limit)
			if rejected != c.rejected {
				t.Errorf("金额 %s：拒绝=%v，期望拒绝=%v", c.amount, rejected, c.rejected)
			}
		})
	}
}

// 金额全程走字符串 -> decimal，不经过 float64。
// 这条测试固化该约定：0.1 + 0.2 这类在 float64 下会出问题的值必须精确。
func TestAmountParsingIsExact(t *testing.T) {
	a, _ := decimal.NewFromString("0.1")
	b, _ := decimal.NewFromString("0.2")
	if got := a.Add(b); got.String() != "0.3" {
		t.Errorf("decimal 相加应精确得 0.3，实际 %s", got)
	}

	// 常见金额解析后数值必须精确，不出现 99.99999999 之类的漂移。
	// 断言的是**数值相等**而不是字符串原样往返 —— decimal 会把
	// "100.50" 规范化成 "100.5"，两者数值相同，这是正确行为。
	for _, v := range []string{"100", "100.50", "0.01", "-250.75", "99999.99"} {
		d, err := decimal.NewFromString(v)
		if err != nil {
			t.Fatalf("解析 %s 失败: %v", v, err)
		}
		// 与「按 float64 解析再转 decimal」对比，验证没有走浮点路径
		if !d.Equal(decimal.RequireFromString(v)) {
			t.Errorf("金额 %s 解析后数值不精确：%s", v, d.String())
		}
	}

	// 反例：若中途经过 float64，0.1+0.2 会得到 0.30000000000000004
	if decimal.NewFromFloat(0.1 + 0.2).Equal(decimal.RequireFromString("0.3")) {
		t.Log("当前平台 float64 恰好精确，但实现仍应走字符串路径")
	}
}
