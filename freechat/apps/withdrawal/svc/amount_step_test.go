package svc

import (
	"math"
	"testing"
)

// isMultipleOfStep 与 SubmitWithdrawal 中的步长校验保持同一套算法。
// 单独抽出来测，是因为这段逻辑的风险全在浮点精度上，值得锁死。
func isMultipleOfStep(amount, step float64) bool {
	if step <= 0 {
		return true // 0 表示不限制
	}
	amountCents := int64(math.Round(amount * 100))
	stepCents := int64(math.Round(step * 100))
	if stepCents <= 0 {
		return true
	}
	return amountCents%stepCents == 0
}

func TestAmountStep(t *testing.T) {
	cases := []struct {
		name   string
		amount float64
		step   float64
		want   bool
	}{
		// 「只能整百提、200 起步」下的典型值（下限由 MinAmount 单独校验，这里只看步长）
		{"200 整百", 200, 100, true},
		{"300 整百", 300, 100, true},
		{"400 整百", 400, 100, true},
		{"1000 整百", 1000, 100, true},

		// 需求明确要拒绝的：整十但非整百
		{"250 整十非整百", 250, 100, false},
		{"210 整十非整百", 210, 100, false},
		{"990 整十非整百", 990, 100, false},

		// 带小数、零头
		{"200.5 有零头", 200.5, 100, false},
		{"200.01 有零头", 200.01, 100, false},

		// 常见整百金额（这些值 float64 能精确表示，math.Mod 也能正确处理，
		// 列在这里是作为回归基线）
		{"700", 700.00, 100, true},
		{"1100", 1100.00, 100, true},
		{"2900", 2900.00, 100, true},

		// 表示误差归一化：客户端若由计算得出金额，可能传来带极小误差的值。
		// 用户本意是 300，四舍五入到分之后应当判为合法。
		{"表示误差 300.0000000000001", 300.0000000000001, 100, true},
		{"表示误差 299.9999999999999", 299.9999999999999, 100, true},

		// 步长为 0 表示不限制，任何金额都放行
		{"步长0不限制-250", 250, 0, true},
		{"步长0不限制-5", 5, 0, true},

		// 其它步长配置也应工作
		{"步长10-250", 250, 10, true},
		{"步长10-255", 255, 10, false},
		{"步长50-250", 250, 50, true},
		{"步长50-275", 275, 50, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isMultipleOfStep(c.amount, c.step); got != c.want {
				t.Errorf("金额 %.2f 步长 %.2f：得到 %v，期望 %v", c.amount, c.step, got, c.want)
			}
		})
	}
}

// TestNaiveModRejectsTinyRepresentationError 记录「为什么不直接用 math.Mod」。
//
// 对整数金额两种写法等价；差别出现在金额带极小表示误差时：
// math.Mod 会把用户本意合法的金额拒掉，先归一化到分则不会。
func TestNaiveModRejectsTinyRepresentationError(t *testing.T) {
	const amount = 300.0000000000001
	const step = 100.0

	if math.Mod(amount, step) == 0 {
		t.Skip("当前平台上该值 math.Mod 恰好为 0，本用例不适用")
	}
	if !isMultipleOfStep(amount, step) {
		t.Errorf("金额 %v 归一化到分后应为合法整百，却被拒绝", amount)
	}
	t.Logf("math.Mod(%v, %v) = %v（非 0，会误拒）；当前实现判定为合法",
		amount, step, math.Mod(amount, step))
}

// TestEffectiveWithdrawDefaults 锁定「整百 + 200 起步」的系统默认兜底：
// 后台未配置（或配置低于默认值）时按 200 起步 / 整百步长校验，配置更高值则保留。
func TestEffectiveWithdrawDefaults(t *testing.T) {
	minCases := []struct {
		name       string
		configured float64
		want       float64
	}{
		{"未配置默认5兜底到200", 5, 200},
		{"0兜底到200", 0, 200},
		{"配50被提到200", 50, 200},
		{"配200保持200", 200, 200},
		{"配300保留300", 300, 300},
	}
	for _, c := range minCases {
		if got := effectiveWithdrawMinAmount(c.configured); got != c.want {
			t.Errorf("最低金额[%s]：configured=%.2f 得到 %.2f，期望 %.2f", c.name, c.configured, got, c.want)
		}
	}

	stepCases := []struct {
		name       string
		configured float64
		want       float64
	}{
		{"未配置0兜底到100", 0, 100},
		{"负数兜底到100", -1, 100},
		{"配100保持100", 100, 100},
		{"配50保留50（整十）", 50, 50},
		{"配200保留200（整二百）", 200, 200},
	}
	for _, c := range stepCases {
		if got := effectiveWithdrawAmountStep(c.configured); got != c.want {
			t.Errorf("步长[%s]：configured=%.2f 得到 %.2f，期望 %.2f", c.name, c.configured, got, c.want)
		}
	}
}
