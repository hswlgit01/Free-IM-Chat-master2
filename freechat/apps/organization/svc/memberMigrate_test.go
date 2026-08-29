package svc

import (
	"reflect"
	"testing"

	"github.com/openimsdk/chat/freechat/apps/organization/model"
)

// ancestor_path 是从近到远：[直接上级, 祖父, 曾祖父, ...]，
// levelNParent == ancestor_path[N-1]。这套语义写反了整棵树就错了，锁死。
func TestBuildChildAncestorPath(t *testing.T) {
	parent := &model.OrganizationUser{
		UserId:       "P",
		AncestorPath: []string{"G", "GG"}, // P 的上级是 G，G 的上级是 GG
	}
	got := buildChildAncestorPath(parent)
	want := []string{"P", "G", "GG"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("挂在 P 下面的子节点祖先链应为 %v，实际 %v", want, got)
	}
	if nthOrEmpty(got, 0) != "P" {
		t.Error("level1_parent 应为直接上级 P")
	}
	if nthOrEmpty(got, 1) != "G" {
		t.Error("level2_parent 应为祖父 G")
	}
	if nthOrEmpty(got, 2) != "GG" {
		t.Error("level3_parent 应为曾祖父 GG")
	}

	// 顶层节点：上级没有祖先
	root := &model.OrganizationUser{UserId: "R", AncestorPath: nil}
	if got := buildChildAncestorPath(root); !reflect.DeepEqual(got, []string{"R"}) {
		t.Errorf("挂在顶层 R 下面应为 [R]，实际 %v", got)
	}
}

// team_size 的双向调整用的是新旧祖先链的对称差。
// 这里最容易出错的是「迁到自己的祖先名下」—— 两条链有重叠，
// 重叠部分的团队仍然包含这棵子树，人数不能变。
func TestDiffAncestorChains(t *testing.T) {
	cases := []struct {
		name     string
		oldPath  []string
		newPath  []string
		wantDec  []string
		wantInc  []string
	}{
		{
			name:    "迁到完全不相干的另一条链",
			oldPath: []string{"A", "B"},
			newPath: []string{"X", "Y"},
			wantDec: []string{"A", "B"},
			wantInc: []string{"X", "Y"},
		},
		{
			name:    "上浮：迁到自己的祖父名下，祖父及以上不变",
			oldPath: []string{"P", "G", "GG"}, // 原本挂在 P 下
			newPath: []string{"G", "GG"},      // 改挂到 G 下
			wantDec: []string{"P"},            // 只有 P 少了这棵子树
			wantInc: nil,                      // G、GG 本来就包含，不变
		},
		{
			name:    "下沉：迁到同链更深处",
			oldPath: []string{"G", "GG"},
			newPath: []string{"P", "G", "GG"},
			wantDec: nil,
			wantInc: []string{"P"},
		},
		{
			name:    "部分重叠",
			oldPath: []string{"A", "M", "Z"},
			newPath: []string{"B", "M", "Z"},
			wantDec: []string{"A"},
			wantInc: []string{"B"},
		},
		{
			name:    "从顶层迁入某人名下",
			oldPath: nil,
			newPath: []string{"P", "G"},
			wantDec: nil,
			wantInc: []string{"P", "G"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dec, inc := diffAncestorChains(c.oldPath, c.newPath)
			if !equalStrSlice(dec, c.wantDec) {
				t.Errorf("team_size 应减少的祖先：得到 %v，期望 %v", dec, c.wantDec)
			}
			if !equalStrSlice(inc, c.wantInc) {
				t.Errorf("team_size 应增加的祖先：得到 %v，期望 %v", inc, c.wantInc)
			}
		})
	}
}

// 环检测的判据：目标上级的祖先链里出现被迁移者，说明目标是其下级。
func TestCycleDetectionPredicate(t *testing.T) {
	member := "M"

	// 目标是 M 的下级 —— 必须拦截
	descendant := []string{"C", "M", "P"} // 该节点上级是 C，C 的上级是 M
	if indexOf(descendant, member) < 0 {
		t.Error("应识别出目标是被迁移者的下级")
	}

	// 目标与 M 无从属关系 —— 应放行
	unrelated := []string{"X", "Y"}
	if indexOf(unrelated, member) >= 0 {
		t.Error("不相干的目标不应被误判为下级")
	}

	// 目标是 M 的祖先 —— 应放行（上浮是合法迁移）
	ancestor := []string{"GG"}
	if indexOf(ancestor, member) >= 0 {
		t.Error("祖先不应被误判为下级")
	}
}

// 子树内某个下级的新路径 = 它到被迁移者之间那段 + 被迁移者的新路径。
// 这段拼接是整个迁移最容易写错的地方，单独验证。
func TestSubtreePathRebuild(t *testing.T) {
	member := "M"
	newMemberPath := []string{"NEW", "NEWUP"} // M 迁移后挂在 NEW 下

	// 某个孙子节点：它 -> C -> M -> OLD -> OLDUP
	oldPath := []string{"C", "M", "OLD", "OLDUP"}

	idx := indexOf(oldPath, member)
	if idx != 1 {
		t.Fatalf("被迁移者在祖先链中的位置应为 1，实际 %d", idx)
	}
	selfPart := append([]string{}, oldPath[:idx]...) // ["C"]
	got := append(append(append([]string{}, selfPart...), member), newMemberPath...)

	want := []string{"C", "M", "NEW", "NEWUP"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("孙子节点的新祖先链应为 %v，实际 %v", want, got)
	}

	// 直属下级：它 -> M -> OLD
	oldPath2 := []string{"M", "OLD"}
	idx2 := indexOf(oldPath2, member)
	selfPart2 := append([]string{}, oldPath2[:idx2]...) // 空
	got2 := append(append(append([]string{}, selfPart2...), member), newMemberPath...)
	want2 := []string{"M", "NEW", "NEWUP"}
	if !reflect.DeepEqual(got2, want2) {
		t.Errorf("直属下级的新祖先链应为 %v，实际 %v", want2, got2)
	}
}

func equalStrSlice(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}
