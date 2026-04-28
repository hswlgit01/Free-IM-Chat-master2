package svc

import "testing"

func TestNicknameCanUseAdminSubstringRule(t *testing.T) {
	tests := []struct {
		name      string
		adminNick string
		want      bool
	}{
		{name: "short ascii admin nick", adminNick: "zz", want: false},
		{name: "three ascii admin nick", adminNick: "admin", want: true},
		{name: "short chinese admin nick", adminNick: "李伟", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nicknameCanUseAdminSubstringRule(tt.adminNick); got != tt.want {
				t.Fatalf("nicknameCanUseAdminSubstringRule(%q) = %v, want %v", tt.adminNick, got, tt.want)
			}
		})
	}
}

func TestShortAsciiAdminNickOnlyBlocksExactMatch(t *testing.T) {
	if !nicknameEqualFold("zz", "ZZ") {
		t.Fatal("short ascii admin nick should still block exact case-insensitive match")
	}
	if nicknameCanUseAdminSubstringRule("zz") && nicknameContainsFold("zzmax", "zz") {
		t.Fatal("short ascii admin nick should not block containing nickname like zzmax")
	}
}
