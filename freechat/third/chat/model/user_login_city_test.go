package model

import "testing"

func TestFirstKnownLoginCity(t *testing.T) {
	resolve := func(ip string) string {
		return map[string]string{
			"old-public": "上海市",
			"new-public": "北京市",
		}[ip]
	}
	tests := []struct {
		name    string
		records []*UserLoginRecord
		city    string
		ip      string
	}{
		{name: "empty"},
		{
			name: "newest resolvable wins",
			records: []*UserLoginRecord{
				{IP: "new-public"},
				{IP: "old-public"},
			},
			city: "北京市",
			ip:   "new-public",
		},
		{
			name: "latest unknown falls back to older public record",
			records: []*UserLoginRecord{
				{IP: "private-or-invalid"},
				nil,
				{IP: "old-public"},
			},
			city: "上海市",
			ip:   "old-public",
		},
		{
			name:    "all unknown",
			records: []*UserLoginRecord{{IP: "unknown"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			city, ip := firstKnownLoginCity(tt.records, resolve)
			if city != tt.city || ip != tt.ip {
				t.Fatalf("got (%q, %q), want (%q, %q)", city, ip, tt.city, tt.ip)
			}
		})
	}
}

func TestCompareLoginCities(t *testing.T) {
	tests := []struct {
		name        string
		currentCity string
		boundCity   string
		allowed     bool
		reason      string
		wantErr     bool
	}{
		{
			name:        "same city allows a different ip",
			currentCity: "上海市",
			boundCity:   "上海市",
			allowed:     true,
			reason:      LoginCityReasonSameCity,
		},
		{
			name:        "different city rejected",
			currentCity: "北京市",
			boundCity:   "上海市",
			allowed:     false,
			reason:      LoginCityReasonCityMismatch,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := compareLoginCities(tt.currentCity, tt.boundCity)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if result.Allowed != tt.allowed || result.Reason != tt.reason {
				t.Fatalf("result = %+v, want allowed=%v reason=%q", result, tt.allowed, tt.reason)
			}
		})
	}
}
