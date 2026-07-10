package ip2regionUtils

import "testing"

func TestGetCityByIPRejectsUnknownAndPrivateAddresses(t *testing.T) {
	for _, ip := range []string{
		"",
		"not-an-ip",
		"127.0.0.1",
		"127.0.0.1:1234",
		"10.0.0.1",
		"192.168.1.1:8080",
		"[::1]:1234",
		"[fd00::1]:443",
	} {
		t.Run(ip, func(t *testing.T) {
			if city := GetCityByIP(ip); city != "" {
				t.Fatalf("GetCityByIP(%q) = %q, want empty", ip, city)
			}
		})
	}
}

func TestGetCityByIPResolvesObservedPublicAddresses(t *testing.T) {
	for _, ip := range []string{"218.93.220.22", "61.111.248.254"} {
		if city := GetCityByIP(ip); city == "" {
			t.Errorf("GetCityByIP(%q) returned empty location", ip)
		} else {
			t.Logf("GetCityByIP(%q) = %q", ip, city)
		}
	}
}
