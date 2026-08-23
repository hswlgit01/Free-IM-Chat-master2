package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRTCConfigFromEnvironment(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "chat-rpc-chat.yml")
	// Deliberately omit rtc: to cover existing mounted configurations created
	// before TRTC support was added.
	configYAML := []byte("allowRegister: true\n")
	if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	const prefix = "TEST_CHAT_RPC"
	t.Setenv(prefix+"_RTC_PROVIDER", "trtc")
	t.Setenv(prefix+"_RTC_TOKENTTLSECONDS", "7200")
	t.Setenv(prefix+"_RTC_TRTC_SDKAPPID", "12345678")
	t.Setenv(prefix+"_RTC_TRTC_SECRETKEY", "test-only-secret")

	var cfg Chat
	if err := loadConfig(configPath, prefix, &cfg); err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.RTC.Provider != "trtc" {
		t.Fatalf("provider = %q, want trtc", cfg.RTC.Provider)
	}
	if cfg.RTC.TokenTTLSeconds != 7200 {
		t.Fatalf("token TTL = %d, want 7200", cfg.RTC.TokenTTLSeconds)
	}
	if cfg.RTC.TRTC.SDKAppID != 12345678 {
		t.Fatalf("SDKAppID = %d, want 12345678", cfg.RTC.TRTC.SDKAppID)
	}
	if cfg.RTC.TRTC.SecretKey != "test-only-secret" {
		t.Fatal("SDKSecretKey was not loaded from the environment")
	}
}
