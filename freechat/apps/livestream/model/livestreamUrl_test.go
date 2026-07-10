package model

import "testing"

func TestSelectPublicUrl(t *testing.T) {
	tests := []struct {
		name     string
		internal string
		public   string
		want     string
		wantErr  bool
	}{
		{name: "public client address", internal: "ws://127.0.0.1:7880", public: "wss://8.148.66.77", want: "wss://8.148.66.77"},
		{name: "fallback for local development", internal: "ws://localhost:7880", want: "ws://localhost:7880"},
		{name: "invalid public address", internal: "ws://127.0.0.1:7880", public: "://bad", wantErr: true},
		{name: "reject HTTP scheme", internal: "ws://127.0.0.1:7880", public: "https://rtc.example.com", wantErr: true},
		{name: "reject localhost", internal: "ws://127.0.0.1:7880", public: "ws://localhost:7880", wantErr: true},
		{name: "reject loopback", internal: "ws://127.0.0.1:7880", public: "ws://127.0.0.1:7880", wantErr: true},
		{name: "reject wildcard", internal: "ws://127.0.0.1:7880", public: "ws://0.0.0.0:7880", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selectPublicUrl(tt.internal, tt.public)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("result = %q, want %q", got, tt.want)
			}
		})
	}
}
