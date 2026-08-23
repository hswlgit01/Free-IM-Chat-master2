package chat

import "testing"

func TestValidateRTCIdentity(t *testing.T) {
	tests := []struct {
		name          string
		authenticated string
		requested     string
		want          string
		wantErr       bool
	}{
		{name: "matching legacy request", authenticated: "im-user-1", requested: "im-user-1", want: "im-user-1"},
		{name: "server derives identity", authenticated: "im-user-1", want: "im-user-1"},
		{name: "reject impersonation", authenticated: "im-user-1", requested: "im-user-2", wantErr: true},
		{name: "reject empty authenticated identity", requested: "im-user-1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateRTCIdentity(tt.authenticated, tt.requested)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateRTCIdentity() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("validateRTCIdentity() = %q, want %q", got, tt.want)
			}
		})
	}
}
