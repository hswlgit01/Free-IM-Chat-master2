package organization

import "testing"

func TestConstantTimeSecretEqual(t *testing.T) {
	tests := []struct {
		name               string
		expected, provided string
		want               bool
	}{
		{name: "equal", expected: "shared-secret", provided: "shared-secret", want: true},
		{name: "different", expected: "shared-secret", provided: "wrong"},
		{name: "missing provided", expected: "shared-secret"},
		{name: "missing configured", provided: "shared-secret"},
		{name: "both empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := constantTimeSecretEqual(tt.expected, tt.provided); got != tt.want {
				t.Fatalf("constantTimeSecretEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}
