package rabbitmq

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	base, max := 100*time.Millisecond, time.Second
	tests := []struct {
		name     string
		attempt  int
		min, max time.Duration
	}{
		{"#1 - OK - first attempt", 0, 50 * time.Millisecond, 100 * time.Millisecond},
		{"#2 - OK - doubles", 2, 200 * time.Millisecond, 400 * time.Millisecond},
		{"#3 - OK - capped", 20, 500 * time.Millisecond, time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 50 {
				d := backoff(tt.attempt, base, max)
				if d < tt.min || d > tt.max {
					t.Fatalf("backoff = %v, want in [%v, %v]", d, tt.min, tt.max)
				}
			}
		})
	}
	if d := backoff(3, 0, max); d != 0 {
		t.Fatalf("zero base must disable backoff, got %v", d)
	}
}
