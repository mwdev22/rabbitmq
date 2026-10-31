package rabbitmq

import (
	"errors"
	"fmt"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPermanent(t *testing.T) {
	base := errors.New("bad payload")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"#1 - OK - permanent", Permanent(base), true},
		{"#2 - OK - wrapped permanent", fmt.Errorf("handle: %w", Permanent(base)), true},
		{"#3 - FAIL - plain error", base, false},
		{"#4 - FAIL - nil", Permanent(nil), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanent(tt.err); got != tt.want {
				t.Fatalf("IsPermanent = %v, want %v", got, tt.want)
			}
		})
	}
	if !errors.Is(Permanent(base), base) {
		t.Fatal("Permanent must unwrap to the original error")
	}
}

func TestToMessage(t *testing.T) {
	tests := []struct {
		name      string
		d         amqp.Delivery
		wantID    string
		wantRetry int
	}{
		{"#1 - OK - id header wins", amqp.Delivery{MessageId: "mid", Headers: amqp.Table{HeaderID: "hid"}}, "hid", 0},
		{"#2 - OK - falls back to message id", amqp.Delivery{MessageId: "mid"}, "mid", 0},
		{"#3 - OK - int32 retry count", amqp.Delivery{Headers: amqp.Table{HeaderRetryCount: int32(2)}}, "", 2},
		{"#4 - OK - int64 retry count", amqp.Delivery{Headers: amqp.Table{HeaderRetryCount: int64(3)}}, "", 3},
		{"#5 - OK - string retry count", amqp.Delivery{Headers: amqp.Table{HeaderRetryCount: "4"}}, "", 4},
		{"#6 - FAIL - garbage retry count is 0", amqp.Delivery{Headers: amqp.Table{HeaderRetryCount: "x"}}, "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := toMessage(tt.d, "q")
			if m.ID != tt.wantID {
				t.Errorf("ID = %q, want %q", m.ID, tt.wantID)
			}
			if m.RetryCount != tt.wantRetry {
				t.Errorf("RetryCount = %d, want %d", m.RetryCount, tt.wantRetry)
			}
			if m.Queue != "q" {
				t.Errorf("Queue = %q, want q", m.Queue)
			}
		})
	}
}
