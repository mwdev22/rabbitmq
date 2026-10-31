package rabbitmq

import (
	"testing"
	"time"
)

func TestTopologyQueues(t *testing.T) {
	top := newTopology(ConsumerOptions{
		Queue:    "billing.orders",
		RetryTTL: 5 * time.Second,
	})
	got := map[string]queueSpec{}
	for _, q := range top.queues() {
		got[q.name] = q
	}
	tests := []struct {
		name  string
		queue string
		arg   string
		want  any
	}{
		{"#1 - OK - main dead-letters to dlq", "billing.orders", "x-dead-letter-routing-key", "billing.orders.dlq"},
		{"#2 - OK - main uses default exchange", "billing.orders", "x-dead-letter-exchange", ""},
		{"#3 - OK - retry ttl", "billing.orders.retry", "x-message-ttl", int64(5000)},
		{"#4 - OK - retry goes back to main queue only", "billing.orders.retry", "x-dead-letter-routing-key", "billing.orders"},
		{"#5 - OK - dlq has no args", "billing.orders.dlq", "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, ok := got[tt.queue]
			if !ok {
				t.Fatalf("queue %q not declared", tt.queue)
			}
			if tt.arg == "" {
				if len(q.args) != 0 {
					t.Fatalf("args = %v, want none", q.args)
				}
				return
			}
			if q.args[tt.arg] != tt.want {
				t.Fatalf("%s = %#v, want %#v", tt.arg, q.args[tt.arg], tt.want)
			}
		})
	}
}
