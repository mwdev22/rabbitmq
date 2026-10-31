package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const testQueue = "billing.orders"

func delivery(a *fakeAcker, tag uint64, id string, retry int) amqp.Delivery {
	h := amqp.Table{HeaderID: id}
	if retry > 0 {
		h[HeaderRetryCount] = int32(retry)
	}
	return amqp.Delivery{
		Acknowledger: a,
		DeliveryTag:  tag,
		Headers:      h,
		Exchange:     "orders",
		RoutingKey:   "order.created",
		Body:         []byte(`{}`),
	}
}

func runUntilIdle(t *testing.T, c *Consumer, src *fakeSource) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()
	select {
	case <-src.exhausted:
		cancel()
		return <-errc
	case err := <-errc:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not finish")
		return nil
	}
}

func TestConsumerRun(t *testing.T) {
	boom := errors.New("db down")
	type pubWant struct {
		key   string
		retry int64
	}
	tests := []struct {
		name      string
		workers   int
		ds        func(a *fakeAcker) []amqp.Delivery
		handler   func(call int) error
		pubErrs   []error
		wantCalls int
		wantAcks  []ackRecord
		wantPub   []pubWant
		wantErr   error
	}{
		{
			name:      "#1 - OK - success acks",
			ds:        func(a *fakeAcker) []amqp.Delivery { return []amqp.Delivery{delivery(a, 1, "m1", 0)} },
			handler:   func(int) error { return nil },
			wantCalls: 1,
			wantAcks:  []ackRecord{{tag: 1, ack: true}},
		},
		{
			name: "#2 - OK - transient then success → retry queue, both acked",
			ds: func(a *fakeAcker) []amqp.Delivery {
				return []amqp.Delivery{delivery(a, 1, "m1", 0), delivery(a, 2, "m1", 1)}
			},
			handler: func(call int) error {
				if call == 1 {
					return boom
				}
				return nil
			},
			wantCalls: 2,
			wantAcks:  []ackRecord{{tag: 1, ack: true}, {tag: 2, ack: true}},
			wantPub:   []pubWant{{testQueue + ".retry", 1}},
		},
		{
			name:      "#3 - OK - transient exhausted → dlq + ack",
			ds:        func(a *fakeAcker) []amqp.Delivery { return []amqp.Delivery{delivery(a, 1, "m1", 3)} },
			handler:   func(int) error { return boom },
			wantCalls: 1,
			wantAcks:  []ackRecord{{tag: 1, ack: true}},
			wantPub:   []pubWant{{testQueue + ".dlq", 3}},
		},
		{
			name:      "#4 - OK - permanent → dlq without retry",
			ds:        func(a *fakeAcker) []amqp.Delivery { return []amqp.Delivery{delivery(a, 1, "m1", 0)} },
			handler:   func(int) error { return Permanent(boom) },
			wantCalls: 1,
			wantAcks:  []ackRecord{{tag: 1, ack: true}},
			wantPub:   []pubWant{{testQueue + ".dlq", 0}},
		},
		{
			name:      "#5 - FAIL - dlq publish fails → nack requeue, ErrDLQ",
			ds:        func(a *fakeAcker) []amqp.Delivery { return []amqp.Delivery{delivery(a, 1, "m1", 0)} },
			handler:   func(int) error { return Permanent(boom) },
			pubErrs:   []error{errors.New("confirm timeout")},
			wantCalls: 1,
			wantAcks:  []ackRecord{{tag: 1, requeue: true}},
			wantErr:   ErrDLQ,
		},
		{
			name:      "#6 - FAIL - retry publish fails → nack requeue, ErrDLQ",
			ds:        func(a *fakeAcker) []amqp.Delivery { return []amqp.Delivery{delivery(a, 1, "m1", 0)} },
			handler:   func(int) error { return boom },
			pubErrs:   []error{errors.New("confirm timeout")},
			wantCalls: 1,
			wantAcks:  []ackRecord{{tag: 1, requeue: true}},
			wantErr:   ErrDLQ,
		},
		{
			// dedupe on Message.ID is the caller's job, the lib hands over both copies
			name: "#7 - OK - same message delivered twice → handler called twice",
			ds: func(a *fakeAcker) []amqp.Delivery {
				return []amqp.Delivery{delivery(a, 1, "m1", 0), delivery(a, 2, "m1", 0)}
			},
			handler:   func(int) error { return nil },
			wantCalls: 2,
			wantAcks:  []ackRecord{{tag: 1, ack: true}, {tag: 2, ack: true}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acker := &fakeAcker{}
			cons := newFakeChannel(tt.ds(acker)...)
			close(cons.deliveries)
			pub := newFakeChannel()
			pub.publishErrs = tt.pubErrs
			src := newFakeSource([]*fakeChannel{pub}, cons)

			var calls atomic.Int32
			h := func(_ context.Context, m Message) error {
				if m.ID != "m1" {
					t.Errorf("Message.ID = %q, want m1", m.ID)
				}
				return tt.handler(int(calls.Add(1)))
			}
			workers := tt.workers
			if workers == 0 {
				workers = 1
			}
			c, err := newConsumer(src, h, WithQueue(testQueue), WithExchange("orders", ""), WithWorkers(workers))
			if err != nil {
				t.Fatalf("newConsumer: %v", err)
			}

			err = runUntilIdle(t, c, src)
			if !errors.Is(err, tt.wantErr) || (err != nil) != (tt.wantErr != nil) {
				t.Fatalf("Run err = %v, want %v", err, tt.wantErr)
			}
			if got := int(calls.Load()); got != tt.wantCalls {
				t.Errorf("handler calls = %d, want %d", got, tt.wantCalls)
			}
			if got := acker.snapshot(); fmt.Sprint(got) != fmt.Sprint(tt.wantAcks) {
				t.Errorf("acks = %v, want %v", got, tt.wantAcks)
			}
			gotPub := pub.snapshot()
			if len(gotPub) != len(tt.wantPub) {
				t.Fatalf("published %d, want %d", len(gotPub), len(tt.wantPub))
			}
			for i, w := range tt.wantPub {
				p := gotPub[i]
				if p.exchange != "" || p.key != w.key {
					t.Errorf("published to %q/%q, want default exchange/%q", p.exchange, p.key, w.key)
				}
				if p.msg.Headers[HeaderRetryCount] != w.retry {
					t.Errorf("x-retry-count = %v, want %d", p.msg.Headers[HeaderRetryCount], w.retry)
				}
				if p.msg.Headers[HeaderOriginalKey] != "order.created" || p.msg.Headers[HeaderID] != "m1" {
					t.Errorf("original headers not kept: %v", p.msg.Headers)
				}
				if w.key == testQueue+".dlq" && (p.msg.Headers[HeaderError] == nil || p.msg.Headers[HeaderOriginalQueue] != testQueue) {
					t.Errorf("dlq headers missing: %v", p.msg.Headers)
				}
			}
			if len(cons.declared) != 1 || cons.qos != 10 {
				t.Errorf("declared %d times, qos %d", len(cons.declared), cons.qos)
			}
		})
	}
}

func TestConsumerRunCancelMidHandler(t *testing.T) {
	acker := &fakeAcker{}
	cons := newFakeChannel(delivery(acker, 1, "m1", 0))
	src := newFakeSource([]*fakeChannel{newFakeChannel()}, cons)

	started, release := make(chan struct{}), make(chan struct{})
	var hctxErr error
	h := func(ctx context.Context, _ Message) error {
		close(started)
		<-release
		hctxErr = ctx.Err()
		return nil
	}
	c, err := newConsumer(src, h, WithQueue(testQueue))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()

	<-started
	cancel()
	close(release)

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Run err = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if hctxErr != nil {
		t.Errorf("handler ctx cancelled with parent: %v", hctxErr)
	}
	if got := acker.snapshot(); len(got) != 1 || !got[0].ack {
		t.Errorf("acks = %v, want one ack", got)
	}
	if !cons.cancelled || !cons.closed {
		t.Errorf("consumer cancelled=%v closed=%v, want both", cons.cancelled, cons.closed)
	}
}

func TestConsumerRunShutdownTimeout(t *testing.T) {
	acker := &fakeAcker{}
	cons := newFakeChannel(delivery(acker, 1, "m1", 0))
	src := newFakeSource([]*fakeChannel{newFakeChannel()}, cons)

	started := make(chan struct{})
	h := func(ctx context.Context, _ Message) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	c, err := newConsumer(src, h, WithQueue(testQueue), WithShutdownTimeout(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()
	<-started
	cancel()
	select {
	case <-errc:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not cancelled after the shutdown timeout")
	}
}

func TestConsumerRunResubscribes(t *testing.T) {
	acker := &fakeAcker{}
	first := newFakeChannel(delivery(acker, 1, "m1", 0))
	close(first.deliveries) // connection lost
	second := newFakeChannel(delivery(acker, 2, "m1", 0))
	close(second.deliveries)
	src := newFakeSource([]*fakeChannel{newFakeChannel()}, first, second)

	c, err := newConsumer(src, func(context.Context, Message) error { return nil }, WithQueue(testQueue))
	if err != nil {
		t.Fatal(err)
	}
	if err := runUntilIdle(t, c, src); err != nil {
		t.Fatalf("Run err = %v", err)
	}
	if got := acker.snapshot(); len(got) != 2 {
		t.Errorf("acks = %v, want 2", got)
	}
	for i, ch := range []*fakeChannel{first, second} {
		if len(ch.declared) != 1 || ch.qos != 10 {
			t.Errorf("channel %d: declared %d, qos %d; topology must be redeclared on resubscribe", i, len(ch.declared), ch.qos)
		}
	}
}

func TestConsumerRunWorkers(t *testing.T) {
	acker := &fakeAcker{}
	var ds []amqp.Delivery
	for i := range 20 {
		ds = append(ds, delivery(acker, uint64(i+1), "m", 0))
	}
	cons := newFakeChannel(ds...)
	close(cons.deliveries)
	src := newFakeSource([]*fakeChannel{newFakeChannel()}, cons)

	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	h := func(context.Context, Message) error {
		mu.Lock()
		inFlight++
		maxInFlight = max(maxInFlight, inFlight)
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return nil
	}
	c, err := newConsumer(src, h, WithQueue(testQueue), WithWorkers(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := runUntilIdle(t, c, src); err != nil {
		t.Fatalf("Run err = %v", err)
	}
	if got := len(acker.snapshot()); got != 20 {
		t.Errorf("acks = %d, want 20", got)
	}
	if maxInFlight < 2 || maxInFlight > 4 {
		t.Errorf("max in flight = %d, want 2..4", maxInFlight)
	}
}

func TestConsumerRetryBackoff(t *testing.T) {
	c, err := newConsumer(newFakeSource(nil), func(context.Context, Message) error { return nil },
		WithQueue(testQueue), WithBackoff(100*time.Millisecond, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	d := delivery(&fakeAcker{}, 1, "m1", 2)
	key, msg := c.reroute(d, toMessage(d, testQueue), errors.New("x"))
	if key != testQueue+".retry" {
		t.Fatalf("key = %q", key)
	}
	if msg.Expiration == "" {
		t.Fatal("expiration not set with WithBackoff")
	}
}

func TestNewConsumer(t *testing.T) {
	h := func(context.Context, Message) error { return nil }
	tests := []struct {
		name    string
		h       Handler
		opts    []ConsumerOption
		wantErr error
	}{
		{"#1 - OK", h, []ConsumerOption{WithQueue("q")}, nil},
		{"#2 - FAIL - missing handler", nil, []ConsumerOption{WithQueue("q")}, ErrMissingHandler},
		{"#3 - FAIL - missing queue", h, nil, ErrMissingQueue},
		{"#4 - FAIL - zero workers", h, []ConsumerOption{WithQueue("q"), WithWorkers(0)}, ErrInvalidOption},
		{"#5 - FAIL - zero prefetch", h, []ConsumerOption{WithQueue("q"), WithPrefetch(0)}, ErrInvalidOption},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newConsumer(newFakeSource(nil), tt.h, tt.opts...)
			if !errors.Is(err, tt.wantErr) || (err != nil) != (tt.wantErr != nil) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
	if _, err := NewConsumer(nil, h, WithQueue("q")); !errors.Is(err, ErrMissingConn) {
		t.Fatalf("err = %v, want ErrMissingConn", err)
	}
}
