package rabbitmq

import (
	"context"
	"errors"
	"testing"
)

func TestError(t *testing.T) {
	brokerDown := errors.New("broker down")

	runWith := func(t *testing.T, herr error) error {
		pub := newFakeChannel()
		pub.publishErrs = []error{brokerDown}
		src := newFakeSource([]*fakeChannel{pub}, newFakeChannel(delivery(&fakeAcker{}, 1, "m-1", 0)))
		h := func(context.Context, Message) error { return herr }
		c, err := newConsumer(src, h, WithQueue(testQueue))
		if err != nil {
			t.Fatal(err)
		}
		return runUntilIdle(t, c, src)
	}

	tests := []struct {
		name     string
		call     func(t *testing.T) error
		wantOp   string
		wantIs   error
		wantText string
	}{
		{
			name:     "#1 - FAIL - producer config",
			call:     func(*testing.T) error { _, err := NewProducer(nil); return err },
			wantOp:   OpConfig,
			wantIs:   ErrMissingConn,
			wantText: "rabbitmq: config: missing connection",
		},
		{
			name: "#2 - FAIL - consumer config",
			call: func(*testing.T) error {
				_, err := newConsumer(newFakeSource(nil), func(context.Context, Message) error { return nil })
				return err
			},
			wantOp:   OpConfig,
			wantIs:   ErrMissingQueue,
			wantText: "rabbitmq: config: missing queue",
		},
		{
			name: "#3 - FAIL - publish",
			call: func(*testing.T) error {
				pub := newFakeChannel()
				pub.publishErrs = []error{brokerDown}
				return newProducer(newFakeSource([]*fakeChannel{pub}), defaultProducerOptions()).Publish(context.Background(), "orders", "order.created", nil, nil)
			},
			wantOp:   OpPublish,
			wantIs:   brokerDown,
			wantText: `rabbitmq: publish: to "orders" key "order.created": broker down`,
		},
		{
			name:     "#4 - FAIL - retry publish",
			call:     func(t *testing.T) error { return runWith(t, errors.New("db down")) },
			wantOp:   OpRetry,
			wantIs:   ErrDLQ,
			wantText: `rabbitmq: retry: publish failed for m-1: to "" key "billing.orders.retry": broker down`,
		},
		{
			name:     "#5 - FAIL - dlq publish",
			call:     func(t *testing.T) error { return runWith(t, Permanent(errors.New("bad payload"))) },
			wantOp:   OpDLQ,
			wantIs:   ErrDLQ,
			wantText: `rabbitmq: dlq: publish failed for m-1: to "" key "billing.orders.dlq": broker down`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call(t)

			var re *Error
			if !errors.As(err, &re) {
				t.Fatalf("err = %v (%T), want *Error", err, err)
			}
			if re.Op != tt.wantOp {
				t.Fatalf("Op = %q, want %q", re.Op, tt.wantOp)
			}
			if !errors.Is(err, tt.wantIs) {
				t.Fatalf("errors.Is(%v, %v) = false", err, tt.wantIs)
			}
			if err.Error() != tt.wantText {
				t.Fatalf("Error() = %q, want %q", err.Error(), tt.wantText)
			}
		})
	}
}

func TestWrapErrNil(t *testing.T) {
	if err := WrapErr(OpPublish, nil); err != nil {
		t.Fatalf("WrapErr(nil) = %v, want nil", err)
	}
}
