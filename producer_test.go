package rabbitmq

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestProducerPublish(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name        string
		publishErrs []error
		headers     map[string]string
		wantErr     error
		wantOpened  int
		wantID      string
	}{
		{"#1 - OK - generates id", nil, nil, nil, 1, ""},
		{"#2 - OK - keeps id header", nil, map[string]string{HeaderID: "abc"}, nil, 1, "abc"},
		{"#3 - OK - stale channel retried once", []error{amqp.ErrClosed}, nil, nil, 2, ""},
		{"#4 - FAIL - nack", []error{ErrNack}, nil, ErrNack, 1, ""},
		{"#5 - FAIL - closed twice", []error{amqp.ErrClosed, amqp.ErrClosed}, nil, amqp.ErrClosed, 2, ""},
		{"#6 - FAIL - other error not retried", []error{boom}, nil, boom, 1, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := newFakeChannel()
			ch.publishErrs = tt.publishErrs
			src := newFakeSource([]*fakeChannel{ch})
			p := newProducer(src, defaultProducerOptions())

			err := p.Publish(context.Background(), "orders", "order.created", []byte("x"), tt.headers)
			if !errors.Is(err, tt.wantErr) || (err != nil) != (tt.wantErr != nil) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if src.opened != tt.wantOpened {
				t.Errorf("opened %d channels, want %d", src.opened, tt.wantOpened)
			}
			if err != nil {
				return
			}
			pub := ch.snapshot()
			if len(pub) != 1 {
				t.Fatalf("published %d, want 1", len(pub))
			}
			m := pub[0].msg
			if m.DeliveryMode != amqp.Persistent {
				t.Errorf("delivery mode = %d, want persistent", m.DeliveryMode)
			}
			if m.MessageId == "" || m.Headers[HeaderID] != m.MessageId {
				t.Errorf("message id %q, header %v", m.MessageId, m.Headers[HeaderID])
			}
			if tt.wantID != "" && m.MessageId != tt.wantID {
				t.Errorf("message id = %q, want %q", m.MessageId, tt.wantID)
			}
		})
	}
}

func TestProducerPublishEvent(t *testing.T) {
	ch := newFakeChannel()
	p := newProducer(newFakeSource([]*fakeChannel{ch}), defaultProducerOptions())
	e, _ := NewEvent("order.created", 2, map[string]string{"id": "o1"})

	if err := p.PublishEvent(context.Background(), "orders", "order.created", e); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	m := ch.snapshot()[0].msg
	if m.MessageId != e.ID || m.Headers[HeaderID] != e.ID || m.Type != "order.created" || m.ContentType != "application/json" {
		t.Fatalf("unexpected publishing %+v", m)
	}
}

func TestNewProducer(t *testing.T) {
	if _, err := NewProducer(nil); !errors.Is(err, ErrMissingConn) {
		t.Fatalf("err = %v, want ErrMissingConn", err)
	}
}
