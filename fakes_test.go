package rabbitmq

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type published struct {
	exchange, key string
	msg           amqp.Publishing
}

type fakeChannel struct {
	mu          sync.Mutex
	deliveries  chan amqp.Delivery
	publishErrs []error // popped per publish, empty means success
	published   []published
	declared    []topology
	qos         int
	cancelled   bool
	closed      bool
}

func newFakeChannel(ds ...amqp.Delivery) *fakeChannel {
	ch := make(chan amqp.Delivery, len(ds))
	for _, d := range ds {
		ch <- d
	}
	return &fakeChannel{deliveries: ch}
}

func (f *fakeChannel) Qos(n int) error { f.mu.Lock(); f.qos = n; f.mu.Unlock(); return nil }
func (f *fakeChannel) declare(t topology) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.declared = append(f.declared, t)
	return nil
}
func (f *fakeChannel) Consume(string, string) (<-chan amqp.Delivery, error) { return f.deliveries, nil }
func (f *fakeChannel) Cancel(string) error {
	f.mu.Lock()
	f.cancelled = true
	f.mu.Unlock()
	return nil
}
func (f *fakeChannel) Close() error { f.mu.Lock(); f.closed = true; f.mu.Unlock(); return nil }

func (f *fakeChannel) Publish(_ context.Context, exchange, key string, msg amqp.Publishing) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.publishErrs) > 0 {
		err := f.publishErrs[0]
		f.publishErrs = f.publishErrs[1:]
		if err != nil {
			return err
		}
	}
	f.published = append(f.published, published{exchange, key, msg})
	return nil
}

func (f *fakeChannel) snapshot() []published {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]published(nil), f.published...)
}

// exhausted is closed once consume channels run out, i.e. Run is idle
type fakeSource struct {
	mu        sync.Mutex
	consume   []*fakeChannel
	pub       []*fakeChannel
	opened    int
	exhausted chan struct{}
	once      sync.Once
}

func newFakeSource(pub []*fakeChannel, consume ...*fakeChannel) *fakeSource {
	return &fakeSource{consume: consume, pub: pub, exhausted: make(chan struct{})}
}

func (s *fakeSource) channel(ctx context.Context, confirm bool) (channel, error) {
	s.mu.Lock()
	s.opened++
	if confirm && len(s.pub) > 0 {
		ch := s.pub[0]
		if len(s.pub) > 1 {
			s.pub = s.pub[1:]
		}
		s.mu.Unlock()
		return ch, nil
	}
	if !confirm && len(s.consume) > 0 {
		ch := s.consume[0]
		s.consume = s.consume[1:]
		s.mu.Unlock()
		return ch, nil
	}
	s.mu.Unlock()
	s.once.Do(func() { close(s.exhausted) })
	<-ctx.Done()
	return nil, ctx.Err()
}

type ackRecord struct {
	tag      uint64
	ack      bool
	requeue  bool
	multiple bool
}

type fakeAcker struct {
	mu   sync.Mutex
	acks []ackRecord
}

func (a *fakeAcker) Ack(tag uint64, multiple bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acks = append(a.acks, ackRecord{tag: tag, ack: true, multiple: multiple})
	return nil
}

func (a *fakeAcker) Nack(tag uint64, multiple, requeue bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acks = append(a.acks, ackRecord{tag: tag, requeue: requeue, multiple: multiple})
	return nil
}

func (a *fakeAcker) Reject(tag uint64, requeue bool) error { return a.Nack(tag, false, requeue) }

func (a *fakeAcker) snapshot() []ackRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]ackRecord(nil), a.acks...)
}
