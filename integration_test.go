//go:build integration

package rabbitmq

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func integrationURL() string {
	if u := os.Getenv("RABBITMQ_URL"); u != "" {
		return u
	}
	return "amqp://guest:guest@localhost:5672/"
}

type itEnv struct {
	conn     *Conn
	prod     *Producer
	exchange string
	queue    string
}

func newITEnv(t *testing.T) itEnv {
	t.Helper()
	conn, err := Dial(integrationURL(), WithReconnectBackoff(100*time.Millisecond, time.Second))
	if err != nil {
		t.Fatalf("dial (is `docker compose up -d` running?): %v", err)
	}
	prod, err := NewProducer(conn)
	if err != nil {
		t.Fatal(err)
	}
	suffix := newID()[:8]
	env := itEnv{
		conn:     conn,
		prod:     prod,
		exchange: "it." + suffix,
		queue:    "it." + suffix + ".q",
	}
	t.Cleanup(func() {
		if ac, err := amqp.Dial(integrationURL()); err == nil {
			if ch, err := ac.Channel(); err == nil {
				for _, q := range []string{env.queue, env.queue + ".retry", env.queue + ".dlq"} {
					_, _ = ch.QueueDelete(q, false, false, false)
				}
				_ = ch.ExchangeDelete(env.exchange, false, false)
			}
			_ = ac.Close()
		}
		_ = prod.Close()
		_ = conn.Close()
	})
	return env
}

func (e itEnv) startConsumer(t *testing.T, h Handler, opts ...ConsumerOption) (stop func() error) {
	t.Helper()
	opts = append([]ConsumerOption{WithExchange(e.exchange, "topic"), WithQueue(e.queue), WithRoutingKey("order.*")}, opts...)
	c, err := NewConsumer(e.conn, h, opts...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		err := e.prod.Publish(context.Background(), "", e.queue+".dlq", nil, map[string]string{"probe": "1"})
		if err == nil {
			break
		}
		if !errors.Is(err, ErrUnroutable) || time.Now().After(deadline) {
			t.Fatalf("topology not ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.drain(t, e.queue+".dlq")

	return func() error {
		cancel()
		_ = c.Close()
		return <-errc
	}
}

func (e itEnv) drain(t *testing.T, queue string) {
	t.Helper()
	for {
		if _, ok := e.get(t, queue); !ok {
			return
		}
	}
}

func (e itEnv) get(t *testing.T, queue string) (amqp.Delivery, bool) {
	t.Helper()
	ac, err := amqp.Dial(integrationURL())
	if err != nil {
		t.Fatal(err)
	}
	defer ac.Close()
	ch, err := ac.Channel()
	if err != nil {
		t.Fatal(err)
	}
	d, ok, err := ch.Get(queue, true)
	if err != nil {
		t.Fatal(err)
	}
	return d, ok
}

func TestIntegrationRoundTrip(t *testing.T) {
	env := newITEnv(t)
	got := make(chan Message, 1)
	stop := env.startConsumer(t, func(_ context.Context, m Message) error {
		got <- m
		return nil
	})

	e, _ := NewEvent("order.created", 1, map[string]string{"order_id": "o1"})
	if err := env.prod.PublishEvent(context.Background(), env.exchange, "order.created", e); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	select {
	case m := <-got:
		if m.ID != e.ID || m.Key != "order.created" || m.Exchange != env.exchange {
			t.Errorf("unexpected message %+v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message not consumed")
	}
	if err := stop(); err != nil {
		t.Fatalf("Run err = %v", err)
	}
	if _, ok := env.get(t, env.queue); ok {
		t.Error("message left in queue after ack")
	}
}

func TestIntegrationUnroutable(t *testing.T) {
	env := newITEnv(t)
	err := env.prod.Publish(context.Background(), "", "no.such.queue."+newID(), []byte("x"), nil)
	if !errors.Is(err, ErrUnroutable) {
		t.Fatalf("err = %v, want ErrUnroutable", err)
	}
}

func TestIntegrationDLQ(t *testing.T) {
	env := newITEnv(t)
	stop := env.startConsumer(t, func(context.Context, Message) error {
		return Permanent(errors.New("bad payload"))
	})

	if err := env.prod.Publish(context.Background(), env.exchange, "order.created", []byte("x"), map[string]string{HeaderID: "m1"}); err != nil {
		t.Fatal(err)
	}
	var d amqp.Delivery
	deadline := time.Now().Add(5 * time.Second)
	for ok := false; !ok; {
		if time.Now().After(deadline) {
			t.Fatal("message not dead-lettered")
		}
		time.Sleep(50 * time.Millisecond)
		d, ok = env.get(t, env.queue+".dlq")
	}
	if err := stop(); err != nil {
		t.Fatalf("Run err = %v", err)
	}
	if d.Headers[HeaderError] != "bad payload" || d.Headers[HeaderOriginalQueue] != env.queue || d.Headers[HeaderID] != "m1" {
		t.Errorf("dlq headers = %v", d.Headers)
	}
}

func TestIntegrationRetryTTL(t *testing.T) {
	env := newITEnv(t)
	var (
		mu      sync.Mutex
		retries []int
		keys    []string
	)
	done := make(chan struct{})
	stop := env.startConsumer(t, func(_ context.Context, m Message) error {
		mu.Lock()
		defer mu.Unlock()
		retries = append(retries, m.RetryCount)
		keys = append(keys, m.Key)
		if len(retries) < 3 {
			return errors.New("transient")
		}
		close(done)
		return nil
	}, WithRetryTTL(200*time.Millisecond), WithMaxRetries(3))

	start := time.Now()
	if err := env.prod.Publish(context.Background(), env.exchange, "order.paid", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("message did not come back from the retry queue")
	}
	if err := stop(); err != nil {
		t.Fatalf("Run err = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Errorf("two retries took %v, retry ttl not applied", elapsed)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(retries) != 3 || retries[0] != 0 || retries[1] != 1 || retries[2] != 2 {
		t.Errorf("retry counts = %v, want [0 1 2]", retries)
	}
	for _, k := range keys {
		if k != "order.paid" {
			t.Errorf("routing key = %q on retry, want order.paid", k)
		}
	}
	if _, ok := env.get(t, env.queue+".dlq"); ok {
		t.Error("message dead-lettered although it succeeded within retries")
	}
}
