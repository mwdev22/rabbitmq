package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// channel is the subset of an amqp channel the producer and consumer use, kept
// small so tests can swap in fakes
type channel interface {
	Qos(prefetch int) error
	declare(t topology) error
	Consume(queue, tag string) (<-chan amqp.Delivery, error)
	Cancel(tag string) error
	// Publish blocks until the broker confirms the message
	Publish(ctx context.Context, exchange, key string, msg amqp.Publishing) error
	Close() error
}

type channelSource interface {
	channel(ctx context.Context, confirm bool) (channel, error)
}

// Conn is an amqp connection that redials with backoff when the broker drops
// it. producers and consumers ask it for fresh channels after a reconnect
type Conn struct {
	url  string
	opts ConnOptions

	mu    sync.Mutex
	conn  *amqp.Connection
	ready chan struct{} // closed while conn is usable

	done      chan struct{}
	closeOnce sync.Once
}

// Dial connects to url. the first dial fails fast, later drops are retried
// in the background until Close
func Dial(url string, opts ...ConnOption) (*Conn, error) {
	o := defaultConnOptions()
	for _, opt := range opts {
		opt(&o)
	}
	ac, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: dial: %w", err)
	}
	c := &Conn{url: url, opts: o, ready: make(chan struct{}), done: make(chan struct{})}
	c.setConn(ac)
	go c.watch(ac)
	return c, nil
}

func (c *Conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.done)
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.conn != nil {
			err = c.conn.Close()
		}
	})
	return err
}

func (c *Conn) setConn(ac *amqp.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = ac
	close(c.ready)
}

func (c *Conn) watch(ac *amqp.Connection) {
	for {
		closed := ac.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case <-c.done:
			return
		case err := <-closed:
			select {
			case <-c.done:
				return
			default:
			}
			c.opts.Logger.Printf("rabbitmq: connection lost: %v, reconnecting", err)
			c.mu.Lock()
			c.conn = nil
			c.ready = make(chan struct{})
			c.mu.Unlock()
			if ac = c.redial(); ac == nil {
				return
			}
		}
	}
}

func (c *Conn) redial() *amqp.Connection {
	for attempt := 0; ; attempt++ {
		select {
		case <-c.done:
			return nil
		case <-time.After(backoff(attempt, c.opts.ReconnectBase, c.opts.ReconnectMax)):
		}
		ac, err := amqp.Dial(c.url)
		if err != nil {
			c.opts.Logger.Printf("rabbitmq: reconnect attempt %d: %v", attempt+1, err)
			continue
		}
		c.setConn(ac)
		c.opts.Logger.Println("rabbitmq: reconnected")
		return ac
	}
}

// channel opens a channel, waiting for a reconnect if the connection is down
func (c *Conn) channel(ctx context.Context, confirm bool) (channel, error) {
	for attempt := 0; ; attempt++ {
		select {
		case <-c.done:
			return nil, ErrConnClosed
		default:
		}
		c.mu.Lock()
		ac, ready := c.conn, c.ready
		c.mu.Unlock()

		if ac == nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.done:
				return nil, ErrConnClosed
			case <-ready:
			}
			continue
		}

		ch, err := openChannel(ac, confirm)
		if err == nil {
			return ch, nil
		}
		// the connection may be dying before watch notices, so back off instead of spinning
		c.opts.Logger.Printf("rabbitmq: open channel: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
			return nil, ErrConnClosed
		case <-time.After(backoff(attempt, c.opts.ReconnectBase, c.opts.ReconnectMax)):
		}
	}
}

type amqpChannel struct {
	ch      *amqp.Channel
	returns chan amqp.Return
}

func openChannel(ac *amqp.Connection, confirm bool) (*amqpChannel, error) {
	ch, err := ac.Channel()
	if err != nil {
		return nil, err
	}
	c := &amqpChannel{ch: ch}
	if confirm {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return nil, fmt.Errorf("rabbitmq: confirm mode: %w", err)
		}
		// buffered so the connection reader never blocks on it, callers publish one at a time
		c.returns = ch.NotifyReturn(make(chan amqp.Return, 1))
	}
	return c, nil
}

func (c *amqpChannel) Qos(prefetch int) error {
	return c.ch.Qos(prefetch, 0, false)
}

func (c *amqpChannel) Consume(queue, tag string) (<-chan amqp.Delivery, error) {
	return c.ch.Consume(queue, tag, false, false, false, false, nil)
}

func (c *amqpChannel) Cancel(tag string) error {
	return c.ch.Cancel(tag, false)
}

func (c *amqpChannel) Close() error {
	return c.ch.Close()
}

// Publish publishes with mandatory set and waits for the confirm. the broker
// sends basic.return before the ack, so a return is visible once the ack lands
func (c *amqpChannel) Publish(ctx context.Context, exchange, key string, msg amqp.Publishing) error {
	if c.returns == nil {
		return fmt.Errorf("rabbitmq: channel not in confirm mode")
	}
	select {
	case <-c.returns:
	default:
	}
	dc, err := c.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, true, false, msg)
	if err != nil {
		return err
	}
	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	select {
	case r := <-c.returns:
		return fmt.Errorf("%w: %d %s (exchange %q, key %q)", ErrUnroutable, r.ReplyCode, r.ReplyText, exchange, key)
	default:
	}
	if !acked {
		return ErrNack
	}
	return nil
}
