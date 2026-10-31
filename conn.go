package rabbitmq

import (
	"context"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Conn struct {
	url  string
	opts ConnOptions

	mu    sync.Mutex
	conn  *amqp.Connection
	ready chan struct{} // closed while conn is usable

	done      chan struct{}
	closeOnce sync.Once
}

// first dial fails fast, later drops are redialled in the background
func Dial(url string, opts ...ConnOption) (*Conn, error) {
	o := defaultConnOptions()
	for _, opt := range opts {
		opt(&o)
	}
	ac, err := amqp.Dial(url)
	if err != nil {
		return nil, WrapErr(OpDial, err)
	}
	c := &Conn{
		url:   url,
		opts:  o,
		ready: make(chan struct{}),
		done:  make(chan struct{}),
	}
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
	return WrapErr(OpClose, err)
}

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

func (c *Conn) setConn(ac *amqp.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = ac
	close(c.ready)
}
