package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	src  channelSource
	h    Handler
	opts ConsumerOptions
	top  topology
	pub  *Producer
	tag  string
}

func NewConsumer(conn *Conn, h Handler, opts ...ConsumerOption) (*Consumer, error) {
	if conn == nil {
		return nil, ErrMissingConn
	}
	return newConsumer(conn, h, opts...)
}

func newConsumer(src channelSource, h Handler, opts ...ConsumerOption) (*Consumer, error) {
	if h == nil {
		return nil, ErrMissingHandler
	}
	o := defaultConsumerOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	po := defaultProducerOptions()
	po.Logger = o.Logger
	return &Consumer{
		src:  src,
		h:    h,
		opts: o,
		top:  newTopology(o),
		pub:  newProducer(src, po),
		tag:  "rabbitmq-" + newID(),
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		ch, err := c.src.channel(ctx, false)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		deliveries, err := c.subscribe(ch)
		if err != nil {
			_ = ch.Close()
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, amqp.ErrClosed) {
				continue
			}
			return err
		}

		err = c.consume(ctx, deliveries)
		switch {
		case errors.Is(err, ErrDLQ):
			_ = ch.Close()
			return err
		case ctx.Err() != nil:
			// unacked prefetched messages go back to the queue on close
			_ = ch.Cancel(c.tag)
			_ = ch.Close()
			return nil
		case err != nil:
			_ = ch.Close()
			return err
		}
		_ = ch.Close()
		c.opts.Logger.Printf("rabbitmq: deliveries closed on %q, resubscribing", c.top.queue)
	}
}

// does not close the Conn
func (c *Consumer) Close() error {
	return c.pub.Close()
}

func (c *Consumer) subscribe(ch channel) (<-chan amqp.Delivery, error) {
	if err := ch.Qos(c.opts.Prefetch); err != nil {
		return nil, fmt.Errorf("rabbitmq: qos: %w", err)
	}
	if c.opts.Declare {
		if err := ch.declare(c.top); err != nil {
			return nil, err
		}
	}
	d, err := ch.Consume(c.top.queue, c.tag)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: consume %q: %w", c.top.queue, err)
	}
	return d, nil
}

// first failure stops new pulls, in-flight handlers still finish
func (c *Consumer) consume(ctx context.Context, deliveries <-chan amqp.Delivery) error {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg    sync.WaitGroup
		once  sync.Once
		first error
	)
	for range c.opts.Workers {
		wg.Go(func() {
			if err := c.work(ctx, wctx, deliveries); err != nil {
				once.Do(func() {
					first = err
					cancel()
				})
			}
		})
	}
	wg.Wait()
	return first
}

func (c *Consumer) work(ctx, wctx context.Context, deliveries <-chan amqp.Delivery) error {
	for {
		select {
		case <-wctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			if err := c.handle(ctx, d); err != nil {
				return err
			}
		}
	}
}

func (c *Consumer) handle(ctx context.Context, d amqp.Delivery) error {
	m := toMessage(d, c.top.queue)
	hctx, cancel := shutdownCtx(ctx, c.opts.ShutdownTimeout)
	defer cancel()

	herr := c.h(hctx, m)
	if herr == nil {
		return c.ack(d)
	}

	key, msg := c.reroute(d, m, herr)
	if err := c.pub.publish(hctx, "", key, msg); err != nil {
		_ = d.Nack(false, true)
		return fmt.Errorf("%w: %s: %v", ErrDLQ, key, err)
	}
	return c.ack(d)
}

func (c *Consumer) reroute(d amqp.Delivery, m Message, herr error) (string, amqp.Publishing) {
	headers := make(amqp.Table, len(d.Headers)+4)
	for k, v := range d.Headers {
		headers[k] = v
	}
	if _, ok := headers[HeaderOriginalKey]; !ok {
		headers[HeaderOriginalKey] = d.RoutingKey
		headers[HeaderOriginalExchange] = d.Exchange
	}
	msg := amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    amqp.Persistent,
		CorrelationId:   d.CorrelationId,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		AppId:           d.AppId,
		Body:            d.Body,
	}

	if !IsPermanent(herr) && m.RetryCount < c.opts.MaxRetries {
		headers[HeaderRetryCount] = int64(m.RetryCount + 1)
		if c.opts.BackoffBase > 0 {
			delay := backoff(m.RetryCount, c.opts.BackoffBase, c.opts.BackoffMax)
			msg.Expiration = strconv.FormatInt(delay.Milliseconds(), 10)
		}
		c.opts.Logger.Printf("rabbitmq: message %s failed (attempt %d/%d), retrying: %v", m.ID, m.RetryCount+1, c.opts.MaxRetries+1, herr)
		return c.top.retryQueue(), msg
	}

	headers[HeaderRetryCount] = int64(m.RetryCount)
	headers[HeaderError] = herr.Error()
	headers[HeaderOriginalQueue] = c.top.queue
	c.opts.Logger.Printf("rabbitmq: message %s sent to dlq after %d retries: %v", m.ID, m.RetryCount, herr)
	return c.top.dlq(), msg
}

func (c *Consumer) ack(d amqp.Delivery) error {
	err := d.Ack(false)
	if err == nil {
		return nil
	}
	// the channel died with the connection, the broker redelivers and Run resubscribes
	if errors.Is(err, amqp.ErrClosed) {
		return nil
	}
	return fmt.Errorf("rabbitmq: ack: %w", err)
}

// detached from ctx so in-flight work can finish, cancelled timeout after ctx is done
func shutdownCtx(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	hctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, func() {
		t := time.AfterFunc(timeout, cancel)
		context.AfterFunc(hctx, func() { t.Stop() })
	})
	return hctx, func() {
		stop()
		cancel()
	}
}
