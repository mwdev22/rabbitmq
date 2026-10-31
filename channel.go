package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// kept small so tests can swap in fakes
type channel interface {
	Qos(prefetch int) error
	declare(t topology) error
	Consume(queue, tag string) (<-chan amqp.Delivery, error)
	Cancel(tag string) error
	Publish(ctx context.Context, exchange, key string, msg amqp.Publishing) error
	Close() error
}

type channelSource interface {
	channel(ctx context.Context, confirm bool) (channel, error)
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

// basic.return arrives before the ack, so a return is visible once the ack lands
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
