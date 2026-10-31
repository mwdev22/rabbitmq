package rabbitmq

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// topology is what a consumer declares:
//
//	exchange --routingKey--> queue --(rejected/expired)--> queue.dlq
//	consumer --transient err--> queue.retry --ttl--> queue
//	consumer --permanent/exhausted--> queue.dlq
//
// retry and dlq go through the default exchange keyed by queue name, so a retry
// only lands back in this queue and not in other queues bound to the exchange
type topology struct {
	exchange   string
	kind       string
	queue      string
	routingKey string
	retryTTL   time.Duration
}

func newTopology(o ConsumerOptions) topology {
	return topology{
		exchange:   o.Exchange,
		kind:       o.ExchangeKind,
		queue:      o.Queue,
		routingKey: o.RoutingKey,
		retryTTL:   o.RetryTTL,
	}
}

func (t topology) retryQueue() string { return fmt.Sprintf("%s.retry", t.queue) }
func (t topology) dlq() string        { return fmt.Sprintf("%s.dlq", t.queue) }

type queueSpec struct {
	name string
	args amqp.Table
}

func (t topology) queues() []queueSpec {
	return []queueSpec{
		{name: t.dlq()},
		{name: t.retryQueue(), args: amqp.Table{
			"x-message-ttl":             t.retryTTL.Milliseconds(),
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": t.queue,
		}},
		{name: t.queue, args: amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": t.dlq(),
		}},
	}
}

func (c *amqpChannel) declare(t topology) error {
	if t.exchange != "" {
		if err := c.ch.ExchangeDeclare(t.exchange, t.kind, true, false, false, false, nil); err != nil {
			return fmt.Errorf("rabbitmq: declare exchange %q: %w", t.exchange, err)
		}
	}
	for _, q := range t.queues() {
		if _, err := c.ch.QueueDeclare(q.name, true, false, false, false, q.args); err != nil {
			return fmt.Errorf("rabbitmq: declare queue %q: %w", q.name, err)
		}
	}
	if t.exchange != "" {
		if err := c.ch.QueueBind(t.queue, t.routingKey, t.exchange, false, nil); err != nil {
			return fmt.Errorf("rabbitmq: bind %q to %q: %w", t.queue, t.exchange, err)
		}
	}
	return nil
}
