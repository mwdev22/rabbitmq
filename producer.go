package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	src  channelSource
	opts ProducerOptions

	// publishes are serialized so a basic.return can be matched to its publish
	mu sync.Mutex
	ch channel
}

func NewProducer(conn *Conn, opts ...ProducerOption) (*Producer, error) {
	if conn == nil {
		return nil, ErrMissingConn
	}
	o := defaultProducerOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return newProducer(conn, o), nil
}

func newProducer(src channelSource, o ProducerOptions) *Producer {
	return &Producer{src: src, opts: o}
}

// headers["id"] becomes the message id, generated when missing
func (p *Producer) Publish(ctx context.Context, exchange, key string, body []byte, headers map[string]string) error {
	t := make(amqp.Table, len(headers)+1)
	for k, v := range headers {
		t[k] = v
	}
	id := headers[HeaderID]
	if id == "" {
		id = newID()
		t[HeaderID] = id
	}
	return p.publish(ctx, exchange, key, amqp.Publishing{
		Headers:      t,
		MessageId:    id,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now().UTC(),
		Body:         body,
	})
}

func (p *Producer) PublishEvent(ctx context.Context, exchange, key string, e Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("rabbitmq: marshal event: %w", err)
	}
	return p.publish(ctx, exchange, key, amqp.Publishing{
		Headers: amqp.Table{
			HeaderID:  e.ID,
			"type":    e.Type,
			"version": strconv.Itoa(e.Version),
		},
		MessageId:    e.ID,
		Type:         e.Type,
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    e.OccurredAt,
		Body:         body,
	})
}

func (p *Producer) publish(ctx context.Context, exchange, key string, msg amqp.Publishing) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, p.opts.ConfirmTimeout)
	defer cancel()

	err := p.publishOnce(ctx, exchange, key, msg)
	// a closed channel never sent the message, one retry on a fresh channel covers a reconnect
	if errors.Is(err, amqp.ErrClosed) {
		err = p.publishOnce(ctx, exchange, key, msg)
	}
	if err != nil {
		return fmt.Errorf("rabbitmq: publish to %q key %q: %w", exchange, key, err)
	}
	return nil
}

func (p *Producer) publishOnce(ctx context.Context, exchange, key string, msg amqp.Publishing) error {
	if p.ch == nil {
		ch, err := p.src.channel(ctx, true)
		if err != nil {
			return err
		}
		p.ch = ch
	}
	if err := p.ch.Publish(ctx, exchange, key, msg); err != nil {
		// after a timeout or close the channel state is unknown, start clean
		_ = p.ch.Close()
		p.ch = nil
		return err
	}
	return nil
}

// closes only the producer's channel, not the Conn
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch == nil {
		return nil
	}
	err := p.ch.Close()
	p.ch = nil
	return err
}
