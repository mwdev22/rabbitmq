package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	HeaderID            = "id"
	HeaderRetryCount    = "x-retry-count"
	HeaderError         = "x-error"
	HeaderOriginalQueue = "x-original-queue"
	// retries travel through the default exchange, these keep the original
	// routing so Message.Key/Exchange stay the same on every attempt
	HeaderOriginalExchange = "x-original-exchange"
	HeaderOriginalKey      = "x-original-routing-key"
)

// Message is a delivery handed to a Handler. ID is stable across redeliveries
// (the "id" header, else the amqp MessageId), use it to dedupe
type Message struct {
	ID      string
	Key     string
	Body    []byte
	Headers map[string]string

	Exchange    string
	Queue       string
	Redelivered bool
	RetryCount  int
}

// Handler processes a message. nil acks it, Permanent(err) sends it straight to
// the dlq, any other error is retried up to WithMaxRetries times.
// delivery is at-least-once, so handlers must be idempotent
type Handler func(ctx context.Context, m Message) error

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks err as not retryable
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

func toMessage(d amqp.Delivery, queue string) Message {
	headers := make(map[string]string, len(d.Headers))
	for k, v := range d.Headers {
		headers[k] = fmt.Sprint(v)
	}
	id := headers[HeaderID]
	if id == "" {
		id = d.MessageId
	}
	key, exchange := d.RoutingKey, d.Exchange
	if k, ok := headers[HeaderOriginalKey]; ok {
		key, exchange = k, headers[HeaderOriginalExchange]
	}
	return Message{
		ID:          id,
		Key:         key,
		Body:        d.Body,
		Headers:     headers,
		Exchange:    exchange,
		Queue:       queue,
		Redelivered: d.Redelivered,
		RetryCount:  retryCount(d.Headers),
	}
}

func retryCount(h amqp.Table) int {
	switch v := h[HeaderRetryCount].(type) {
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case string:
		var n int
		if _, err := fmt.Sscan(v, &n); err == nil {
			return n
		}
	}
	return 0
}
