package rabbitmq

import (
	"errors"
	"time"

	"github.com/mwdev22/logging"
)

var (
	ErrMissingConn    = errors.New("missing connection")
	ErrMissingHandler = errors.New("missing handler")
	ErrMissingQueue   = errors.New("missing queue")
	ErrInvalidOption  = errors.New("invalid option")
	ErrConnClosed     = errors.New("connection closed")
	ErrNack           = errors.New("publish nacked by broker")
	ErrUnroutable     = errors.New("message unroutable")
	// the message was nacked with requeue, so it isn't lost
	ErrDLQ = errors.New("publish failed")
)

type ConnOption func(*ConnOptions)

type ConnOptions struct {
	ReconnectBase time.Duration
	ReconnectMax  time.Duration
	Logger        logging.Logger
}

func defaultConnOptions() ConnOptions {
	return ConnOptions{
		ReconnectBase: 500 * time.Millisecond,
		ReconnectMax:  30 * time.Second,
		Logger:        logging.DefaultLogger(),
	}
}

func WithReconnectBackoff(base, max time.Duration) ConnOption {
	return func(o *ConnOptions) { o.ReconnectBase, o.ReconnectMax = base, max }
}

func WithConnLogger(l logging.Logger) ConnOption {
	return func(o *ConnOptions) { o.Logger = l }
}

type ProducerOption func(*ProducerOptions)

type ProducerOptions struct {
	ConfirmTimeout time.Duration
	Logger         logging.Logger
}

func defaultProducerOptions() ProducerOptions {
	return ProducerOptions{
		ConfirmTimeout: 5 * time.Second,
		Logger:         logging.DefaultLogger(),
	}
}

func WithConfirmTimeout(d time.Duration) ProducerOption {
	return func(o *ProducerOptions) { o.ConfirmTimeout = d }
}

func WithProducerLogger(l logging.Logger) ProducerOption {
	return func(o *ProducerOptions) { o.Logger = l }
}

type ConsumerOption func(*ConsumerOptions)

type ConsumerOptions struct {
	Exchange     string
	ExchangeKind string
	Queue        string
	RoutingKey   string
	RetryTTL     time.Duration
	Declare      bool

	Prefetch int
	Workers  int

	MaxRetries  int
	BackoffBase time.Duration
	BackoffMax  time.Duration

	ShutdownTimeout time.Duration
	Logger          logging.Logger
}

func defaultConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		ExchangeKind:    "topic",
		RetryTTL:        5 * time.Second,
		Declare:         true,
		Prefetch:        10,
		Workers:         1,
		MaxRetries:      3,
		ShutdownTimeout: 10 * time.Second,
		Logger:          logging.DefaultLogger(),
	}
}

func (o ConsumerOptions) validate() error {
	switch {
	case o.Queue == "":
		return ErrMissingQueue
	case o.Prefetch < 1, o.Workers < 1, o.MaxRetries < 0, o.RetryTTL <= 0:
		return ErrInvalidOption
	}
	return nil
}

// without it the queue is consumed as is, with no binding
func WithExchange(name, kind string) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.Exchange = name
		if kind != "" {
			o.ExchangeKind = kind
		}
	}
}

func WithQueue(name string) ConsumerOption {
	return func(o *ConsumerOptions) { o.Queue = name }
}

func WithRoutingKey(key string) ConsumerOption {
	return func(o *ConsumerOptions) { o.RoutingKey = key }
}

func WithRetryTTL(d time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) { o.RetryTTL = d }
}

// false when infra owns the topology
func WithDeclare(declare bool) ConsumerOption {
	return func(o *ConsumerOptions) { o.Declare = declare }
}

func WithPrefetch(n int) ConsumerOption {
	return func(o *ConsumerOptions) { o.Prefetch = n }
}

// ordering is not preserved with n > 1
func WithWorkers(n int) ConsumerOption {
	return func(o *ConsumerOptions) { o.Workers = n }
}

func WithMaxRetries(n int) ConsumerOption {
	return func(o *ConsumerOptions) { o.MaxRetries = n }
}

// grows the retry delay per attempt via per-message expiration, capped by the retry ttl, off by default
func WithBackoff(base, max time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) { o.BackoffBase, o.BackoffMax = base, max }
}

func WithShutdownTimeout(d time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) { o.ShutdownTimeout = d }
}

func WithConsumerLogger(l logging.Logger) ConsumerOption {
	return func(o *ConsumerOptions) { o.Logger = l }
}
