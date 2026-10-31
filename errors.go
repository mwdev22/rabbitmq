package rabbitmq

import "fmt"

const (
	OpConfig    = "config"
	OpDial      = "dial"
	OpChannel   = "channel"
	OpSubscribe = "subscribe"
	OpPublish   = "publish"
	OpAck       = "ack"
	OpRetry     = "retry"
	OpDLQ       = "dlq"
	OpClose     = "close"
	OpEvent     = "event"
)

// every error leaving the package is an *Error, so callers can map broker failures with errors.As
type Error struct {
	Op  string
	Err error
}

func (e *Error) Error() string { return fmt.Sprintf("rabbitmq: %s: %v", e.Op, e.Err) }

func (e *Error) Unwrap() error { return e.Err }

func WrapErr(op string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{
		Op:  op,
		Err: err,
	}
}
