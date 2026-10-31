package rabbitmq

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"
)

// Event is the envelope our producers publish. its ID becomes Message.ID on the
// consumer side
type Event struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Version    int             `json:"version"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
}

func NewEvent(typ string, version int, payload any) (Event, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("rabbitmq: marshal payload: %w", err)
	}
	return Event{
		ID:         newID(),
		Type:       typ,
		Version:    version,
		OccurredAt: time.Now().UTC(),
		Payload:    b,
	}, nil
}

// newID returns a random uuid v4
func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
