package rabbitmq

import (
	"encoding/json"
	"regexp"
	"testing"
)

func TestNewEvent(t *testing.T) {
	uuidRe := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	tests := []struct {
		name    string
		payload any
		wantErr bool
	}{
		{"#1 - OK", map[string]int{"amount": 10}, false},
		{"#2 - FAIL - unmarshalable payload", make(chan int), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := NewEvent("order.created", 1, tt.payload)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !uuidRe.MatchString(e.ID) {
				t.Errorf("ID %q is not a uuid v4", e.ID)
			}
			if e.OccurredAt.IsZero() || e.Type != "order.created" || e.Version != 1 {
				t.Errorf("unexpected event %+v", e)
			}
			var p map[string]int
			if err := json.Unmarshal(e.Payload, &p); err != nil || p["amount"] != 10 {
				t.Errorf("payload = %s", e.Payload)
			}
		})
	}
}
