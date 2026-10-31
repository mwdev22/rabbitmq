package rabbitmq

import (
	"math/rand/v2"
	"time"
)

// backoff returns base*2^attempt capped at max, with equal jitter so
// reconnecting clients don't stampede the broker
func backoff(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	half := d / 2
	return half + rand.N(half+1)
}
