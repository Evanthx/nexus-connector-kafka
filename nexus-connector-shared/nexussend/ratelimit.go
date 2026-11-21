package nexussend

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"
)

// RateLimited sends at most a set number of messages a minute, spread evenly: it waits before
// each operation until the operation's messages fit in the rate. The limit is per RateLimited,
// so per process: two Inbound Connectors with a limit of 600 send up to 1,200 a minute between
// them.
//
// Waiting messages stay in the system they came from, not in Temporal. A wait ends early if ctx
// ends, and the messages it was waiting for aren't counted.
type RateLimited struct {
	next    Sender
	limiter *rate.Limiter
}

// NewRateLimited returns a RateLimited that sends through next at most perMinute messages a minute
// on average. burst is the most messages one operation may hold, such as the batch size: up to
// that many can go at once, so any one minute sees at most perMinute + burst. burst must be at
// least 1 and at most perMinute.
func NewRateLimited(next Sender, perMinute, burst int) (*RateLimited, error) {
	if perMinute < 1 || burst < 1 || burst > perMinute {
		return nil, fmt.Errorf("a rate limit needs 1 <= burst <= perMinute; got burst %d, perMinute %d", burst, perMinute)
	}
	return &RateLimited{next: next, limiter: rate.NewLimiter(rate.Every(time.Minute/time.Duration(perMinute)), burst)}, nil
}

// Send waits until op's messages fit in the rate, then sends op. It returns ctx's error if ctx
// ends first, and an error if op holds more messages than the burst, which could never fit.
func (r *RateLimited) Send(ctx context.Context, op Operation) error {
	if op.Messages > r.limiter.Burst() {
		return fmt.Errorf("operation %s holds %d messages, more than the rate limit lets through at once (%d)", op.ID, op.Messages, r.limiter.Burst())
	}
	// Reserve and wait here rather than with WaitN, which gives up at once when ctx has a deadline
	// sooner than the wait: this waits until ctx actually ends.
	reservation := r.limiter.ReserveN(time.Now(), max(op.Messages, 1))
	if delay := reservation.Delay(); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			reservation.Cancel() // the messages weren't sent, so they don't count
			return ctx.Err()
		}
	}
	return r.next.Send(ctx, op)
}
