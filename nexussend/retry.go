package nexussend

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Retrying retries an operation until it is sent, it is refused as too large, or ctx ends.
// Outages and setup errors are retried, never given up on, so the Inbound Connector waits for
// them to be fixed instead of sending messages to the dead-letter destination.
type Retrying struct {
	next   Sender
	delay  func(attempt int) time.Duration
	logger *slog.Logger
}

// NewRetrying returns a Retrying that sends through next, waiting delay(attempt) after each failed
// attempt and logging each failure to logger.
func NewRetrying(next Sender, delay func(attempt int) time.Duration, logger *slog.Logger) *Retrying {
	return &Retrying{next: next, delay: delay, logger: logger}
}

// Send sends op, retrying until it is sent. It returns an error only if op was refused as too
// large (ErrTooLarge), failed for good (ErrFailed), or ctx ended.
func (r *Retrying) Send(ctx context.Context, op Operation) error {
	for attempt := 1; ; attempt++ {
		err := r.next.Send(ctx, op)
		if err == nil || errors.Is(err, ErrTooLarge) || errors.Is(err, ErrFailed) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.logger.Warn("The operation didn't start or didn't succeed; retrying", "operationID", op.ID, "attempt", attempt, "error", err)
		select {
		case <-time.After(r.delay(attempt)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Backoff is a delay for NewRetrying: one second, doubling, at most 30 seconds.
func Backoff(attempt int) time.Duration {
	return min(time.Second<<min(attempt-1, 5), 30*time.Second)
}
