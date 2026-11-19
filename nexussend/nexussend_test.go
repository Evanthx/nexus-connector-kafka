package nexussend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
)

// recorder is a Sender that records when each attempt arrived. fail, if set, decides an
// attempt's error.
type recorder struct {
	mu       sync.Mutex
	attempts []time.Time
	fail     func(attempt int) error
}

func (r *recorder) Send(context.Context, Operation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts = append(r.attempts, time.Now())
	if r.fail != nil {
		return r.fail(len(r.attempts))
	}
	return nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.attempts)
}

func op(messages int) Operation { return Operation{ID: "op", Messages: messages} }

// Only a refusal for size counts as too large; other refusals concern the setup, so they are
// retried rather than sending every batch to the dead-letter destination.
func TestRefusedAsTooLarge(t *testing.T) {
	require.True(t, refusedAsTooLarge(serviceerror.NewInvalidArgument("input exceeds size limit")))
	require.False(t, refusedAsTooLarge(serviceerror.NewInvalidArgument("schedule-to-close timeout exceeds the namespace maximum")))
	require.False(t, refusedAsTooLarge(serviceerror.NewNotFound("endpoint not found")))
}

func TestBackoff(t *testing.T) {
	require.Equal(t, time.Second, Backoff(1))
	require.Equal(t, 2*time.Second, Backoff(2))
	require.Equal(t, 16*time.Second, Backoff(5))
	require.Equal(t, 30*time.Second, Backoff(6))
	require.Equal(t, 30*time.Second, Backoff(100))
}

func fastRetry(int) time.Duration { return time.Millisecond }

func TestRetrying_RetriesUntilSent(t *testing.T) {
	next := &recorder{fail: func(attempt int) error {
		if attempt < 3 {
			return errors.New("unavailable")
		}
		return nil
	}}
	require.NoError(t, NewRetrying(next, fastRetry, slog.Default()).Send(context.Background(), op(1)))
	require.Equal(t, 3, next.count())
}

// Too large is final: retrying the same input can't help.
func TestRetrying_TooLargeIsNotRetried(t *testing.T) {
	next := &recorder{fail: func(int) error { return fmt.Errorf("%w: input exceeds size limit", ErrTooLarge) }}
	err := NewRetrying(next, fastRetry, slog.Default()).Send(context.Background(), op(1))
	require.ErrorIs(t, err, ErrTooLarge)
	require.Equal(t, 1, next.count())
}

func TestRetrying_StopsWhenContextEnds(t *testing.T) {
	next := &recorder{fail: func(int) error { return errors.New("unavailable") }}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := NewRetrying(next, fastRetry, slog.Default()).Send(ctx, op(1))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestNewRateLimited_RefusesABurstThatCanNeverFit(t *testing.T) {
	for _, tc := range []struct{ perMinute, burst int }{{0, 1}, {10, 0}, {10, 11}} {
		_, err := NewRateLimited(&recorder{}, tc.perMinute, tc.burst)
		require.Error(t, err, "perMinute %d, burst %d", tc.perMinute, tc.burst)
	}
}

// A burst goes at once; after it, operations wait for their messages to fit in the rate.
func TestRateLimited_SpreadsMessagesEvenly(t *testing.T) {
	next := &recorder{}
	limited, err := NewRateLimited(next, 600, 5) // 10 a second
	require.NoError(t, err)
	start := time.Now()
	require.NoError(t, limited.Send(context.Background(), op(5)))
	require.Less(t, time.Since(start), 100*time.Millisecond, "the first burst must not wait")
	require.NoError(t, limited.Send(context.Background(), op(5)))
	waited := time.Since(start)
	require.GreaterOrEqual(t, waited, 450*time.Millisecond, "5 more messages at 10 a second take 500ms")
	require.Less(t, waited, 2*time.Second)
	require.Equal(t, 2, next.count())
}

func TestRateLimited_RefusesAnOperationLargerThanTheBurst(t *testing.T) {
	next := &recorder{}
	limited, err := NewRateLimited(next, 600, 5)
	require.NoError(t, err)
	require.Error(t, limited.Send(context.Background(), op(6)))
	require.Zero(t, next.count())
}

// A wait ends when ctx ends, without sending, and the messages it waited for aren't counted.
func TestRateLimited_WaitEndsWithContext(t *testing.T) {
	next := &recorder{}
	limited, err := NewRateLimited(next, 60, 5) // 1 a second
	require.NoError(t, err)
	require.NoError(t, limited.Send(context.Background(), op(5)))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.ErrorIs(t, limited.Send(ctx, op(5)), context.DeadlineExceeded)
	require.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond, "it must wait for ctx, not give up at once")
	require.Equal(t, 1, next.count())

	// The cancelled wait returned its reservation: one message is due a second after the burst,
	// not five seconds later.
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, limited.Send(ctx, op(1)))
}
