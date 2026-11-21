// Package deadline is the time an Outbound Connector's call may take: its own limit, or less when
// the caller's Nexus operation times out first, in which case the call ends CallbackMargin before,
// so the caller gets the call's last error rather than a timeout.
package deadline

import (
	"errors"
	"fmt"
	"time"
)

// CallbackMargin is how long before the caller's Nexus operation times out a call stops trying:
// time to start the activity and deliver the completion callback, so its last error reaches the
// caller before the caller gives up.
const CallbackMargin = 10 * time.Second

// ErrTooShort means the caller's Nexus operation has too little time left to run the call, or its
// operation-timeout header can't be read. Outbound Connectors report it as an InvalidRequest.
var ErrTooShort = errors.New("invalid operation timeout")

// ScheduleToClose is how long a call may take in all, retries included: limit, or less if the
// caller's Nexus operation times out first. header is the operation-timeout header, the time the
// caller's operation has left, which its server sets when the operation has a timeout; "" means it
// has none. attempt is the least time one attempt needs, or 0 for any. The error wraps ErrTooShort
// if no time is left, after CallbackMargin, for one attempt, or if header can't be read.
func ScheduleToClose(header string, limit, attempt time.Duration) (time.Duration, error) {
	if header == "" {
		return limit, nil
	}
	left, err := time.ParseDuration(header)
	if err != nil {
		return 0, fmt.Errorf("%w: can't read the operation-timeout header %q: %w", ErrTooShort, header, err)
	}
	total := left - CallbackMargin
	switch {
	case attempt > 0 && total < attempt:
		return 0, fmt.Errorf("%w: the caller's Nexus operation times out in %s, which leaves no time for an attempt: "+
			"it needs at least one attempt (%s) plus %s for the result to reach the caller; give the operation a longer timeout",
			ErrTooShort, left.Round(time.Millisecond), attempt, CallbackMargin)
	case total <= 0:
		return 0, fmt.Errorf("%w: the caller's Nexus operation times out in %s, which leaves no time to run: "+
			"it needs more than %s for the result to reach the caller; give the operation a longer timeout",
			ErrTooShort, left.Round(time.Millisecond), CallbackMargin)
	}
	return min(total, limit), nil
}
