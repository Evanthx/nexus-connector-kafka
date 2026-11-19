package main

import (
	"errors"
	"fmt"
	"time"
)

const (
	// callbackMargin is how long before the caller's Nexus operation times out the operation stops
	// trying, so its last error reaches the caller before the caller gives up.
	callbackMargin = 10 * time.Second
	// defaultWriteMax is how long a write may take, retries included, unless
	// KAFKA_MAX_DURATION_MINUTES says otherwise.
	defaultWriteMax = 10 * time.Minute
)

// errInvalidTimeout means the caller's Nexus operation has too little time left to run, or its
// operation-timeout header can't be read.
var errInvalidTimeout = errors.New("invalid operation timeout")

// writeScheduleToClose is how long a write may take in all: maxDuration
// (KAFKA_MAX_DURATION_MINUTES), or defaultWriteMax if unset, or less if the caller's Nexus
// operation times out first, in which case it ends callbackMargin before, so the caller gets the
// write's last error rather than a timeout. operationTimeout is the operation-timeout header, the
// time the caller's operation has left, which its server sets when it has a timeout; empty means
// none. It is an errInvalidTimeout if no time is left after callbackMargin.
func writeScheduleToClose(maxDuration time.Duration, operationTimeout string) (time.Duration, error) {
	limit := defaultWriteMax
	if maxDuration > 0 {
		limit = maxDuration
	}
	if operationTimeout == "" {
		return limit, nil
	}
	left, err := time.ParseDuration(operationTimeout)
	if err != nil {
		return 0, fmt.Errorf("%w: can't read the operation-timeout header %q: %w", errInvalidTimeout, operationTimeout, err)
	}
	total := left - callbackMargin
	if total <= 0 {
		return 0, fmt.Errorf("%w: the caller's Nexus operation times out in %s, which leaves no time to run: "+
			"it needs more than %s for the result to reach the caller; give the operation a longer timeout",
			errInvalidTimeout, left.Round(time.Millisecond), callbackMargin)
	}
	return min(total, limit), nil
}
