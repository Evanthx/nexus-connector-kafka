package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
)

const (
	// callbackMargin is how long before the caller's Nexus operation times out the operation stops
	// trying, so its last error reaches the caller before the caller gives up.
	callbackMargin = 10 * time.Second
	// defaultWriteMax is how long a write may take, retries included, unless
	// KAFKA_MAX_DURATION_MINUTES says otherwise.
	defaultWriteMax = 10 * time.Minute
	// readSlack is how much longer than its time limit a read may take, retries included, unless
	// KAFKA_MAX_DURATION_MINUTES says otherwise.
	readSlack = 5 * time.Minute
)

// errInvalidTimeout means the caller's Nexus operation has too little time left to run, or its
// operation-timeout header can't be read.
var errInvalidTimeout = errors.New("invalid operation timeout")

// readTimeLimit is how long a read waits for its batch to fill.
func readTimeLimit(in kafkaapi.ReadInput) time.Duration {
	return time.Duration(in.TimeLimitSecondsOrDefault()) * time.Second
}

// readAttempt is how long one read attempt may take: its time limit plus a minute.
func readAttempt(in kafkaapi.ReadInput) time.Duration {
	return readTimeLimit(in) + time.Minute
}

// readScheduleToClose is how long a read may take in all. maxDuration is
// KAFKA_MAX_DURATION_MINUTES (0 if unset); operationTimeout is the operation-timeout header (see
// scheduleToClose). The read's maximum is maxDuration, or its time limit plus readSlack if unset,
// but never less than one attempt.
func readScheduleToClose(in kafkaapi.ReadInput, maxDuration time.Duration, operationTimeout string) (time.Duration, error) {
	limit := readTimeLimit(in) + readSlack
	if maxDuration > 0 {
		limit = max(maxDuration, readAttempt(in))
	}
	return scheduleToClose(limit, readTimeLimit(in), operationTimeout,
		fmt.Sprintf("timeLimitSeconds (%s)", readTimeLimit(in)), "or lower timeLimitSeconds")
}

// writeScheduleToClose is how long a write may take in all: maxDuration
// (KAFKA_MAX_DURATION_MINUTES), or defaultWriteMax if unset, ending earlier if the caller's
// operation does (see scheduleToClose).
func writeScheduleToClose(maxDuration time.Duration, operationTimeout string) (time.Duration, error) {
	limit := defaultWriteMax
	if maxDuration > 0 {
		limit = maxDuration
	}
	return scheduleToClose(limit, 0, operationTimeout, "", "")
}

// scheduleToClose is how long an operation may take in all: limit, or less if the caller's Nexus
// operation times out first, in which case it ends callbackMargin before, so the caller gets the
// operation's last error rather than a timeout. operationTimeout is the operation-timeout header,
// the time the caller's operation has left, which its server sets when it has a timeout; empty
// means none. It is an errInvalidTimeout if the time left after callbackMargin is no more than
// need, the time the request itself asks for (needText describes it, fix says how to lower it).
func scheduleToClose(limit, need time.Duration, operationTimeout, needText, fix string) (time.Duration, error) {
	if operationTimeout == "" {
		return limit, nil
	}
	left, err := time.ParseDuration(operationTimeout)
	if err != nil {
		return 0, fmt.Errorf("%w: can't read the operation-timeout header %q: %w", errInvalidTimeout, operationTimeout, err)
	}
	total := left - callbackMargin
	if total <= need {
		msg := fmt.Sprintf("%s for the result to reach the caller", callbackMargin)
		if needText != "" {
			msg = needText + " plus " + msg
		}
		return 0, fmt.Errorf("%w: the caller's Nexus operation times out in %s, which leaves no time to run: "+
			"it needs more than %s; give the operation a longer timeout%s",
			errInvalidTimeout, left.Round(time.Millisecond), msg, prefixSpace(fix))
	}
	return min(total, limit), nil
}

func prefixSpace(s string) string {
	if s == "" {
		return ""
	}
	return " " + s
}
