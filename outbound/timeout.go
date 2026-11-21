package main

import (
	"time"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/deadline"
)

// defaultWriteMax is how long a write may take, retries included, unless KAFKA_MAX_DURATION_MINUTES
// says otherwise.
const defaultWriteMax = 10 * time.Minute

// writeScheduleToClose is how long a write may take in all: maxDuration (KAFKA_MAX_DURATION_MINUTES),
// or defaultWriteMax if unset, or less if the caller's Nexus operation times out first
// (deadline.ScheduleToClose). operationTimeout is the operation-timeout header.
func writeScheduleToClose(maxDuration time.Duration, operationTimeout string) (time.Duration, error) {
	limit := defaultWriteMax
	if maxDuration > 0 {
		limit = maxDuration
	}
	return deadline.ScheduleToClose(operationTimeout, limit, 0)
}
