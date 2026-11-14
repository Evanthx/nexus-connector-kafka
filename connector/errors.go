package main

import (
	"errors"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"go.temporal.io/sdk/temporal"
)

// Error types. A failure retrying can't fix is an ApplicationError with one of these as its
// type; callers find it with errors.As. Any other failure is retried until the operation times
// out.
const (
	// ErrorTypeInvalidRequest: the caller's Nexus operation times out too soon to run the write.
	ErrorTypeInvalidRequest = "InvalidRequest"
	// ErrorTypeRejected: Kafka refused the write, for example because a message is larger than the
	// broker allows.
	ErrorTypeRejected = "Rejected"
)

// applicationError turns an error from the kafka package into the ApplicationError a caller
// sees. Errors retrying can't fix become non-retryable; others are returned as they are.
func applicationError(err error) error {
	errType := ""
	switch {
	case errors.Is(err, errInvalidTimeout):
		errType = ErrorTypeInvalidRequest
	case errors.Is(err, kafka.ErrRejected):
		errType = ErrorTypeRejected
	default:
		return err
	}
	return temporal.NewApplicationErrorWithOptions(err.Error(), errType, temporal.ApplicationErrorOptions{NonRetryable: true})
}
