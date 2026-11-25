package main

import (
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"go.temporal.io/sdk/temporal"
)

// ErrorTypeRejected is the type of the ApplicationError a caller gets when Kafka refused the
// write for a reason retrying can't fix, for example a message larger than the broker allows.
// Callers find it with errors.As. Any other failure is retried until the caller's Nexus operation
// times out.
const ErrorTypeRejected = "Rejected"

// handlerError turns an error from the kafka package into what the write operation returns. A
// rejected write is a non-retryable handler error, which ends the operation, carrying Kafka's
// message and a Rejected ApplicationError the caller finds with errors.As. Any other error is a
// retryable UNAVAILABLE handler error, which the caller's server retries.
func handlerError(err error) error {
	if errors.Is(err, kafka.ErrRejected) {
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeInternal,
			Message:       err.Error(),
			Cause:         temporal.NewApplicationErrorWithOptions(err.Error(), ErrorTypeRejected, temporal.ApplicationErrorOptions{NonRetryable: true}),
			RetryBehavior: nexus.HandlerErrorRetryBehaviorNonRetryable,
		}
	}
	return &nexus.HandlerError{Type: nexus.HandlerErrorTypeUnavailable, Message: err.Error(), Cause: err}
}
