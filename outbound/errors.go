package main

import (
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"go.temporal.io/sdk/temporal"
)

// ErrorTypeRejected is the type of the ApplicationError a caller gets when Kafka refused one or
// more of a write's messages for a reason retrying can't fix, for example a message larger than
// the broker allows. Its details are a kafkaapi.WriteFailures: every message not written, by
// index, with Kafka's reason; every message not listed was written. Callers find it with
// errors.As. Any other failure is retried until the caller's Nexus operation times out.
const ErrorTypeRejected = "Rejected"

// handlerError turns an error from the kafka package into what the write operation returns. A
// write with a rejected message is a non-retryable handler error, which ends the operation,
// carrying Kafka's message and a Rejected ApplicationError, with the messages not written in its
// details, that the caller finds with errors.As. Any other error is a retryable UNAVAILABLE
// handler error: the caller's server retries the whole write.
func handlerError(err error) error {
	if errors.Is(err, kafka.ErrRejected) {
		opts := temporal.ApplicationErrorOptions{NonRetryable: true}
		var we *kafka.WriteError
		if errors.As(err, &we) {
			opts.Details = []any{writeFailures(we)}
		}
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeInternal,
			Message:       err.Error(),
			Cause:         temporal.NewApplicationErrorWithOptions(err.Error(), ErrorTypeRejected, opts),
			RetryBehavior: nexus.HandlerErrorRetryBehaviorNonRetryable,
		}
	}
	return &nexus.HandlerError{Type: nexus.HandlerErrorTypeUnavailable, Message: err.Error(), Cause: err}
}

// writeFailures is the details of a Rejected error: every message we lists, rejected or not.
func writeFailures(we *kafka.WriteError) kafkaapi.WriteFailures {
	out := kafkaapi.WriteFailures{Failures: make([]kafkaapi.WriteFailure, len(we.Failures))}
	for i, f := range we.Failures {
		out.Failures[i] = kafkaapi.WriteFailure{Index: int64(f.Index), Reason: f.Err.Error()}
	}
	return out
}
