package main

import (
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"go.temporal.io/sdk/temporal"
)

// Error types of the ApplicationError a caller gets when a write ends with messages not written.
// Both are final: the caller's server doesn't retry the write. Their details are a
// kafkaapi.WriteFailures: every message not written, by index, with Kafka's reason; every message
// not listed was written. Callers find them with errors.As. A write that wrote nothing and failed
// only with errors retrying could fix is retried instead, until the caller's Nexus operation
// times out.
const (
	// ErrorTypeRejected: Kafka refused one or more messages for a reason retrying can't fix, for
	// example a message larger than the broker allows.
	ErrorTypeRejected = "Rejected"
	// ErrorTypePartlyWritten: some messages were written and the others failed with errors
	// retrying could fix. The write isn't retried as a whole, which would write the others again:
	// resend only the listed messages.
	ErrorTypePartlyWritten = "PartlyWritten"
)

// handlerError turns an error from the kafka package into what the write operation returns:
//
//   - a write with a rejected message: Rejected, final;
//   - a write that wrote some messages and not others: PartlyWritten, final;
//   - anything else, a write that wrote nothing: a retryable UNAVAILABLE handler error, so the
//     caller's server retries the whole write.
//
// A final one is a non-retryable handler error carrying Kafka's message and the ApplicationError,
// with the messages not written in its details.
func handlerError(err error) error {
	var we *kafka.WriteError
	isWriteErr := errors.As(err, &we)
	switch {
	case errors.Is(err, kafka.ErrRejected):
		return finalError(err, ErrorTypeRejected, we)
	case isWriteErr && len(we.Failures) < we.Records:
		return finalError(err, ErrorTypePartlyWritten, we)
	}
	return &nexus.HandlerError{Type: nexus.HandlerErrorTypeUnavailable, Message: err.Error(), Cause: err}
}

// finalError is the non-retryable handler error for err, with an ApplicationError of errType
// whose details list we's failures, if we isn't nil.
func finalError(err error, errType string, we *kafka.WriteError) error {
	opts := temporal.ApplicationErrorOptions{NonRetryable: true}
	if we != nil {
		opts.Details = []any{writeFailures(we)}
	}
	return &nexus.HandlerError{
		Type:          nexus.HandlerErrorTypeInternal,
		Message:       err.Error(),
		Cause:         temporal.NewApplicationErrorWithOptions(err.Error(), errType, opts),
		RetryBehavior: nexus.HandlerErrorRetryBehaviorNonRetryable,
	}
}

// writeFailures is the details of a final error: every message we lists, rejected or not.
func writeFailures(we *kafka.WriteError) kafkaapi.WriteFailures {
	out := kafkaapi.WriteFailures{Failures: make([]kafkaapi.WriteFailure, len(we.Failures))}
	for i, f := range we.Failures {
		out.Failures[i] = kafkaapi.WriteFailure{Index: int64(f.Index), Reason: f.Err.Error()}
	}
	return out
}
