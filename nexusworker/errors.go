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
	// ErrorTypeInvalidRequest: the ack isn't a well-formed position for this group and topic, or
	// it is from an earlier topic of the same name. Positions aren't signed: callers are trusted
	// to pass back what a read returned.
	ErrorTypeInvalidRequest = "InvalidRequest"
	// ErrorTypeGroupInUse: the group has live members, such as a streamer, so its offsets can't
	// be committed from here. Use a group of its own.
	ErrorTypeGroupInUse = "GroupInUse"
	// ErrorTypeRecordTooLarge: the next record is larger than the batch byte limit on its own.
	ErrorTypeRecordTooLarge = "RecordTooLarge"
	// ErrorTypeRejected: Kafka refused the write, for example because a message is larger than the
	// broker allows.
	ErrorTypeRejected = "Rejected"
)

// applicationError turns an error from the kafka package into the ApplicationError a caller
// sees. Errors retrying can't fix become non-retryable; others are returned as they are.
func applicationError(err error) error {
	errType := ""
	switch {
	case errors.Is(err, kafka.ErrInvalidPosition):
		errType = ErrorTypeInvalidRequest
	case errors.Is(err, kafka.ErrGroupInUse):
		errType = ErrorTypeGroupInUse
	case errors.Is(err, kafka.ErrRecordTooLarge):
		errType = ErrorTypeRecordTooLarge
	case errors.Is(err, kafka.ErrRejected):
		errType = ErrorTypeRejected
	default:
		return err
	}
	return temporal.NewApplicationErrorWithOptions(err.Error(), errType, temporal.ApplicationErrorOptions{NonRetryable: true})
}
