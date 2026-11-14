package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/twmb/franz-go/pkg/kgo"
)

// batch collects records up to a number of records.
type batch struct {
	maxRecords int
	records    []kafkaapi.Record
}

func newBatch(maxRecords int) *batch {
	return &batch{maxRecords: maxRecords, records: []kafkaapi.Record{}}
}

// add adds r.
func (b *batch) add(r kafkaapi.Record) {
	b.records = append(b.records, r)
}

// full reports whether the batch takes no more records.
func (b *batch) full() bool {
	return len(b.records) >= b.maxRecords
}

// EncodedSize is the number of bytes r adds to an encoded batch: its JSON, as Temporal carries
// it, plus the comma between records. It is also about the size of r as a Nexus operation's input.
func EncodedSize(r kafkaapi.Record) int {
	b, err := json.Marshal(r)
	if err != nil {
		return 0 // can't happen for a record read from Kafka; count it as free rather than fail
	}
	return len(b) + 1
}

// pollUntil calls poll until done reports true or, if timeLimit isn't 0, until timeLimit has
// passed. Each poll gets a context that ends at the time limit, and blocks until it has records or
// its context ends. With no time limit, poll's context is ctx itself: done must become true.
func pollUntil(ctx context.Context, timeLimit time.Duration, done func() bool, poll func(context.Context) error) error {
	pollCtx := ctx
	if timeLimit > 0 {
		var cancel context.CancelFunc
		pollCtx, cancel = context.WithTimeout(ctx, timeLimit)
		defer cancel()
	}
	for !done() {
		err := poll(pollCtx)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if pollCtx.Err() != nil {
			return nil // the time limit passed
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// fetchErrors returns the errors in fetches that should end a read. A context error only means a
// poll's time was up. franz-go reports data loss (records removed before they were read, after
// which it resumes from the oldest left) and a lost group session (after which it rejoins) to
// inform, and recovers from both itself, so they are logged rather than returned.
func fetchErrors(fetches kgo.Fetches) error {
	var errs error
	fetches.EachError(func(topic string, part int32, err error) {
		var dataLoss *kgo.ErrDataLoss
		var lostSession *kgo.ErrGroupSession
		switch {
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		case errors.As(err, &dataLoss), errors.As(err, &lostSession):
			slog.Warn("Kafka reported a problem and recovered", "topic", topic, "partition", part, "error", err)
		default:
			errs = errors.Join(errs, fmt.Errorf("fetch %s[%d]: %w", topic, part, err))
		}
	})
	return errs
}
