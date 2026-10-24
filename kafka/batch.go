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

// batch collects records up to a number of records and, if maxBytes isn't 0, a number of encoded
// bytes. A batch always takes its first record, however large, so a large record can't stop
// reading; the caller decides what to do with a batch that is too big (see ErrRecordTooLarge).
type batch struct {
	maxRecords int
	maxBytes   int
	records    []kafkaapi.Record
	bytes      int
	refused    bool // a record didn't fit, so the batch takes no more
}

func newBatch(maxRecords, maxBytes int) *batch {
	return &batch{maxRecords: maxRecords, maxBytes: maxBytes, records: []kafkaapi.Record{}}
}

// add adds r and reports whether it fit. Once a record doesn't fit, the batch refuses every later
// one, so no record is skipped in favor of a smaller one after it.
func (b *batch) add(r kafkaapi.Record) bool {
	if b.full() {
		return false
	}
	size := EncodedSize(r)
	if b.maxBytes > 0 && len(b.records) > 0 && b.bytes+size > b.maxBytes {
		b.refused = true
		return false
	}
	b.records = append(b.records, r)
	b.bytes += size
	return true
}

// full reports whether the batch takes no more records.
func (b *batch) full() bool {
	return b.refused || len(b.records) >= b.maxRecords
}

// tooLarge reports whether the batch's encoded size is over its byte limit, which happens only
// when its one record is.
func (b *batch) tooLarge() bool {
	return b.maxBytes > 0 && b.bytes > b.maxBytes
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
