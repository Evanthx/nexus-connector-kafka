package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// DedupeHeader is the header each written message gets. Its value is the same on every attempt
// of one write, so a consumer can drop the copies a retried write leaves.
const DedupeHeader = "dedupe-id"

// topicWriter writes to the one topic the Outbound Connector serves.
type topicWriter struct {
	writer *kafka.Writer
	topic  string
}

// write writes in.Messages in order, in one attempt (see writeAll). Each gets a dedupe-id header
// built from requestID, the Nexus request ID, which is the same on every attempt of one operation.
func (w *topicWriter) write(ctx context.Context, requestID string, in kafkaapi.WriteInput) (kafkaapi.WriteOutput, error) {
	if err := w.writeAll(ctx, w.records(requestID, in)); err != nil {
		return kafkaapi.WriteOutput{}, err
	}
	return kafkaapi.WriteOutput{Written: int64(len(in.Messages))}, nil
}

// records are in.Messages as records for the topic, the i-th with the dedupe-id
// kafka-write-<requestID>-<i>.
func (w *topicWriter) records(requestID string, in kafkaapi.WriteInput) []*kgo.Record {
	records := make([]*kgo.Record, len(in.Messages))
	for i, m := range in.Messages {
		dedupe := kgo.RecordHeader{Key: DedupeHeader, Value: fmt.Appendf(nil, "kafka-write-%s-%d", requestID, i)}
		records[i] = kafka.ToKafkaRecord(w.topic, m, dedupe)
	}
	return records
}

// defaultHalfway is when the third try starts if ctx has no deadline.
const defaultHalfway = 5 * time.Second

// writeAll writes records, trying again within this attempt the ones that failed with an error
// kgo marks final but a moment later can get past (kafka.RecordFailure.RetrySoon): once at once,
// then, if that is before halfway to ctx's deadline, once more at halfway, giving Kafka a few
// seconds to recover. Only the records that failed are sent again. A RetrySoon failure left after
// the last try is final (rejected). The error is nil, or a *kafka.WriteError listing every record
// not written by its index in records.
func (w *topicWriter) writeAll(ctx context.Context, records []*kgo.Record) error {
	start := time.Now()
	halfway := start.Add(defaultHalfway)
	if d, ok := ctx.Deadline(); ok {
		halfway = start.Add(d.Sub(start) / 2)
	}
	err := w.writer.Write(ctx, records...)
	for try := 2; try <= 3; try++ {
		var we *kafka.WriteError
		if !errors.As(err, &we) {
			return err
		}
		retry := retrySoon(we)
		if len(retry) == 0 {
			break
		}
		if try == 3 && !waitUntil(ctx, halfway) {
			break
		}
		subset := make([]*kgo.Record, len(retry))
		for i, idx := range retry {
			subset[i] = records[idx]
		}
		err = merge(we, retry, w.writer.Write(ctx, subset...))
	}
	return finalize(err)
}

// retrySoon is the indexes of we's RetrySoon failures.
func retrySoon(we *kafka.WriteError) []int {
	var out []int
	for _, f := range we.Failures {
		if f.RetrySoon {
			out = append(out, f.Index)
		}
	}
	return out
}

// waitUntil waits until t, if it is still ahead, and reports whether a try may start then: false
// if t has passed already or ctx ended first.
func waitUntil(ctx context.Context, t time.Time) bool {
	wait := time.Until(t)
	if wait <= 0 {
		return false
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// merge is we with the records at retried, which were sent again, replaced by how that went:
// retryErr is that write's error, whose indexes are positions in retried.
func merge(we *kafka.WriteError, retried []int, retryErr error) error {
	var failures []kafka.RecordFailure
	for _, f := range we.Failures {
		if !slices.Contains(retried, f.Index) {
			failures = append(failures, f)
		}
	}
	var re *kafka.WriteError
	if errors.As(retryErr, &re) {
		for _, f := range re.Failures {
			f.Index = retried[f.Index]
			failures = append(failures, f)
		}
	}
	if len(failures) == 0 {
		return nil
	}
	slices.SortFunc(failures, func(a, b kafka.RecordFailure) int { return a.Index - b.Index })
	return &kafka.WriteError{Failures: failures, Records: we.Records}
}

// finalize makes any RetrySoon failure left in err final: there is no try left.
func finalize(err error) error {
	var we *kafka.WriteError
	if !errors.As(err, &we) {
		return err
	}
	for i := range we.Failures {
		if we.Failures[i].RetrySoon {
			we.Failures[i].RetrySoon, we.Failures[i].Rejected = false, true
		}
	}
	return we
}
