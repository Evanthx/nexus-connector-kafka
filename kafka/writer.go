package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrRejected means Kafka refused a write for a reason that retrying won't change, such as a
// record larger than the broker allows or a topic the Outbound Connector may not write to.
var ErrRejected = errors.New("kafka rejected the write")

// Writer writes records and waits for the brokers to acknowledge them.
type Writer struct {
	client *kgo.Client
}

// NewWriter connects to the brokers in cfg.
func NewWriter(cfg Config) (*Writer, error) {
	client, err := kgo.NewClient(cfg.clientOpts()...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	return &Writer{client: client}, nil
}

// RecordFailure is a record a write didn't write.
type RecordFailure struct {
	// Index is the record's position in the write, from 0.
	Index int
	// Err is Kafka's error for it.
	Err error
	// Rejected is true if retrying won't change it.
	Rejected bool
}

// WriteError is a write that failed for some of its records. Kafka acknowledged every other
// record: those were written. It wraps ErrRejected if any record was rejected.
type WriteError struct {
	// Failures are the records not written, in order.
	Failures []RecordFailure
	// Records is how many records the write had.
	Records int
}

// maxListed is how many failures WriteError's message lists; Failures has them all.
const maxListed = 3

func (e *WriteError) Error() string {
	var b strings.Builder
	if e.rejected() {
		b.WriteString(ErrRejected.Error() + ": ")
	}
	fmt.Fprintf(&b, "%d of %d messages weren't written (", len(e.Failures), e.Records)
	for i, f := range e.Failures[:min(len(e.Failures), maxListed)] {
		if i > 0 {
			b.WriteString("; ")
		}
		fmt.Fprintf(&b, "message %d: %v", f.Index, f.Err)
	}
	if more := len(e.Failures) - maxListed; more > 0 {
		fmt.Fprintf(&b, "; and %d more", more)
	}
	b.WriteString("); the others were written")
	return b.String()
}

// Unwrap is ErrRejected if any record was rejected.
func (e *WriteError) Unwrap() error {
	if e.rejected() {
		return ErrRejected
	}
	return nil
}

func (e *WriteError) rejected() bool {
	for _, f := range e.Failures {
		if f.Rejected {
			return true
		}
	}
	return false
}

// Write writes records in order and waits for Kafka to acknowledge each. If any isn't written, the
// error is a *WriteError listing them; every other record was written. It wraps ErrRejected if
// any record won't succeed if retried.
func (w *Writer) Write(ctx context.Context, records ...*kgo.Record) error {
	index := make(map[*kgo.Record]int, len(records))
	for i, r := range records {
		index[r] = i
	}
	var failures []RecordFailure
	for _, r := range w.client.ProduceSync(ctx, records...) {
		if r.Err == nil {
			continue
		}
		var ke *kerr.Error
		failures = append(failures, RecordFailure{Index: index[r.Record], Err: r.Err, Rejected: errors.As(r.Err, &ke) && !ke.Retriable})
	}
	if failures == nil {
		return nil
	}
	slices.SortFunc(failures, func(a, b RecordFailure) int { return a.Index - b.Index })
	return &WriteError{Failures: failures, Records: len(records)}
}

// Close closes the writer's connections.
func (w *Writer) Close() {
	w.client.Close()
}
