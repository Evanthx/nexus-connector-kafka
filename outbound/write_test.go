package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/test/testenv"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.temporal.io/sdk/temporal"
)

// Each message's dedupe-id comes from the Nexus request ID and its place in the write, so every
// attempt of one operation gives a message the same dedupe-id.
func TestRecords_DedupeIDFromRequestID(t *testing.T) {
	w := &topicWriter{}
	in := kafkaapi.WriteInput{Topic: "orders", Messages: []kafkaapi.Message{{Value: []byte("a")}, {Value: []byte("b")}}}
	for range 2 { // two attempts
		recs := w.records("req-1", in)
		require.Len(t, recs, 2)
		for i, want := range []string{"kafka-write-req-1-0", "kafka-write-req-1-1"} {
			require.Equal(t, "orders", recs[i].Topic)
			require.Len(t, recs[i].Headers, 1)
			require.Equal(t, DedupeHeader, recs[i].Headers[0].Key)
			require.Equal(t, want, string(recs[i].Headers[0].Value))
		}
	}
	require.NotEqual(t, w.records("req-1", in)[0].Headers, w.records("req-2", in)[0].Headers, "another operation, another dedupe-id")
}

// writeKeyed writes n keyed messages, which spread over the partitions, through w with a ctx of
// the given timeout, and returns how long it took and the error.
func writeKeyed(t *testing.T, w *topicWriter, n int, timeout time.Duration) (time.Duration, error) {
	t.Helper()
	in := kafkaapi.WriteInput{Topic: testenv.Topic}
	for i := range n {
		in.Messages = append(in.Messages, kafkaapi.Message{Key: fmt.Appendf(nil, "key-%d", i), Value: fmt.Appendf(nil, "%d", i)})
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	_, err := w.write(ctx, "req", in)
	return time.Since(start), err
}

// refusingFirst is a Kafka whose partition 1 refuses its first n writes with UNKNOWN_SERVER_ERROR,
// which kgo marks final but writeAll tries again, and a topicWriter for it.
func refusingFirst(t *testing.T, n int32) (*topicWriter, kafka.Config, *atomic.Int32) {
	t.Helper()
	var refused atomic.Int32
	cfg, _ := testenv.Refusing(t, func(p int32) bool { return p == 1 && refused.Add(1) <= n }, kerr.UnknownServerError)
	writer, err := kafka.NewWriter(cfg)
	require.NoError(t, err)
	t.Cleanup(writer.Close)
	return newTopicWriter(writer, []string{testenv.Topic}), cfg, &refused
}

// requireEachOnce checks every one of the n messages is in the topic exactly once.
func requireEachOnce(t *testing.T, cfg kafka.Config, n int, except ...int) {
	t.Helper()
	var want []string
	for i := range n {
		if !slices.Contains(except, i) {
			want = append(want, fmt.Sprint(i))
		}
	}
	got := values(consume(t, cfg, len(want)))
	require.ElementsMatch(t, want, got, "each once")

	// And nothing more: the topic's partitions end after exactly that many records.
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	require.NoError(t, err)
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(context.Background(), testenv.Topic)
	require.NoError(t, err)
	var total int64
	ends.Each(func(o kadm.ListedOffset) { total += o.Offset })
	require.Equal(t, int64(len(want)), total, "no message written twice")
}

// A failure a moment later can get past is tried again at once, with only the failed records:
// every message is written exactly once.
func TestWriteAll_RetriesOnlyTheFailedAtOnce(t *testing.T) {
	w, cfg, _ := refusingFirst(t, 1)
	took, err := writeKeyed(t, w, 30, 8*time.Second)
	require.NoError(t, err)
	require.Less(t, took, 2*time.Second, "the second try is at once")
	requireEachOnce(t, cfg, 30)
}

// If the second try fails too, the third waits until halfway to the deadline.
func TestWriteAll_ThirdTryAtHalfway(t *testing.T) {
	w, cfg, _ := refusingFirst(t, 2)
	took, err := writeKeyed(t, w, 30, 4*time.Second)
	require.NoError(t, err)
	require.InDelta(t, 2*time.Second, took, float64(500*time.Millisecond), "halfway to the 4s deadline")
	requireEachOnce(t, cfg, 30)
}

// After three tries the failure is final: Rejected, listing only the failed records, and every
// other message was written once.
func TestWriteAll_FinalAfterThreeTries(t *testing.T) {
	w, cfg, refused := refusingFirst(t, 1000)
	took, err := writeKeyed(t, w, 30, 4*time.Second)
	require.ErrorIs(t, err, kafka.ErrRejected)
	var we *kafka.WriteError
	require.ErrorAs(t, err, &we)
	require.Equal(t, int32(3), refused.Load(), "three tries")
	require.Less(t, took, 3*time.Second)
	var failed []int
	for _, f := range we.Failures {
		require.True(t, f.Rejected)
		require.False(t, f.RetrySoon)
		require.ErrorIs(t, f.Err, kerr.UnknownServerError)
		failed = append(failed, f.Index)
	}
	require.NotEmpty(t, failed)
	require.Less(t, len(failed), 30)
	requireEachOnce(t, cfg, 30, failed...)
}

// A write that wrote some messages ends with a final error, so its caller resends only the rest;
// one that wrote none is retried by the caller's server.
func TestHandlerError_PartlyWrittenIsFinal(t *testing.T) {
	transient := errors.New("REQUEST_TIMED_OUT")
	partly := &kafka.WriteError{Records: 3, Failures: []kafka.RecordFailure{{Index: 1, Err: transient}}}
	var he *nexus.HandlerError
	require.ErrorAs(t, handlerError(partly), &he)
	require.False(t, he.Retryable())
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, he, &appErr)
	require.Equal(t, ErrorTypePartlyWritten, appErr.Type())
	var details kafkaapi.WriteFailures
	require.NoError(t, appErr.Details(&details))
	require.Equal(t, []kafkaapi.WriteFailure{{Index: 1, Reason: "REQUEST_TIMED_OUT"}}, details.Failures)

	none := &kafka.WriteError{Records: 2, Failures: []kafka.RecordFailure{{Index: 0, Err: transient}, {Index: 1, Err: transient}}}
	require.ErrorAs(t, handlerError(none), &he)
	require.True(t, he.Retryable(), "nothing was written: retrying duplicates nothing")
	require.Equal(t, nexus.HandlerErrorTypeUnavailable, he.Type)
}

// A write to a topic not on the list is Rejected before anything is sent to Kafka, with every
// message listed, and its message never shows the topics that are allowed.
func TestWrite_TopicNotAllowed(t *testing.T) {
	writer, err := kafka.NewWriter(kafka.Config{Brokers: []string{"127.0.0.1:1"}}) // never reached
	require.NoError(t, err)
	t.Cleanup(writer.Close)
	w := newTopicWriter(writer, []string{"orders", "billing"})
	in := kafkaapi.WriteInput{Topic: "payroll", Messages: []kafkaapi.Message{{Value: []byte("a")}, {Value: []byte("b")}, {Value: []byte("c")}}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = w.write(ctx, "req", in)
	require.Error(t, err)

	var he *nexus.HandlerError
	require.ErrorAs(t, handlerError(err), &he)
	require.False(t, he.Retryable())
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, he, &appErr)
	require.Equal(t, ErrorTypeRejected, appErr.Type())
	require.Equal(t, `topic "payroll" isn't one this Outbound Connector writes to`, appErr.Message())
	require.NotContains(t, he.Message, "orders")
	require.NotContains(t, he.Message, "billing")
	var details kafkaapi.WriteFailures
	require.NoError(t, appErr.Details(&details))
	reason := `topic "payroll" isn't one this Outbound Connector writes to`
	require.Equal(t, []kafkaapi.WriteFailure{{Index: 0, Reason: reason}, {Index: 1, Reason: reason}, {Index: 2, Reason: reason}}, details.Failures)
}
