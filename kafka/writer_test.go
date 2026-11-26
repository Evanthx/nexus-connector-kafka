package kafka_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/test/testenv"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// A write Kafka partly rejects reports each record not written, by index, and every other record
// is in the topic.
func TestWriter_PartlyRejected(t *testing.T) {
	cfg, _ := testenv.Refusing(t, func(p int32) bool { return p == 1 }, kerr.MessageTooLarge)
	w, err := kafka.NewWriter(cfg)
	require.NoError(t, err)
	t.Cleanup(w.Close)

	const n = 30
	records := make([]*kgo.Record, n)
	for i := range records {
		// Keys spread the records over the partitions.
		records[i] = &kgo.Record{Topic: testenv.Topic, Key: fmt.Appendf(nil, "key-%d", i), Value: fmt.Appendf(nil, "%d", i)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = w.Write(ctx, records...)
	require.ErrorIs(t, err, kafka.ErrRejected)
	var we *kafka.WriteError
	require.ErrorAs(t, err, &we)
	require.Equal(t, n, we.Records)
	require.NotEmpty(t, we.Failures)
	require.Less(t, len(we.Failures), n, "only partition 1 refuses")

	failed := map[int]bool{}
	for i, f := range we.Failures {
		require.True(t, f.Rejected)
		require.ErrorIs(t, f.Err, kerr.MessageTooLarge)
		require.Equal(t, int32(1), records[f.Index].Partition, "message %d", f.Index)
		if i > 0 {
			require.Greater(t, f.Index, we.Failures[i-1].Index, "in order")
		}
		failed[f.Index] = true
	}
	// Every record not listed was written.
	var want []string
	for i := range n {
		if !failed[i] {
			want = append(want, fmt.Sprint(i))
		}
	}
	require.ElementsMatch(t, want, consumeValues(t, cfg, len(want)))
	require.Contains(t, err.Error(), fmt.Sprintf("%d of %d messages weren't written (message %d: MESSAGE_TOO_LARGE", len(we.Failures), n, we.Failures[0].Index))
	require.Contains(t, err.Error(), "the others were written")
	if len(we.Failures) > 3 {
		require.Contains(t, err.Error(), fmt.Sprintf("and %d more", len(we.Failures)-3))
	}
}

// A write that fails only with errors retrying could fix isn't Rejected.
func TestWriter_RetryableIsNotRejected(t *testing.T) {
	w, err := kafka.NewWriter(kafka.Config{Brokers: []string{"127.0.0.1:1"}})
	require.NoError(t, err)
	t.Cleanup(w.Close)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = w.Write(ctx, &kgo.Record{Topic: "t", Value: []byte("a")}, &kgo.Record{Topic: "t", Value: []byte("b")})
	require.NotErrorIs(t, err, kafka.ErrRejected)
	var we *kafka.WriteError
	require.ErrorAs(t, err, &we)
	require.Equal(t, []int{0, 1}, indexes(we))
}

// Errors kgo marks not retriable but a later attempt can get past, such as a broker's
// UNKNOWN_SERVER_ERROR, aren't Rejected, so the caller's server retries the write.
func TestWriter_UnknownServerErrorIsNotRejected(t *testing.T) {
	cfg, refused := testenv.Refusing(t, func(int32) bool { return true }, kerr.UnknownServerError)
	w, err := kafka.NewWriter(cfg)
	require.NoError(t, err)
	t.Cleanup(w.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = w.Write(ctx, &kgo.Record{Topic: testenv.Topic, Value: []byte("a")})
	require.NotErrorIs(t, err, kafka.ErrRejected)
	var we *kafka.WriteError
	require.ErrorAs(t, err, &we)
	require.Equal(t, []int{0}, indexes(we))
	require.ErrorIs(t, we.Failures[0].Err, kerr.UnknownServerError, "Kafka refused it")
	require.False(t, we.Failures[0].Rejected)
	require.Positive(t, refused())
}

func indexes(we *kafka.WriteError) []int {
	var out []int
	for _, f := range we.Failures {
		out = append(out, f.Index)
	}
	return slices.Clip(out)
}

// consumeValues reads n values from cfg's topic, from every partition, without a consumer group.
func consumeValues(t *testing.T, cfg kafka.Config, n int) []string {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumeTopics(testenv.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var out []string
	for len(out) < n {
		f := cl.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "read %d of %d", len(out), n)
		for _, r := range f.Records() {
			out = append(out, string(r.Value))
		}
	}
	return out
}
