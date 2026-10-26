package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/test/testenv"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.temporal.io/api/serviceerror"
)

// fakeStarter records each start. fail, if set, decides an attempt's error.
type fakeStarter struct {
	mu       sync.Mutex
	started  []kafkaapi.Record   // every record started, in start order
	batches  [][]kafkaapi.Record // each start's records
	ids      []string            // each start's operation ID
	attempts map[string]int
	fail     func(input kafkaapi.ReceiveInput, attempt int) error
	notify   chan struct{}
}

func newFakeStarter() *fakeStarter {
	return &fakeStarter{attempts: map[string]int{}, notify: make(chan struct{}, 1000)}
}

func (f *fakeStarter) start(_ context.Context, id string, input kafkaapi.ReceiveInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts[id]++
	if f.fail != nil {
		if err := f.fail(input, f.attempts[id]); err != nil {
			return err
		}
	}
	f.started = append(f.started, input.Records...)
	f.batches = append(f.batches, input.Records)
	f.ids = append(f.ids, id)
	f.notify <- struct{}{}
	return nil
}

func (f *fakeStarter) values() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.started))
	for i, r := range f.started {
		out[i] = string(r.Value)
	}
	return out
}

// waitFor waits until operations have started for n records.
func (f *fakeStarter) waitFor(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(30 * time.Second)
	for {
		f.mu.Lock()
		got := len(f.started)
		f.mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-f.notify:
		case <-deadline:
			t.Fatalf("started %d of %d operations: %v", got, n, f.values())
		}
	}
}

func produce(t *testing.T, cfg kafka.Config, partition int32, values ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	defer cl.Close()
	for _, v := range values {
		require.NoError(t, cl.ProduceSync(context.Background(), &kgo.Record{Topic: cfg.Topic, Partition: partition, Value: []byte(v)}).FirstErr())
	}
}

// deadLetters reads the dead-letter topic until it has n records.
func deadLetters(t *testing.T, cfg kafka.Config, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumeTopics(testenv.DeadLetterTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var out []*kgo.Record
	for ctx.Err() == nil {
		out = append(out, cl.PollFetches(ctx).Records()...)
		if len(out) >= n {
			break
		}
	}
	return out
}

// running is a streamer running in the background.
type running struct {
	stop func()
	done chan error
}

// stream starts a streamer for group with starter, and stops it when the test ends. options, if
// any, change the streamer before it starts.
func stream(t *testing.T, cfg kafka.Config, group string, starter operationStarter, maxInputBytes int, options ...func(*streamer)) running {
	t.Helper()
	member, err := kafka.NewGroupMember(cfg, group)
	require.NoError(t, err)
	writer, err := kafka.NewWriter(cfg)
	require.NoError(t, err)
	s := &streamer{
		member:          member,
		starter:         starter,
		deadLetters:     writer,
		identify:        member.Identity,
		topic:           cfg.Topic,
		group:           group,
		deadLetterTopic: testenv.DeadLetterTopic,
		batchSize:       10,
		timeLimit:       0,
		maxInputBytes:   maxInputBytes,
		logger:          slog.Default(),
		retryDelay:      func(int) time.Duration { return 10 * time.Millisecond },
	}
	for _, option := range options {
		option(s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := running{done: make(chan error, 1)}
	go func() { r.done <- s.run(ctx) }()
	var once sync.Once
	r.stop = func() {
		once.Do(func() {
			cancel()
			require.NoError(t, <-r.done)
			member.Close()
			writer.Close()
		})
	}
	t.Cleanup(r.stop)
	return r
}

const bigEnough = 2 * 1024 * 1024

// source is the streamer's source key for group, as main works it out.
func source(t *testing.T, cfg kafka.Config, group string) string {
	t.Helper()
	id, err := kafka.Identify(context.Background(), cfg)
	require.NoError(t, err)
	return sourceKey(id, cfg.Topic, group)
}

// opID is the operation ID for group "g" and the records of partition from first to last.
func opID(t *testing.T, cfg kafka.Config, partition, first, last int64) string {
	t.Helper()
	return operationID(source(t, cfg, "g"), []kafkaapi.Record{{Partition: partition, Offset: first}, {Partition: partition, Offset: last}})
}

// Each partition's part of a batch is one operation, named by its offsets, and within a partition
// messages start in order.
func TestStreamer_DeliversEachPartitionInOrder(t *testing.T) {
	cfg := testenv.Kafka(t)
	produce(t, cfg, 0, "a0", "a1", "a2", "a3")
	produce(t, cfg, 1, "b0", "b1")
	starter := newFakeStarter()
	stream(t, cfg, "g", starter, bigEnough)
	starter.waitFor(t, 6)

	var a, b []string
	for _, v := range starter.values() {
		if strings.HasPrefix(v, "a") {
			a = append(a, v)
		} else {
			b = append(b, v)
		}
	}
	require.Equal(t, []string{"a0", "a1", "a2", "a3"}, a)
	require.Equal(t, []string{"b0", "b1"}, b)
	starter.mu.Lock()
	defer starter.mu.Unlock()
	require.Contains(t, starter.ids, opID(t, cfg, 0, 0, 3))
	require.Contains(t, starter.ids, opID(t, cfg, 1, 0, 1))
	for i, batch := range starter.batches {
		require.Equal(t, operationID(source(t, cfg, "g"), batch), starter.ids[i])
		for _, rec := range batch {
			require.Equal(t, batch[0].Partition, rec.Partition, "a batch holds one partition")
		}
	}
}

// A partition's records are split into as few operations as fit the input size limit.
func TestStreamer_SplitsBatchesToFitTheInputLimit(t *testing.T) {
	cfg := testenv.Kafka(t)
	value := strings.Repeat("x", 300)
	produce(t, cfg, 0, value, value, value, value, value)
	// Room for two records; timestamps vary in length by a few bytes, far less than half a record.
	recSize := kafka.EncodedSize(kafkaapi.Record{Topic: "events", Timestamp: "2026-10-23T00:00:00.123456789Z", Value: []byte(value)})
	starter := newFakeStarter()
	stream(t, cfg, "g", starter, inputOverhead+2*recSize+recSize/2)
	starter.waitFor(t, 5)

	starter.mu.Lock()
	defer starter.mu.Unlock()
	var offsets []int64
	for _, batch := range starter.batches {
		require.LessOrEqual(t, len(batch), 2)
		for _, rec := range batch {
			offsets = append(offsets, rec.Offset)
		}
	}
	require.Equal(t, []int64{0, 1, 2, 3, 4}, offsets)
	require.Contains(t, starter.ids, opID(t, cfg, 0, 0, 1))
}

// A delivered batch is committed: a streamer started later for the group delivers only new
// messages.
func TestStreamer_RestartDoesNotRedeliver(t *testing.T) {
	cfg := testenv.Kafka(t)
	produce(t, cfg, 0, "m0", "m1")
	first := newFakeStarter()
	r := stream(t, cfg, "g", first, bigEnough)
	first.waitFor(t, 2)
	r.stop()

	produce(t, cfg, 0, "m2")
	second := newFakeStarter()
	stream(t, cfg, "g", second, bigEnough)
	second.waitFor(t, 1)
	// Anything redelivered would arrive with m2.
	require.Never(t, func() bool { return len(second.values()) > 1 }, time.Second, 50*time.Millisecond)
	require.Equal(t, []string{"m2"}, second.values())
}

// A streamer stopped mid-batch commits nothing, so the next one delivers the whole batch again. If
// the batch is the same, so is its operation ID: the operation already started matches.
func TestStreamer_UncommittedBatchIsDeliveredAgainWithTheSameID(t *testing.T) {
	cfg := testenv.Kafka(t)
	produce(t, cfg, 0, "m0", "m1", "m2")
	stuck := newFakeStarter()
	stuck.fail = func(kafkaapi.ReceiveInput, int) error { return errors.New("unavailable") }
	r := stream(t, cfg, "g", stuck, bigEnough)
	id := opID(t, cfg, 0, 0, 2)
	require.Eventually(t, func() bool {
		stuck.mu.Lock()
		defer stuck.mu.Unlock()
		return stuck.attempts[id] > 0
	}, 30*time.Second, 10*time.Millisecond)
	r.stop()

	again := newFakeStarter()
	stream(t, cfg, "g", again, bigEnough)
	again.waitFor(t, 3)
	require.Equal(t, []string{"m0", "m1", "m2"}, again.values())
	require.Equal(t, []string{opID(t, cfg, 0, 0, 2)}, again.ids)
}

// The duplicate the README warns about: a batch delivered again can end at a different offset,
// so it gets a different operation ID and its messages start a second operation.
func TestStreamer_RedeliveredBatchWithMoreMessagesIsANewOperation(t *testing.T) {
	cfg := testenv.Kafka(t)
	produce(t, cfg, 0, "m0", "m1")
	produce(t, cfg, 1, "other")
	// Partition 0's operation starts; partition 1's never does, so the batch isn't committed.
	stuck := newFakeStarter()
	stuck.fail = func(input kafkaapi.ReceiveInput, _ int) error {
		if input.Records[0].Partition == 1 {
			return errors.New("unavailable")
		}
		return nil
	}
	r := stream(t, cfg, "g", stuck, bigEnough)
	stuck.waitFor(t, 2)
	r.stop()
	require.Equal(t, []string{opID(t, cfg, 0, 0, 1)}, stuck.ids)

	produce(t, cfg, 0, "m2")
	again := newFakeStarter()
	stream(t, cfg, "g", again, bigEnough)
	again.waitFor(t, 4)
	require.Contains(t, again.ids, opID(t, cfg, 0, 0, 2))
	require.Subset(t, again.values(), []string{"m0", "m1"}, "m0 and m1 are delivered a second time")
}

// A batch that can't be delivered doesn't hold up a rebalance: when another streamer joins the
// group, the stuck one gives up its batch without committing it, so the partitions it hands over
// are delivered by the newcomer and the ones it keeps are delivered again, not skipped.
func TestStreamer_StuckBatchDoesNotBlockARebalance(t *testing.T) {
	cfg := testenv.Kafka(t)
	produce(t, cfg, 0, "a0", "a1")
	produce(t, cfg, 1, "b0", "b1")
	produce(t, cfg, 2, "c0", "c1")
	var down atomic.Bool
	down.Store(true)
	stuck := newFakeStarter()
	stuck.fail = func(kafkaapi.ReceiveInput, int) error {
		if down.Load() {
			return errors.New("unavailable")
		}
		return nil
	}
	stream(t, cfg, "g", stuck, bigEnough)
	require.Eventually(t, func() bool {
		stuck.mu.Lock()
		defer stuck.mu.Unlock()
		return len(stuck.attempts) > 0
	}, 30*time.Second, 10*time.Millisecond, "the first streamer has a batch")

	newcomer := newFakeStarter()
	stream(t, cfg, "g", newcomer, bigEnough)
	// Kafka's default rebalance timeout is a minute; a blocked rebalance would take that long.
	newcomer.waitFor(t, 2)

	down.Store(false)
	require.Eventually(t, func() bool {
		got := append(stuck.values(), newcomer.values()...)
		for _, v := range []string{"a0", "a1", "b0", "b1", "c0", "c1"} {
			if !slices.Contains(got, v) {
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond, "every message is delivered by one streamer or the other")
}

// Errors other than an undeliverable message are retried until the start succeeds.
func TestStreamer_RetriesUntilStarted(t *testing.T) {
	cfg := testenv.Kafka(t)
	produce(t, cfg, 0, "m0")
	starter := newFakeStarter()
	starter.fail = func(_ kafkaapi.ReceiveInput, attempt int) error {
		if attempt < 3 {
			return errors.New("unavailable")
		}
		return nil
	}
	stream(t, cfg, "g", starter, bigEnough)
	starter.waitFor(t, 1)
	require.Equal(t, 3, starter.attempts[opID(t, cfg, 0, 0, 0)])
}

// Messages the server refuses go to the dead-letter topic, with headers saying where each came
// from and why, and the stream carries on and commits past them.
func TestStreamer_UndeliverableGoesToTheDeadLetterTopic(t *testing.T) {
	cfg := testenv.Kafka(t)
	produce(t, cfg, 0, "bad0", "bad1")
	produce(t, cfg, 1, "good")
	starter := newFakeStarter()
	starter.fail = func(input kafkaapi.ReceiveInput, _ int) error {
		if input.Records[0].Partition == 0 {
			return fmt.Errorf("%w: input exceeds size limit", errUndeliverable)
		}
		return nil
	}
	r := stream(t, cfg, "g", starter, bigEnough)
	starter.waitFor(t, 1)
	require.Equal(t, []string{"good"}, starter.values())

	dls := deadLetters(t, cfg, 2)
	require.Len(t, dls, 2)
	starter.mu.Lock()
	require.Equal(t, 1, starter.attempts[opID(t, cfg, 0, 0, 1)], "an undeliverable batch isn't retried")
	starter.mu.Unlock()
	for i, dl := range dls {
		require.Equal(t, fmt.Sprintf("bad%d", i), string(dl.Value))
		headers := map[string]string{}
		for _, h := range dl.Headers {
			headers[h.Key] = string(h.Value)
		}
		require.Equal(t, fmt.Sprintf("%s/0/%d", testenv.Topic, i), headers[kafka.DeadLetterSourceHeader])
		require.Equal(t, "g", headers[kafka.DeadLetterGroupHeader])
		require.Contains(t, headers[kafka.DeadLetterReasonHeader], "input exceeds size limit")
	}

	r.stop()
	produce(t, cfg, 0, "after")
	next := newFakeStarter()
	stream(t, cfg, "g", next, bigEnough)
	next.waitFor(t, 1)
	require.Equal(t, []string{"after"}, next.values(), "the dead-lettered messages were committed past")
}

// Messages the server refuses as too large together are split until each part is accepted, so
// none of them goes to the dead-letter topic.
func TestStreamer_RefusedPartIsSplit(t *testing.T) {
	cfg := testenv.Kafka(t)
	produce(t, cfg, 0, "m0", "m1", "m2", "m3")
	starter := newFakeStarter()
	starter.fail = func(input kafkaapi.ReceiveInput, _ int) error {
		if len(input.Records) > 1 {
			return fmt.Errorf("%w: input exceeds size limit", errUndeliverable)
		}
		return nil
	}
	stream(t, cfg, "g", starter, bigEnough)
	starter.waitFor(t, 4)
	require.Equal(t, []string{"m0", "m1", "m2", "m3"}, starter.values(), "delivered in order")
	require.Equal(t, []string{opID(t, cfg, 0, 0, 0), opID(t, cfg, 0, 1, 1), opID(t, cfg, 0, 2, 2), opID(t, cfg, 0, 3, 3)}, starter.ids)
}

// The topic's identity is asked for every batch, so a topic deleted and created again while the
// streamer runs gets operation IDs of its own.
func TestStreamer_IdentityIsCheckedForEveryBatch(t *testing.T) {
	cfg := testenv.Kafka(t)
	var current atomic.Value
	current.Store(kafka.TopicIdentity{Cluster: "c", TopicID: "first"})
	starter := newFakeStarter()
	stream(t, cfg, "g", starter, bigEnough, func(s *streamer) {
		s.identify = func(context.Context) (kafka.TopicIdentity, error) { return current.Load().(kafka.TopicIdentity), nil }
	})

	produce(t, cfg, 0, "m0")
	starter.waitFor(t, 1)
	second := kafka.TopicIdentity{Cluster: "c", TopicID: "second"}
	current.Store(second)
	produce(t, cfg, 0, "m1")
	starter.waitFor(t, 2)

	starter.mu.Lock()
	defer starter.mu.Unlock()
	require.Equal(t, operationID(sourceKey(second, cfg.Topic, "g"), starter.batches[1]), starter.ids[1])
}

// With DELETE_DEAD_LETTERS, a message that can't be delivered is deleted: nothing is written, and
// the stream commits past it.
func TestStreamer_DeleteDeadLetters(t *testing.T) {
	cfg := testenv.Kafka(t)
	produce(t, cfg, 0, "bad", "good")
	starter := newFakeStarter()
	starter.fail = func(input kafkaapi.ReceiveInput, _ int) error {
		if string(input.Records[0].Value) == "bad" {
			return fmt.Errorf("%w: input exceeds size limit", errUndeliverable)
		}
		return nil
	}
	r := stream(t, cfg, "g", starter, bigEnough, func(s *streamer) {
		s.deleteDeadLetters = true
		s.deadLetters = failingWriter{t}
	})
	starter.waitFor(t, 1)
	require.Equal(t, []string{"good"}, starter.values())
	require.Empty(t, deadLetters(t, cfg, 1), "nothing reaches the dead-letter topic")

	r.stop()
	produce(t, cfg, 0, "after")
	next := newFakeStarter()
	stream(t, cfg, "g", next, bigEnough)
	next.waitFor(t, 1)
	require.Equal(t, []string{"after"}, next.values(), "the deleted message was committed past")
}

// failingWriter fails the test if anything is written to the dead-letter topic.
type failingWriter struct{ t *testing.T }

func (w failingWriter) Write(context.Context, ...*kgo.Record) error {
	w.t.Error("wrote to the dead-letter topic")
	return nil
}

// A message too large to be an operation's input goes to the dead-letter topic without a start,
// and no operation spans it: the messages on either side of it start separate operations.
func TestStreamer_OversizedMessageGoesToTheDeadLetterTopic(t *testing.T) {
	cfg := testenv.Kafka(t)
	produce(t, cfg, 0, "small0", strings.Repeat("x", 500), "small1")
	starter := newFakeStarter()
	stream(t, cfg, "g", starter, 400)
	starter.waitFor(t, 2)
	require.Equal(t, []string{"small0", "small1"}, starter.values())
	require.Equal(t, []string{opID(t, cfg, 0, 0, 0), opID(t, cfg, 0, 2, 2)}, starter.ids)

	dls := deadLetters(t, cfg, 1)
	require.Len(t, dls, 1)
	require.Len(t, dls[0].Value, 500)
}

func TestOperationID(t *testing.T) {
	recs := []kafkaapi.Record{{Partition: 2, Offset: 41}, {Partition: 2, Offset: 45}}
	require.Equal(t, "kafka-src-2-41-45", operationID("src", recs))
	require.Equal(t, "kafka-src-2-41-41", operationID("src", recs[:1]))
}

// The source key is stable, and differs for every part of the source, including names that
// would run together if joined with the ID's separator.
func TestSourceKey(t *testing.T) {
	id := kafka.TopicIdentity{Cluster: "cluster", TopicID: "topic-id"}
	key := sourceKey(id, "orders", "billing")
	require.Len(t, key, 32)
	require.Equal(t, key, sourceKey(id, "orders", "billing"))
	// Topic "a-b" with group "c", and topic "a" with group "b-c", both join to "a-b-c".
	require.NotEqual(t, sourceKey(id, "a-b", "c"), sourceKey(id, "a", "b-c"))
	require.NotEqual(t, key, sourceKey(id, "orders", "shipping"))
	require.NotEqual(t, key, sourceKey(id, "orders2", "billing"))
	require.NotEqual(t, key, sourceKey(kafka.TopicIdentity{Cluster: "other", TopicID: "topic-id"}, "orders", "billing"))
	require.NotEqual(t, key, sourceKey(kafka.TopicIdentity{Cluster: "cluster", TopicID: "recreated"}, "orders", "billing"))
}

// The streamer finds the topic's identity in Kafka.
func TestIdentify(t *testing.T) {
	cfg := testenv.Kafka(t)
	id, err := kafka.Identify(context.Background(), cfg)
	require.NoError(t, err)
	require.NotEmpty(t, id.Cluster)
	require.NotEmpty(t, id.TopicID)
}

// Only a refusal for size counts as undeliverable; other refusals concern the setup, so they are
// retried rather than sending every batch to the dead-letter topic.
func TestRefusedAsTooLarge(t *testing.T) {
	require.True(t, refusedAsTooLarge(serviceerror.NewInvalidArgument("input exceeds size limit")))
	require.False(t, refusedAsTooLarge(serviceerror.NewInvalidArgument("schedule-to-close timeout exceeds the namespace maximum")))
	require.False(t, refusedAsTooLarge(serviceerror.NewNotFound("endpoint not found")))
}

func TestBackoff(t *testing.T) {
	require.Equal(t, time.Second, backoff(1))
	require.Equal(t, 2*time.Second, backoff(2))
	require.Equal(t, 16*time.Second, backoff(5))
	require.Equal(t, 30*time.Second, backoff(6))
	require.Equal(t, 30*time.Second, backoff(100))
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	t.Setenv("KAFKA_TOPIC", "events")
	t.Setenv("KAFKA_GROUP", "")
	t.Setenv("KAFKA_DEAD_LETTER_TOPIC", "")
	t.Setenv("STREAM_ENDPOINT", "")
	t.Setenv("DELETE_DEAD_LETTERS", "")
	_, err := configFromEnv()
	require.EqualError(t, err, "set KAFKA_GROUP, STREAM_ENDPOINT")

	t.Setenv("KAFKA_GROUP", "g")
	t.Setenv("STREAM_ENDPOINT", "ep")
	_, err = configFromEnv()
	require.ErrorContains(t, err, "set KAFKA_DEAD_LETTER_TOPIC, the topic undeliverable messages go to, or DELETE_DEAD_LETTERS=true")

	t.Setenv("KAFKA_DEAD_LETTER_TOPIC", "events-dlq")
	cfg, err := configFromEnv()
	require.NoError(t, err)
	require.False(t, cfg.deleteDeadLetters)
	require.Equal(t, 100, cfg.batchSize)
	require.Zero(t, cfg.timeLimit)
	require.Equal(t, 24*time.Hour, cfg.operationTimeout)
	require.Equal(t, kafkaapi.KafkaMessageReceiver.ServiceName, cfg.service)
	require.Equal(t, "receive", cfg.operation)

	t.Setenv("KAFKA_BATCH_TIME_LIMIT", "2s")
	t.Setenv("KAFKA_BATCH_SIZE", "500")
	cfg, err = configFromEnv()
	require.NoError(t, err)
	require.Equal(t, 2*time.Second, cfg.timeLimit)
	require.Equal(t, 500, cfg.batchSize)

	t.Setenv("KAFKA_DEAD_LETTER_TOPIC", "events")
	_, err = configFromEnv()
	require.ErrorContains(t, err, "must differ")
}

// DELETE_DEAD_LETTERS replaces KAFKA_DEAD_LETTER_TOPIC: one or the other, not both.
func TestConfigFromEnv_DeleteDeadLetters(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	t.Setenv("KAFKA_TOPIC", "events")
	t.Setenv("KAFKA_GROUP", "g")
	t.Setenv("STREAM_ENDPOINT", "ep")
	t.Setenv("KAFKA_DEAD_LETTER_TOPIC", "")
	t.Setenv("DELETE_DEAD_LETTERS", "true")
	cfg, err := configFromEnv()
	require.NoError(t, err)
	require.True(t, cfg.deleteDeadLetters)

	t.Setenv("KAFKA_DEAD_LETTER_TOPIC", "events-dlq")
	_, err = configFromEnv()
	require.ErrorContains(t, err, "not both")

	t.Setenv("KAFKA_DEAD_LETTER_TOPIC", "")
	t.Setenv("DELETE_DEAD_LETTERS", "yes please")
	_, err = configFromEnv()
	require.ErrorContains(t, err, `DELETE_DEAD_LETTERS must be true or false, not "yes please"`)
}

// The startup report shows every setting, marks the ones left to their defaults, never shows the
// password, and says what the streamer will do with undeliverable messages.
func TestReport(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	t.Setenv("KAFKA_TOPIC", "events")
	t.Setenv("KAFKA_USERNAME", "connector")
	t.Setenv("KAFKA_PASSWORD", "secret-password")
	t.Setenv("KAFKA_GROUP", "g")
	t.Setenv("STREAM_ENDPOINT", "ep")
	t.Setenv("KAFKA_DEAD_LETTER_TOPIC", "events-dlq")
	t.Setenv("DELETE_DEAD_LETTERS", "")
	t.Setenv("KAFKA_BATCH_SIZE", "")
	cfg, err := configFromEnv()
	require.NoError(t, err)
	report := cfg.report("kafka-connector", "localhost:7233")
	require.NotContains(t, report, "secret-password")
	require.Contains(t, report, "KAFKA_PASSWORD is set")
	require.Regexp(t, `KAFKA_BATCH_SIZE +100 \(default\)`, report)
	require.Regexp(t, `KAFKA_GROUP +g\n`, report)
	require.Contains(t, report, `Send messages that can't be delivered to the topic "events-dlq".`)
	require.Contains(t, report, "can be delivered twice")

	t.Setenv("KAFKA_DEAD_LETTER_TOPIC", "")
	t.Setenv("DELETE_DEAD_LETTERS", "true")
	cfg, err = configFromEnv()
	require.NoError(t, err)
	report = cfg.report("kafka-connector", "localhost:7233")
	require.Contains(t, report, "Delete messages that can't be delivered")
	require.Regexp(t, `KAFKA_DEAD_LETTER_TOPIC +\(not set\)`, report)
}
