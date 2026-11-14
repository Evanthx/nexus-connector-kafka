package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/settings"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

const testTopic = "events"

// partialWait is the time limit for tests that expect a partial batch when it passes. It is longer
// than the client's 5-second fetch wait, because the in-memory Kafka sometimes answers a fetch only
// when that wait ends, even when it has records.
const partialWait = 6 * time.Second

// newCluster starts an in-memory Kafka cluster with a three-partition topic.
func newCluster(t *testing.T) Config {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, testTopic))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return Config{Brokers: c.ListenAddrs(), Topic: testTopic, StartFrom: StartFromOldest}
}

// produce writes values to one partition.
func produce(t *testing.T, cfg Config, partition int32, values ...string) {
	t.Helper()
	require.NoError(t, write(cfg, partition, values...))
}

// produceAfter writes values to one partition after delay, while the test goes on. The test fails
// if the write does.
func produceAfter(t *testing.T, cfg Config, delay time.Duration, partition int32, values ...string) {
	t.Helper()
	errs := make(chan error, 1)
	time.AfterFunc(delay, func() { errs <- write(cfg, partition, values...) })
	t.Cleanup(func() { require.NoError(t, <-errs) })
}

func write(cfg Config, partition int32, values ...string) error {
	return writeAt(cfg, partition, time.Time{}, values...)
}

// produceAt writes values to one partition with the timestamp at, so a test can order messages
// across partitions without waiting for the clock.
func produceAt(t *testing.T, cfg Config, partition int32, at time.Time, values ...string) {
	t.Helper()
	require.NoError(t, writeAt(cfg, partition, at, values...))
}

// writeAt writes values to one partition. A zero at lets the client stamp each record with the
// time it is produced.
func writeAt(cfg Config, partition int32, at time.Time, values ...string) error {
	cl, err := kgo.NewClient(cfg.clientOpts(kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	if err != nil {
		return err
	}
	defer cl.Close()
	recs := make([]*kgo.Record, len(values))
	for i, v := range values {
		recs[i] = &kgo.Record{Topic: cfg.Topic, Partition: partition, Value: []byte(v), Timestamp: at}
	}
	return cl.ProduceSync(context.Background(), recs...).FirstErr()
}

func values(recs []kafkaapi.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.Value)
	}
	return out
}

func TestStartOffset(t *testing.T) {
	for _, tc := range []struct {
		name      string
		committed int64
		known     bool
		startFrom StartFrom
		want      int64
	}{
		{"committed, in the log", 5, true, StartFromOldest, 5},
		{"committed, in the log, newest", 5, true, StartFromNewest, 5},
		{"committed at the end", 10, true, StartFromOldest, 10},
		{"none, oldest", 0, false, StartFromOldest, 2},
		{"none, newest", 0, false, StartFromNewest, 10},
		{"removed by retention", 1, true, StartFromOldest, 2},
		{"past the end", 11, true, StartFromOldest, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, startOffset(tc.committed, tc.known, 2, 10, tc.startFrom))
		})
	}
}

func newMember(t *testing.T, cfg Config, group string) *GroupMember {
	t.Helper()
	m, err := NewGroupMember(cfg, group)
	require.NoError(t, err)
	t.Cleanup(m.Close)
	return m
}

// With no time limit, Next waits for records and returns as soon as some arrive.
func TestGroupMember_NoTimeLimitWaitsForRecords(t *testing.T) {
	cfg := newCluster(t)
	m := newMember(t, cfg, "g")

	produceAfter(t, cfg, 300*time.Millisecond, 1, "first")
	b, err := m.Next(context.Background(), 100, 0)
	require.NoError(t, err)
	require.Equal(t, []string{"first"}, values(b.Records))
}

func TestGroupMember_BatchSizeAndTimeLimit(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "m0", "m1", "m2")
	m := newMember(t, cfg, "g")

	b, err := m.Next(context.Background(), 2, 10*time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"m0", "m1"}, values(b.Records))
	require.NoError(t, m.Commit(context.Background(), b))

	start := time.Now()
	b, err = m.Next(context.Background(), 2, partialWait)
	require.NoError(t, err)
	require.Equal(t, []string{"m2"}, values(b.Records))
	require.GreaterOrEqual(t, time.Since(start), partialWait)
	require.NoError(t, m.Commit(context.Background(), b))

	b, err = m.Next(context.Background(), 2, 500*time.Millisecond)
	require.NoError(t, err)
	require.Empty(t, b.Records)
}

// Committed records aren't read again by the group; uncommitted ones are, by the next member.
func TestGroupMember_CommitIsWhereTheGroupResumes(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "m0", "m1", "m2", "m3")

	first := newMember(t, cfg, "g")
	b, err := first.Next(context.Background(), 2, 5*time.Second)
	require.NoError(t, err)
	require.NoError(t, first.Commit(context.Background(), b))
	b, err = first.Next(context.Background(), 1, 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"m2"}, values(b.Records))
	first.Close() // without committing m2

	second := newMember(t, cfg, "g")
	// The batch size is the number of records left, so this returns as soon as they arrive.
	b, err = second.Next(context.Background(), 2, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"m2", "m3"}, values(b.Records))
}

func TestGroupMember_StartFromNewest(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "old")
	cfg.StartFrom = StartFromNewest
	m := newMember(t, cfg, "g")

	b, err := m.Next(context.Background(), 10, time.Second)
	require.NoError(t, err)
	require.Empty(t, b.Records)
	produce(t, cfg, 0, "new")
	b, err = m.Next(context.Background(), 1, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"new"}, values(b.Records))
}

// A batch released before the group ever committed survives a handoff, even starting from newest:
// the member commits the start it chose before consuming, so the next member starts there too
// rather than at the end, past the released batch.
func TestGroupMember_ReleasedFirstBatchSurvivesAHandoff(t *testing.T) {
	cfg := newCluster(t)
	cfg.StartFrom = StartFromNewest
	first, err := NewGroupMember(cfg, "g")
	require.NoError(t, err)
	empty, err := first.Next(context.Background(), 10, time.Second) // joins at the end
	require.NoError(t, err)
	require.NoError(t, first.Commit(context.Background(), empty))

	produce(t, cfg, 0, "must-survive")
	b, err := first.Next(context.Background(), 1, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"must-survive"}, values(b.Records))
	first.Release(b)
	first.Close()

	next := newMember(t, cfg, "g")
	again, err := next.Next(context.Background(), 1, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"must-survive"}, values(again.Records))
}

// A released batch is read again: the member's position goes back to the batch's first records.
func TestGroupMember_ReleaseReadsTheBatchAgain(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "m0", "m1")
	m := newMember(t, cfg, "g")

	b, err := m.Next(context.Background(), 2, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"m0", "m1"}, values(b.Records))
	m.Release(b)
	again, err := m.Next(context.Background(), 2, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"m0", "m1"}, values(again.Records))
}

// When another member joins while a batch is out, RebalanceWanted says so, and a Next waiting out
// its time limit returns what it has at once.
func TestGroupMember_RebalanceWanted(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "m0")
	first := newMember(t, cfg, "g")
	b, err := first.Next(context.Background(), 1, 30*time.Second)
	require.NoError(t, err)

	second := newMember(t, cfg, "g")
	go func() { _, _ = second.Next(context.Background(), 1, 0) }() // polling is what joins the group
	select {
	case <-first.RebalanceWanted():
	case <-time.After(30 * time.Second):
		t.Fatal("no rebalance wanted")
	}
	require.NoError(t, first.Commit(context.Background(), b))

	produce(t, cfg, 0, "m1")
	start := time.Now()
	third := newMember(t, cfg, "g")
	go func() { _, _ = third.Next(context.Background(), 1, 0) }()
	_, err = first.Next(context.Background(), 100, 2*time.Minute)
	require.NoError(t, err)
	require.Less(t, time.Since(start), time.Minute, "returned before the time limit")
}

// A tombstone (a null value) and an empty value stay apart, through a batch and into a dead letter.
func TestTombstone(t *testing.T) {
	cfg := newCluster(t)
	cl, err := kgo.NewClient(cfg.clientOpts(kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	require.NoError(t, err)
	defer cl.Close()
	require.NoError(t, cl.ProduceSync(context.Background(),
		&kgo.Record{Topic: testTopic, Key: []byte("k"), Value: nil},
		&kgo.Record{Topic: testTopic, Key: []byte("k"), Value: []byte{}},
	).FirstErr())

	b, err := newMember(t, cfg, "g").Next(context.Background(), 2, 0)
	require.NoError(t, err)
	require.Len(t, b.Records, 2)
	tombstone, empty := b.Records[0], b.Records[1]
	require.True(t, tombstone.Tombstone)
	require.False(t, empty.Tombstone)
	require.Nil(t, DeadLetter("dl", tombstone, "g", errors.New("x")).Value)
	require.NotNil(t, DeadLetter("dl", empty, "g", errors.New("x")).Value)
}

// Errors franz-go reports only to inform, and context errors, don't end a read; others do.
func TestFetchErrors(t *testing.T) {
	fetches := func(err error) kgo.Fetches {
		return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: testTopic, Partitions: []kgo.FetchPartition{{Partition: 0, Err: err}}}}}}
	}
	require.NoError(t, fetchErrors(fetches(nil)))
	require.NoError(t, fetchErrors(fetches(context.DeadlineExceeded)))
	require.NoError(t, fetchErrors(fetches(&kgo.ErrDataLoss{Topic: testTopic})))
	require.NoError(t, fetchErrors(fetches(&kgo.ErrGroupSession{Err: errors.New("kicked")})))
	require.ErrorContains(t, fetchErrors(fetches(errors.New("corrupt batch"))), "corrupt batch")
}

// WaitForTopic reports each failed attempt and keeps trying: here until the topic exists. It
// gives up only when its context ends.
func TestWaitForTopic(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	cfg := Config{Brokers: c.ListenAddrs(), Topic: testTopic}

	var mu sync.Mutex
	var failures []error
	created := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := WaitForTopic(context.Background(), cfg, 10*time.Millisecond, time.Second, func(_ int, err error) {
			mu.Lock()
			defer mu.Unlock()
			failures = append(failures, err)
			if len(failures) == 2 {
				close(created)
			}
		})
		result <- err
	}()
	select {
	case <-created:
	case <-time.After(30 * time.Second):
		t.Fatal("no failed attempts reported")
	}
	cl, err := kgo.NewClient(cfg.clientOpts()...)
	require.NoError(t, err)
	defer cl.Close()
	_, err = kadm.NewClient(cl).CreateTopic(context.Background(), 1, 1, nil, testTopic)
	require.NoError(t, err)
	require.NoError(t, <-result)
	mu.Lock()
	defer mu.Unlock()
	require.ErrorContains(t, failures[0], "UNKNOWN_TOPIC_OR_PARTITION")

	// Nothing listening: it fails each attempt until its context ends.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	attempts := 0
	_, err = WaitForTopic(ctx, Config{Brokers: []string{"127.0.0.1:1"}, Topic: testTopic}, 10*time.Millisecond, time.Second,
		func(int, error) { attempts++ })
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Positive(t, attempts)
}

func TestToKafkaRecord(t *testing.T) {
	m := kafkaapi.Message{Key: []byte("k"), Value: []byte("v"), Headers: []kafkaapi.Header{{Key: "a", Value: []byte("1")}}}
	r := ToKafkaRecord(testTopic, m, kgo.RecordHeader{Key: "dedupe-id", Value: []byte("x-0")})
	require.Equal(t, testTopic, r.Topic)
	require.Equal(t, "k", string(r.Key))
	require.Equal(t, "v", string(r.Value))
	require.Equal(t, []kgo.RecordHeader{{Key: "a", Value: []byte("1")}, {Key: "dedupe-id", Value: []byte("x-0")}}, r.Headers)
}

func TestLoadConfig(t *testing.T) {
	cfg, err := LoadConfig(settings.Map{"KAFKA_BROKERS": "a:9092,b:9092", "KAFKA_TOPIC": "t"})
	require.NoError(t, err)
	require.Equal(t, []string{"a:9092", "b:9092"}, cfg.Brokers)

	_, err = LoadConfig(settings.Map{})
	require.EqualError(t, err, "set KAFKA_BROKERS: the Kafka brokers, as comma-separated host:port"+
		"\nset KAFKA_TOPIC: the topic to serve", "every invalid value is reported")
}
