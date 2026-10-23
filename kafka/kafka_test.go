package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
	cl, err := kgo.NewClient(cfg.clientOpts(kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	if err != nil {
		return err
	}
	defer cl.Close()
	recs := make([]*kgo.Record, len(values))
	for i, v := range values {
		recs[i] = &kgo.Record{Topic: cfg.Topic, Partition: partition, Value: []byte(v)}
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

func newReader(t *testing.T, cfg Config) *GroupReader {
	t.Helper()
	r, err := NewGroupReader(cfg, 0)
	require.NoError(t, err)
	t.Cleanup(r.Close)
	return r
}

func read(t *testing.T, r *GroupReader, req ReadRequest) ReadResult {
	t.Helper()
	res, err := r.Read(context.Background(), req)
	require.NoError(t, err)
	return res
}

func TestGroupReader_NewGroupStartsAtOldest(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "a0", "a1", "a2")
	produce(t, cfg, 1, "b0")
	r := newReader(t, cfg)

	res := read(t, r, ReadRequest{Group: "g", BatchSize: 100})
	require.ElementsMatch(t, []string{"a0", "a1", "a2", "b0"}, values(res.Records))
	for _, rec := range res.Records {
		require.Equal(t, testTopic, rec.Topic)
		require.NotEmpty(t, rec.Timestamp)
	}
}

func TestGroupReader_StartFromNewest(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "old")
	cfg.StartFrom = StartFromNewest
	r := newReader(t, cfg)

	res := read(t, r, ReadRequest{Group: "g", BatchSize: 100})
	require.Empty(t, res.Records)

	produce(t, cfg, 0, "new")
	res = read(t, r, ReadRequest{Group: "g", BatchSize: 100, Ack: res.Position})
	require.Equal(t, []string{"new"}, values(res.Records))
}

// A first read that is retried, because its activity failed before reporting, returns the same
// records. Starting from newest, a retry that worked out the start again would begin after them.
func TestGroupReader_RetriedFirstReadReturnsTheSameRecords(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "old")
	cfg.StartFrom = StartFromNewest
	r := newReader(t, cfg)

	produceAfter(t, cfg, time.Second, 0, "new")
	first := read(t, r, ReadRequest{Group: "g", BatchSize: 1, TimeLimit: 30 * time.Second})
	require.Equal(t, []string{"new"}, values(first.Records))
	retry := read(t, r, ReadRequest{Group: "g", BatchSize: 1, TimeLimit: 30 * time.Second})
	require.Equal(t, []string{"new"}, values(retry.Records))
}

// Without an ack, a read starts from the group's committed offsets, so it returns the same batch.
func TestGroupReader_ReadWithoutAckReturnsTheSameBatch(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "m0", "m1", "m2", "m3")
	r := newReader(t, cfg)

	first := read(t, r, ReadRequest{Group: "g", BatchSize: 2})
	require.Equal(t, []string{"m0", "m1"}, values(first.Records))
	again := read(t, r, ReadRequest{Group: "g", BatchSize: 2})
	require.Equal(t, []string{"m0", "m1"}, values(again.Records))
}

// An ack commits the batch it ends: the read after it, and any later read without an ack, carry
// on from there. A different reader (another worker process) sees the same commit.
func TestGroupReader_AckCommits(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "m0", "m1", "m2", "m3", "m4")
	r := newReader(t, cfg)

	first := read(t, r, ReadRequest{Group: "g", BatchSize: 2})
	second := read(t, r, ReadRequest{Group: "g", BatchSize: 2, Ack: first.Position})
	require.Equal(t, []string{"m2", "m3"}, values(second.Records))

	other := newReader(t, cfg)
	restarted := read(t, other, ReadRequest{Group: "g", BatchSize: 10})
	require.Equal(t, []string{"m2", "m3", "m4"}, values(restarted.Records))
}

func TestGroupReader_BatchSizeZeroOnlyCommits(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "m0", "m1", "m2")
	r := newReader(t, cfg)

	first := read(t, r, ReadRequest{Group: "g", BatchSize: 2})
	commitOnly := read(t, r, ReadRequest{Group: "g", BatchSize: 0, Ack: first.Position})
	require.Empty(t, commitOnly.Records)
	require.NotNil(t, commitOnly.Records)
	require.Equal(t, first.Position, commitOnly.Position)

	next := read(t, r, ReadRequest{Group: "g", BatchSize: 10})
	require.Equal(t, []string{"m2"}, values(next.Records))
}

// Groups keep separate positions.
// A read with batch size 0 only commits its ack: it doesn't ask Kafka for the group's offsets or
// the topic's.
func TestGroupReader_AckOnlyReadOnlyCommits(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, testTopic))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	cfg := Config{Brokers: c.ListenAddrs(), Topic: testTopic, StartFrom: StartFromOldest}
	produce(t, cfg, 0, "m0", "m1")
	r := newReader(t, cfg)
	first := read(t, r, ReadRequest{Group: "g", BatchSize: 1})

	var lookups atomic.Int32
	count := func(kmsg.Request) (kmsg.Response, error, bool) {
		lookups.Add(1)
		return nil, nil, false
	}
	c.ControlKey(kmsg.ListOffsets.Int16(), count)
	c.ControlKey(kmsg.OffsetFetch.Int16(), count)
	ackOnly := read(t, r, ReadRequest{Group: "g", BatchSize: 0, Ack: first.Position})
	require.Empty(t, ackOnly.Records)
	require.Zero(t, lookups.Load())

	next := read(t, r, ReadRequest{Group: "g", BatchSize: 1, Ack: ackOnly.Position})
	require.Equal(t, []string{"m1"}, values(next.Records))
}

// fetchMaxBytes caps what each fetch asks Kafka for, and a batch larger than one fetch still
// fills, over several.
func TestGroupReader_FetchMaxBytes(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, testTopic))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	cfg := Config{Brokers: c.ListenAddrs(), Topic: testTopic, StartFrom: StartFromOldest}
	value := strings.Repeat("x", 200)
	for range 5 {
		produce(t, cfg, 0, value) // one producer batch each, so a fetch can stop between them
	}

	var mu sync.Mutex
	var asked []int32
	c.ControlKey(kmsg.Fetch.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		fetch := req.(*kmsg.FetchRequest)
		mu.Lock()
		defer mu.Unlock()
		asked = append(asked, fetch.MaxBytes)
		for _, topic := range fetch.Topics {
			for _, part := range topic.Partitions {
				asked = append(asked, part.PartitionMaxBytes)
			}
		}
		return nil, nil, false
	})
	r, err := NewGroupReader(cfg, 300)
	require.NoError(t, err)
	t.Cleanup(r.Close)

	res := read(t, r, ReadRequest{Group: "g", BatchSize: 5})
	require.Len(t, res.Records, 5)
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, asked)
	for _, n := range asked {
		require.Equal(t, int32(300), n)
	}
}

func TestGroupReader_GroupsAreIndependent(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "m0", "m1")
	r := newReader(t, cfg)

	a := read(t, r, ReadRequest{Group: "a", BatchSize: 1})
	read(t, r, ReadRequest{Group: "a", BatchSize: 0, Ack: a.Position})
	b := read(t, r, ReadRequest{Group: "b", BatchSize: 10})
	require.Equal(t, []string{"m0", "m1"}, values(b.Records))
}

// The byte limit stops a batch before the record that would pass it, and the next read starts
// at that record, so none is skipped.
func TestGroupReader_ByteLimit(t *testing.T) {
	cfg := newCluster(t)
	big := strings.Repeat("x", 300)
	produce(t, cfg, 0, big+"0", big+"1", big+"2", big+"3")
	r := newReader(t, cfg)
	oneRecord := EncodedSize(kafkaapi.Record{Topic: testTopic, Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Value: []byte(big + "0")})

	first := read(t, r, ReadRequest{Group: "g", BatchSize: 100, MaxBytes: 2*oneRecord + oneRecord/2})
	require.Equal(t, []string{big + "0", big + "1"}, values(first.Records))
	second := read(t, r, ReadRequest{Group: "g", BatchSize: 100, MaxBytes: 2*oneRecord + oneRecord/2, Ack: first.Position})
	require.Equal(t, []string{big + "2", big + "3"}, values(second.Records))
}

// Once a record doesn't fit, a smaller one after it in the partition isn't taken instead: that
// would move the position past the one that didn't fit, and it would never be read.
func TestGroupReader_ByteLimitNeverSkipsARecord(t *testing.T) {
	cfg := newCluster(t)
	unit := strings.Repeat("x", 300)
	produce(t, cfg, 0, unit, unit+unit, unit)
	r := newReader(t, cfg)
	oneUnit := EncodedSize(kafkaapi.Record{Topic: testTopic, Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Value: []byte(unit)})
	limit := 2*oneUnit + oneUnit/2 // fits the first and the third, not the first and the second

	first := read(t, r, ReadRequest{Group: "g", BatchSize: 100, MaxBytes: limit})
	require.Equal(t, []string{unit}, values(first.Records))
	second := read(t, r, ReadRequest{Group: "g", BatchSize: 100, MaxBytes: limit, Ack: first.Position})
	require.Equal(t, []string{unit + unit}, values(second.Records))
}

func TestGroupReader_RecordTooLarge(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, strings.Repeat("x", 1000))
	r := newReader(t, cfg)

	_, err := r.Read(context.Background(), ReadRequest{Group: "g", BatchSize: 10, MaxBytes: 500})
	require.ErrorIs(t, err, ErrRecordTooLarge)
	require.ErrorContains(t, err, "partition 0 offset 0")
}

func TestGroupReader_NoTimeLimitReturnsAtOnceWhenEmpty(t *testing.T) {
	cfg := newCluster(t)
	r := newReader(t, cfg)

	start := time.Now()
	res := read(t, r, ReadRequest{Group: "g", BatchSize: 10})
	require.Empty(t, res.Records)
	require.Less(t, time.Since(start), 2*time.Second)
}

// With a time limit, a read waits for new records and returns as soon as the batch is full.
func TestGroupReader_TimeLimitReturnsWhenFull(t *testing.T) {
	cfg := newCluster(t)
	r := newReader(t, cfg)

	produceAfter(t, cfg, 300*time.Millisecond, 2, "late0", "late1")
	start := time.Now()
	res := read(t, r, ReadRequest{Group: "g", BatchSize: 2, TimeLimit: 20 * time.Second})
	require.Equal(t, []string{"late0", "late1"}, values(res.Records))
	require.Less(t, time.Since(start), 10*time.Second)
}

// With a time limit, a batch that doesn't fill is returned when the time limit passes.
func TestGroupReader_TimeLimitReturnsWhatItHas(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "only")
	r := newReader(t, cfg)

	start := time.Now()
	res := read(t, r, ReadRequest{Group: "g", BatchSize: 10, TimeLimit: partialWait})
	require.Equal(t, []string{"only"}, values(res.Records))
	require.GreaterOrEqual(t, time.Since(start), partialWait)
}

func TestGroupReader_RejectsAnotherGroupsAck(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "m0")
	r := newReader(t, cfg)

	a := read(t, r, ReadRequest{Group: "a", BatchSize: 1})
	_, err := r.Read(context.Background(), ReadRequest{Group: "b", BatchSize: 1, Ack: a.Position})
	require.ErrorIs(t, err, ErrInvalidPosition)
	require.ErrorContains(t, err, `returned for group "a"`)

	_, err = r.Read(context.Background(), ReadRequest{Group: "a", BatchSize: 1, Ack: "not a position"})
	require.ErrorIs(t, err, ErrInvalidPosition)
}

// A reader can't commit for a group that has live members, such as a streamer's: not an ack, and
// not the start it chose for a partition the group has no offset for.
func TestGroupReader_GroupInUse(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "m0", "m1")
	member, err := NewGroupMember(cfg, "live")
	require.NoError(t, err)
	t.Cleanup(member.Close)
	b, err := member.Next(context.Background(), 1, 0)
	require.NoError(t, err)
	require.NoError(t, member.Commit(context.Background(), b))

	r := newReader(t, cfg)
	_, err = r.Read(context.Background(), ReadRequest{Group: "live", BatchSize: 1})
	require.ErrorIs(t, err, ErrGroupInUse, "partitions 1 and 2 have no committed offset")
	ack := position{Group: "live", Topic: testTopic, Offsets: map[int32]int64{0: 2}}.encode()
	_, err = r.Read(context.Background(), ReadRequest{Group: "live", BatchSize: 0, Ack: ack})
	require.ErrorIs(t, err, ErrGroupInUse)
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

func TestPosition_RoundTrip(t *testing.T) {
	p := position{Group: "g", Topic: testTopic, Offsets: map[int32]int64{0: 3, 2: 7}}
	got, err := decodePosition(p.encode(), "g", testTopic)
	require.NoError(t, err)
	require.Equal(t, p, got)

	_, err = decodePosition(p.encode(), "g", "other-topic")
	require.ErrorIs(t, err, ErrInvalidPosition)
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

// A tombstone (a null value) and an empty value stay apart, through a read and into a dead letter.
func TestTombstone(t *testing.T) {
	cfg := newCluster(t)
	cl, err := kgo.NewClient(cfg.clientOpts(kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	require.NoError(t, err)
	defer cl.Close()
	require.NoError(t, cl.ProduceSync(context.Background(),
		&kgo.Record{Topic: testTopic, Key: []byte("k"), Value: nil},
		&kgo.Record{Topic: testTopic, Key: []byte("k"), Value: []byte{}},
	).FirstErr())

	res := read(t, newReader(t, cfg), ReadRequest{Group: "g", BatchSize: 2})
	require.Len(t, res.Records, 2)
	tombstone, empty := res.Records[0], res.Records[1]
	require.True(t, tombstone.Tombstone)
	require.False(t, empty.Tombstone)
	require.Nil(t, DeadLetter("dl", tombstone, "g", errors.New("x")).Value)
	require.NotNil(t, DeadLetter("dl", empty, "g", errors.New("x")).Value)
}

func TestToKafkaRecord(t *testing.T) {
	m := kafkaapi.Message{Key: []byte("k"), Value: []byte("v"), Headers: []kafkaapi.Header{{Key: "a", Value: []byte("1")}}}
	r := ToKafkaRecord(testTopic, m, kgo.RecordHeader{Key: "dedupe-id", Value: []byte("x-0")})
	require.Equal(t, testTopic, r.Topic)
	require.Equal(t, "k", string(r.Key))
	require.Equal(t, "v", string(r.Value))
	require.Equal(t, []kgo.RecordHeader{{Key: "a", Value: []byte("1")}, {Key: "dedupe-id", Value: []byte("x-0")}}, r.Headers)
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "a:9092,b:9092")
	t.Setenv("KAFKA_TOPIC", "t")
	t.Setenv("KAFKA_START_FROM", "")
	cfg, err := ConfigFromEnv()
	require.NoError(t, err)
	require.Equal(t, []string{"a:9092", "b:9092"}, cfg.Brokers)
	require.Equal(t, StartFromOldest, cfg.StartFrom)

	t.Setenv("KAFKA_START_FROM", "latest")
	_, err = ConfigFromEnv()
	require.ErrorContains(t, err, "KAFKA_START_FROM must be oldest or newest")

	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("KAFKA_BROKERS", "")
	_, err = ConfigFromEnv()
	require.EqualError(t, err, fmt.Sprintf("set %s and %s", "KAFKA_BROKERS", "KAFKA_TOPIC"))
}
