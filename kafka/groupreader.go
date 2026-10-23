package kafka

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrGroupInUse means the group has live members, such as a streamer or another application
// consuming from it, so Kafka won't accept commits from a reader that isn't one of them.
var ErrGroupInUse = errors.New("the consumer group has live members")

// ErrRecordTooLarge means the next record is larger than the batch byte limit on its own, so it
// can't be returned.
var ErrRecordTooLarge = errors.New("record is larger than the batch byte limit")

// stallWait bounds how long a read with no time limit waits for records it already knows exist.
// It guards against a stalled fetch; it is not a wait for new records.
const stallWait = 10 * time.Second

// GroupReader reads batches for consumer groups without joining them. A group's committed
// offsets say where its reader is; a read starts there and returns a position, and the next read
// commits that position (its ack) before reading on. Nothing is kept between reads, so any
// process can serve any read.
type GroupReader struct {
	cfg       Config
	admin     *kadm.Client
	adminCl   *kgo.Client
	consumers *consumerPool
}

// NewGroupReader connects to the topic in cfg. fetchMaxBytes caps how much one fetch from Kafka
// returns, across all partitions; 0 keeps the Kafka client's defaults (1 MiB per partition, 50
// MiB in all). Set it to about the most a read returns: a read keeps only its batch and drops the
// rest of what it fetched, so a larger fetch is mostly fetched again by the next read. Kafka
// still sends at least one whole producer batch, however small this is.
func NewGroupReader(cfg Config, fetchMaxBytes int) (*GroupReader, error) {
	adminCl, err := kgo.NewClient(cfg.clientOpts()...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka client: %w", err)
	}
	// Keep control records, so the transaction markers they occupy still move the position on;
	// without them a partition whose last offset is a marker would never reach its end offset.
	opts := []kgo.Opt{kgo.KeepControlRecords()}
	if fetchMaxBytes > 0 {
		opts = append(opts, kgo.FetchMaxBytes(int32(fetchMaxBytes)), kgo.FetchMaxPartitionBytes(int32(fetchMaxBytes)))
	}
	consumers := newConsumerPool(cfg.readOpts(opts...))
	return &GroupReader{cfg: cfg, admin: kadm.NewClient(adminCl), adminCl: adminCl, consumers: consumers}, nil
}

// Close closes the reader's connections.
func (r *GroupReader) Close() {
	r.consumers.close()
	r.adminCl.Close()
}

// ReadRequest is one read for a group.
type ReadRequest struct {
	Group string
	// Ack is the position the group's previous read returned, or "". It is committed before
	// reading.
	Ack string
	// BatchSize is the most records to return. 0 only commits the ack.
	BatchSize int
	// TimeLimit is how long to wait for the batch to fill. 0 returns at once with the records
	// already in the topic.
	TimeLimit time.Duration
	// MaxBytes is the most encoded bytes to return; 0 for no limit.
	MaxBytes int
}

// ReadResult is a batch of records and the position after it.
type ReadResult struct {
	Records  []kafkaapi.Record
	Position string
}

// CheckAck reports whether ack is well formed and names group and this reader's topic. It can't
// tell whether a read really returned it: callers are trusted not to make positions up. An error
// wraps ErrInvalidPosition.
func (r *GroupReader) CheckAck(group, ack string) error {
	if ack == "" {
		return nil
	}
	_, err := decodePosition(ack, group, r.cfg.Topic)
	return err
}

// Read commits req.Ack, if given, then reads the next batch for req.Group.
func (r *GroupReader) Read(ctx context.Context, req ReadRequest) (ReadResult, error) {
	var ack position
	if req.Ack != "" {
		var err error
		if ack, err = decodePosition(req.Ack, req.Group, r.cfg.Topic); err != nil {
			return ReadResult{}, err
		}
	}
	if len(ack.Offsets) > 0 {
		if err := r.commit(ctx, req.Group, ack.Offsets); err != nil {
			return ReadResult{}, err
		}
	}
	if req.BatchSize == 0 {
		// Only acknowledging: the group is now at the ack.
		pos := position{Group: req.Group, Topic: r.cfg.Topic, Offsets: ack.Offsets}
		return ReadResult{Records: []kafkaapi.Record{}, Position: pos.encode()}, nil
	}
	committed, err := r.committedOffsets(ctx, req.Group)
	if err != nil {
		return ReadResult{}, err
	}
	p, err := r.plan(ctx, committed, req.TimeLimit > 0)
	if err != nil {
		return ReadResult{}, err
	}
	// Where KAFKA_START_FROM chose the start, commit it before reading, so a retry of this read
	// starts at the same place. Starting from newest, a retry that chose again would start after
	// any records written meanwhile, and the ack after it would skip them.
	if chosen := p.chosenStarts(committed); len(chosen) > 0 {
		if err := r.commit(ctx, req.Group, chosen); err != nil {
			return ReadResult{}, err
		}
	}
	b := newBatch(req.BatchSize, req.MaxBytes)
	if err := r.read(ctx, req, p, b); err != nil {
		return ReadResult{}, err
	}
	if b.tooLarge() {
		rec := b.records[0]
		return ReadResult{}, fmt.Errorf("%w: partition %d offset %d is %d bytes encoded, and the limit is %d",
			ErrRecordTooLarge, rec.Partition, rec.Offset, b.bytes, req.MaxBytes)
	}
	pos := position{Group: req.Group, Topic: r.cfg.Topic, Offsets: p.next}
	return ReadResult{Records: b.records, Position: pos.encode()}, nil
}

// committedOffsets returns the group's committed offset for each partition of the topic that has
// one. A group that has never committed has none.
func (r *GroupReader) committedOffsets(ctx context.Context, group string) (map[int32]int64, error) {
	out := map[int32]int64{}
	resp, err := r.admin.FetchOffsets(ctx, group)
	if err == nil {
		err = resp.Error()
	}
	if errors.Is(err, kerr.GroupIDNotFound) {
		return out, nil // a group that has never committed
	}
	if err != nil {
		return nil, fmt.Errorf("fetch committed offsets for group %q: %w", group, err)
	}
	resp.Each(func(o kadm.OffsetResponse) {
		if o.Topic == r.cfg.Topic && o.At >= 0 {
			out[o.Partition] = o.At
		}
	})
	return out, nil
}

// commit commits offsets for group. Kafka accepts these commits only from a group with no live
// members.
func (r *GroupReader) commit(ctx context.Context, group string, offsets map[int32]int64) error {
	var toCommit kadm.Offsets
	for part, off := range offsets {
		toCommit.Add(kadm.Offset{Topic: r.cfg.Topic, Partition: part, At: off, LeaderEpoch: -1})
	}
	resp, err := r.admin.CommitOffsets(ctx, group, toCommit)
	if err == nil {
		err = resp.Error()
	}
	switch {
	case err == nil:
		return nil
	case errors.Is(err, kerr.UnknownMemberID), errors.Is(err, kerr.IllegalGeneration),
		errors.Is(err, kerr.RebalanceInProgress), errors.Is(err, kerr.FencedInstanceID):
		return fmt.Errorf("%w: commit for group %q: %v", ErrGroupInUse, group, err)
	default:
		return fmt.Errorf("commit for group %q: %w", group, err)
	}
}

// readPlan is where each partition starts and ends for one read.
type readPlan struct {
	next   map[int32]int64      // partition -> next offset to read
	end    map[int32]int64      // partition -> offset to stop before
	assign map[int32]kgo.Offset // partitions to fetch, at their start offsets
}

// plan resolves where to start in each partition, from the group's committed offsets.
//
// With no time limit, a read returns only the records already in the topic, so each partition
// ends at its last stable offset (the end of the committed records, all a read-committed client
// can return) and only partitions with records left are fetched. With a time limit, records
// written during the read count too, so every partition is fetched and none has an end.
func (r *GroupReader) plan(ctx context.Context, committed map[int32]int64, waitForNew bool) (readPlan, error) {
	starts, err := r.admin.ListStartOffsets(ctx, r.cfg.Topic)
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		return readPlan{}, fmt.Errorf("list start offsets of %q: %w", r.cfg.Topic, err)
	}
	ends, err := r.admin.ListCommittedOffsets(ctx, r.cfg.Topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return readPlan{}, fmt.Errorf("list end offsets of %q: %w", r.cfg.Topic, err)
	}

	p := readPlan{next: map[int32]int64{}, end: map[int32]int64{}, assign: map[int32]kgo.Offset{}}
	ends.Each(func(end kadm.ListedOffset) {
		start, _ := starts.Lookup(r.cfg.Topic, end.Partition)
		stored, ok := committed[end.Partition]
		from := startOffset(stored, ok, start.Offset, end.Offset, r.cfg.StartFrom)
		p.next[end.Partition] = from
		switch {
		case waitForNew:
			p.end[end.Partition] = math.MaxInt64
			p.assign[end.Partition] = kgo.NewOffset().At(from)
		case from < end.Offset:
			p.end[end.Partition] = end.Offset
			p.assign[end.Partition] = kgo.NewOffset().At(from)
		default:
			p.end[end.Partition] = end.Offset
		}
	})
	return p, nil
}

// chosenStarts returns the start of each partition that doesn't start at the group's committed
// offset: one the group has no offset for, or whose offset is no longer in the log.
func (p readPlan) chosenStarts(committed map[int32]int64) map[int32]int64 {
	chosen := map[int32]int64{}
	for part, next := range p.next {
		if off, ok := committed[part]; !ok || off != next {
			chosen[part] = next
		}
	}
	return chosen
}

// startOffset is where to read a partition from, given the group's committed offset (if any)
// and the partition's oldest and end offsets. A committed offset is used as is when it is still
// in the log. Otherwise startFrom decides: the partition is new to the group, retention removed
// the committed offset, or the topic was recreated so the offset is past the end. These are the
// cases in which a streamer's Kafka client uses its reset offset too.
func startOffset(committed int64, known bool, oldest, end int64, startFrom StartFrom) int64 {
	if known && committed >= oldest && committed <= end {
		return committed
	}
	if startFrom == StartFromNewest {
		return end
	}
	return oldest
}

// read fetches records into b, following the plan.
//
// A partition is finished when its next offset reaches its end, not when a number of records
// has arrived: offsets aren't records. Transaction markers and aborted records take offsets but
// carry no data, and compaction removes records, so a range of N offsets can hold fewer.
func (r *GroupReader) read(ctx context.Context, req ReadRequest, p readPlan, b *batch) error {
	if len(p.assign) == 0 {
		return nil
	}
	consumer, err := r.consumers.get(ctx)
	if err != nil {
		return err
	}
	healthy := false
	defer func() { r.consumers.put(consumer, healthy) }()
	partitions := make([]int32, 0, len(p.assign))
	for part := range p.assign {
		partitions = append(partitions, part)
	}
	consumer.AddConsumePartitions(map[string]map[int32]kgo.Offset{r.cfg.Topic: p.assign})
	defer consumer.RemoveConsumePartitions(map[string][]int32{r.cfg.Topic: partitions})

	done := func() bool { return b.full() || p.finished() }
	poll := func(pollCtx context.Context) error {
		fetches := consumer.PollRecords(pollCtx, req.BatchSize-len(b.records))
		var fetchErr error
		fetches.EachError(func(topic string, part int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				fetchErr = errors.Join(fetchErr, fmt.Errorf("fetch %s[%d]: %w", topic, part, err))
			}
		})
		if fetchErr != nil {
			return fetchErr
		}
		fetches.EachRecord(func(rec *kgo.Record) { p.take(rec, b) })
		return nil
	}
	timeLimit := req.TimeLimit
	if timeLimit == 0 {
		// The records are already in the topic, so this only guards against a stalled fetch.
		timeLimit = stallWait
	}
	if err := pollUntil(ctx, timeLimit, done, poll); err != nil {
		return err
	}
	healthy = true
	return nil
}

// take adds one fetched record to b and moves the partition's position past it. A record before
// the position (already read) or at or past the end (left for the next read) is ignored, and so
// is every record once b is full, so the position never passes a record that wasn't returned. A
// transaction marker moves the position on but isn't data.
func (p readPlan) take(rec *kgo.Record, b *batch) {
	part := rec.Partition
	if rec.Offset < p.next[part] || rec.Offset >= p.end[part] || b.full() {
		return
	}
	if !rec.Attrs.IsControl() && !b.add(toRecord(rec)) {
		return
	}
	p.next[part] = rec.Offset + 1
}

// finished reports whether every fetched partition has reached its end.
func (p readPlan) finished() bool {
	for part := range p.assign {
		if p.next[part] < p.end[part] {
			return false
		}
	}
	return true
}
