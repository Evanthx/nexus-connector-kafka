package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// GroupMember reads the topic as a live member of a consumer group. Kafka splits the topic's
// partitions among the group's members, so running more members spreads the load, up to one per
// partition.
//
// Nothing is committed until Commit. While a batch is out, the group can't rebalance, so the
// partitions a batch came from stay this member's until it is committed or released. A rebalance
// that waits too long gets the member kicked out of the group, so when one is waiting,
// RebalanceWanted says so, and the batch should be committed or released quickly.
type GroupMember struct {
	client    *kgo.Client
	ready     chan struct{} // closed once client is set, for the callbacks that use it
	topic     string
	startFrom StartFrom

	mu     sync.Mutex
	wanted chan struct{} // closed when a rebalance is waiting for the batch that is out
}

// NewGroupMember joins group for the topic in cfg. A partition the group has no committed offset
// for, or whose committed offset is no longer in the log, starts where cfg.StartFrom says, the
// same way a GroupReader's does (see startOffset).
func NewGroupMember(cfg Config, group string) (*GroupMember, error) {
	reset := kgo.NewOffset().AtStart()
	if cfg.StartFrom == StartFromNewest {
		reset = kgo.NewOffset().AtEnd()
	}
	m := &GroupMember{ready: make(chan struct{}), topic: cfg.Topic, startFrom: cfg.StartFrom, wanted: make(chan struct{})}
	client, err := kgo.NewClient(cfg.readOpts(
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(reset), // not normally used: chooseStarts resolves every start
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsCallbackBlocked(func(context.Context, *kgo.Client) { m.rebalanceWaiting() }),
		kgo.AdjustFetchOffsetsFn(m.chooseStarts),
	)...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	m.client = client
	close(m.ready)
	return m, nil
}

// chooseStarts runs when the member is given partitions, after the group's committed offsets are
// fetched and before consuming starts. Where StartFrom decides a partition's start, it commits
// that start before anything is read. Otherwise nothing would record it: a batch released before
// the group's first commit, or a member that stopped before it, would leave the next member to
// choose again, and starting from newest that skips the batch.
func (m *GroupMember) chooseStarts(ctx context.Context, fetched map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	<-m.ready
	parts := fetched[m.topic]
	if len(parts) == 0 {
		return fetched, nil
	}
	starts, ends, err := listBounds(ctx, kadm.NewClient(m.client), m.topic)
	if err != nil {
		return nil, err
	}
	chosen := map[int32]kgo.EpochOffset{}
	for part, off := range parts {
		committed := off.EpochOffset().Offset // negative if the group has no commit for it
		start, _ := starts.Lookup(m.topic, part)
		end, _ := ends.Lookup(m.topic, part)
		from := startOffset(committed, committed >= 0, start.Offset, end.Offset, m.startFrom)
		if from != committed {
			chosen[part] = kgo.EpochOffset{Epoch: -1, Offset: from}
			parts[part] = kgo.NewOffset().At(from).WithEpoch(-1)
		}
	}
	if len(chosen) == 0 {
		return fetched, nil
	}
	var commitErr error
	m.client.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{m.topic: chosen},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				commitErr = err
				return
			}
			for _, t := range resp.Topics {
				for _, p := range t.Partitions {
					commitErr = errors.Join(commitErr, kerr.ErrorForCode(p.ErrorCode))
				}
			}
		})
	if commitErr != nil {
		return nil, fmt.Errorf("commit the starts chosen for %q: %w", m.topic, commitErr)
	}
	return fetched, nil
}

// Identity asks Kafka for the topic's identity, through the member's own connection. It asks every
// time, so it notices a topic deleted and created again.
func (m *GroupMember) Identity(ctx context.Context) (TopicIdentity, error) {
	return identify(ctx, m.client, m.topic)
}

// RebalanceWanted returns a channel that is closed when a rebalance is waiting for the batch that
// is out. Commit or Release the batch to let it go ahead.
func (m *GroupMember) RebalanceWanted() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.wanted
}

func (m *GroupMember) rebalanceWaiting() {
	m.mu.Lock()
	defer m.mu.Unlock()
	select {
	case <-m.wanted:
	default:
		close(m.wanted)
	}
}

// resetRebalanceWanted starts a new batch with no rebalance waiting.
func (m *GroupMember) resetRebalanceWanted() {
	m.mu.Lock()
	defer m.mu.Unlock()
	select {
	case <-m.wanted:
		m.wanted = make(chan struct{})
	default:
	}
}

// Batch is a batch of records read by a GroupMember. Commit it once it is processed.
type Batch struct {
	Records []kafkaapi.Record
	raw     []*kgo.Record
}

// Next returns up to batchSize records. With a time limit, it returns once it has batchSize or
// the time limit passes, which can be with no records. With no time limit, it waits for records
// and returns as soon as Kafka returns some. Either way, it returns what it has at once if a
// rebalance is waiting.
func (m *GroupMember) Next(ctx context.Context, batchSize int, timeLimit time.Duration) (Batch, error) {
	m.resetRebalanceWanted()
	pollCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-m.RebalanceWanted():
			cancel()
		case <-pollCtx.Done():
		}
	}()
	b := newBatch(batchSize, 0)
	var raw []*kgo.Record
	done := func() bool { return b.full() || (timeLimit == 0 && len(raw) > 0) }
	poll := func(pollCtx context.Context) error {
		fetches := m.client.PollRecords(pollCtx, batchSize-len(raw))
		if fetches.IsClientClosed() {
			return errors.New("the Kafka client is closed")
		}
		if err := fetchErrors(fetches); err != nil {
			return err
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			b.add(toRecord(rec))
			raw = append(raw, rec)
		})
		return nil
	}
	err := pollUntil(pollCtx, timeLimit, done, poll)
	if err != nil && ctx.Err() == nil && pollCtx.Err() != nil {
		err = nil // a rebalance is waiting: return what there is
	}
	if err != nil {
		m.client.AllowRebalance()
		return Batch{}, err
	}
	return Batch{Records: b.records, raw: raw}, nil
}

// Commit commits the batch's records, so the group resumes after them, and lets the group
// rebalance again.
func (m *GroupMember) Commit(ctx context.Context, b Batch) error {
	defer m.client.AllowRebalance()
	if len(b.raw) == 0 {
		return nil
	}
	if err := m.client.CommitRecords(ctx, b.raw...); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	return nil
}

// Release gives up a batch without committing it and lets the group rebalance. Its partitions are
// rewound to the batch's first records, so whichever member has them next, this one or another,
// reads the batch again.
func (m *GroupMember) Release(b Batch) {
	rewind := map[int32]kgo.EpochOffset{}
	for _, rec := range b.raw {
		if _, ok := rewind[rec.Partition]; !ok {
			rewind[rec.Partition] = kgo.EpochOffset{Epoch: rec.LeaderEpoch, Offset: rec.Offset}
		}
	}
	if len(rewind) > 0 {
		// Rewinding is safe here: the rebalance is still blocked, so the partitions are still ours.
		m.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{m.topic: rewind})
	}
	m.client.AllowRebalance()
}

// Close leaves the group, so its partitions go to the other members at once. A batch that wasn't
// committed is read again by whichever member gets its partitions.
func (m *GroupMember) Close() {
	// Leaving is a rebalance, which waits for AllowRebalance while a batch is out.
	m.client.AllowRebalance()
	m.client.Close()
}
