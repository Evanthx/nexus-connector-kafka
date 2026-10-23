package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/twmb/franz-go/pkg/kgo"
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
	client *kgo.Client
	topic  string

	mu     sync.Mutex
	wanted chan struct{} // closed when a rebalance is waiting for the batch that is out
}

// NewGroupMember joins group for the topic in cfg. A partition the group has no committed offset
// for starts where cfg.StartFrom says.
func NewGroupMember(cfg Config, group string) (*GroupMember, error) {
	reset := kgo.NewOffset().AtStart()
	if cfg.StartFrom == StartFromNewest {
		reset = kgo.NewOffset().AtEnd()
	}
	m := &GroupMember{topic: cfg.Topic, wanted: make(chan struct{})}
	client, err := kgo.NewClient(cfg.readOpts(
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(reset),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsCallbackBlocked(func(context.Context, *kgo.Client) { m.rebalanceWaiting() }),
	)...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	m.client = client
	return m, nil
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
		var fetchErr error
		fetches.EachError(func(topic string, part int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				fetchErr = errors.Join(fetchErr, fmt.Errorf("fetch %s[%d]: %w", topic, part, err))
			}
		})
		if fetchErr != nil {
			return fetchErr
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
