package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// commitTimeout bounds committing a delivered batch.
const commitTimeout = 10 * time.Second

// errUndeliverable marks a batch that no retry can deliver. Its messages go to the dead-letter
// topic.
var errUndeliverable = errors.New("undeliverable")

// operationStarter starts the Nexus operation for one batch. An error wrapping errUndeliverable
// means the batch itself was refused; any other error is retried.
type operationStarter interface {
	start(ctx context.Context, operationID string, input kafkaapi.ReceiveInput) error
}

// deadLetterWriter writes records to the dead-letter topic.
type deadLetterWriter interface {
	Write(ctx context.Context, records ...*kgo.Record) error
}

// streamer reads the topic as a member of a consumer group and starts one Nexus operation for each
// partition's part of a batch. It commits a batch only after every message in it has an operation
// (or is in the dead-letter topic), so a crash means messages are delivered again, never lost.
type streamer struct {
	member          *kafka.GroupMember
	starter         operationStarter
	deadLetters     deadLetterWriter
	source          string // names the cluster, topic, and group in operation IDs; see sourceKey
	group           string
	deadLetterTopic string
	batchSize       int
	timeLimit       time.Duration
	maxInputBytes   int
	logger          *slog.Logger
	retryDelay      func(attempt int) time.Duration
}

// run streams until ctx ends, then returns nil. It returns an error only if Kafka can't be read.
func (s *streamer) run(ctx context.Context) error {
	for {
		batch, err := s.member.Next(ctx, s.batchSize, s.timeLimit)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.deliverBeforeRebalance(ctx, batch.Records); err != nil {
			if ctx.Err() != nil {
				return nil // stopping; the batch isn't committed, so it is delivered again
			}
			// A rebalance is waiting, and Kafka removes a member that holds one up too long.
			s.logger.Warn("A rebalance is waiting; releasing the batch, which will be delivered again", "records", len(batch.Records))
			s.member.Release(batch)
			continue
		}
		// Commit even if the streamer is stopping, so a clean stop doesn't deliver a finished batch
		// again.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
		err = s.member.Commit(commitCtx, batch)
		cancel()
		if err != nil {
			// Delivered but not committed: the group will deliver these again, and their
			// operation IDs match the operations already started.
			s.logger.Warn("Commit failed; these messages will be delivered again", "records", len(batch.Records), "error", err)
		}
		if len(batch.Records) > 0 {
			s.logger.Info("Delivered batch", "records", len(batch.Records))
		}
	}
}

// deliverBeforeRebalance delivers a batch, giving up if a rebalance is waiting. It returns an
// error if ctx ends or it gave up.
func (s *streamer) deliverBeforeRebalance(ctx context.Context, records []kafkaapi.Record) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-s.member.RebalanceWanted():
			cancel()
		case <-ctx.Done():
		}
	}()
	return s.deliver(ctx, records)
}

// deliver delivers a batch. Partitions are delivered in parallel; within a partition, operations
// start in offset order. It returns an error only if ctx ends first.
func (s *streamer) deliver(ctx context.Context, records []kafkaapi.Record) error {
	byPartition := map[int64][]kafkaapi.Record{}
	for _, rec := range records {
		byPartition[rec.Partition] = append(byPartition[rec.Partition], rec)
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(byPartition))
	for _, recs := range byPartition {
		wg.Go(func() {
			if err := s.deliverPartition(ctx, recs); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	return <-errs
}

// inputOverhead is the encoded size of a ReceiveInput apart from its records.
const inputOverhead = len(`{"records":[]}`)

// deliverPartition delivers one partition's records, as few operations as fit maxInputBytes. A
// message too large for an input on its own goes to the dead-letter topic.
func (s *streamer) deliverPartition(ctx context.Context, recs []kafkaapi.Record) error {
	var part []kafkaapi.Record
	size := inputOverhead
	flush := func() error {
		if len(part) == 0 {
			return nil
		}
		err := s.deliverPart(ctx, part)
		part, size = nil, inputOverhead
		return err
	}
	for _, rec := range recs {
		recSize := kafka.EncodedSize(rec)
		if inputOverhead+recSize > s.maxInputBytes {
			if err := flush(); err != nil {
				return err
			}
			reason := fmt.Errorf("the message is %d bytes encoded, more than an operation's input can be (%d)", recSize, s.maxInputBytes)
			if err := s.deadLetter(ctx, rec, reason); err != nil {
				return err
			}
			continue
		}
		if size+recSize > s.maxInputBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		part = append(part, rec)
		size += recSize
	}
	return flush()
}

// deliverPart starts the operation for consecutive records of one partition, retrying until it
// starts or ctx ends. If the server refuses them, they go to the dead-letter topic instead.
func (s *streamer) deliverPart(ctx context.Context, recs []kafkaapi.Record) error {
	id := operationID(s.source, recs)
	for attempt := 1; ; attempt++ {
		err := s.starter.start(ctx, id, kafkaapi.ReceiveInput{Records: recs})
		switch {
		case err == nil:
			return nil
		case errors.Is(err, errUndeliverable):
			s.logger.Error("The operation was refused", "operationID", id, "error", err)
			for _, rec := range recs {
				if err := s.deadLetter(ctx, rec, err); err != nil {
					return err
				}
			}
			return nil
		}
		s.logger.Warn("Could not start the operation; retrying", "operationID", id, "attempt", attempt, "error", err)
		if err := s.sleep(ctx, attempt); err != nil {
			return err
		}
	}
}

// deadLetter writes rec to the dead-letter topic, retrying until it is written or ctx ends: the
// batch is committed once this returns, so the message must not be dropped.
func (s *streamer) deadLetter(ctx context.Context, rec kafkaapi.Record, reason error) error {
	s.logger.Error("Sending message to the dead-letter topic", "partition", rec.Partition, "offset", rec.Offset, "topic", s.deadLetterTopic, "reason", reason)
	dl := kafka.DeadLetter(s.deadLetterTopic, rec, s.group, reason)
	for attempt := 1; ; attempt++ {
		err := s.deadLetters.Write(ctx, dl)
		if err == nil {
			return nil
		}
		s.logger.Error("Could not write to the dead-letter topic; retrying", "attempt", attempt, "error", err)
		if err := s.sleep(ctx, attempt); err != nil {
			return err
		}
	}
}

func (s *streamer) sleep(ctx context.Context, attempt int) error {
	select {
	case <-time.After(s.retryDelay(attempt)):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// backoff is the delay before retry attempt+1: one second, doubling, at most 30 seconds.
func backoff(attempt int) time.Duration {
	return min(time.Second<<min(attempt-1, 5), 30*time.Second)
}

// sourceKey names where messages come from: the Kafka cluster, the topic (its ID changes if it
// is deleted and created again), and the consumer group. It is a hash, so names that contain the
// ID's separator can't run together: topic "a-b" with group "c" and topic "a" with group "b-c"
// get different keys.
func sourceKey(id kafka.TopicIdentity, topic, group string) string {
	b, _ := json.Marshal([]string{id.Cluster, id.TopicID, topic, group}) // strings always marshal
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}

// operationID identifies the operation for consecutive records of one partition: their source
// (see sourceKey), partition, and first and last offsets. Records delivered again in the same
// batch get the same ID, so they match the operation already started. A batch delivered again can
// end at a different offset, though, and then gets a new ID: see "Duplicates" in the README.
func operationID(source string, recs []kafkaapi.Record) string {
	first, last := recs[0], recs[len(recs)-1]
	return fmt.Sprintf("kafka-%s-%d-%d-%d", source, first.Partition, first.Offset, last.Offset)
}
