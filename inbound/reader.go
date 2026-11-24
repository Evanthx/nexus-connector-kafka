package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/nexussend"
	"github.com/twmb/franz-go/pkg/kgo"
)

// commitTimeout bounds committing a delivered batch.
const commitTimeout = 10 * time.Second

// deadLetterWriter writes records to the dead-letter topic.
type deadLetterWriter interface {
	Write(ctx context.Context, records ...*kgo.Record) error
}

// reader reads the topic as a member of a consumer group and starts one Nexus operation for each
// partition's part of a batch. It commits a batch only after every message in it has an operation
// (or is in the dead-letter topic), so a crash means messages are delivered again, never lost.
type reader struct {
	member *kafka.GroupMember
	// sender starts each operation: retrying until it starts, and within the rate limit if one is
	// set. It returns an error only if the input is refused as too large or ctx ends.
	sender          nexussend.Sender
	deadLetters     deadLetterWriter                                   // nil when deleteDeadLetters is set
	identify        func(context.Context) (kafka.TopicIdentity, error) // the topic's current identity
	topic           string
	group           string
	deadLetterTopic string
	// deleteDeadLetters drops undeliverable messages instead of writing them to deadLetterTopic.
	deleteDeadLetters bool
	batchSize         int
	timeLimit         time.Duration
	maxInputBytes     int
	logger            *slog.Logger
	retryDelay        func(attempt int) time.Duration
	// statusInterval is how long the Inbound Connector goes without delivering before it logs a
	// status line, and how often after; 0 for never.
	statusInterval time.Duration
	partitions     func() []int32 // the partitions this member has, for the status line
	lastDelivered  atomic.Int64   // UnixNano of the last delivery, or of starting
	batchStarted   atomic.Int64   // UnixNano when the batch being delivered was read; 0 between batches
}

// run streams until ctx ends, then returns nil. It returns an error only if Kafka can't be read.
func (r *reader) run(ctx context.Context) error {
	r.lastDelivered.Store(time.Now().UnixNano())
	r.batchStarted.Store(0)
	if r.statusInterval > 0 {
		go r.reportStatus(ctx)
	}
	for {
		batch, err := r.member.Next(ctx, r.batchSize, r.timeLimit)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		if len(batch.Records) > 0 {
			r.batchStarted.Store(time.Now().UnixNano())
		}
		err = r.deliverBeforeRebalance(ctx, batch.Records)
		r.batchStarted.Store(0)
		if err != nil {
			if ctx.Err() != nil {
				return nil // stopping; the batch isn't committed, so it is delivered again
			}
			// A rebalance is waiting, and Kafka removes a member that holds one up too long.
			r.logger.Warn("A rebalance is waiting; releasing the batch, which will be delivered again", "records", len(batch.Records))
			r.member.Release(batch)
			continue
		}
		// Commit even if the Inbound Connector is stopping, so a clean stop doesn't deliver a
		// finished batch again.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
		err = r.member.Commit(commitCtx, batch)
		cancel()
		if err != nil {
			// Delivered but not committed. If this Inbound Connector keeps the partitions, its next
			// commit covers these too. If they move to another Inbound Connector, or this one
			// restarts, they are delivered again.
			r.logger.Warn("Commit failed; these messages may be delivered again", "records", len(batch.Records), "error", err)
		}
		if len(batch.Records) > 0 {
			r.lastDelivered.Store(time.Now().UnixNano())
			r.logger.Info("Delivered batch", "records", len(batch.Records))
		}
	}
}

// deliverBeforeRebalance delivers a batch, giving up if a rebalance is waiting. It returns an
// error if ctx ends or it gave up.
func (r *reader) deliverBeforeRebalance(ctx context.Context, records []kafkaapi.Record) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-r.member.RebalanceWanted():
			cancel()
		case <-ctx.Done():
		}
	}()
	if len(records) == 0 {
		return nil
	}
	source, err := r.source(ctx)
	if err != nil {
		return err
	}
	return r.deliver(ctx, source, records)
}

// deliver delivers a batch. Partitions are delivered in parallel; within a partition, operations
// start in offset order. It returns an error only if ctx ends first.
func (r *reader) deliver(ctx context.Context, source string, records []kafkaapi.Record) error {
	byPartition := map[int64][]kafkaapi.Record{}
	for _, rec := range records {
		byPartition[rec.Partition] = append(byPartition[rec.Partition], rec)
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(byPartition))
	for _, recs := range byPartition {
		wg.Go(func() {
			if err := r.deliverPartition(ctx, source, recs); err != nil {
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
func (r *reader) deliverPartition(ctx context.Context, source string, recs []kafkaapi.Record) error {
	var part []kafkaapi.Record
	size := inputOverhead
	flush := func() error {
		if len(part) == 0 {
			return nil
		}
		err := r.deliverPart(ctx, source, part)
		part, size = nil, inputOverhead
		return err
	}
	for _, rec := range recs {
		recSize := kafka.EncodedSize(rec)
		if inputOverhead+recSize > r.maxInputBytes {
			if err := flush(); err != nil {
				return err
			}
			reason := fmt.Errorf("the message is %d bytes encoded, more than an operation's input can be (%d)", recSize, r.maxInputBytes)
			if err := r.deadLetter(ctx, rec, reason); err != nil {
				return err
			}
			continue
		}
		if size+recSize > r.maxInputBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		part = append(part, rec)
		size += recSize
	}
	return flush()
}

// deliverPart starts the operation for consecutive records of one partition. The sender retries
// until it starts or ctx ends. If the server refuses the records as too large, they are split in
// two and each half delivered in turn, so only a message refused on its own goes to the
// dead-letter topic.
func (r *reader) deliverPart(ctx context.Context, source string, recs []kafkaapi.Record) error {
	id := operationID(source, recs)
	first, last := recs[0], recs[len(recs)-1]
	err := r.sender.Send(ctx, nexussend.Operation{
		ID:       id,
		Input:    kafkaapi.ReceiveInput{Records: recs},
		Messages: len(recs),
		Summary:  fmt.Sprintf("Kafka %s/%d/%d-%d", first.Topic, first.Partition, first.Offset, last.Offset),
	})
	switch {
	case errors.Is(err, nexussend.ErrTooLarge) && len(recs) > 1:
		r.logger.Warn("The operation was refused as too large; splitting it", "operationID", id, "records", len(recs), "error", err)
		half := len(recs) / 2
		if err := r.deliverPart(ctx, source, recs[:half]); err != nil {
			return err
		}
		return r.deliverPart(ctx, source, recs[half:])
	case errors.Is(err, nexussend.ErrTooLarge):
		r.logger.Error("The operation was refused", "operationID", id, "error", err)
		return r.deadLetter(ctx, recs[0], err)
	case errors.Is(err, nexussend.ErrFailed):
		// Only in ordered mode, which waits for each operation. The messages were delivered, so the
		// operation isn't started again and they don't go to the dead-letter topic: the failure is
		// handled in Temporal, and the partition carries on.
		r.logger.Error("The operation failed; its messages were delivered and won't be sent again, so handle the failure in Temporal",
			"operationID", id, "records", len(recs), "error", err)
		return nil
	}
	return err
}

// deadLetter writes rec to the dead-letter topic, retrying until it is written or ctx ends: the
// batch is committed once this returns, so the message must not be dropped. With
// deleteDeadLetters, it deletes rec instead: it logs it and returns, so the batch is committed
// past it and it is never delivered. It stays in Kafka until retention removes it.
func (r *reader) deadLetter(ctx context.Context, rec kafkaapi.Record, reason error) error {
	if r.deleteDeadLetters {
		r.logger.Error("Deleting a message that can't be delivered (DELETE_DEAD_LETTERS)",
			"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "reason", reason)
		return nil
	}
	r.logger.Error("Sending message to the dead-letter topic", "partition", rec.Partition, "offset", rec.Offset, "topic", r.deadLetterTopic, "reason", reason)
	dl := kafka.DeadLetter(r.deadLetterTopic, rec, r.group, reason)
	for attempt := 1; ; attempt++ {
		err := r.deadLetters.Write(ctx, dl)
		if err == nil {
			return nil
		}
		r.logger.Error("Could not write to the dead-letter topic; retrying", "attempt", attempt, "error", err)
		if err := r.sleep(ctx, attempt); err != nil {
			return err
		}
	}
}

func (r *reader) sleep(ctx context.Context, attempt int) error {
	select {
	case <-time.After(r.retryDelay(attempt)):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
