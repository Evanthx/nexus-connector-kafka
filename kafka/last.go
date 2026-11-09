package kafka

import (
	"context"
	"fmt"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReadLast returns the newest message in the topic in cfg: of each partition's last message, the
// one with the latest timestamp (the higher partition on a tie). ok is false if the topic has no
// messages. It reads without a consumer group and commits nothing, so it returns the same message
// each time until a newer one is written. It is for checking settings, not for processing.
//
// A partition whose last offset is a transaction marker, which holds no message, has nothing to
// return from there; ReadLast then waits until ctx ends for it and uses the other partitions.
func ReadLast(ctx context.Context, cfg Config) (rec kafkaapi.Record, ok bool, err error) {
	admin, err := kgo.NewClient(cfg.clientOpts()...)
	if err != nil {
		return kafkaapi.Record{}, false, fmt.Errorf("create Kafka client: %w", err)
	}
	defer admin.Close()
	starts, ends, err := listBounds(ctx, kadm.NewClient(admin), cfg.Topic)
	if err != nil {
		return kafkaapi.Record{}, false, err
	}

	// Read each non-empty partition from its last offset.
	assign := map[int32]kgo.Offset{}
	ends.Each(func(end kadm.ListedOffset) {
		start, _ := starts.Lookup(cfg.Topic, end.Partition)
		if end.Offset > start.Offset {
			assign[end.Partition] = kgo.NewOffset().At(end.Offset - 1)
		}
	})
	if len(assign) == 0 {
		return kafkaapi.Record{}, false, nil
	}
	cl, err := kgo.NewClient(cfg.readOpts(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.Topic: assign}))...)
	if err != nil {
		return kafkaapi.Record{}, false, fmt.Errorf("create Kafka consumer: %w", err)
	}
	defer cl.Close()

	last := map[int32]*kgo.Record{}
	for len(last) < len(assign) {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		for _, fe := range fetches.Errors() {
			return kafkaapi.Record{}, false, fmt.Errorf("read partition %d of %q: %w", fe.Partition, cfg.Topic, fe.Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if _, wanted := assign[r.Partition]; wanted {
				last[r.Partition] = r
			}
		})
	}
	if len(last) == 0 {
		return kafkaapi.Record{}, false, fmt.Errorf("read the last message of %q: none arrived before the time limit: %w", cfg.Topic, ctx.Err())
	}

	var newest *kgo.Record
	for _, r := range last {
		if newest == nil || r.Timestamp.After(newest.Timestamp) ||
			(r.Timestamp.Equal(newest.Timestamp) && r.Partition > newest.Partition) {
			newest = r
		}
	}
	return toRecord(newest), true, nil
}
