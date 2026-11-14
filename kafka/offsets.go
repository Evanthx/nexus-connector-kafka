package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
)

// listBounds lists each partition's oldest offset and its end: the last stable offset, the end of
// the committed records, which is all a read-committed client can read.
func listBounds(ctx context.Context, admin *kadm.Client, topic string) (starts, ends kadm.ListedOffsets, err error) {
	starts, err = admin.ListStartOffsets(ctx, topic)
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("list start offsets of %q: %w", topic, err)
	}
	ends, err = admin.ListCommittedOffsets(ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("list end offsets of %q: %w", topic, err)
	}
	return starts, ends, nil
}

// startOffset is where to read a partition from, given the consumer group's committed offset (if any)
// and the partition's oldest and end offsets. A committed offset is used as is when it is still
// in the log. Otherwise startFrom decides: the partition is new to the group, retention removed
// the committed offset, or the topic was recreated so the offset is past the end. These are the
// cases in which a relay's Kafka client uses its reset offset too.
func startOffset(committed int64, known bool, oldest, end int64, startFrom StartFrom) int64 {
	if known && committed >= oldest && committed <= end {
		return committed
	}
	if startFrom == StartFromNewest {
		return end
	}
	return oldest
}
