package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.temporal.io/sdk/activity"
)

// DedupeHeader is the header each written message gets. Its value is the same on every attempt
// of one write, so a consumer can drop the copies a retried write leaves.
const DedupeHeader = "dedupe-id"

// heartbeatInterval is how often a read heartbeats; the read's heartbeat timeout is twice this
// (see readOptions). It is longer than the test caller's 60-second reads, so a read of a minute or
// less sends no heartbeat, and longer than the SDK's longest throttle (60 seconds), so each
// heartbeat reaches the server when it is sent. It and recordHeartbeat are variables so tests can
// watch heartbeats.
var (
	heartbeatInterval = 70 * time.Second
	recordHeartbeat   = activity.RecordHeartbeat
)

// resultReserve is the part of the batch byte limit kept for the result around the records: the
// position and the JSON envelope.
const resultReserve = 64 * 1024

// recordBytes is how many encoded bytes of records a read's result can hold.
func recordBytes(maxBatchBytes int) int {
	return maxBatchBytes - resultReserve
}

// activities are the standalone activities the Nexus operations start.
type activities struct {
	reader        *kafka.GroupReader
	writer        *kafka.Writer
	topic         string
	group         string // the only consumer group reads may use (KAFKA_GROUP); "" lets callers choose
	maxBatchBytes int    // the most bytes a read's result may encode to
}

// errInvalidGroup means a read names no consumer group, or one this worker doesn't serve.
var errInvalidGroup = errors.New("invalid consumer group")

// groupFor returns the consumer group a read uses, given the one it asked for ("" if none). A
// worker with a consumer group of its own serves only that one, so a read may leave it out or
// name it, but not name another. A worker without one serves any, so a read must name it.
func (a *activities) groupFor(requested string) (string, error) {
	switch {
	case a.group == "" && requested == "":
		return "", fmt.Errorf("%w: set group, the consumer group to read for: this endpoint has none of its own", errInvalidGroup)
	case a.group == "":
		return requested, nil
	case requested == "" || requested == a.group:
		return a.group, nil
	default:
		return "", fmt.Errorf("%w: this endpoint reads only consumer group %q; leave group empty or set it to %q, not %q",
			errInvalidGroup, a.group, a.group, requested)
	}
}

// Read acknowledges the previous batch, if in.Ack is set, and reads the next. It heartbeats every
// heartbeatInterval for as long as it runs, from the start: talking to Kafka before polling
// counts, not only polling.
func (a *activities) Read(ctx context.Context, in kafkaapi.ReadInput) (kafkaapi.ReadOutput, error) {
	stop := heartbeat(ctx)
	defer stop()
	group, err := a.groupFor(deref(in.Group))
	if err != nil {
		return kafkaapi.ReadOutput{}, applicationError(err)
	}
	res, err := a.reader.Read(ctx, kafka.ReadRequest{
		Group:     group,
		Ack:       deref(in.Ack),
		BatchSize: int(in.BatchSizeOrDefault()),
		TimeLimit: time.Duration(in.TimeLimitSecondsOrDefault()) * time.Second,
		MaxBytes:  recordBytes(a.maxBatchBytes),
	})
	if err != nil {
		return kafkaapi.ReadOutput{}, applicationError(err)
	}
	activity.GetLogger(ctx).Info("Read batch", "group", group, "records", len(res.Records))
	return kafkaapi.ReadOutput{Records: res.Records, Position: res.Position}, nil
}

// deref returns *s, or "" if s is nil: an optional field left out.
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// heartbeat heartbeats every heartbeatInterval until stop is called. The first heartbeat comes
// after one interval, not at once: the SDK sends a first heartbeat to the server straight away,
// and each one that reaches the server is a billable Action, so a read that finishes within the
// interval costs none.
func heartbeat(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				recordHeartbeat(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
	return cancel
}

// Write writes in.Messages in order. Each gets a dedupe-id header built from the activity's ID,
// which is the same on every attempt.
func (a *activities) Write(ctx context.Context, in kafkaapi.WriteInput) (kafkaapi.WriteOutput, error) {
	id := activity.GetInfo(ctx).ActivityID
	records := make([]*kgo.Record, len(in.Messages))
	for i, m := range in.Messages {
		dedupe := kgo.RecordHeader{Key: DedupeHeader, Value: fmt.Appendf(nil, "%s-%d", id, i)}
		records[i] = kafka.ToKafkaRecord(a.topic, m, dedupe)
	}
	if err := a.writer.Write(ctx, records...); err != nil {
		return kafkaapi.WriteOutput{}, applicationError(err)
	}
	activity.GetLogger(ctx).Info("Wrote messages", "count", len(records))
	return kafkaapi.WriteOutput{Written: int64(len(records))}, nil
}
