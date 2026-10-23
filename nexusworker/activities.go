package main

import (
	"context"
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
	maxBatchBytes int // the most bytes a read's result may encode to
}

// Read acknowledges the previous batch, if in.Ack is set, and reads the next. It heartbeats the
// whole time, including while it talks to Kafka before polling and while it waits for a consumer.
func (a *activities) Read(ctx context.Context, in kafkaapi.ReadInput) (kafkaapi.ReadOutput, error) {
	stop := heartbeat(ctx)
	defer stop()
	ack := ""
	if in.Ack != nil {
		ack = *in.Ack
	}
	res, err := a.reader.Read(ctx, kafka.ReadRequest{
		Group:     in.Group,
		Ack:       ack,
		BatchSize: int(in.BatchSizeOrDefault()),
		TimeLimit: time.Duration(in.TimeLimitSecondsOrDefault()) * time.Second,
		MaxBytes:  recordBytes(a.maxBatchBytes),
	})
	if err != nil {
		return kafkaapi.ReadOutput{}, applicationError(err)
	}
	activity.GetLogger(ctx).Info("Read batch", "group", in.Group, "records", len(res.Records))
	return kafkaapi.ReadOutput{Records: res.Records, Position: res.Position}, nil
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
