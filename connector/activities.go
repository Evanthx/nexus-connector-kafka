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

// activities is the standalone activity the write operation starts.
type activities struct {
	writer      *kafka.Writer
	topic       string
	maxDuration time.Duration // the longest a write may take (KAFKA_MAX_DURATION_MINUTES); 0 for the default
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
