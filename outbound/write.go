package main

import (
	"context"
	"fmt"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// DedupeHeader is the header each written message gets. Its value is the same on every attempt
// of one write, so a consumer can drop the copies a retried write leaves.
const DedupeHeader = "dedupe-id"

// topicWriter writes to the one topic the Outbound Connector serves.
type topicWriter struct {
	writer *kafka.Writer
	topic  string
}

// write writes in.Messages in order, in one attempt. Each gets a dedupe-id header built from
// requestID, the Nexus request ID, which is the same on every attempt of one operation.
func (w *topicWriter) write(ctx context.Context, requestID string, in kafkaapi.WriteInput) (kafkaapi.WriteOutput, error) {
	if err := w.writer.Write(ctx, w.records(requestID, in)...); err != nil {
		return kafkaapi.WriteOutput{}, err
	}
	return kafkaapi.WriteOutput{Written: int64(len(in.Messages))}, nil
}

// records are in.Messages as records for the topic, the i-th with the dedupe-id
// kafka-write-<requestID>-<i>.
func (w *topicWriter) records(requestID string, in kafkaapi.WriteInput) []*kgo.Record {
	records := make([]*kgo.Record, len(in.Messages))
	for i, m := range in.Messages {
		dedupe := kgo.RecordHeader{Key: DedupeHeader, Value: fmt.Appendf(nil, "kafka-write-%s-%d", requestID, i)}
		records[i] = kafka.ToKafkaRecord(w.topic, m, dedupe)
	}
	return records
}
