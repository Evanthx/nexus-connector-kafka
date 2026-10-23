package kafka

import (
	"fmt"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Headers a dead letter gets, after the original message's own.
const (
	// DeadLetterReasonHeader says why the message couldn't be delivered.
	DeadLetterReasonHeader = "dead-letter-reason"
	// DeadLetterSourceHeader is where the message was: topic/partition/offset.
	DeadLetterSourceHeader = "dead-letter-source"
	// DeadLetterGroupHeader is the consumer group that couldn't deliver it.
	DeadLetterGroupHeader = "dead-letter-group"
)

// DeadLetter is rec as a record for the dead-letter topic: the same key, value, and headers, plus
// headers saying where it came from and why it couldn't be delivered. A tombstone stays one.
func DeadLetter(deadLetterTopic string, rec kafkaapi.Record, group string, reason error) *kgo.Record {
	m := kafkaapi.Message{Key: rec.Key, Value: rec.Value, Headers: rec.Headers}
	dl := ToKafkaRecord(deadLetterTopic, m,
		kgo.RecordHeader{Key: DeadLetterReasonHeader, Value: []byte(reason.Error())},
		kgo.RecordHeader{Key: DeadLetterSourceHeader, Value: fmt.Appendf(nil, "%s/%d/%d", rec.Topic, rec.Partition, rec.Offset)},
		kgo.RecordHeader{Key: DeadLetterGroupHeader, Value: []byte(group)},
	)
	if rec.Tombstone {
		dl.Value = nil
	}
	return dl
}
