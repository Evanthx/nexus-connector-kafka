package kafka

import (
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/twmb/franz-go/pkg/kgo"
)

// toRecord converts a record read from Kafka to the contract's Record. A null value is a
// tombstone, which the contract marks apart from an empty value.
func toRecord(r *kgo.Record) kafkaapi.Record {
	value := r.Value
	if value == nil {
		value = []byte{} // the contract requires a value
	}
	rec := kafkaapi.Record{
		Topic:     r.Topic,
		Partition: int64(r.Partition),
		Offset:    r.Offset,
		Timestamp: r.Timestamp.UTC().Format(time.RFC3339Nano),
		Key:       r.Key,
		Value:     value,
		Tombstone: r.Value == nil,
	}
	for _, h := range r.Headers {
		rec.Headers = append(rec.Headers, kafkaapi.Header{Key: h.Key, Value: h.Value})
	}
	return rec
}

// ToKafkaRecord converts a message to write into a Kafka record for topic, with extra headers
// after the message's own.
func ToKafkaRecord(topic string, m kafkaapi.Message, extra ...kgo.RecordHeader) *kgo.Record {
	r := &kgo.Record{Topic: topic, Key: m.Key, Value: m.Value}
	for _, h := range m.Headers {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	r.Headers = append(r.Headers, extra...)
	return r
}
