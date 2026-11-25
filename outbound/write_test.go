package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
)

// Each message's dedupe-id comes from the Nexus request ID and its place in the write, so every
// attempt of one operation gives a message the same dedupe-id.
func TestRecords_DedupeIDFromRequestID(t *testing.T) {
	w := &topicWriter{topic: "orders"}
	in := kafkaapi.WriteInput{Messages: []kafkaapi.Message{{Value: []byte("a")}, {Value: []byte("b")}}}
	for range 2 { // two attempts
		recs := w.records("req-1", in)
		require.Len(t, recs, 2)
		for i, want := range []string{"kafka-write-req-1-0", "kafka-write-req-1-1"} {
			require.Equal(t, "orders", recs[i].Topic)
			require.Len(t, recs[i].Headers, 1)
			require.Equal(t, DedupeHeader, recs[i].Headers[0].Key)
			require.Equal(t, want, string(recs[i].Headers[0].Value))
		}
	}
	require.NotEqual(t, w.records("req-1", in)[0].Headers, w.records("req-2", in)[0].Headers, "another operation, another dedupe-id")
}
