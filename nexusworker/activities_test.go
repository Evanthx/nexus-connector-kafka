package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
)

// A read heartbeats before it reaches Kafka, not only while it polls: here Kafka accepts
// connections but never answers, so the read never gets past asking for the group's offsets.
func TestRead_HeartbeatsBeforePolling(t *testing.T) {
	var heartbeats atomic.Int32
	interval, record := heartbeatInterval, recordHeartbeat
	heartbeatInterval = 10 * time.Millisecond
	recordHeartbeat = func(context.Context, ...any) { heartbeats.Add(1) }
	t.Cleanup(func() { heartbeatInterval, recordHeartbeat = interval, record })

	silent, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = silent.Close() })
	go func() {
		for {
			conn, err := silent.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	reader, err := kafka.NewGroupReader(kafka.Config{Brokers: []string{silent.Addr().String()}, Topic: "events"}, 0)
	require.NoError(t, err)
	t.Cleanup(reader.Close)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a := &activities{reader: reader, topic: "events", maxBatchBytes: defaultMaxBatchBytes}
	_, err = a.Read(ctx, kafkaapi.ReadInput{Group: "g"})
	require.Error(t, err)
	require.GreaterOrEqual(t, heartbeats.Load(), int32(10))
}
