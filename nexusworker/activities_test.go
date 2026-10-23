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
	"github.com/temporalio/nexus-connector-kafka/test/nexuscaller"
	"github.com/temporalio/nexus-connector-kafka/test/testenv"
	"go.temporal.io/sdk/testsuite"
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

// A read that finishes within the heartbeat interval sends no heartbeat, since each one that
// reaches the server is a billable Action.
func TestRead_ShortReadDoesNotHeartbeat(t *testing.T) {
	var heartbeats atomic.Int32
	record := recordHeartbeat
	recordHeartbeat = func(context.Context, ...any) { heartbeats.Add(1) }
	t.Cleanup(func() { recordHeartbeat = record })

	reader, err := kafka.NewGroupReader(testenv.Kafka(t), 0)
	require.NoError(t, err)
	t.Cleanup(reader.Close)
	a := &activities{reader: reader, topic: testenv.Topic, maxBatchBytes: defaultMaxBatchBytes}
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(a)
	_, err = env.ExecuteActivity(a.Read, kafkaapi.ReadInput{Group: "g"})
	require.NoError(t, err)
	require.Zero(t, heartbeats.Load())
}

// The test caller's default read ends before the first heartbeat is due, and a read's heartbeat
// timeout leaves room for one missed heartbeat.
func TestHeartbeatTiming(t *testing.T) {
	read := time.Duration(nexuscaller.DefaultTimeLimitSeconds) * time.Second
	require.Less(t, read, heartbeatInterval)
	require.Equal(t, 2*heartbeatInterval, readOptions("r", kafkaapi.ReadInput{Group: "g"}).HeartbeatTimeout)
}
