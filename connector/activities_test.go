package main

import (
	"context"
	"maps"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/settings"
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
	_, err = a.Read(ctx, kafkaapi.ReadInput{Group: ptr("g")})
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
	_, err = env.ExecuteActivity(a.Read, kafkaapi.ReadInput{Group: ptr("g")})
	require.NoError(t, err)
	require.Zero(t, heartbeats.Load())
}

// The test caller's default read ends before the first heartbeat is due, and a read's heartbeat
// timeout leaves room for one missed heartbeat.
func TestHeartbeatTiming(t *testing.T) {
	read := time.Duration(nexuscaller.DefaultTimeLimitSeconds) * time.Second
	require.Less(t, read, heartbeatInterval)
	require.Equal(t, 2*heartbeatInterval, readOptions("r", readQueue(defaultTaskQueue), kafkaapi.ReadInput{Group: ptr("g")}, time.Minute).HeartbeatTimeout)
}

// A connector with a consumer group of its own serves only that one; a connector without one
// needs each read to name one.
func TestGroupFor(t *testing.T) {
	fixed := &activities{group: "fixed"}
	for _, requested := range []string{"", "fixed"} {
		group, err := fixed.groupFor(requested)
		require.NoError(t, err)
		require.Equal(t, "fixed", group)
	}
	_, err := fixed.groupFor("other")
	require.ErrorIs(t, err, errInvalidGroup)
	require.ErrorContains(t, err, `reads only consumer group "fixed"; leave group empty or set it to "fixed", not "other"`)

	open := &activities{}
	group, err := open.groupFor("mine")
	require.NoError(t, err)
	require.Equal(t, "mine", group)
	_, err = open.groupFor("")
	require.ErrorIs(t, err, errInvalidGroup)
	require.ErrorContains(t, err, "set group")
}

// kafkaEnv is the Kafka variables the connector needs, with vars added.
func kafkaEnv(vars settings.Map) settings.Map {
	env := settings.Map{"KAFKA_BROKERS": "localhost:9092", "KAFKA_TOPIC": "orders"}
	maps.Copy(env, vars)
	return env
}

func TestConfigFrom(t *testing.T) {
	cfg, s, err := configFrom(kafkaEnv(nil))
	require.NoError(t, err)
	require.Equal(t, "orders", cfg.Topic)
	require.Equal(t, config{taskQueue: defaultTaskQueue, maxBatchBytes: defaultMaxBatchBytes}, s)

	_, s, err = configFrom(kafkaEnv(settings.Map{"KAFKA_GROUP": "orders-reader", "NEXUS_TASK_QUEUE": "nexus-connector-kafka-orders"}))
	require.NoError(t, err)
	require.Equal(t, "orders-reader", s.group)
	require.Equal(t, "nexus-connector-kafka-orders", s.taskQueue)
	require.Equal(t, "nexus-connector-kafka-orders-reads", readQueue(s.taskQueue))

	_, _, err = configFrom(kafkaEnv(settings.Map{"KAFKA_MAX_BATCH_BYTES": "100"}))
	require.ErrorContains(t, err, "KAFKA_MAX_BATCH_BYTES")

	_, s, err = configFrom(kafkaEnv(settings.Map{"KAFKA_MAX_DURATION_MINUTES": "90"}))
	require.NoError(t, err)
	require.Equal(t, 90*time.Minute, s.maxDuration)
	for _, bad := range []string{"0", "-1", "1.5", "1h", " 2"} {
		_, _, err = configFrom(kafkaEnv(settings.Map{"KAFKA_MAX_DURATION_MINUTES": bad}))
		require.ErrorContains(t, err, "KAFKA_MAX_DURATION_MINUTES", "value %q", bad)
	}
}

// configFrom collects all invalid values in one error rather than stopping at the first.
func TestConfigFrom_ReportsEveryInvalidValue(t *testing.T) {
	_, _, err := configFrom(settings.Map{
		"KAFKA_MAX_BATCH_BYTES":      "100", // too small (≤ resultReserve)
		"KAFKA_MAX_DURATION_MINUTES": "0",   // must be ≥ 1
	})
	require.EqualError(t, err, "set KAFKA_BROKERS: the Kafka brokers, as comma-separated host:port\n"+
		"set KAFKA_TOPIC: the topic to serve\n"+
		`KAFKA_MAX_BATCH_BYTES must be a number of bytes greater than 65536, not "100"`+"\n"+
		`KAFKA_MAX_DURATION_MINUTES must be a whole number of minutes, at least 1, not "0"`)
}

// The startup report says which consumer groups this connector serves and where to point its
// endpoint, and never shows the password.
func TestReport(t *testing.T) {
	env := kafkaEnv(settings.Map{
		"KAFKA_USERNAME": "connector", "KAFKA_PASSWORD": "secret-password",
		"KAFKA_GROUP": "orders-reader", "NEXUS_TASK_QUEUE": "nexus-connector-kafka-orders",
	})
	cfg, fixed, err := configFrom(env)
	require.NoError(t, err)
	report := fixed.report(env, cfg, "connectors", "localhost:7233")
	require.NotContains(t, report, "secret-password")
	require.Contains(t, report, `Serve only the consumer group "orders-reader"`)
	require.Contains(t, report, `point this connector's Nexus endpoint at it`)
	require.Contains(t, report, `"nexus-connector-kafka-orders-reads"`)
	require.Regexp(t, `KAFKA_MAX_BATCH_BYTES +2097152 \(default\)`, report)
	require.Regexp(t, `NEXUS_TASK_QUEUE +nexus-connector-kafka-orders\n`, report)

	env = kafkaEnv(nil)
	cfg, open, err := configFrom(env)
	require.NoError(t, err)
	report = open.report(env, cfg, "connectors", "localhost:7233")
	require.Contains(t, report, "Serve any consumer group: each read must name one")
	require.Regexp(t, `NEXUS_TASK_QUEUE +nexus-connector-kafka \(default\)`, report)
}
