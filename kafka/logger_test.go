package kafka

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/settings"
)

// syncBuffer is a bytes.Buffer safe to write from franz-go's goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// captureLog sends the default logger to a buffer until the test ends.
func captureLog(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	return buf
}

// franz-go logs nothing unless it is given a logger. Every client is built with clientOpts, which
// gives it one, so a broker it can't reach shows in the log, not only in the error a caller may or
// may not log.
func TestClientLogger_BrokerFailuresAreLogged(t *testing.T) {
	buf := captureLog(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := Identify(ctx, Config{Brokers: []string{"127.0.0.1:1"}, Topic: "t"})
	require.Error(t, err)
	require.Eventually(t, func() bool {
		out := buf.String()
		return strings.Contains(out, "level=WARN") && strings.Contains(out, "component=kafka-client") &&
			strings.Contains(out, "unable to open connection to broker")
	}, 5*time.Second, 50*time.Millisecond, "got log:\n%s", buf.String())
}

// The level filters franz-go's lines: at error, a broker it can't reach (a warning) isn't logged.
func TestClientLogger_Level(t *testing.T) {
	require.Equal(t, "WARN", (Config{}).clientLogLevel().String(), "unset is warn")

	buf := captureLog(t)
	l := clientLogger{level: slog.LevelError}
	require.Equal(t, "ERROR", l.Level().String())
	l.Log(l.Level()+1, "a warning")
	l.Log(l.Level(), "an error")
	require.NotContains(t, buf.String(), "a warning")
	require.Contains(t, buf.String(), "Kafka client: an error")
}

func TestConfigFrom_ClientLogLevel(t *testing.T) {
	env := settings.Map{"KAFKA_BROKERS": "b:9092", "KAFKA_TOPIC": "t"}
	cfg, err := LoadConfig(env)
	require.NoError(t, err)
	require.Equal(t, slog.LevelWarn, cfg.clientLogLevel())

	env["KAFKA_CLIENT_LOG_LEVEL"] = "debug"
	cfg, err = LoadConfig(env)
	require.NoError(t, err)
	require.Equal(t, slog.LevelDebug, cfg.clientLogLevel())

	env["KAFKA_CLIENT_LOG_LEVEL"] = "loud"
	_, err = LoadConfig(env)
	require.EqualError(t, err, `KAFKA_CLIENT_LOG_LEVEL must be error, warn, info or debug, not "loud"`)
}

// The member logs partitions it is given, and keeps them for the Inbound Connector's status line.
func TestGroupMember_LogsAssignedPartitions(t *testing.T) {
	buf := captureLog(t)
	cfg := newCluster(t)
	m := newMember(t, cfg, "g")
	produce(t, cfg, 1, "one")
	b, err := m.Next(context.Background(), 1, 0)
	require.NoError(t, err)
	require.NoError(t, m.Commit(context.Background(), b))
	require.Equal(t, []int32{0, 1, 2}, m.Partitions())
	require.Contains(t, buf.String(), "The consumer group gave this Inbound Connector partitions")
}

// Lost partitions, such as after Kafka was unreachable, are a warning; revoked ones aren't.
func TestGroupMember_LostPartitionsAreAWarning(t *testing.T) {
	buf := captureLog(t)
	m := &GroupMember{topic: "t", assigned: map[int32]bool{}}
	m.assign(map[string][]int32{"t": {2, 0, 1}})
	require.Equal(t, []int32{0, 1, 2}, m.Partitions())
	m.unassign(map[string][]int32{"t": {1}}, false)
	m.unassign(map[string][]int32{"t": {0}}, true)
	require.Equal(t, []int32{2}, m.Partitions())
	out := buf.String()
	require.Contains(t, out, `level=INFO msg="The consumer group took partitions`)
	require.Contains(t, out, `level=WARN msg="This Inbound Connector lost partitions`)
}
