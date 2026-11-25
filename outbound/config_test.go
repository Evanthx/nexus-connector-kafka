package main

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"go.temporal.io/sdk/client"
)

// kafkaEnv is the Kafka variables the Outbound Connector needs, with vars added.
func kafkaEnv(vars settings.Map) settings.Map {
	env := settings.Map{"KAFKA_BROKERS": "localhost:9092", "KAFKA_TOPIC": "orders"}
	maps.Copy(env, vars)
	return env
}

func TestConfigFrom(t *testing.T) {
	cfg, s, err := configFrom(kafkaEnv(nil))
	require.NoError(t, err)
	require.Equal(t, "orders", cfg.Topic)
	require.Equal(t, config{taskQueue: defaultTaskQueue}, s, "the defaults")

	_, s, err = configFrom(kafkaEnv(settings.Map{"NEXUS_TASK_QUEUE": "nexus-connector-kafka-orders"}))
	require.NoError(t, err)
	require.Equal(t, "nexus-connector-kafka-orders", s.taskQueue)
}

// configFrom collects all invalid values in one error rather than stopping at the first.
func TestConfigFrom_ReportsEveryInvalidValue(t *testing.T) {
	_, _, err := configFrom(settings.Map{})
	require.EqualError(t, err, "set KAFKA_BROKERS: the Kafka brokers, as comma-separated host:port\n"+
		"set KAFKA_TOPIC: the topic to serve")
}

// The startup report says where to point the endpoint, and never shows the password.
func TestReport(t *testing.T) {
	env := kafkaEnv(settings.Map{
		"KAFKA_USERNAME": "connector", "KAFKA_PASSWORD": "secret-password",
		"NEXUS_TASK_QUEUE": "nexus-connector-kafka-orders",
	})
	cfg, s, err := configFrom(env)
	require.NoError(t, err)
	report := s.report(env, cfg, client.Options{Namespace: "connectors", HostPort: "localhost:7233"})
	require.NotContains(t, report, "secret-password")
	require.Contains(t, report, "with the operation write")
	require.Contains(t, report, `point this Outbound Connector's Nexus endpoint at it`)
	require.Regexp(t, `NEXUS_TASK_QUEUE +nexus-connector-kafka-orders\n`, report)
	require.Contains(t, report, "with no activity")
	require.NotContains(t, report, "KAFKA_START_FROM", "only the Inbound Connector reads it")

	env = kafkaEnv(nil)
	cfg, s, err = configFrom(env)
	require.NoError(t, err)
	report = s.report(env, cfg, client.Options{Namespace: "connectors", HostPort: "localhost:7233"})
	require.Regexp(t, `NEXUS_TASK_QUEUE +nexus-connector-kafka \(default\)`, report)
}
