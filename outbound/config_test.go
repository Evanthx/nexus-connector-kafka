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
	env := settings.Map{"KAFKA_BROKERS": "localhost:9092", "KAFKA_TOPICS": "orders,billing"}
	maps.Copy(env, vars)
	return env
}

func TestConfigFrom(t *testing.T) {
	_, s, err := configFrom(kafkaEnv(nil))
	require.NoError(t, err)
	require.Equal(t, config{topics: []string{"orders", "billing"}, taskQueue: defaultTaskQueue}, s, "the defaults")

	_, s, err = configFrom(kafkaEnv(settings.Map{"NEXUS_TASK_QUEUE": "nexus-connector-kafka-orders"}))
	require.NoError(t, err)
	require.Equal(t, "nexus-connector-kafka-orders", s.taskQueue)
}

// configFrom collects all invalid values in one error rather than stopping at the first.
func TestConfigFrom_ReportsEveryInvalidValue(t *testing.T) {
	_, _, err := configFrom(settings.Map{})
	require.EqualError(t, err, "set KAFKA_BROKERS: the Kafka brokers, as comma-separated host:port\n"+
		"set KAFKA_TOPICS: the topics callers may write to, comma-separated")
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
	require.Regexp(t, `KAFKA_TOPICS +orders,billing\n`, report)
	require.Contains(t, report, `for the topics "orders", "billing"`)
	require.NotContains(t, report, "KAFKA_START_FROM", "only the Inbound Connector reads it")

	env = kafkaEnv(nil)
	cfg, s, err = configFrom(env)
	require.NoError(t, err)
	report = s.report(env, cfg, client.Options{Namespace: "connectors", HostPort: "localhost:7233"})
	require.Regexp(t, `NEXUS_TASK_QUEUE +nexus-connector-kafka \(default\)`, report)
}

// KAFKA_TOPICS is exact names: an empty entry, a duplicate, or a name Kafka doesn't allow is an
// error, each reported. A name starting with _ is allowed.
func TestConfigFrom_Topics(t *testing.T) {
	_, s, err := configFrom(kafkaEnv(settings.Map{"KAFKA_TOPICS": "_private, orders"}))
	require.NoError(t, err)
	require.Equal(t, []string{"_private", "orders"}, s.topics)

	_, _, err = configFrom(kafkaEnv(settings.Map{"KAFKA_TOPICS": "orders,,billing,orders,bad*name,.."}))
	require.EqualError(t, err, "KAFKA_TOPICS: entry 2 is empty; remove the extra comma\n"+
		`KAFKA_TOPICS lists "orders" twice`+"\n"+
		`KAFKA_TOPICS: topic name "bad*name" has characters Kafka doesn't allow: use letters, digits, '.', '_' and '-'`+"\n"+
		`KAFKA_TOPICS: topic name ".." isn't allowed by Kafka`)
}
