package setup

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

func TestLoadKafka(t *testing.T) {
	cfg, err := LoadKafka(settings.Map{"KAFKA_BROKERS": "a:9092,b:9092", "KAFKA_TOPIC": "t"})
	require.NoError(t, err)
	require.Equal(t, []string{"a:9092", "b:9092"}, cfg.Brokers)

	_, err = LoadKafka(settings.Map{})
	require.EqualError(t, err, "set KAFKA_BROKERS: the Kafka brokers, as comma-separated host:port"+
		"\nset KAFKA_TOPIC: the topic to serve", "every invalid value is reported")
}

func TestLoadKafka_ClientLogLevel(t *testing.T) {
	env := settings.Map{"KAFKA_BROKERS": "b:9092", "KAFKA_TOPIC": "t"}
	cfg, err := LoadKafka(env)
	require.NoError(t, err)
	require.Equal(t, slog.LevelWarn, cfg.ClientLogLevel.Level(), "unset is warn")

	env["KAFKA_CLIENT_LOG_LEVEL"] = "debug"
	cfg, err = LoadKafka(env)
	require.NoError(t, err)
	require.Equal(t, slog.LevelDebug, cfg.ClientLogLevel.Level())

	env["KAFKA_CLIENT_LOG_LEVEL"] = "loud"
	_, err = LoadKafka(env)
	require.EqualError(t, err, `KAFKA_CLIENT_LOG_LEVEL must be error, warn, info or debug, not "loud"`)
}
