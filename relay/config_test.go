package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// configFromEnv collects all invalid values in one error rather than stopping at the first.
func TestConfigFromEnv_ReportsEveryInvalidValue(t *testing.T) {
	// Set required vars so we reach the numeric/duration checks.
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	t.Setenv("KAFKA_TOPIC", "test-topic")
	t.Setenv("KAFKA_GROUP", "test-group")
	t.Setenv("RELAY_ENDPOINT", "test-endpoint")
	t.Setenv("KAFKA_DEAD_LETTER_TOPIC", "dead-letters")
	// Set multiple invalid values independently.
	t.Setenv("KAFKA_BATCH_SIZE", "bad")
	t.Setenv("RELAY_MAX_INPUT_BYTES", "bad")
	_, err := configFromEnv()
	require.ErrorContains(t, err, "KAFKA_BATCH_SIZE")
	require.ErrorContains(t, err, "RELAY_MAX_INPUT_BYTES")
}

// A missing KAFKA_TOPIC is reported alone: with no dead-letter topic set, the two aren't compared.
func TestConfigFromEnv_MissingTopicNotComparedToDeadLetterTopic(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("KAFKA_GROUP", "g")
	t.Setenv("RELAY_ENDPOINT", "ep")
	t.Setenv("KAFKA_DEAD_LETTER_TOPIC", "")
	t.Setenv("DELETE_DEAD_LETTERS", "true")
	_, err := configFromEnv()
	require.EqualError(t, err, "set KAFKA_TOPIC")
}
