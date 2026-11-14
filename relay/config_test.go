package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/settings"
)

// configFrom collects all invalid values in one error rather than stopping at the first.
func TestConfigFrom_ReportsEveryInvalidValue(t *testing.T) {
	_, err := configFrom(relayEnv(settings.Map{"KAFKA_BATCH_SIZE": "bad", "RELAY_MAX_INPUT_BYTES": "bad"}))
	require.EqualError(t, err, `KAFKA_BATCH_SIZE must be a whole number, at least 1, not "bad"`+"\n"+
		`RELAY_MAX_INPUT_BYTES must be a whole number, at least 1, not "bad"`)
}

// A missing KAFKA_TOPIC is reported alone: with no dead-letter topic set, the two aren't compared.
func TestConfigFrom_MissingTopicNotComparedToDeadLetterTopic(t *testing.T) {
	_, err := configFrom(relayEnv(settings.Map{"KAFKA_TOPIC": "", "KAFKA_DEAD_LETTER_TOPIC": "", "DELETE_DEAD_LETTERS": "true"}))
	require.EqualError(t, err, "set KAFKA_TOPIC: the topic to serve")
}
