package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/nexussend"
	"github.com/temporalio/nexus-connector-kafka/settings"
)

// configFrom collects all invalid values in one error rather than stopping at the first.
func TestConfigFrom_ReportsEveryInvalidValue(t *testing.T) {
	_, err := configFrom(inboundEnv(settings.Map{"KAFKA_BATCH_SIZE": "bad", "INBOUND_MAX_INPUT_BYTES": "bad"}))
	require.EqualError(t, err, `KAFKA_BATCH_SIZE must be a whole number, at least 1, not "bad"`+"\n"+
		`INBOUND_MAX_INPUT_BYTES must be a whole number, at least 1, not "bad"`)
}

// A missing KAFKA_TOPIC is reported alone: with no dead-letter topic set, the two aren't compared.
func TestConfigFrom_MissingTopicNotComparedToDeadLetterTopic(t *testing.T) {
	_, err := configFrom(inboundEnv(settings.Map{"KAFKA_TOPIC": "", "KAFKA_DEAD_LETTER_TOPIC": "", "DELETE_DEAD_LETTERS": "true"}))
	require.EqualError(t, err, "set KAFKA_TOPIC: the topic to serve")
}

// A batch larger than a minute's messages could never be sent, so the Inbound Connector refuses to
// start with one and says what to change.
func TestConfigFrom_BatchMustFitTheRateLimit(t *testing.T) {
	_, err := configFrom(inboundEnv(settings.Map{"KAFKA_BATCH_SIZE": "100", "INBOUND_MAX_MESSAGES_PER_MINUTE": "10"}))
	require.EqualError(t, err, "KAFKA_BATCH_SIZE (100) is more than INBOUND_MAX_MESSAGES_PER_MINUTE (10), so a full batch "+
		"could never be sent: set KAFKA_BATCH_SIZE to 10 or less, or raise INBOUND_MAX_MESSAGES_PER_MINUTE")

	cfg, err := configFrom(inboundEnv(settings.Map{"KAFKA_BATCH_SIZE": "10", "INBOUND_MAX_MESSAGES_PER_MINUTE": "10"}))
	require.NoError(t, err)
	require.Equal(t, 10, cfg.maxPerMinute)
}

func TestConfigFrom_RateLimitMustBeAWholeNumber(t *testing.T) {
	for _, v := range []string{"0", "-1", "ten", "1.5"} {
		_, err := configFrom(inboundEnv(settings.Map{"INBOUND_MAX_MESSAGES_PER_MINUTE": v}))
		require.ErrorContains(t, err, "INBOUND_MAX_MESSAGES_PER_MINUTE must be a whole number of messages, at least 1", v)
	}
	cfg, err := configFrom(inboundEnv(settings.Map{}))
	require.NoError(t, err)
	require.Zero(t, cfg.maxPerMinute, "unset means no limit")
}

// The report says the limit and what it does, or that there is none.
func TestReport_RateLimit(t *testing.T) {
	env := inboundEnv(settings.Map{"KAFKA_BATCH_SIZE": "10", "INBOUND_MAX_MESSAGES_PER_MINUTE": "600"})
	cfg, err := configFrom(env)
	require.NoError(t, err)
	report := cfg.report(env, "connectors", "localhost:7233")
	require.Contains(t, report, "INBOUND_MAX_MESSAGES_PER_MINUTE")
	require.Contains(t, report, "Send at most 600 messages a minute from this process")

	env = inboundEnv(settings.Map{})
	cfg, err = configFrom(env)
	require.NoError(t, err)
	require.Contains(t, cfg.report(env, "connectors", "localhost:7233"), "INBOUND_MAX_MESSAGES_PER_MINUTE isn't set")
}

// The rate limit is outside the retries: a start that takes several attempts counts its batch
// once. If each attempt were counted, this batch's two retries would wait a second each.
func TestLayers_RetriesDontCountAgainstTheRate(t *testing.T) {
	attempts := 0
	base := nexussend.SenderFunc(func(context.Context, nexussend.Operation) error {
		attempts++
		if attempts < 3 {
			return errors.New("unavailable")
		}
		return nil
	})
	sender, err := layers(base, config{maxPerMinute: 600, batchSize: 10}, func(int) time.Duration { return time.Millisecond }, slog.Default())
	require.NoError(t, err)
	start := time.Now()
	require.NoError(t, sender.Send(context.Background(), nexussend.Operation{ID: "op", Messages: 10}))
	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.Equal(t, 3, attempts)
}
