package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/nexussend"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"go.temporal.io/sdk/client"
)

// The report says each route's limit and what it does, or that there is none.
func TestReport_RateLimit(t *testing.T) {
	env := inboundEnv(settings.Map{"INBOUND_CONFIG": `{"routes": [
		{"topic": "events", "endpoint": "ep", "deleteDeadLetters": true, "batchSize": 10, "maxMessagesPerMinute": 600},
		{"topic": "orders", "endpoint": "ep", "deleteDeadLetters": true}]}`})
	cfg, err := configFrom(env)
	require.NoError(t, err)
	report := cfg.report(env, client.Options{Namespace: "connectors", HostPort: "localhost:7233"})
	require.Regexp(t, `maxMessagesPerMinute +600\n`, report)
	require.Contains(t, report, `Topic "events": Send at most 600 messages a minute from this process`)
	require.Contains(t, report, `Topic "orders": Send messages as fast as Kafka has them: maxMessagesPerMinute isn't set.`)
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
	sender, err := layers(base, setup.Route{MaxMessagesPerMinute: 600, BatchSize: 10}, func(int) time.Duration { return time.Millisecond }, slog.Default())
	require.NoError(t, err)
	start := time.Now()
	require.NoError(t, sender.Send(context.Background(), nexussend.Operation{ID: "op", Messages: 10}))
	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.Equal(t, 3, attempts)
}

// Each route has its own rate limit: one route using up its minute doesn't hold up another.
func TestLayers_RateLimitIsPerRoute(t *testing.T) {
	base := nexussend.SenderFunc(func(context.Context, nexussend.Operation) error { return nil })
	route := setup.Route{MaxMessagesPerMinute: 10, BatchSize: 10}
	a, err := layers(base, route, func(int) time.Duration { return time.Millisecond }, slog.Default())
	require.NoError(t, err)
	b, err := layers(base, route, func(int) time.Duration { return time.Millisecond }, slog.Default())
	require.NoError(t, err)

	require.NoError(t, a.Send(context.Background(), nexussend.Operation{ID: "a1", Messages: 10}))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, b.Send(ctx, nexussend.Operation{ID: "b1", Messages: 10}), "the other route's minute is its own")
	require.Error(t, a.Send(ctx, nexussend.Operation{ID: "a2", Messages: 10}), "this route's minute is used up")
}

func TestConfigFrom_StatusInterval(t *testing.T) {
	cfg, err := configFrom(inboundEnv(settings.Map{"INBOUND_STATUS_INTERVAL": "0"}))
	require.NoError(t, err)
	require.Zero(t, cfg.statusInterval, "0 turns it off")
}

func TestConfigFrom_Ordering(t *testing.T) {
	env := inboundEnv(settings.Map{"INBOUND_CONFIG": `{"routes": [{"topic": "events", "endpoint": "ep", "deleteDeadLetters": true, "ordering": "partition"}]}`})
	cfg, err := configFrom(env)
	require.NoError(t, err)
	require.True(t, cfg.routes[0].Ordered)
	require.Contains(t, cfg.report(env, client.Options{Namespace: "connectors", HostPort: "localhost:7233"}), "Process each partition in order")
}
