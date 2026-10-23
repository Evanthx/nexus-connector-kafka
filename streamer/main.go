// Command streamer streams the connector's topic into Temporal. It joins a consumer group and
// starts one standalone Nexus operation per partition of each batch, through a Nexus endpoint. It only sends messages in; nothing comes back. Run several to share the topic's
// partitions.
//
// Settings come from the environment: see configFromEnv and kafka.ConfigFromEnv. It connects to
// Temporal with the standard TEMPORAL_* variables and the temporal CLI's profile file; the
// namespace defaults to kafka-connector.
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
)

const defaultNamespace = "kafka-connector"

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		log.Fatalf("Settings: %v", err)
	}
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		log.Fatalf("Temporal client settings: %v", err)
	}
	if opts.Namespace == "" {
		opts.Namespace = defaultNamespace
	}
	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalf("Failed to create Temporal client: %v", err)
	}
	defer c.Close()
	starter, err := newNexusStarter(c, cfg)
	if err != nil {
		log.Fatal(err)
	}
	member, err := kafka.NewGroupMember(cfg.kafka, cfg.group)
	if err != nil {
		log.Fatal(err)
	}
	defer member.Close()
	deadLetters, err := kafka.NewWriter(cfg.kafka)
	if err != nil {
		log.Fatal(err)
	}
	defer deadLetters.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	identity, err := kafka.Identify(ctx, cfg.kafka)
	if err != nil {
		log.Fatal(err)
	}
	s := &streamer{
		member:          member,
		starter:         starter,
		deadLetters:     deadLetters,
		source:          sourceKey(identity, cfg.kafka.Topic, cfg.group),
		group:           cfg.group,
		deadLetterTopic: cfg.deadLetterTopic,
		batchSize:       cfg.batchSize,
		timeLimit:       cfg.timeLimit,
		maxInputBytes:   cfg.maxInputBytes,
		logger:          slog.Default(),
		retryDelay:      backoff,
	}
	log.Printf("Streaming topic %q as group %q to endpoint %q, operation %s/%s",
		cfg.kafka.Topic, cfg.group, cfg.endpoint, cfg.service, cfg.operation)
	if err := s.run(ctx); err != nil {
		log.Fatalf("Streaming stopped: %v", err)
	}
	log.Print("Stopped")
}
