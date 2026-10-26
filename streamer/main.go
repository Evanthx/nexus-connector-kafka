// Command streamer streams the connector's topic into Temporal. It joins a consumer group and
// starts one standalone Nexus operation per partition of each batch, through a Nexus endpoint. It
// only sends messages in: it doesn't wait for results. Run several to share the topic's
// partitions.
//
// Settings come from the environment: see configFromEnv and kafka.ConfigFromEnv. It connects to
// Temporal with the standard TEMPORAL_* variables and the temporal CLI's profile file; the
// namespace defaults to kafka-connector.
package main

import (
	"context"
	"fmt"
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
	if err := run(); err != nil {
		log.Fatal(err)
	}
	log.Print("Stopped")
}

// run streams until interrupted. It returns only after closing its connections, so leaving the
// group hands this streamer's partitions to the others at once, even when it stops on an error.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := configFromEnv()
	if err != nil {
		return fmt.Errorf("settings: %w", err)
	}
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		return fmt.Errorf("temporal client settings: %w", err)
	}
	if opts.Namespace == "" {
		opts.Namespace = defaultNamespace
	}
	address := opts.HostPort
	if address == "" {
		address = client.DefaultHostPort
	}
	// Logged before connecting, so it shows even if connecting fails.
	log.Print(cfg.report(opts.Namespace, address))
	if err := kafka.ConnectAtStartup(ctx, cfg.kafka); err != nil {
		if ctx.Err() != nil {
			return nil // stopped while waiting for Kafka
		}
		return err
	}
	c, err := client.Dial(opts)
	if err != nil {
		return fmt.Errorf("create Temporal client: %w", err)
	}
	defer c.Close()
	starter, err := newNexusStarter(c, cfg)
	if err != nil {
		return err
	}
	member, err := kafka.NewGroupMember(cfg.kafka, cfg.group)
	if err != nil {
		return err
	}
	defer member.Close()
	var deadLetters *kafka.Writer
	if !cfg.deleteDeadLetters {
		if deadLetters, err = kafka.NewWriter(cfg.kafka); err != nil {
			return err
		}
		defer deadLetters.Close()
	}

	s := &streamer{
		member:            member,
		starter:           starter,
		deadLetters:       deadLetters,
		identify:          member.Identity,
		topic:             cfg.kafka.Topic,
		group:             cfg.group,
		deadLetterTopic:   cfg.deadLetterTopic,
		deleteDeadLetters: cfg.deleteDeadLetters,
		batchSize:         cfg.batchSize,
		timeLimit:         cfg.timeLimit,
		maxInputBytes:     cfg.maxInputBytes,
		logger:            slog.Default(),
		retryDelay:        backoff,
	}
	log.Printf("Streaming topic %q as group %q to endpoint %q, operation %s/%s",
		cfg.kafka.Topic, cfg.group, cfg.endpoint, cfg.service, cfg.operation)
	if err := s.run(ctx); err != nil {
		return fmt.Errorf("streaming stopped: %w", err)
	}
	return nil
}
