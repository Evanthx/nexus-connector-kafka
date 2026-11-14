// Command relay is the Nexus Connector Relay. It streams the topic named by KAFKA_TOPIC into
// Temporal: it joins a consumer group and starts one standalone Nexus operation per partition of
// each batch, through a Nexus endpoint. It only sends messages in: it doesn't wait for results. Run
// several to share the topic's partitions.
//
// Settings come from the environment: every variable it reads is listed in setup.Relay. It
// connects to Temporal with the standard TEMPORAL_* variables and the temporal CLI's profile file;
// the namespace defaults to connectors.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/settings"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"go.temporal.io/sdk/client"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	log.Print("Stopped")
}

// run streams until interrupted. It returns only after closing its connections, so leaving the
// group hands this relay's partitions to the others at once, even when it stops on an error.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Check the relay's configuration and the connection to Temporal before stopping on
	// either, so one run reports every problem rather than only the first.
	var problems []string
	env := settings.OS()
	cfg, err := configFrom(env)
	if err != nil {
		problems = append(problems, "Invalid relay configuration:\n"+indent(err.Error()))
	}
	opts, err := setup.ClientOptions(env)
	if err != nil {
		problems = append(problems, "Failed to load Temporal client configuration:\n"+indent(err.Error()))
	}
	var c client.Client
	if err == nil {
		c, err = client.Dial(opts)
		if err != nil {
			problems = append(problems, "Failed to connect to Temporal:\n"+indent(err.Error()))
		}
	}
	if len(problems) > 0 {
		if c != nil {
			c.Close()
		}
		return fmt.Errorf("the relay can't start:\n%s", strings.Join(problems, "\n"))
	}
	defer c.Close()

	address := opts.HostPort
	if address == "" {
		address = client.DefaultHostPort
	}
	// Logged before connecting to Kafka, so it shows even if Kafka is slow to answer.
	log.Print(cfg.report(env, opts.Namespace, address))
	if err := kafka.ConnectAtStartup(ctx, cfg.kafka); err != nil {
		if ctx.Err() != nil {
			return nil // stopped while waiting for Kafka
		}
		return err
	}
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

	r := &relay{
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
	if err := r.run(ctx); err != nil {
		return fmt.Errorf("streaming stopped: %w", err)
	}
	return nil
}

// indent prefixes each line of s with "  - ", one line per problem errors.Join reported.
func indent(s string) string {
	return "  - " + strings.ReplaceAll(s, "\n", "\n  - ")
}
