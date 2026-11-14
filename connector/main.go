// Command connector is the Nexus Connector. It serves the topic named by KAFKA_TOPIC as the
// KafkaService Nexus service, whose one operation, write, writes messages to it. Each call runs as
// a standalone activity. To consume the topic, use the relay.
//
// Every environment variable it reads is listed in setup.Connector: the topic's KAFKA_* settings
// and its own. It connects to Temporal with the standard TEMPORAL_* variables and the temporal
// CLI's profile file; the namespace defaults to connectors.
//
// One process serves one topic through one Nexus endpoint. To serve another topic, run another
// process with its own KAFKA_TOPIC and NEXUS_TASK_QUEUE, behind an endpoint of its own.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/settings"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	// Check the connector's configuration and the connection to Temporal before stopping on
	// either, so one run reports every problem rather than only the first.
	var problems []string
	env := settings.OS()
	cfg, s, err := configFrom(env)
	if err != nil {
		problems = append(problems, "Invalid connector configuration:\n"+indent(err.Error()))
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
		log.Fatalf("The connector can't start:\n%s", strings.Join(problems, "\n"))
	}
	defer c.Close()

	writer, err := kafka.NewWriter(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()

	address := opts.HostPort
	if address == "" {
		address = client.DefaultHostPort
	}
	// Logged before connecting to Kafka, so it shows even if Kafka is slow to answer.
	log.Print(s.report(env, cfg, opts.Namespace, address))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := kafka.ConnectAtStartup(ctx, cfg); err != nil {
		if ctx.Err() != nil {
			log.Print("Stopped while waiting for Kafka")
			return
		}
		log.Fatal(err)
	}

	a := &activities{writer: writer, topic: cfg.Topic, maxDuration: s.maxDuration}
	w := newWorker(c, s.taskQueue, a)
	log.Printf("Serving topic %q on task queue %q", cfg.Topic, s.taskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker stopped: %v", err)
	}
}

// indent prefixes each line of s with "  - ", one line per problem errors.Join reported.
func indent(s string) string {
	return "  - " + strings.ReplaceAll(s, "\n", "\n  - ")
}

// config is the connector's settings besides the topic's.
type config struct {
	taskQueue   string        // NEXUS_TASK_QUEUE: the task queue the Nexus endpoint targets
	maxDuration time.Duration // KAFKA_MAX_DURATION_MINUTES: the longest a write may take; 0 for the default
}

// configFrom checks every variable the connector reads (setup.Connector) in env, reporting every
// problem at once, and returns the topic's settings and the connector's own.
func configFrom(env settings.Env) (kafka.Config, config, error) {
	vals, err := setup.Connector.Load(env)
	if err != nil {
		return kafka.Config{}, config{}, err
	}
	return kafka.ConfigFrom(vals), config{
		taskQueue:   setup.TaskQueue.From(vals),
		maxDuration: setup.MaxDuration.From(vals),
	}, nil
}

// report is the configuration and what the connector will do with it, for the log when it
// starts. env is what s and cfg, the topic's settings, were read from; namespace and address are
// where it connects to Temporal.
func (s config) report(env settings.Env, cfg kafka.Config, namespace, address string) string {
	maxDuration := kafka.Setting{Name: setup.MaxDuration.Name, Value: "10m (default)"}
	if s.maxDuration > 0 {
		maxDuration.Value = s.maxDuration.String()
	}
	sections := []kafka.Section{
		cfg.Settings(env),
		{Title: "Connector", Settings: []kafka.Setting{
			kafka.EnvSetting(env, "NEXUS_TASK_QUEUE", s.taskQueue),
			maxDuration,
		}},
		{Title: "Temporal", Settings: []kafka.Setting{
			{Name: "namespace", Value: namespace},
			{Name: "address", Value: address},
		}},
	}
	behavior := []string{
		fmt.Sprintf("Serve the topic %q as the KafkaService Nexus service, with the operation write.", cfg.Topic),
		fmt.Sprintf("Take Nexus requests and run writes on the task queue %q: point this connector's Nexus endpoint at it.", s.taskQueue),
		fmt.Sprintf("Write messages to the topic %q, each with a dedupe-id header.", cfg.Topic),
	}
	return kafka.Report("connector", sections, behavior)
}
