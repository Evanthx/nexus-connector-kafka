// Command connector is the Nexus Connector. It serves the topic named by KAFKA_TOPIC as the
// KafkaService Nexus service: read, to read batches for a consumer group, and write, to write
// messages. Each call runs as a standalone activity.
//
// Every environment variable it reads is listed in setup.Connector: the topic's KAFKA_* settings
// and its own. It connects to Temporal with the standard TEMPORAL_* variables and the temporal
// CLI's profile file; the namespace defaults to connectors.
//
// One process serves one topic through one Nexus endpoint. To serve another topic, or the same
// topic for a fixed consumer group, run another process with its own KAFKA_TOPIC or KAFKA_GROUP
// and NEXUS_TASK_QUEUE, behind an endpoint of its own.
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

// defaultMaxBatchBytes is KAFKA_MAX_BATCH_BYTES unless set.
const defaultMaxBatchBytes = setup.DefaultMaxBatchBytes

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

	// Records encode larger than Kafka sends them (base64 in JSON), so a fetch of this size holds
	// a full batch or more.
	reader, err := kafka.NewGroupReader(cfg, recordBytes(s.maxBatchBytes))
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()
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

	a := &activities{reader: reader, writer: writer, topic: cfg.Topic, group: s.group, maxBatchBytes: s.maxBatchBytes, maxDuration: s.maxDuration}
	w, reads := newWorkers(c, s.taskQueue, a)
	if err := reads.Start(); err != nil {
		log.Fatalf("Failed to start the read worker: %v", err)
	}
	defer reads.Stop()
	log.Printf("Serving topic %q on task queues %q and %q", cfg.Topic, s.taskQueue, readQueue(s.taskQueue))
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
	group         string        // KAFKA_GROUP: the only consumer group reads may use; empty lets each read name one
	taskQueue     string        // NEXUS_TASK_QUEUE: the task queue the Nexus endpoint targets
	maxBatchBytes int           // KAFKA_MAX_BATCH_BYTES: the most bytes a read's result may encode to
	maxDuration   time.Duration // KAFKA_MAX_DURATION_MINUTES: the longest an operation may take; 0 for the defaults
}

// configFrom checks every variable the connector reads (setup.Connector) in env, reporting every
// problem at once, and returns the topic's settings and the connector's own.
func configFrom(env settings.Env) (kafka.Config, config, error) {
	vals, err := setup.Connector.Load(env)
	if err != nil {
		return kafka.Config{}, config{}, err
	}
	return kafka.ConfigFrom(vals), config{
		group:         setup.ConnectorGroup.From(vals),
		taskQueue:     setup.TaskQueue.From(vals),
		maxBatchBytes: setup.MaxBatchBytes.From(vals),
		maxDuration:   setup.MaxDuration.From(vals),
	}, nil
}

// report is the configuration and what the connector will do with it, for the log when it
// starts. env is what s and cfg, the topic's settings, were read from; namespace and address are
// where it connects to Temporal.
func (s config) report(env settings.Env, cfg kafka.Config, namespace, address string) string {
	group := kafka.Setting{Name: "KAFKA_GROUP", Value: s.group}
	groupBehavior := fmt.Sprintf("Serve only the consumer group %q: a read may leave group empty or name it; "+
		"a read naming another consumer group is refused.", s.group)
	if s.group == "" {
		group.Value = "(not set: each read names its consumer group)"
		groupBehavior = "Serve any consumer group: each read must name one; a read without one is refused."
	}
	maxDuration := kafka.Setting{Name: setup.MaxDuration.Name, Value: "(not set: 10m for a write, a read's time limit plus 5m for a read)"}
	if s.maxDuration > 0 {
		maxDuration.Value = s.maxDuration.String()
	}
	sections := []kafka.Section{
		cfg.Settings(env),
		{Title: "Connector", Settings: []kafka.Setting{
			group,
			kafka.EnvSetting(env, "NEXUS_TASK_QUEUE", s.taskQueue),
			kafka.EnvSetting(env, "KAFKA_MAX_BATCH_BYTES", s.maxBatchBytes),
			maxDuration,
		}},
		{Title: "Temporal", Settings: []kafka.Setting{
			{Name: "namespace", Value: namespace},
			{Name: "address", Value: address},
		}},
	}
	behavior := []string{
		fmt.Sprintf("Serve the topic %q as the KafkaService Nexus service, with the operations read and write.", cfg.Topic),
		fmt.Sprintf("Take Nexus requests and run writes on the task queue %q: point this connector's Nexus endpoint at it.", s.taskQueue),
		fmt.Sprintf("Run reads on the task queue %q, up to %d at once.", readQueue(s.taskQueue), kafka.MaxConcurrentReads),
		groupBehavior,
		"A partition a consumer group has no committed offset for " + cfg.StartFromMeaning() + ".",
		fmt.Sprintf("Return at most %d bytes of encoded messages per read.", recordBytes(s.maxBatchBytes)),
		fmt.Sprintf("Write messages to the topic %q, each with a dedupe-id header.", cfg.Topic),
	}
	return kafka.Report("connector", sections, behavior)
}
