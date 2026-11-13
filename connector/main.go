// Command connector is the Nexus Connector. It serves the topic named by KAFKA_TOPIC as the
// KafkaService Nexus service: read, to read batches for a consumer group, and write, to write
// messages. Each call runs as a standalone activity.
//
// It reads the topic's settings from KAFKA_* environment variables (see kafka.ConfigFromEnv), and
// its own (see settingsFromEnv). It connects to Temporal with the standard TEMPORAL_* variables
// and the temporal CLI's profile file; the namespace defaults to connectors.
//
// One process serves one topic through one Nexus endpoint. To serve another topic, or the same
// topic for a fixed consumer group, run another process with its own KAFKA_TOPIC or KAFKA_GROUP
// and NEXUS_TASK_QUEUE, behind an endpoint of its own.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/worker"
)

const (
	defaultNamespace     = "connectors"
	defaultMaxBatchBytes = 2 * 1024 * 1024
)

func main() {
	// Check the connector's configuration and the connection to Temporal before stopping on
	// either, so one run reports every problem rather than only the first.
	var problems []string
	cfg, kafkaErr := kafka.ConfigFromEnv()
	s, settingsErr := settingsFromEnv()
	if err := errors.Join(kafkaErr, settingsErr); err != nil {
		problems = append(problems, "Invalid connector configuration:\n"+indent(err.Error()))
	}
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		problems = append(problems, "Failed to load Temporal client configuration:\n"+indent(err.Error()))
	}
	if opts.Namespace == "" {
		opts.Namespace = defaultNamespace
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
	log.Print(s.report(cfg, opts.Namespace, address))
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

// settings are the connector's settings besides the topic's.
type settings struct {
	group         string        // KAFKA_GROUP: the only consumer group reads may use; empty lets each read name one
	taskQueue     string        // NEXUS_TASK_QUEUE: the task queue the Nexus endpoint targets
	maxBatchBytes int           // KAFKA_MAX_BATCH_BYTES: the most bytes a read's result may encode to
	maxDuration   time.Duration // KAFKA_MAX_DURATION_MINUTES: the longest an operation may take; 0 for the defaults
}

// settingsFromEnv reads KAFKA_GROUP (optional), NEXUS_TASK_QUEUE (default nexus-connector-kafka),
// KAFKA_MAX_BATCH_BYTES (default 2 MiB, Temporal's payload limit), and KAFKA_MAX_DURATION_MINUTES
// (default: 10 minutes for a write, a read's time limit plus 5 minutes for a read).
func settingsFromEnv() (settings, error) {
	s := settings{group: os.Getenv("KAFKA_GROUP"), taskQueue: os.Getenv("NEXUS_TASK_QUEUE"), maxBatchBytes: defaultMaxBatchBytes}
	if s.taskQueue == "" {
		s.taskQueue = defaultTaskQueue
	}
	var errs []error
	if v := os.Getenv("KAFKA_MAX_BATCH_BYTES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= resultReserve {
			errs = append(errs, fmt.Errorf("KAFKA_MAX_BATCH_BYTES must be a number of bytes greater than %d, not %q", resultReserve, v))
		} else {
			s.maxBatchBytes = n
		}
	}
	maxDuration, err := readMaxDuration()
	if err != nil {
		errs = append(errs, err)
	}
	s.maxDuration = maxDuration
	return s, errors.Join(errs...)
}

// report is the configuration and what the connector will do with it, for the log when it
// starts. cfg is the topic's settings; namespace and address are where it connects to Temporal.
func (s settings) report(cfg kafka.Config, namespace, address string) string {
	group := kafka.Setting{Name: "KAFKA_GROUP", Value: s.group}
	groupBehavior := fmt.Sprintf("Serve only the consumer group %q: a read may leave group empty or name it; "+
		"a read naming another consumer group is refused.", s.group)
	if s.group == "" {
		group.Value = "(not set: each read names its consumer group)"
		groupBehavior = "Serve any consumer group: each read must name one; a read without one is refused."
	}
	maxDuration := kafka.Setting{Name: maxDurationEnvVar, Value: "(not set: 10m for a write, a read's time limit plus 5m for a read)"}
	if s.maxDuration > 0 {
		maxDuration.Value = s.maxDuration.String()
	}
	sections := []kafka.Section{
		cfg.Settings(),
		{Title: "Connector", Settings: []kafka.Setting{
			group,
			kafka.EnvSetting("NEXUS_TASK_QUEUE", s.taskQueue),
			kafka.EnvSetting("KAFKA_MAX_BATCH_BYTES", s.maxBatchBytes),
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
