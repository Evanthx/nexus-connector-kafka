// Command outbound is the Outbound Connector. It serves the topic named by KAFKA_TOPIC as the
// KafkaService Nexus service, whose one operation, write, writes messages to it. Each call runs as
// a standalone activity. To consume the topic, use the Inbound Connector.
//
// Every environment variable it reads is listed in setup.Outbound: the topic's KAFKA_* settings
// and its own. It connects to Temporal with the standard TEMPORAL_* variables and the temporal
// CLI's profile file; the namespace defaults to connectors.
//
// One process serves one topic through one Nexus endpoint. To serve another topic, run another
// process with its own KAFKA_TOPIC and NEXUS_TASK_QUEUE, behind an endpoint of its own.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/report"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/startup"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/temporalclient"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	env := settings.OS()
	s := startup.New("Outbound Connector", env)
	cfg, own, err := configFrom(env)
	s.Config(err)
	opts, c := s.Temporal()
	if err := s.Problems(); err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	writer, err := kafka.NewWriter(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()

	// Logged before waiting for Kafka, so it shows even if Kafka is slow to answer.
	log.Print(own.report(env, cfg, opts))
	if err := s.Ready(ctx, func(ctx context.Context) error { return kafka.ConnectAtStartup(ctx, cfg) }); err != nil {
		if errors.Is(err, startup.ErrStopped) {
			log.Print("Stopped while starting")
			return
		}
		log.Fatal(err)
	}

	a := &activities{writer: writer, topic: cfg.Topic, maxDuration: own.maxDuration}
	w := newWorker(c, own.taskQueue, a)
	log.Printf("Serving topic %q on task queue %q", cfg.Topic, own.taskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker stopped: %v", err)
	}
}

// config is the Outbound Connector's settings besides the topic's.
type config struct {
	taskQueue   string        // NEXUS_TASK_QUEUE: the task queue the Nexus endpoint targets
	maxDuration time.Duration // KAFKA_MAX_DURATION_MINUTES: the longest a write may take; 0 for the default
}

// configFrom checks every variable the Outbound Connector reads (setup.Outbound) in env, reporting
// every problem at once, and returns the topic's settings and the Outbound Connector's own.
func configFrom(env settings.Env) (kafka.Config, config, error) {
	vals, err := setup.Outbound.Load(env)
	if err != nil {
		return kafka.Config{}, config{}, err
	}
	return kafka.ConfigFrom(vals), config{
		taskQueue:   setup.TaskQueue.From(vals),
		maxDuration: setup.MaxDuration.From(vals),
	}, nil
}

// report is the configuration and what the Outbound Connector will do with it, for the log when it
// starts. env is what s and cfg, the topic's settings, were read from; temporal is the
// client options it connects to Temporal with.
func (s config) report(env settings.Env, cfg kafka.Config, temporal client.Options) string {
	maxDuration := report.Setting{Name: setup.MaxDuration.Name, Value: "10m (default)"}
	if s.maxDuration > 0 {
		maxDuration.Value = s.maxDuration.String()
	}
	sections := []report.Section{
		cfg.Settings(env),
		{Title: "Outbound Connector", Settings: []report.Setting{
			report.Env(env, "NEXUS_TASK_QUEUE", s.taskQueue),
			maxDuration,
		}},
		temporalclient.Section(env, temporal),
	}
	behavior := []string{
		fmt.Sprintf("Serve the topic %q as the KafkaService Nexus service, with the operation write.", cfg.Topic),
		fmt.Sprintf("Take Nexus requests and run writes on the task queue %q: point this Outbound Connector's Nexus endpoint at it.", s.taskQueue),
		fmt.Sprintf("Write messages to the topic %q, each with a dedupe-id header.", cfg.Topic),
	}
	return report.Format("Outbound Connector", sections, behavior)
}
