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
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/startup"
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
