// Command outbound is the Outbound Connector. It serves the KafkaService Nexus service, whose one
// operation, write, writes messages to the topic the write names, if KAFKA_TOPICS lists it. Each
// call is a synchronous Nexus operation, with no activity. To consume a topic, use the Inbound
// Connector.
//
// Every environment variable it reads is listed in setup.Outbound: the KAFKA_* settings and its
// own. It connects to Temporal with the standard TEMPORAL_* variables and the temporal
// CLI's profile file; the namespace defaults to connectors.
//
// One process serves its topics through one Nexus endpoint. Callers with different access run
// separate Outbound Connectors, each with its own KAFKA_TOPICS and NEXUS_TASK_QUEUE, behind an
// endpoint of its own.
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
	if err := s.Ready(ctx, func(ctx context.Context) error { return kafka.ConnectAtStartup(ctx, cfg, own.topics) }); err != nil {
		if errors.Is(err, startup.ErrStopped) {
			log.Print("Stopped while starting")
			return
		}
		log.Fatal(err)
	}

	w := newWorker(c, own.taskQueue, newTopicWriter(writer, own.topics))
	log.Printf("Serving topics %s on task queue %q", quoted(own.topics), own.taskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker stopped: %v", err)
	}
}
