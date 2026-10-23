// Command nexusworker serves the connector's topic as the KafkaService Nexus service: read, to read
// batches for a consumer group, and write, to write messages. Each call runs as a standalone
// activity.
//
// It reads the topic's settings from KAFKA_* environment variables (see kafka.ConfigFromEnv), and
// KAFKA_MAX_BATCH_BYTES, the most bytes a read's result may encode to (2 MiB, Temporal's payload
// limit, by default). It connects to Temporal with the standard TEMPORAL_* variables and the
// temporal CLI's profile file; the namespace defaults to kafka-connector.
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/worker"
)

const (
	defaultNamespace     = "kafka-connector"
	defaultMaxBatchBytes = 2 * 1024 * 1024
)

func main() {
	cfg, err := kafka.ConfigFromEnv()
	if err != nil {
		log.Fatalf("Kafka settings: %v", err)
	}
	maxBatchBytes, err := maxBatchBytesFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	// Records encode larger than Kafka sends them (base64 in JSON), so a fetch of this size holds
	// a full batch or more.
	reader, err := kafka.NewGroupReader(cfg, recordBytes(maxBatchBytes))
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()
	writer, err := kafka.NewWriter(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()

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

	w := worker.New(c, taskQueue, workerOptions())
	register(w, &activities{reader: reader, writer: writer, topic: cfg.Topic, maxBatchBytes: maxBatchBytes})
	log.Printf("Serving topic %q on namespace %q, task queue %q", cfg.Topic, opts.Namespace, taskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker stopped: %v", err)
	}
}

// maxBatchBytesFromEnv reads KAFKA_MAX_BATCH_BYTES.
func maxBatchBytesFromEnv() (int, error) {
	v := os.Getenv("KAFKA_MAX_BATCH_BYTES")
	if v == "" {
		return defaultMaxBatchBytes, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= resultReserve {
		return 0, fmt.Errorf("KAFKA_MAX_BATCH_BYTES must be a number of bytes greater than %d, not %q", resultReserve, v)
	}
	return n, nil
}
