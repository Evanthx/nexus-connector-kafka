package main

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
)

// config is the streamer's settings, read from the environment by configFromEnv.
type config struct {
	kafka            kafka.Config
	group            string        // KAFKA_GROUP: the consumer group the streamer joins
	deadLetterTopic  string        // KAFKA_DEAD_LETTER_TOPIC: where undeliverable messages go
	batchSize        int           // KAFKA_BATCH_SIZE: most messages per batch; 1 for exactly one operation per message
	timeLimit        time.Duration // KAFKA_BATCH_TIME_LIMIT: how long a batch waits to fill
	endpoint         string        // STREAM_ENDPOINT: the Nexus endpoint messages are sent to
	service          string        // STREAM_SERVICE: the Nexus service
	operation        string        // STREAM_OPERATION: the operation started for each batch
	operationTimeout time.Duration // STREAM_OPERATION_TIMEOUT: how long each operation may take
	maxInputBytes    int           // STREAM_MAX_INPUT_BYTES: the largest encoded input an operation can take
}

func configFromEnv() (config, error) {
	kafkaCfg, err := kafka.ConfigFromEnv()
	if err != nil {
		return config{}, err
	}
	c := config{
		kafka:           kafkaCfg,
		group:           os.Getenv("KAFKA_GROUP"),
		deadLetterTopic: os.Getenv("KAFKA_DEAD_LETTER_TOPIC"),
		endpoint:        os.Getenv("STREAM_ENDPOINT"),
		service:         envOr("STREAM_SERVICE", kafkaapi.KafkaMessageReceiver.ServiceName),
		operation:       envOr("STREAM_OPERATION", kafkaapi.KafkaMessageReceiver.Receive.Name()),
	}
	var missing []string
	for name, v := range map[string]string{"KAFKA_GROUP": c.group, "KAFKA_DEAD_LETTER_TOPIC": c.deadLetterTopic, "STREAM_ENDPOINT": c.endpoint} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return config{}, fmt.Errorf("set %s", strings.Join(missing, ", "))
	}
	if c.deadLetterTopic == c.kafka.Topic {
		return config{}, errors.New("KAFKA_DEAD_LETTER_TOPIC must differ from KAFKA_TOPIC")
	}
	if c.batchSize, err = positiveInt("KAFKA_BATCH_SIZE", 100); err != nil {
		return config{}, err
	}
	if c.maxInputBytes, err = positiveInt("STREAM_MAX_INPUT_BYTES", 2*1024*1024); err != nil {
		return config{}, err
	}
	if c.timeLimit, err = duration("KAFKA_BATCH_TIME_LIMIT", 0); err != nil {
		return config{}, err
	}
	if c.operationTimeout, err = duration("STREAM_OPERATION_TIMEOUT", 24*time.Hour); err != nil {
		return config{}, err
	}
	if c.operationTimeout == 0 {
		return config{}, errors.New("STREAM_OPERATION_TIMEOUT must be more than 0")
	}
	return c, nil
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func positiveInt(name string, def int) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s must be a whole number, at least 1, not %q", name, v)
	}
	return n, nil
}

func duration(name string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%s must be a duration such as 500ms or 2s, not %q", name, v)
	}
	return d, nil
}
