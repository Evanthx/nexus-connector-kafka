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

// config is the relay's settings, read from the environment by configFromEnv.
type config struct {
	kafka             kafka.Config
	group             string        // KAFKA_GROUP: the consumer group the relay joins
	deadLetterTopic   string        // KAFKA_DEAD_LETTER_TOPIC: where undeliverable messages go
	deleteDeadLetters bool          // DELETE_DEAD_LETTERS: log and skip undeliverable messages instead
	batchSize         int           // KAFKA_BATCH_SIZE: most messages per batch; 1 for exactly one operation per message
	timeLimit         time.Duration // KAFKA_BATCH_TIME_LIMIT: how long a batch waits to fill
	endpoint          string        // RELAY_ENDPOINT: the Nexus endpoint messages are sent to
	service           string        // RELAY_SERVICE: the Nexus service
	operation         string        // RELAY_OPERATION: the operation started for each batch
	operationTimeout  time.Duration // RELAY_OPERATION_TIMEOUT: how long each operation may take
	maxInputBytes     int           // RELAY_MAX_INPUT_BYTES: the largest encoded input sent to an operation
}

// defaultMaxInputBytes is under Temporal's 2 MiB payload limit by 64 KiB, the same reserve the
// connector keeps: the limit applies to the whole payload, metadata included, and the size
// counted here is an estimate of the encoded records.
const defaultMaxInputBytes = 2*1024*1024 - 64*1024

func configFromEnv() (config, error) {
	kafkaCfg, kafkaErr := kafka.ConfigFromEnv()
	c := config{
		kafka:           kafkaCfg,
		group:           os.Getenv("KAFKA_GROUP"),
		deadLetterTopic: os.Getenv("KAFKA_DEAD_LETTER_TOPIC"),
		endpoint:        os.Getenv("RELAY_ENDPOINT"),
		service:         envOr("RELAY_SERVICE", kafkaapi.KafkaMessageReceiver.ServiceName),
		operation:       envOr("RELAY_OPERATION", kafkaapi.KafkaMessageReceiver.Receive.Name()),
	}
	var errs []error
	if kafkaErr != nil {
		errs = append(errs, kafkaErr)
	}
	var err error
	deleteDeadLettersOK := true
	if c.deleteDeadLetters, err = boolean("DELETE_DEAD_LETTERS"); err != nil {
		errs = append(errs, err)
		deleteDeadLettersOK = false
	}
	var missing []string
	for name, v := range map[string]string{"KAFKA_GROUP": c.group, "RELAY_ENDPOINT": c.endpoint} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		errs = append(errs, fmt.Errorf("set %s", strings.Join(missing, ", ")))
	}
	if deleteDeadLettersOK {
		switch {
		case c.deadLetterTopic == "" && !c.deleteDeadLetters:
			errs = append(errs, errors.New("set KAFKA_DEAD_LETTER_TOPIC, the topic undeliverable messages go to, "+
				"or DELETE_DEAD_LETTERS=true to delete them instead"))
		case c.deadLetterTopic != "" && c.deleteDeadLetters:
			errs = append(errs, errors.New("set KAFKA_DEAD_LETTER_TOPIC or DELETE_DEAD_LETTERS=true, not both: "+
				"undeliverable messages either go to that topic or are deleted"))
		case c.deadLetterTopic != "" && c.deadLetterTopic == os.Getenv("KAFKA_TOPIC"):
			// KAFKA_TOPIC from the environment: c.kafka is empty when kafka.ConfigFromEnv failed.
			errs = append(errs, errors.New("KAFKA_DEAD_LETTER_TOPIC must differ from KAFKA_TOPIC"))
		}
	}
	if c.batchSize, err = positiveInt("KAFKA_BATCH_SIZE", 100); err != nil {
		errs = append(errs, err)
	}
	if c.maxInputBytes, err = positiveInt("RELAY_MAX_INPUT_BYTES", defaultMaxInputBytes); err != nil {
		errs = append(errs, err)
	}
	if c.timeLimit, err = duration("KAFKA_BATCH_TIME_LIMIT", 0); err != nil {
		errs = append(errs, err)
	}
	if c.operationTimeout, err = duration("RELAY_OPERATION_TIMEOUT", 24*time.Hour); err != nil {
		errs = append(errs, err)
	} else if c.operationTimeout == 0 {
		errs = append(errs, errors.New("RELAY_OPERATION_TIMEOUT must be more than 0"))
	}
	return c, errors.Join(errs...)
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

// boolean reads name as true or false; unset is false.
func boolean(name string) (bool, error) {
	v := os.Getenv(name)
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false, not %q", name, v)
	}
	return b, nil
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

// report is the configuration and what the relay will do with it, for the log when it starts.
// namespace and address are where it connects to Temporal.
func (c config) report(namespace, address string) string {
	deadLetters := kafka.Setting{Name: "KAFKA_DEAD_LETTER_TOPIC", Value: c.deadLetterTopic}
	if c.deadLetterTopic == "" {
		deadLetters.Value = "(not set)"
	}
	sections := []kafka.Section{
		c.kafka.Settings(),
		{Title: "Relay", Settings: []kafka.Setting{
			{Name: "KAFKA_GROUP", Value: c.group},
			deadLetters,
			kafka.EnvSetting("DELETE_DEAD_LETTERS", c.deleteDeadLetters),
			kafka.EnvSetting("KAFKA_BATCH_SIZE", c.batchSize),
			kafka.EnvSetting("KAFKA_BATCH_TIME_LIMIT", c.timeLimit),
			{Name: "RELAY_ENDPOINT", Value: c.endpoint},
			kafka.EnvSetting("RELAY_SERVICE", c.service),
			kafka.EnvSetting("RELAY_OPERATION", c.operation),
			kafka.EnvSetting("RELAY_OPERATION_TIMEOUT", c.operationTimeout),
			kafka.EnvSetting("RELAY_MAX_INPUT_BYTES", c.maxInputBytes),
		}},
		{Title: "Temporal", Settings: []kafka.Setting{
			{Name: "namespace", Value: namespace},
			{Name: "address", Value: address},
		}},
	}

	batching := "delivering as soon as Kafka has any"
	if c.timeLimit > 0 {
		batching = fmt.Sprintf("waiting up to %s for a batch to fill", c.timeLimit)
	}
	duplicates := "Batches hold one message, so a message is never delivered to two operations."
	if c.batchSize > 1 {
		duplicates = "After a crash or a rebalance, a message can be delivered twice, in two operations; " +
			"set KAFKA_BATCH_SIZE=1 to rule that out."
	}
	undeliverable := fmt.Sprintf("Send messages that can't be delivered to the topic %q.", c.deadLetterTopic)
	if c.deleteDeadLetters {
		undeliverable = "Delete messages that can't be delivered: log each one and skip it, so it is never delivered."
	}
	behavior := []string{
		fmt.Sprintf("Join the consumer group %q and read the topic %q.", c.group, c.kafka.Topic),
		"A partition the consumer group has no committed offset for " + c.kafka.StartFromMeaning() + ".",
		fmt.Sprintf("Read batches of up to %d messages, %s.", c.batchSize, batching),
		fmt.Sprintf("For each batch, start one standalone Nexus operation per partition, %s/%s through the endpoint %q, "+
			"each allowed %s to complete. Nothing comes back: results aren't collected.", c.service, c.operation, c.endpoint, c.operationTimeout),
		duplicates,
		undeliverable,
	}
	return kafka.Report("relay", sections, behavior)
}
