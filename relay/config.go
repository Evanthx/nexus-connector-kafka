package main

import (
	"fmt"
	"time"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/settings"
	"github.com/temporalio/nexus-connector-kafka/setup"
)

// config is the relay's settings, read from the environment by configFrom.
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

// configFrom checks every variable the relay reads (setup.Relay) in env, reporting every problem
// at once, and returns them.
func configFrom(env settings.Env) (config, error) {
	vals, err := setup.Relay.Load(env)
	if err != nil {
		return config{}, err
	}
	return config{
		kafka:             kafka.ConfigFrom(vals),
		group:             setup.RelayGroup.From(vals),
		deadLetterTopic:   setup.DeadLetterTopic.From(vals),
		deleteDeadLetters: setup.DeleteDeadLetters.From(vals),
		batchSize:         setup.BatchSize.From(vals),
		timeLimit:         setup.BatchTimeLimit.From(vals),
		endpoint:          setup.RelayEndpoint.From(vals),
		service:           setup.RelayService.From(vals),
		operation:         setup.RelayOperation.From(vals),
		operationTimeout:  setup.RelayOperationTimeout.From(vals),
		maxInputBytes:     setup.RelayMaxInputBytes.From(vals),
	}, nil
}

// report is the configuration and what the relay will do with it, for the log when it starts. env
// is what c was read from; namespace and address are where it connects to Temporal.
func (c config) report(env settings.Env, namespace, address string) string {
	deadLetters := kafka.Setting{Name: "KAFKA_DEAD_LETTER_TOPIC", Value: c.deadLetterTopic}
	if c.deadLetterTopic == "" {
		deadLetters.Value = "(not set)"
	}
	sections := []kafka.Section{
		c.kafka.Settings(env),
		{Title: "Relay", Settings: []kafka.Setting{
			{Name: "KAFKA_GROUP", Value: c.group},
			kafka.EnvSetting(env, "KAFKA_START_FROM", c.kafka.StartFrom),
			deadLetters,
			kafka.EnvSetting(env, "DELETE_DEAD_LETTERS", c.deleteDeadLetters),
			kafka.EnvSetting(env, "KAFKA_BATCH_SIZE", c.batchSize),
			kafka.EnvSetting(env, "KAFKA_BATCH_TIME_LIMIT", c.timeLimit),
			{Name: "RELAY_ENDPOINT", Value: c.endpoint},
			kafka.EnvSetting(env, "RELAY_SERVICE", c.service),
			kafka.EnvSetting(env, "RELAY_OPERATION", c.operation),
			kafka.EnvSetting(env, "RELAY_OPERATION_TIMEOUT", c.operationTimeout),
			kafka.EnvSetting(env, "RELAY_MAX_INPUT_BYTES", c.maxInputBytes),
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
