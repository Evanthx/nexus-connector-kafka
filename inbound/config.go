package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/report"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/temporalclient"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"go.temporal.io/sdk/client"
)

// config is the Inbound Connector's settings, read from the environment by configFrom.
type config struct {
	kafka             kafka.Config
	group             string        // KAFKA_GROUP: the consumer group the Inbound Connector joins
	deadLetterTopic   string        // KAFKA_DEAD_LETTER_TOPIC: where undeliverable messages go
	deleteDeadLetters bool          // DELETE_DEAD_LETTERS: log and skip undeliverable messages instead
	batchSize         int           // KAFKA_BATCH_SIZE: most messages per batch; 1 for exactly one operation per message
	timeLimit         time.Duration // KAFKA_BATCH_TIME_LIMIT: how long a batch waits to fill
	endpoint          string        // INBOUND_ENDPOINT: the Nexus endpoint messages are sent to
	service           string        // INBOUND_SERVICE: the Nexus service
	operation         string        // INBOUND_OPERATION: the operation started for each batch
	operationTimeout  time.Duration // INBOUND_OPERATION_TIMEOUT: how long each operation may take
	maxInputBytes     int           // INBOUND_MAX_INPUT_BYTES: the largest encoded input sent to an operation
	maxPerMinute      int           // INBOUND_MAX_MESSAGES_PER_MINUTE: the most messages sent a minute; 0 for no limit
	ordered           bool          // INBOUND_ORDERING=partition: wait for each operation to complete before the next
	statusInterval    time.Duration // INBOUND_STATUS_INTERVAL: log a status line after this long with nothing delivered; 0 for never
}

// configFrom checks every variable the Inbound Connector reads (setup.Inbound) in env, reporting
// every problem at once, and returns them.
func configFrom(env settings.Env) (config, error) {
	vals, err := setup.Inbound.Load(env)
	if err != nil {
		return config{}, err
	}
	kafkaCfg, err := setup.Kafka(vals)
	if err != nil {
		return config{}, err
	}
	return config{
		kafka:             kafkaCfg,
		group:             setup.InboundGroup.From(vals),
		deadLetterTopic:   setup.DeadLetterTopic.From(vals),
		deleteDeadLetters: setup.DeleteDeadLetters.From(vals),
		batchSize:         setup.BatchSize.From(vals),
		timeLimit:         setup.BatchTimeLimit.From(vals),
		endpoint:          setup.InboundEndpoint.From(vals),
		service:           setup.InboundService.From(vals),
		operation:         setup.InboundOperation.From(vals),
		operationTimeout:  setup.InboundOperationTimeout.From(vals),
		maxInputBytes:     setup.InboundMaxInputBytes.From(vals),
		maxPerMinute:      setup.InboundMaxMessagesPerMinute.From(vals),
		ordered:           setup.InboundOrdering.From(vals),
		statusInterval:    setup.InboundStatusInterval.From(vals),
	}, nil
}

// report is the configuration and what the Inbound Connector will do with it, for the log when it
// starts. env is what c was read from; temporal is the client options it connects to Temporal with.
func (c config) report(env settings.Env, temporal client.Options) string {
	deadLetters := report.Setting{Name: "KAFKA_DEAD_LETTER_TOPIC", Value: c.deadLetterTopic}
	if c.deadLetterTopic == "" {
		deadLetters.Value = "(not set)"
	}
	sections := []report.Section{
		c.kafka.Settings(env),
		{Title: "Inbound Connector", Settings: []report.Setting{
			{Name: "KAFKA_GROUP", Value: c.group},
			report.Env(env, "KAFKA_START_FROM", c.kafka.StartFrom),
			deadLetters,
			report.Env(env, "DELETE_DEAD_LETTERS", c.deleteDeadLetters),
			report.Env(env, "KAFKA_BATCH_SIZE", c.batchSize),
			report.Env(env, "KAFKA_BATCH_TIME_LIMIT", c.timeLimit),
			{Name: "INBOUND_ENDPOINT", Value: c.endpoint},
			report.Env(env, "INBOUND_SERVICE", c.service),
			report.Env(env, "INBOUND_OPERATION", c.operation),
			report.Env(env, "INBOUND_OPERATION_TIMEOUT", c.operationTimeout),
			report.Env(env, "INBOUND_MAX_INPUT_BYTES", c.maxInputBytes),
			report.Env(env, "INBOUND_MAX_MESSAGES_PER_MINUTE", c.rateMeaning()),
			report.Env(env, "INBOUND_ORDERING", c.orderingMeaning()),
			report.Env(env, "INBOUND_STATUS_INTERVAL", c.statusInterval),
		}},
		temporalclient.Section(env, temporal),
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
		c.orderingBehavior(),
		c.rateBehavior(),
		duplicates,
		undeliverable,
	}
	return report.Format("Inbound Connector", sections, behavior)
}

// rateMeaning is INBOUND_MAX_MESSAGES_PER_MINUTE for the report.
func (c config) rateMeaning() string {
	if c.maxPerMinute == 0 {
		return "no limit"
	}
	return strconv.Itoa(c.maxPerMinute)
}

// rateBehavior says how fast the Inbound Connector sends, for the report.
func (c config) rateBehavior() string {
	if c.maxPerMinute == 0 {
		return "Send messages as fast as Kafka has them: INBOUND_MAX_MESSAGES_PER_MINUTE isn't set."
	}
	return fmt.Sprintf("Send at most %d messages a minute from this process, spread evenly; messages waiting their turn stay in Kafka.", c.maxPerMinute)
}

// orderingMeaning is INBOUND_ORDERING for the report.
func (c config) orderingMeaning() string {
	if c.ordered {
		return "partition"
	}
	return "none"
}

// orderingBehavior says whether batches wait for each other, for the report.
func (c config) orderingBehavior() string {
	if c.ordered {
		return "Process each partition in order (INBOUND_ORDERING=partition): wait for each operation to complete before " +
			"starting the next for its partition, and commit only then. At most one operation per partition runs at once. " +
			"A failed operation starts again if its error is retryable; otherwise its messages go to the dead-letter topic."
	}
	return "Start each batch's operation without waiting for the one before (INBOUND_ORDERING=none): a later batch can " +
		"be processed before an earlier one, even from the same partition."
}
