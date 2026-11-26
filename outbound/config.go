package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/report"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/temporalclient"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"go.temporal.io/sdk/client"
)

// config is the Outbound Connector's settings besides Kafka's.
type config struct {
	topics    []string // KAFKA_TOPICS: the topics callers may write to
	taskQueue string   // NEXUS_TASK_QUEUE: the task queue the Nexus endpoint targets
}

// configFrom checks every variable the Outbound Connector reads (setup.Outbound) in env, reporting
// every problem at once, and returns Kafka's settings and the Outbound Connector's own.
func configFrom(env settings.Env) (kafka.Config, config, error) {
	vals, err := setup.Outbound.Load(env)
	if err != nil {
		return kafka.Config{}, config{}, err
	}
	kafkaCfg, err := setup.Kafka(vals)
	if err != nil {
		return kafka.Config{}, config{}, err
	}
	return kafkaCfg, config{
		topics:    setup.KafkaTopics.From(vals),
		taskQueue: setup.TaskQueue.From(vals),
	}, nil
}

// report is the configuration and what the Outbound Connector will do with it, for the log when it
// starts. env is what s and cfg, Kafka's settings, were read from; temporal is the
// client options it connects to Temporal with.
func (s config) report(env settings.Env, cfg kafka.Config, temporal client.Options) string {
	sections := []report.Section{
		cfg.Settings(env),
		{Title: "Outbound Connector", Settings: []report.Setting{
			report.Env(env, "KAFKA_TOPICS", strings.Join(s.topics, ",")),
			report.Env(env, "NEXUS_TASK_QUEUE", s.taskQueue),
		}},
		temporalclient.Section(env, temporal),
	}
	behavior := []string{
		fmt.Sprintf("Serve the KafkaService Nexus service, with the operation write, for the topics %s.", quoted(s.topics)),
		fmt.Sprintf("Take Nexus requests on the task queue %q: point this Outbound Connector's Nexus endpoint at it.", s.taskQueue),
		"Write messages to the topic each write names within its Nexus request, with no activity, each with a dedupe-id header. " +
			"A write to a topic not listed is Rejected and writes nothing. " +
			"A failed write is retried by the caller's Temporal server until the caller's Nexus operation times out.",
	}
	return report.Format("Outbound Connector", sections, behavior)
}

// quoted is topics, each quoted, comma-separated.
func quoted(topics []string) string {
	q := make([]string, len(topics))
	for i, t := range topics {
		q[i] = strconv.Quote(t)
	}
	return strings.Join(q, ", ")
}
