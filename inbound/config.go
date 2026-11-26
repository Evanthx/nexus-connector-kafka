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
	kafka          kafka.Config
	routes         []setup.Route // from INBOUND_CONFIG or INBOUND_CONFIG_FILE: one per topic
	source         string        // where the routes came from, for the report
	statusInterval time.Duration // INBOUND_STATUS_INTERVAL: log a status line after this long with nothing delivered; 0 for never
}

// configFrom checks every variable the Inbound Connector reads (setup.Inbound) in env, and the
// routes, reporting every problem at once, and returns them.
func configFrom(env settings.Env) (config, error) {
	vals, err := setup.Inbound.Load(env)
	if err != nil {
		return config{}, err
	}
	kafkaCfg, err := setup.Kafka(vals)
	if err != nil {
		return config{}, err
	}
	// Load checked the routes already; this reads them again, in case the file changed.
	routes, source, err := setup.LoadRoutes(env)
	if err != nil {
		return config{}, err
	}
	return config{
		kafka:          kafkaCfg,
		routes:         routes,
		source:         source,
		statusInterval: setup.InboundStatusInterval.From(vals),
	}, nil
}

// report is the configuration and what the Inbound Connector will do with it, for the log when it
// starts. env is what c was read from; temporal is the client options it connects to Temporal with.
func (c config) report(env settings.Env, temporal client.Options) string {
	sections := []report.Section{
		c.kafka.Settings(env),
		{Title: "Inbound Connector", Settings: []report.Setting{
			{Name: "routes", Value: c.source},
			report.Env(env, "INBOUND_STATUS_INTERVAL", c.statusInterval),
		}},
		temporalclient.Section(env, temporal),
	}
	var behavior []string
	for i, r := range c.routes {
		sections = append(sections, routeSection(i, r))
		behavior = append(behavior, routeBehavior(r)...)
	}
	return report.Format("Inbound Connector", sections, behavior)
}

// routeSection is r's resolved settings, each marked with where it came from unless the route
// set it.
func routeSection(i int, r setup.Route) report.Section {
	deadLetters := r.DeadLetterTopic
	if deadLetters == "" {
		deadLetters = "(not set)"
	}
	values := map[string]string{
		"topic":                r.Topic,
		"endpoint":             r.Endpoint,
		"group":                r.Group,
		"service":              r.Service,
		"operation":            r.Operation,
		"startFrom":            string(r.StartFrom),
		"deadLetterTopic":      deadLetters,
		"deleteDeadLetters":    strconv.FormatBool(r.DeleteDeadLetters),
		"batchSize":            strconv.Itoa(r.BatchSize),
		"batchTimeLimit":       report.Duration(r.BatchTimeLimit),
		"operationTimeout":     report.Duration(r.OperationTimeout),
		"maxInputBytes":        strconv.Itoa(r.MaxInputBytes),
		"maxMessagesPerMinute": rateMeaning(r),
		"ordering":             orderingMeaning(r),
	}
	s := report.Section{Title: fmt.Sprintf("Route %d: %s", i, r.Topic)}
	for _, f := range setup.RouteFields {
		v := values[f.Name]
		switch r.Origin[f.Name] {
		case setup.FromRoute:
		case setup.FromDefaults:
			v += " (from defaults)"
		default:
			if f.Name == "group" {
				v += " (derived from topic and endpoint)"
			} else {
				v += " (default)"
			}
		}
		s.Settings = append(s.Settings, report.Setting{Name: f.Name, Value: v})
	}
	return s
}

// routeBehavior is what the Inbound Connector will do for r, for the report.
func routeBehavior(r setup.Route) []string {
	batching := "delivering as soon as Kafka has any"
	if r.BatchTimeLimit > 0 {
		batching = fmt.Sprintf("waiting up to %s for a batch to fill", r.BatchTimeLimit)
	}
	duplicates := "Batches hold one message, so a message is never delivered to two operations."
	if r.BatchSize > 1 {
		duplicates = "After a crash or a rebalance, a message can be delivered twice, in two operations; " +
			"set batchSize to 1 to rule that out."
	}
	undeliverable := fmt.Sprintf("Send messages that can't be delivered to the topic %q.", r.DeadLetterTopic)
	if r.DeleteDeadLetters {
		undeliverable = "Delete messages that can't be delivered: log each one and skip it, so it is never delivered."
	}
	lines := []string{
		fmt.Sprintf("Join the consumer group %q and read the topic %q.", r.Group, r.Topic),
		"A partition the consumer group has no committed offset for " + r.StartFrom.Meaning() + ".",
		fmt.Sprintf("Read batches of up to %d messages, %s.", r.BatchSize, batching),
		fmt.Sprintf("For each batch, start one standalone Nexus operation per partition, %s/%s through the endpoint %q, "+
			"each allowed %s to complete. Nothing comes back: results aren't collected.", r.Service, r.Operation, r.Endpoint, r.OperationTimeout),
		orderingBehavior(r),
		rateBehavior(r),
		duplicates,
		undeliverable,
	}
	for i, l := range lines {
		lines[i] = fmt.Sprintf("Topic %q: %s", r.Topic, l)
	}
	return lines
}

// rateMeaning is maxMessagesPerMinute for the report.
func rateMeaning(r setup.Route) string {
	if r.MaxMessagesPerMinute == 0 {
		return "no limit"
	}
	return strconv.Itoa(r.MaxMessagesPerMinute)
}

// rateBehavior says how fast the route sends, for the report.
func rateBehavior(r setup.Route) string {
	if r.MaxMessagesPerMinute == 0 {
		return "Send messages as fast as Kafka has them: maxMessagesPerMinute isn't set."
	}
	return fmt.Sprintf("Send at most %d messages a minute from this process, spread evenly; messages waiting their turn stay in Kafka.", r.MaxMessagesPerMinute)
}

// orderingMeaning is ordering for the report.
func orderingMeaning(r setup.Route) string {
	if r.Ordered {
		return "partition"
	}
	return "none"
}

// orderingBehavior says whether batches wait for each other, for the report.
func orderingBehavior(r setup.Route) string {
	if r.Ordered {
		return "Process each partition in order (ordering partition): wait for each operation to complete before " +
			"starting the next for its partition, and commit only then. At most one operation per partition runs at once. " +
			"A failed operation isn't started again or dead-lettered: it is logged, and the partition carries on."
	}
	return "Start each batch's operation without waiting for the one before (ordering none): a later batch can " +
		"be processed before an earlier one, even from the same partition."
}
