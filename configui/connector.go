package main

// This file is what the setup page knows about this repo's programs, the Inbound Connector and the
// Outbound Connector, and the checks particular to them. The variables themselves come from
// setup.Inbound and setup.Outbound. The rest of the page is the configui package in nexus-connector-shared.

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/configui"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/temporalclient"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
)

const pageTitle = "Kafka connector setup"

var programs = []configui.Program{
	{
		Settings:      setup.Inbound,
		ClientOptions: temporalclient.Options,
		// No endpoint targets the Inbound Connector: it calls one per route, which checkRoutes
		// looks up.
		Check: func(ctx context.Context, env settings.Env, _ map[string]string) configui.Check {
			return checkRoutes(ctx, env)
		},
	},
	{
		Settings:      setup.Outbound,
		ClientOptions: temporalclient.Options,
		TaskQueue: func(env settings.Env) string {
			q, _ := setup.TaskQueue.Get(env) // any value is valid
			return q
		},
		EndpointName:            "kafka-endpoint",
		EndpointDescriptionFile: "./endpoint-description.md",
		Check: func(ctx context.Context, env settings.Env, _ map[string]string) configui.Check {
			c := configui.Check{Title: "Kafka"}
			topics, err := setup.KafkaTopics.Get(env)
			if err != nil {
				c.Skipped = "Fix KAFKA_TOPICS first."
				return c
			}
			for _, topic := range topics {
				checkKafka(ctx, env, topic, &c)
			}
			return c
		},
	},
}

// Time limits for each Kafka step of a test: finding a topic, and reading its last message.
var (
	identifyTimeout = 10 * time.Second
	readTimeout     = 15 * time.Second
)

// endpointTimeout bounds looking up the routes' endpoints in Temporal.
var endpointTimeout = 10 * time.Second

// checkRoutes is the Inbound Connector's check: the routes as resolved, that each route's endpoint
// exists in Temporal, then Kafka for each route. It runs only once the routes are valid.
func checkRoutes(ctx context.Context, env settings.Env) configui.Check {
	c := configui.Check{Title: "Routes"}
	routes, _, err := setup.LoadRoutes(env)
	if err != nil {
		c.Skipped = "Fix the routes first: the Settings check lists what is wrong."
		return c
	}
	for i, r := range routes {
		c.Passed = append(c.Passed, describeRoute(i, r))
	}
	checkEndpoints(ctx, env, routes, &c)
	for _, r := range routes {
		if checkKafka(ctx, env, r.Topic, &c) && r.DeadLetterTopic != "" {
			checkDeadLetterTopic(ctx, env, r.DeadLetterTopic, &c)
		}
	}
	return c
}

// describeRoute is route i, resolved, in one line.
func describeRoute(i int, r setup.Route) string {
	deadLetters := fmt.Sprintf("dead letters to %q", r.DeadLetterTopic)
	if r.DeleteDeadLetters {
		deadLetters = "dead letters deleted"
	}
	ordering, rate := "none", "no rate limit"
	if r.Ordered {
		ordering = "partition"
	}
	if r.MaxMessagesPerMinute > 0 {
		rate = fmt.Sprintf("at most %d messages a minute", r.MaxMessagesPerMinute)
	}
	return fmt.Sprintf("Route %d: topic %q to endpoint %q (%s/%s), group %q, start from %s, batches of up to %d "+
		"waiting up to %s, operations allowed %s, inputs up to %d bytes, %s, ordering %s, %s.",
		i, r.Topic, r.Endpoint, r.Service, r.Operation, r.Group, r.StartFrom, r.BatchSize, r.BatchTimeLimit,
		r.OperationTimeout, r.MaxInputBytes, rate, ordering, deadLetters)
}

// checkEndpoints looks up each route's endpoint in Temporal, connecting as the program would.
func checkEndpoints(ctx context.Context, env settings.Env, routes []setup.Route, c *configui.Check) {
	opts, err := temporalclient.Options(env)
	if err != nil {
		return // the Temporal check reports it
	}
	opts.Logger = sdklog.NewStructuredLogger(slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithTimeout(ctx, endpointTimeout)
	defer cancel()
	cl, err := client.DialContext(ctx, opts)
	if err != nil {
		c.Warnings = append(c.Warnings, "Couldn't connect to Temporal to look up the routes' endpoints; the Temporal check says why.")
		return
	}
	defer cl.Close()
	for _, r := range routes {
		resp, err := cl.OperatorService().ListNexusEndpoints(ctx, &operatorservice.ListNexusEndpointsRequest{PageSize: 1, Name: r.Endpoint})
		switch {
		case err != nil:
			c.Warnings = append(c.Warnings, fmt.Sprintf("Couldn't look up Nexus endpoint %q (topic %q): %v. "+
				"On Temporal Cloud, check in the Cloud UI or with tcld.", r.Endpoint, r.Topic, err))
		case len(resp.GetEndpoints()) == 0:
			c.Problems = append(c.Problems, fmt.Sprintf("Nexus endpoint %q (topic %q) doesn't exist. Create it, targeting the "+
				"namespace and task queue of the worker that serves it (a dev server forgets endpoints when it restarts): "+
				"temporal operator nexus endpoint create --name %s --target-namespace <namespace> --target-task-queue <task queue>",
				r.Endpoint, r.Topic, r.Endpoint))
		default:
			msg := fmt.Sprintf("Nexus endpoint %q (topic %q) exists", r.Endpoint, r.Topic)
			if w := resp.GetEndpoints()[0].GetSpec().GetTarget().GetWorker(); w != nil {
				msg += fmt.Sprintf(": it targets namespace %q and task queue %q", w.GetNamespace(), w.GetTaskQueue())
			}
			c.Passed = append(c.Passed, msg+".")
		}
	}
}

// checkKafka connects to Kafka with the Kafka settings in env, through the programs' own Kafka
// code, finds topic, and reads its last message, without a consumer group and committing nothing,
// adding what it finds to c. It reports whether topic was found.
func checkKafka(ctx context.Context, env settings.Env, topic string, c *configui.Check) bool {
	cfg, err := setup.LoadKafka(env)
	if err != nil {
		c.Skipped = "Fix the Kafka settings first."
		return false
	}
	if !findTopic(ctx, cfg, topic, "the topic", "", c) {
		return false
	}

	readCtx, cancel := context.WithTimeout(ctx, readTimeout)
	rec, ok, err := kafka.ReadLast(readCtx, cfg, topic)
	cancel()
	switch {
	case err != nil:
		c.Problems = append(c.Problems, fmt.Sprintf("Read the last message of %q: %s", topic, withTimeout(err, readTimeout)))
	case !ok:
		c.Passed = append(c.Passed, fmt.Sprintf("The topic %q has no messages to read.", topic))
	default:
		c.Passed = append(c.Passed, describeRecord(topic, rec)...)
	}
	return true
}

// checkDeadLetterTopic checks that a route's dead-letter topic exists and isn't internal.
func checkDeadLetterTopic(ctx context.Context, env settings.Env, topic string, c *configui.Check) {
	cfg, err := setup.LoadKafka(env)
	if err != nil {
		return
	}
	findTopic(ctx, cfg, topic, "the dead-letter topic", "create it, or set deleteDeadLetters to true instead", c)
}

// findTopic connects to Kafka and finds topic, as the programs check every topic at startup: it
// must exist and not be internal. what names it in the messages, such as "the dead-letter topic";
// fix, if any, says what to do when it isn't found. It reports whether topic passed.
func findTopic(ctx context.Context, cfg kafka.Config, topic, what, fix string, c *configui.Check) bool {
	brokers := strings.Join(cfg.Brokers, ",")
	idCtx, cancel := context.WithTimeout(ctx, identifyTimeout)
	id, err := kafka.Identify(idCtx, cfg, topic)
	cancel()
	switch {
	case err != nil:
		msg := fmt.Sprintf("Connect to Kafka at %s and find %s %q", brokers, what, topic)
		if fix != "" {
			msg += " (" + fix + ")"
		}
		c.Problems = append(c.Problems, msg+": "+withTimeout(err, identifyTimeout))
		return false
	case id.Internal:
		c.Problems = append(c.Problems, fmt.Sprintf("The topic %q %v.", topic, kafka.ErrInternalTopic))
		return false
	}
	c.Passed = append(c.Passed, fmt.Sprintf("Connected to Kafka at %s (cluster %s) and found %s %q.", brokers, id.Cluster, what, topic))
	return true
}

// describeRecord is the message in lines for the page: where it is, then its key, value and
// headers, as text, or base64 if they aren't UTF-8.
func describeRecord(topic string, r kafkaapi.Record) []string {
	lines := []string{fmt.Sprintf("Read the last message of %q: partition %d, offset %d, timestamp %s.", topic, r.Partition, r.Offset, r.Timestamp)}
	lines = append(lines, "Key: "+text(r.Key))
	if r.Tombstone {
		lines = append(lines, "Value: none (a tombstone)")
	} else {
		lines = append(lines, "Value: "+text(r.Value))
	}
	for _, h := range r.Headers {
		lines = append(lines, "Header "+h.Key+": "+text(h.Value))
	}
	return lines
}

// text is b as text, or as base64, saying so, if it isn't UTF-8.
func text(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return "(base64) " + base64.StdEncoding.EncodeToString(b)
}

// withTimeout is err's text, saying so when the time limit ran out, which Kafka's client reports
// only as a deadline.
func withTimeout(err error, limit time.Duration) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Sprintf("%v (no answer within %s: check that the brokers are right and reachable)", err, limit)
	}
	return err.Error()
}
