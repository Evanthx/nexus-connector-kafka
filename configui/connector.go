package main

// This file is what the setup page knows about this repo's programs, the relay and the connector,
// and the checks particular to them. The variables themselves come from setup.Relay and
// setup.Connector. The rest of configui is the same in every Go connector repo.

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/settings"
	"github.com/temporalio/nexus-connector-kafka/setup"
)

const pageTitle = "Kafka connector setup"

var programs = []program{
	{
		Settings:      setup.Relay,
		ClientOptions: setup.ClientOptions,
		// No endpoint targets the relay: it calls one, RELAY_ENDPOINT.
		CallsEndpoint: func(env settings.Env) string { return settings.Get(env, setup.RelayEndpoint.Name) },
		Check: func(ctx context.Context, env settings.Env, _ map[string]string) check {
			return checkKafka(ctx, env, relayChecks)
		},
	},
	{
		Settings:      setup.Connector,
		ClientOptions: setup.ClientOptions,
		TaskQueue: func(env settings.Env) string {
			q, _ := setup.TaskQueue.Get(env) // any value is valid
			return q
		},
		EndpointName:            "kafka-endpoint",
		EndpointDescriptionFile: "./endpoint-description.md",
		Check: func(ctx context.Context, env settings.Env, _ map[string]string) check {
			return checkKafka(ctx, env, nil)
		},
	},
}

// Time limits for each Kafka step of a test: finding a topic, and reading the topic's last message.
var (
	identifyTimeout = 10 * time.Second
	readTimeout     = 15 * time.Second
)

// checkKafka connects to Kafka with the Kafka settings in env, through the programs' own Kafka
// code, finds the topic, and reads its last message, without a consumer group and committing
// nothing. Then it runs more, the program's own checks, if any. It stops at the first step that fails,
// with its whole error.
func checkKafka(ctx context.Context, env settings.Env, more func(ctx context.Context, env settings.Env, cfg kafka.Config, c *check)) check {
	c := check{Title: "Kafka"}
	cfg, err := kafka.LoadConfig(env)
	if err != nil {
		c.Skipped = "Fix the Kafka settings first."
		return c
	}
	brokers := strings.Join(cfg.Brokers, ",")
	idCtx, cancel := context.WithTimeout(ctx, identifyTimeout)
	id, err := kafka.Identify(idCtx, cfg)
	cancel()
	if err != nil {
		c.Problems = append(c.Problems, fmt.Sprintf("Connect to Kafka at %s and find the topic %q: %s", brokers, cfg.Topic, withTimeout(err, identifyTimeout)))
		return c
	}
	c.Passed = append(c.Passed, fmt.Sprintf("Connected to Kafka at %s (cluster %s) and found the topic %q.", brokers, id.Cluster, cfg.Topic))

	readCtx, cancel := context.WithTimeout(ctx, readTimeout)
	rec, ok, err := kafka.ReadLast(readCtx, cfg)
	cancel()
	switch {
	case err != nil:
		c.Problems = append(c.Problems, "Read the topic's last message: "+withTimeout(err, readTimeout))
		return c
	case !ok:
		c.Passed = append(c.Passed, "The topic has no messages to read.")
	default:
		c.Passed = append(c.Passed, describeRecord(rec)...)
	}
	if more != nil {
		more(ctx, env, cfg, &c)
	}
	return c
}

// relayChecks checks that the dead-letter topic exists, if one is set.
func relayChecks(ctx context.Context, env settings.Env, cfg kafka.Config, c *check) {
	topic := settings.Get(env, setup.DeadLetterTopic.Name)
	if topic == "" {
		return
	}
	dlq := cfg
	dlq.Topic = topic
	idCtx, cancel := context.WithTimeout(ctx, identifyTimeout)
	_, err := kafka.Identify(idCtx, dlq)
	cancel()
	if err != nil {
		c.Problems = append(c.Problems, fmt.Sprintf("Can't find the dead-letter topic %q; create it, or set DELETE_DEAD_LETTERS=true "+
			"instead: %s", topic, withTimeout(err, identifyTimeout)))
		return
	}
	c.Passed = append(c.Passed, fmt.Sprintf("The dead-letter topic %q exists.", topic))
}

// describeRecord is the message in lines for the page: where it is, then its key, value and
// headers, as text, or base64 if they aren't UTF-8.
func describeRecord(r kafkaapi.Record) []string {
	lines := []string{fmt.Sprintf("Read the topic's last message: partition %d, offset %d, timestamp %s.", r.Partition, r.Offset, r.Timestamp)}
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
