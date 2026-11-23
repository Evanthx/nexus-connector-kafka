package setup

import (
	"fmt"
	"strings"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

// This file lists the Kafka variables both programs read, as one section that Inbound and Outbound
// share. The programs load them with their Program and read them with Kafka; the setup page shows
// them and `make docs` generates docs/setup.md's tables from them. Add a new Kafka variable here
// only, with its help text and check, then run `make docs`.

// KafkaSection is the Kafka variables, in the order the setup page and the docs show them.
var KafkaSection = &settings.Section{Title: "Kafka", Variables: []*settings.Variable{
	brokersSetting.Variable, topicSetting.Variable, usernameSetting.Variable, passwordSetting.Variable,
	clientLogLevelSetting.Variable,
}}

var clientLogLevelSetting = settings.Define(settings.Variable{
	Name:    "KAFKA_CLIENT_LOG_LEVEL",
	Default: "warn",
	Help: "How much the Kafka client library (franz-go) logs, in the program's log; at `warn`, failed connections " +
		"to brokers, failed logins and consumer group errors, which show why the program stopped making progress: " +
		settings.LogLevelHelp,
}, settings.LogLevel("KAFKA_CLIENT_LOG_LEVEL"))

var brokersSetting = settings.Define(settings.Variable{
	Name:     "KAFKA_BROKERS",
	Required: "yes",
	Help: "The Kafka brokers to connect to, as a comma-separated list of `host:port`, such as " +
		"`broker1:9092,broker2:9092`. One reachable broker is enough to find the rest.",
}, func(v string) ([]string, error) {
	if v == "" {
		return nil, settings.Required("KAFKA_BROKERS", "the Kafka brokers, as comma-separated host:port")
	}
	return strings.Split(v, ","), nil
})

var topicSetting = settings.Define(settings.Variable{
	Name:     "KAFKA_TOPIC",
	Required: "yes",
	Help:     "The topic the program serves. One process serves one topic; run one per topic.",
}, settings.RequiredText("KAFKA_TOPIC", "the topic to serve"))

var usernameSetting = settings.Define(settings.Variable{
	Name:    "KAFKA_USERNAME",
	Default: "none: no authentication",
	Help: "The SASL/SCRAM-SHA-256 username. Leave it and `KAFKA_PASSWORD` unset if Kafka needs no login. " +
		"TLS and other SASL mechanisms are not supported.",
}, settings.Text)

var passwordSetting = settings.Define(settings.Variable{
	Name:    "KAFKA_PASSWORD",
	Default: "none",
	Secret:  true,
	Help:    "The password for `KAFKA_USERNAME`. The startup report shows only whether it is set.",
}, settings.Text)

// KafkaStartFrom is KAFKA_START_FROM. Only the Inbound Connector reads it, since only the Inbound
// Connector consumes with a consumer group, so it is in the Inbound Connector's section rather than
// KafkaSection.
var KafkaStartFrom = settings.Define(settings.Variable{
	Name:    "KAFKA_START_FROM",
	Default: string(kafka.StartFromOldest),
	Help: "Where a consumer group with no committed offset starts reading a partition: `oldest`, every " +
		"message still in the topic, or `newest`, only messages written from then on.",
	Docs: "See [How it works](how-it-works.md#where-reading-starts).",
}, func(v string) (kafka.StartFrom, error) {
	switch s := kafka.StartFrom(v); s {
	case "":
		return kafka.StartFromOldest, nil
	case kafka.StartFromOldest, kafka.StartFromNewest:
		return s, nil
	default:
		return "", fmt.Errorf("KAFKA_START_FROM must be %s or %s, not %q", kafka.StartFromOldest, kafka.StartFromNewest, v)
	}
})

// Kafka is the Kafka configuration in vals, which the Load of a program with KafkaSection made.
// StartFrom is set only if the program has KafkaStartFrom too.
func Kafka(vals *settings.Values) kafka.Config {
	return kafka.Config{
		Brokers:        brokersSetting.From(vals),
		Topic:          topicSetting.From(vals),
		Username:       usernameSetting.From(vals),
		Password:       passwordSetting.From(vals),
		StartFrom:      KafkaStartFrom.From(vals),
		ClientLogLevel: clientLogLevelSetting.From(vals),
	}
}

// LoadKafka checks the Kafka variables alone in env, every problem at once, and returns their
// configuration. A program loads them with the rest of its variables instead.
func LoadKafka(env settings.Env) (kafka.Config, error) {
	vals, err := (&settings.Program{Sections: []*settings.Section{KafkaSection}}).Load(env)
	if err != nil {
		return kafka.Config{}, err
	}
	return Kafka(vals), nil
}
