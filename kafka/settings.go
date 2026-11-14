package kafka

import (
	"fmt"
	"strings"

	"github.com/temporalio/nexus-connector-kafka/settings"
)

// This file lists the Kafka variables both programs read, as one section that setup.Relay and
// setup.Connector share. The programs load them with their Program and read them with ConfigFrom;
// the setup page shows them and `make docs` generates docs/setup.md's tables from them. Add a new
// Kafka variable here only, with its help text and check, then run `make docs`.

// SettingsSection is the Kafka variables, in the order the setup page and the docs show them.
var SettingsSection = &settings.Section{Title: "Kafka", Variables: []*settings.Variable{
	brokersSetting.Variable, topicSetting.Variable, usernameSetting.Variable, passwordSetting.Variable,
}}

var brokersSetting = settings.Define(settings.Variable{
	Name:     "KAFKA_BROKERS",
	Required: "yes",
	Help: "The Kafka brokers to connect to, as a comma-separated list of `host:port`, such as " +
		"`broker1:9092,broker2:9092`. One reachable broker is enough to find the rest.",
}, func(v string) ([]string, error) {
	if v == "" {
		return nil, required("KAFKA_BROKERS", "the Kafka brokers, as comma-separated host:port")
	}
	return strings.Split(v, ","), nil
})

var topicSetting = settings.Define(settings.Variable{
	Name:     "KAFKA_TOPIC",
	Required: "yes",
	Help:     "The topic the program serves. One process serves one topic; run one per topic.",
}, func(v string) (string, error) {
	if v == "" {
		return "", required("KAFKA_TOPIC", "the topic to serve")
	}
	return v, nil
})

var usernameSetting = settings.Define(settings.Variable{
	Name:    "KAFKA_USERNAME",
	Default: "none: no authentication",
	Help: "The SASL/SCRAM-SHA-256 username. Leave it and `KAFKA_PASSWORD` unset if Kafka needs no login. " +
		"TLS and other SASL mechanisms are not supported.",
}, text)

var passwordSetting = settings.Define(settings.Variable{
	Name:    "KAFKA_PASSWORD",
	Default: "none",
	Secret:  true,
	Help:    "The password for `KAFKA_USERNAME`. The startup report shows only whether it is set.",
}, text)

// StartFromSetting is KAFKA_START_FROM. Only the relay reads it, since only the relay consumes
// with a consumer group, so it is in the relay's section rather than SettingsSection.
var StartFromSetting = settings.Define(settings.Variable{
	Name:    "KAFKA_START_FROM",
	Default: string(StartFromOldest),
	Help: "Where a consumer group with no committed offset starts reading a partition: `oldest`, every " +
		"message still in the topic, or `newest`, only messages written from then on.",
	Docs: "See [How it works](how-it-works.md#where-reading-starts).",
}, func(v string) (StartFrom, error) {
	switch s := StartFrom(v); s {
	case "":
		return StartFromOldest, nil
	case StartFromOldest, StartFromNewest:
		return s, nil
	default:
		return "", fmt.Errorf("KAFKA_START_FROM must be %s or %s, not %q", StartFromOldest, StartFromNewest, v)
	}
})

// ConfigFrom is the Kafka configuration in vals, which the Load of a program with SettingsSection
// made. StartFrom is set only if the program has StartFromSetting too.
func ConfigFrom(vals *settings.Values) Config {
	return Config{
		Brokers:   brokersSetting.From(vals),
		Topic:     topicSetting.From(vals),
		Username:  usernameSetting.From(vals),
		Password:  passwordSetting.From(vals),
		StartFrom: StartFromSetting.From(vals),
	}
}

// LoadConfig checks the Kafka variables alone in env, every problem at once, and returns their
// configuration. A program loads them with the rest of its variables instead.
func LoadConfig(env settings.Env) (Config, error) {
	vals, err := (&settings.Program{Sections: []*settings.Section{SettingsSection}}).Load(env)
	if err != nil {
		return Config{}, err
	}
	return ConfigFrom(vals), nil
}

// required is the error for a required variable that isn't set; what says what to set it to.
func required(name, what string) error {
	return fmt.Errorf("set %s: %s", name, what)
}

// text is the parse function of a variable that takes any value.
func text(v string) (string, error) { return v, nil }
