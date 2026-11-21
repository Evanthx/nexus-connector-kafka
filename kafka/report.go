package kafka

import (
	"strings"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/report"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

// Settings are the Kafka settings in c, read from env, as a section of a startup report. It never
// shows the password, only whether one is set.
func (c Config) Settings(env settings.Env) report.Section {
	login := "(not set: no authentication)"
	if c.Username != "" {
		login = c.Username + " (SASL/SCRAM-SHA-256; KAFKA_PASSWORD is not set)"
		if c.Password != "" {
			login = c.Username + " (SASL/SCRAM-SHA-256; KAFKA_PASSWORD is set)"
		}
	}
	return report.Section{Title: "Kafka", Settings: []report.Setting{
		{Name: "KAFKA_BROKERS", Value: strings.Join(c.Brokers, ",")},
		{Name: "KAFKA_TOPIC", Value: c.Topic},
		{Name: "KAFKA_USERNAME", Value: login},
		report.Env(env, "KAFKA_CLIENT_LOG_LEVEL", strings.ToLower(c.clientLogLevel().String())),
	}}
}

// StartFromMeaning says in a sentence where a consumer group starts reading a partition it has no
// committed offset for.
func (c Config) StartFromMeaning() string {
	if c.StartFrom == StartFromNewest {
		return "starts at the end, so only messages written from then on are read"
	}
	return "starts at the oldest message still in the topic"
}
