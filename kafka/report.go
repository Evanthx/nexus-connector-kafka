package kafka

import (
	"fmt"
	"strings"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/report"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

// Settings are the Kafka settings in c, read from env, as a section of a startup report. It never
// shows the password, only whether one is set.
func (c Config) Settings(env settings.Env) report.Section {
	login := "(not set: no login)"
	if c.Username != "" {
		password := "KAFKA_PASSWORD is not set"
		if c.Password != "" {
			password = "KAFKA_PASSWORD is set"
		}
		login = fmt.Sprintf("%s (%s; %s)", c.Username, c.mechanism(), password)
	}
	tlsLine := "off"
	if c.TLS != nil {
		tlsLine = "on"
		if c.TLS.RootCAs == nil {
			tlsLine += ", brokers checked with the system's CAs"
		} else {
			tlsLine += ", brokers checked with KAFKA_TLS_SERVER_CA_CERT_PATH"
		}
		if len(c.TLS.Certificates) > 0 {
			tlsLine += ", client certificate (mTLS)"
		}
	}
	return report.Section{Title: "Kafka", Settings: []report.Setting{
		{Name: "KAFKA_BROKERS", Value: strings.Join(c.Brokers, ",")},
		{Name: "KAFKA_USERNAME", Value: login},
		{Name: "KAFKA_TLS", Value: tlsLine},
		report.Env(env, "KAFKA_CLIENT_LOG_LEVEL", strings.ToLower(c.clientLogLevel().String())),
	}}
}

// Meaning says in a sentence where a consumer group starts reading a partition it has no
// committed offset for.
func (s StartFrom) Meaning() string {
	if s == StartFromNewest {
		return "starts at the end, so only messages written from then on are read"
	}
	return "starts at the oldest message still in the topic"
}
