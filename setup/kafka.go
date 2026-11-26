package setup

import (
	"errors"
	"fmt"
	"slices"
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
	brokersSetting.Variable, usernameSetting.Variable, passwordSetting.Variable,
	mechanismSetting.Variable, tlsSetting.Variable, tlsServerCASetting.Variable, tlsClientCertSetting.Variable,
	tlsClientKeySetting.Variable, clientLogLevelSetting.Variable,
}}

// KafkaRules check the Kafka variables against each other. Every program with KafkaSection lists
// them in its Rules.
var KafkaRules = []func(settings.Env) error{mechanismRule, tlsRule}

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

var usernameSetting = settings.Define(settings.Variable{
	Name:    "KAFKA_USERNAME",
	Default: "none: no login",
	Help:    "Your Kafka username, if Kafka needs a login. Leave it and `KAFKA_PASSWORD` unset if it doesn't.",
	Docs:    "See [Connecting to Kafka](#connecting-to-kafka).",
}, settings.Text)

var passwordSetting = settings.Define(settings.Variable{
	Name:    "KAFKA_PASSWORD",
	Default: "none",
	Secret:  true,
	Help:    "The password for `KAFKA_USERNAME`. The startup report shows only whether it is set.",
}, settings.Text)

var mechanismSetting = settings.Define(settings.Variable{
	Name:    "KAFKA_SASL_MECHANISM",
	Default: string(kafka.ScramSHA256),
	Help: "How Kafka checks `KAFKA_USERNAME` and `KAFKA_PASSWORD`: `SCRAM-SHA-256`, `SCRAM-SHA-512` (AWS MSK), or " +
		"`PLAIN` (Confluent Cloud, Azure Event Hubs). `PLAIN` sends the password as it is, so use it with TLS.",
}, func(v string) (kafka.Mechanism, error) {
	if v == "" {
		return kafka.ScramSHA256, nil
	}
	m := kafka.Mechanism(strings.ToUpper(v))
	if !slices.Contains(kafka.Mechanisms, m) {
		return "", fmt.Errorf("KAFKA_SASL_MECHANISM must be SCRAM-SHA-256, SCRAM-SHA-512 or PLAIN, not %q", v)
	}
	return m, nil
})

// mechanismRule refuses a mechanism with no username, which would otherwise be ignored silently.
func mechanismRule(env settings.Env) error {
	if settings.Get(env, mechanismSetting.Name) != "" && settings.Get(env, usernameSetting.Name) == "" {
		return errors.New("KAFKA_SASL_MECHANISM is set but KAFKA_USERNAME isn't: set the username and password, " +
			"or unset KAFKA_SASL_MECHANISM for no login")
	}
	return nil
}

var tlsSetting = settings.Define(settings.Variable{
	Name:    "KAFKA_TLS",
	Default: "on if any KAFKA_TLS_*_PATH is set, else off",
	Help: "`true` to connect to Kafka with TLS (encrypted), as most hosted Kafka services require. The system's " +
		"certificate authorities check the brokers' certificates unless `KAFKA_TLS_SERVER_CA_CERT_PATH` is set.",
}, settings.Bool("KAFKA_TLS"))

var tlsServerCASetting = settings.Define(settings.Variable{
	Name:    "KAFKA_TLS_SERVER_CA_CERT_PATH",
	Default: "the system's",
	Help:    "The path to the certificate (PEM) of the authority that signed the brokers' certificates, if it isn't a public one. Turns on TLS.",
}, settings.Text)

var tlsClientCertSetting = settings.Define(settings.Variable{
	Name:    "KAFKA_TLS_CLIENT_CERT_PATH",
	Default: "none",
	Help:    "For mTLS: the path to the client certificate (PEM) the program logs in with, with `KAFKA_TLS_CLIENT_KEY_PATH`. Turns on TLS.",
}, settings.Text)

var tlsClientKeySetting = settings.Define(settings.Variable{
	Name:    "KAFKA_TLS_CLIENT_KEY_PATH",
	Default: "none",
	Help:    "For mTLS: the path to the client certificate's private key (PEM), with `KAFKA_TLS_CLIENT_CERT_PATH`.",
}, settings.Text)

// tlsFiles are the TLS files in vals, and whether TLS is on.
func tlsFiles(vals *settings.Values) (kafka.TLSFiles, bool) {
	f := kafka.TLSFiles{
		ServerCA:   tlsServerCASetting.From(vals),
		ClientCert: tlsClientCertSetting.From(vals),
		ClientKey:  tlsClientKeySetting.From(vals),
	}
	return f, tlsSetting.From(vals) || f != (kafka.TLSFiles{})
}

// tlsRule checks the TLS variables together, and that their files load.
func tlsRule(env settings.Env) error {
	files := kafka.TLSFiles{
		ServerCA:   settings.Get(env, tlsServerCASetting.Name),
		ClientCert: settings.Get(env, tlsClientCertSetting.Name),
		ClientKey:  settings.Get(env, tlsClientKeySetting.Name),
	}
	tlsOn, _ := tlsSetting.Get(env) // rules run only once every value is valid
	on := tlsOn || files != (kafka.TLSFiles{})
	switch {
	case files != (kafka.TLSFiles{}) && settings.Get(env, tlsSetting.Name) != "" && !tlsOn:
		return errors.New("KAFKA_TLS is false but a KAFKA_TLS_*_PATH is set: unset KAFKA_TLS, or the paths")
	case (files.ClientCert == "") != (files.ClientKey == ""):
		return errors.New("set KAFKA_TLS_CLIENT_CERT_PATH and KAFKA_TLS_CLIENT_KEY_PATH together, for mTLS")
	case on:
		if _, err := files.Load(); err != nil {
			return fmt.Errorf("Kafka TLS: %w", err)
		}
	default: // TLS off
	}
	return nil
}

// Kafka is the Kafka configuration in vals, which the Load of a program with KafkaSection and
// KafkaRules made. It reads the TLS
// files, so it fails if they changed since Load checked them.
func Kafka(vals *settings.Values) (kafka.Config, error) {
	cfg := kafka.Config{
		Brokers:        brokersSetting.From(vals),
		Username:       usernameSetting.From(vals),
		Password:       passwordSetting.From(vals),
		Mechanism:      mechanismSetting.From(vals),
		ClientLogLevel: clientLogLevelSetting.From(vals),
	}
	if files, on := tlsFiles(vals); on {
		tlsCfg, err := files.Load()
		if err != nil {
			return kafka.Config{}, fmt.Errorf("Kafka TLS: %w", err)
		}
		cfg.TLS = tlsCfg
	}
	return cfg, nil
}

// LoadKafka checks the Kafka variables alone in env, every problem at once, and returns their
// configuration. A program loads them with the rest of its variables instead.
func LoadKafka(env settings.Env) (kafka.Config, error) {
	vals, err := (&settings.Program{Sections: []*settings.Section{KafkaSection}, Rules: KafkaRules}).Load(env)
	if err != nil {
		return kafka.Config{}, err
	}
	return Kafka(vals)
}
