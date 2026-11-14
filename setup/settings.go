// Package setup lists every environment variable the relay and the connector read, as one
// settings.Program each, and loads their Temporal client options.
//
// The programs are main packages, which nothing can import, so their variables are declared here
// for the programs, the setup page (configui) and the docs (configui/gendocs) to share. The Kafka
// variables both programs read are kafka.SettingsSection, in kafka/settings.go. Add a new variable
// here, or there for a Kafka one, with its help text and check, then run `make docs`.
package setup

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/settings"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
)

// Relay is the relay's process and every environment variable it reads.
var Relay = &settings.Program{
	ID:      "relay",
	Name:    "Nexus Connector Relay",
	Command: "go run ./relay",
	Sections: []*settings.Section{
		kafka.SettingsSection,
		{Title: "Relay", Variables: []*settings.Variable{
			RelayGroup.Variable, kafka.StartFromSetting.Variable, DeadLetterTopic.Variable, DeleteDeadLetters.Variable, BatchSize.Variable,
			BatchTimeLimit.Variable, RelayEndpoint.Variable, RelayService.Variable, RelayOperation.Variable,
			RelayOperationTimeout.Variable, RelayMaxInputBytes.Variable,
		}},
		temporalSection,
		networkSection,
	},
	Rules: []func(settings.Env) error{deadLetterRule},
}

// Connector is the connector's process and every environment variable it reads.
var Connector = &settings.Program{
	ID:      "connector",
	Name:    "Nexus Connector",
	Command: "go run ./connector",
	Sections: []*settings.Section{
		kafka.SettingsSection,
		{Title: "Connector", Variables: []*settings.Variable{
			TaskQueue.Variable, MaxDuration.Variable,
		}},
		temporalSection,
		networkSection,
	},
}

// temporalSection is the Temporal variables both programs show. The Temporal SDK's envconfig reads
// them, so they have no check here.
var temporalSection = settings.Temporal{
	Namespace: defaultNamespace,
	NamespaceHelp: "The namespace the program runs in. The relay needs standalone Nexus operations enabled in it; the " +
		"connector, standalone activities with completion callbacks. All connectors default to `" +
		defaultNamespace + "` and are kept apart by task queue.",
	Docs: "See [Connecting to Temporal Cloud](#connecting-to-temporal-cloud).",
}.Section()

// networkSection is the proxy variables both programs' connection to Temporal reads. Kafka doesn't
// use them: franz-go dials brokers directly, with a plain net.Dialer.
var networkSection = &settings.Section{Title: "Network", Variables: []*settings.Variable{
	{
		Name: "HTTPS_PROXY", Default: "none", External: "gRPC", Aliases: []string{"https_proxy"},
		Mask: settings.MaskURLPassword,
		Help: "The proxy for the program's connection to Temporal, such as `http://proxy.example:8080`, or " +
			"`http://user:password@proxy.example:8080` for a proxy that asks for a password. Kafka brokers are " +
			"always reached directly, never through it. Connections to `localhost` and loopback addresses never " +
			"use a proxy.",
	},
	{
		Name: "NO_PROXY", Default: "none", External: "gRPC", Aliases: []string{"no_proxy"},
		Help: "Hosts the connection to Temporal reaches without `HTTPS_PROXY`: a comma-separated list of domains " +
			"(each with its subdomains; with a leading `.`, subdomains only), IP addresses or CIDR ranges, each " +
			"optionally with a port, or `*` for every host. Kafka doesn't use it.",
	},
}}

// The relay's own variables.
var (
	RelayGroup = settings.Define(settings.Variable{
		Name:     "KAFKA_GROUP",
		Required: "yes",
		Help: "The consumer group the relay joins. Relays with the same consumer group share the topic's " +
			"partitions. Don't share it with any other consumer.",
	}, requiredText("KAFKA_GROUP", "the consumer group the relay joins"))

	DeadLetterTopic = settings.Define(settings.Variable{
		Name:     "KAFKA_DEAD_LETTER_TOPIC",
		Required: "unless DELETE_DEAD_LETTERS is true",
		Help: "An existing topic, other than `KAFKA_TOPIC`, for messages too large to deliver. Set this or " +
			"`DELETE_DEAD_LETTERS=true`, not both.",
	}, text)

	DeleteDeadLetters = settings.Define(settings.Variable{
		Name:    "DELETE_DEAD_LETTERS",
		Default: "false",
		Help: "`true` to log and skip messages too large to deliver, instead of writing them to " +
			"`KAFKA_DEAD_LETTER_TOPIC`; then leave that unset.",
	}, boolean("DELETE_DEAD_LETTERS"))

	BatchSize = settings.Define(settings.Variable{
		Name:    "KAFKA_BATCH_SIZE",
		Default: "100",
		Help: "The most messages in one batch, and so in one Nexus operation per partition. `1` for one " +
			"operation per message, which also rules out a message being delivered twice.",
	}, positiveInt("KAFKA_BATCH_SIZE", 100))

	BatchTimeLimit = settings.Define(settings.Variable{
		Name:    "KAFKA_BATCH_TIME_LIMIT",
		Default: "0 (deliver as soon as there are messages)",
		Help: "How long a batch waits to fill, such as `500ms` or `1s`. Waiting makes fewer, fuller batches on " +
			"a quiet topic, which costs fewer operations.",
	}, duration("KAFKA_BATCH_TIME_LIMIT", 0))

	RelayEndpoint = settings.Define(settings.Variable{
		Name:     "RELAY_ENDPOINT",
		Required: "yes",
		Help: "The Nexus endpoint the relay delivers batches to. It routes to the receiver you implement, " +
			"`KafkaMessageReceiver.receive`.",
	}, requiredText("RELAY_ENDPOINT", "the Nexus endpoint to deliver to"))

	RelayService = settings.Define(settings.Variable{
		Name:    "RELAY_SERVICE",
		Default: kafkaapi.KafkaMessageReceiver.ServiceName,
		Help:    "The Nexus service the receiver serves.",
	}, textOr(kafkaapi.KafkaMessageReceiver.ServiceName))

	RelayOperation = settings.Define(settings.Variable{
		Name:    "RELAY_OPERATION",
		Default: kafkaapi.KafkaMessageReceiver.Receive.Name(),
		Help:    "The operation started for each batch.",
	}, textOr(kafkaapi.KafkaMessageReceiver.Receive.Name()))

	RelayOperationTimeout = settings.Define(settings.Variable{
		Name:    "RELAY_OPERATION_TIMEOUT",
		Default: "24h",
		Help:    "How long each operation may take, such as `30m` or `24h`. More than 0.",
	}, func(v string) (time.Duration, error) {
		d, err := duration("RELAY_OPERATION_TIMEOUT", 24*time.Hour)(v)
		if err == nil && d == 0 {
			return 0, errors.New("RELAY_OPERATION_TIMEOUT must be more than 0")
		}
		return d, err
	})

	RelayMaxInputBytes = settings.Define(settings.Variable{
		Name:    "RELAY_MAX_INPUT_BYTES",
		Default: strconv.Itoa(DefaultMaxInputBytes) + " (2 MiB less 64 KiB)",
		Help: "The largest encoded batch sent to one operation. Larger batches are split; a single larger " +
			"message goes to the dead-letter topic.",
	}, positiveInt("RELAY_MAX_INPUT_BYTES", DefaultMaxInputBytes))
)

// DefaultMaxInputBytes is under Temporal's 2 MiB payload limit by 64 KiB: the limit applies to
// the whole payload, metadata included, and the size the relay counts is an estimate of the
// encoded records.
const DefaultMaxInputBytes = 2*1024*1024 - 64*1024

// deadLetterRule: undeliverable messages go to KAFKA_DEAD_LETTER_TOPIC, which isn't KAFKA_TOPIC,
// or are deleted with DELETE_DEAD_LETTERS=true; one or the other.
func deadLetterRule(env settings.Env) error {
	topic := settings.Get(env, DeadLetterTopic.Name)
	deleteThem, _ := DeleteDeadLetters.Get(env) // rules run only once every value is valid
	switch {
	case topic == "" && !deleteThem:
		return errors.New("set KAFKA_DEAD_LETTER_TOPIC, the topic undeliverable messages go to, " +
			"or DELETE_DEAD_LETTERS=true to delete them instead")
	case topic != "" && deleteThem:
		return errors.New("set KAFKA_DEAD_LETTER_TOPIC or DELETE_DEAD_LETTERS=true, not both: " +
			"undeliverable messages either go to that topic or are deleted")
	case topic != "" && topic == settings.Get(env, "KAFKA_TOPIC"):
		return errors.New("KAFKA_DEAD_LETTER_TOPIC must differ from KAFKA_TOPIC")
	}
	return nil
}

// The connector's own variables.
var (
	TaskQueue = settings.Define(settings.Variable{
		Name:    "NEXUS_TASK_QUEUE",
		Default: DefaultTaskQueue,
		Help:    "The task queue the connector's Nexus endpoint targets. Give each connector for another topic its own.",
	}, textOr(DefaultTaskQueue))

	// MaxDuration is 0 when unset: a write may then take 10 minutes.
	MaxDuration = settings.Define(settings.Variable{
		Name:    "KAFKA_MAX_DURATION_MINUTES",
		Default: "10",
		Help: "The longest a write keeps retrying, in whole minutes, at least 1. Either way it stops 10 seconds " +
			"before the caller's Nexus operation times out.",
	}, func(v string) (time.Duration, error) {
		if v == "" {
			return 0, nil
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return 0, fmt.Errorf("KAFKA_MAX_DURATION_MINUTES must be a whole number of minutes, at least 1, not %q", v)
		}
		return time.Duration(n) * time.Minute, nil
	})
)

// DefaultTaskQueue is the connector's task queue unless NEXUS_TASK_QUEUE says otherwise.
const DefaultTaskQueue = "nexus-connector-kafka"

// defaultNamespace is both programs' namespace when neither TEMPORAL_NAMESPACE nor the profile
// file sets one.
const defaultNamespace = "connectors"

// ClientOptions loads the Temporal client options from env with the SDK's envconfig, as the
// temporal CLI does (TEMPORAL_* variables and the CLI's profile file), with the namespace
// defaulting to connectors.
func ClientOptions(env settings.Env) (client.Options, error) {
	opts, err := envconfig.LoadClientOptions(envconfig.LoadClientOptionsRequest{EnvLookup: env})
	if err != nil {
		return client.Options{}, err
	}
	if opts.Namespace == "" {
		opts.Namespace = defaultNamespace
	}
	return opts, nil
}

// text is the parse function of a variable that takes any value.
func text(v string) (string, error) { return v, nil }

// textOr is the parse function of a variable that takes any value, def when unset.
func textOr(def string) func(string) (string, error) {
	return func(v string) (string, error) {
		if v == "" {
			return def, nil
		}
		return v, nil
	}
}

// requiredText is the parse function of a required variable that takes any value; what says what
// to set it to.
func requiredText(name, what string) func(string) (string, error) {
	return func(v string) (string, error) {
		if v == "" {
			return "", fmt.Errorf("set %s: %s", name, what)
		}
		return v, nil
	}
}

// positiveInt reads name as a whole number, at least 1, def when unset.
func positiveInt(name string, def int) func(string) (int, error) {
	return func(v string) (int, error) {
		if v == "" {
			return def, nil
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return 0, fmt.Errorf("%s must be a whole number, at least 1, not %q", name, v)
		}
		return n, nil
	}
}

// boolean reads name as true or false (anything strconv.ParseBool takes); unset is false.
func boolean(name string) func(string) (bool, error) {
	return func(v string) (bool, error) {
		if v == "" {
			return false, nil
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("%s must be true or false, not %q", name, v)
		}
		return b, nil
	}
}

// duration reads name as a duration of at least 0, such as 500ms, def when unset.
func duration(name string, def time.Duration) func(string) (time.Duration, error) {
	return func(v string) (time.Duration, error) {
		if v == "" {
			return def, nil
		}
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return 0, fmt.Errorf("%s must be a duration such as 500ms or 2s, not %q", name, v)
		}
		return d, nil
	}
}
