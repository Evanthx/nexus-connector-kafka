// Package setup lists every environment variable the Inbound Connector and the Outbound Connector
// read, as one settings.Program each.
//
// The programs are main packages, which nothing can import, so their variables are declared here
// for the programs, the setup page (configui) and the docs (configui/gendocs) to share. The Kafka
// variables both programs read are KafkaSection, in kafka.go. The Inbound Connector's routes are
// JSON in one variable, described field by field in routes.go. Add a new variable here, or there for
// a Kafka one, with its help text and check, then run `make docs`.
package setup

import (
	"time"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/temporalclient"
)

// Inbound is the Inbound Connector's process and every environment variable it reads.
var Inbound = &settings.Program{
	ID:      "inbound",
	Name:    "Inbound Connector",
	Command: "go run ./inbound",
	Sections: []*settings.Section{
		KafkaSection,
		{Title: "Inbound Connector", Variables: []*settings.Variable{
			InboundConfig.Variable, InboundConfigFile.Variable, InboundStatusInterval.Variable,
		}},
		temporalSection,
		networkSection,
	},
	Rules: append([]func(settings.Env) error{routesRule}, KafkaRules...),
}

// Outbound is the Outbound Connector's process and every environment variable it reads.
var Outbound = &settings.Program{
	ID:      "outbound",
	Name:    "Outbound Connector",
	Command: "go run ./outbound",
	Sections: []*settings.Section{
		KafkaSection,
		{Title: "Outbound Connector", Variables: []*settings.Variable{
			KafkaTopics.Variable, TaskQueue.Variable,
		}},
		temporalSection,
		networkSection,
	},
	Rules: KafkaRules,
}

// temporalSection is the Temporal variables both programs show. The Temporal SDK's envconfig reads
// them, so they have no check here.
var temporalSection = settings.Temporal{
	Namespace: temporalclient.DefaultNamespace,
	NamespaceHelp: "The namespace the program runs in. The Inbound Connector needs standalone Nexus operations enabled in it; the " +
		"Outbound Connector needs nothing extra. All connectors default to `" +
		temporalclient.DefaultNamespace + "` and are kept apart by task queue.",
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

// InboundStatusInterval is how long the Inbound Connector goes without delivering before it logs a
// status line; 0 for never.
var InboundStatusInterval = settings.Define(settings.Variable{
	Name:    "INBOUND_STATUS_INTERVAL",
	Default: "5m",
	Help: "When nothing has been delivered for this long, such as `5m` or `1h`, the Inbound Connector logs a " +
		"status line for each route saying which partitions it has and when it last delivered, and again each interval " +
		"after, so a quiet topic can be told apart from a stalled Inbound Connector. `0` to turn it off.",
}, settings.Duration("INBOUND_STATUS_INTERVAL", 5*time.Minute))

// The Outbound Connector's own variables.
var (
	TaskQueue = settings.Define(settings.Variable{
		Name:    "NEXUS_TASK_QUEUE",
		Default: DefaultTaskQueue,
		Help:    "The task queue the Outbound Connector's Nexus endpoint targets. Give each Outbound Connector in a namespace its own.",
	}, settings.TextOr(DefaultTaskQueue))
)

// DefaultTaskQueue is the Outbound Connector's task queue unless NEXUS_TASK_QUEUE says otherwise.
const DefaultTaskQueue = "nexus-connector-kafka"
