// Package temporalclient loads the Temporal client options every connector program connects with:
// the standard TEMPORAL_* variables and the temporal CLI's profile file, as the CLI reads them, with
// the namespace defaulting to DefaultNamespace and the payload codec, if one is set, applied.
package temporalclient

import (
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/codec"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/report"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
)

// DefaultNamespace is the namespace when neither TEMPORAL_NAMESPACE nor the profile file sets one.
const DefaultNamespace = "connectors"

// Options loads the Temporal client options from env: the TEMPORAL_* variables and the profile
// file, with the namespace defaulting to DefaultNamespace. With a payload codec set
// (TEMPORAL_CODEC_ENDPOINT, or codecs compiled in), every payload, and every error's message and
// details, goes through it: see the codec package.
func Options(env settings.Env) (client.Options, error) {
	opts, err := load(env)
	if err != nil {
		return client.Options{}, err
	}
	if opts.Namespace == "" {
		opts.Namespace = DefaultNamespace
	}
	// After the namespace is final: the codec server is told it.
	if _, err := codec.Apply(env, &opts); err != nil {
		return client.Options{}, err
	}
	return opts, nil
}

// Section is the Temporal section of a program's startup report: where opts, from Options(env),
// connect, marking the defaults, and what happens to payloads. It never shows a credential.
func Section(env settings.Env, opts client.Options) report.Section {
	namespace, address := opts.Namespace, opts.HostPort
	if raw, err := load(env); err == nil && raw.Namespace == "" {
		namespace += " (default)"
	}
	if address == "" {
		address = client.DefaultHostPort + " (default)"
	}
	return report.Section{Title: "Temporal", Settings: []report.Setting{
		{Name: "namespace", Value: namespace},
		{Name: "address", Value: address},
		{Name: "payload codec", Value: codec.Describe(env)},
	}}
}

func load(env settings.Env) (client.Options, error) {
	return envconfig.LoadClientOptions(envconfig.LoadClientOptionsRequest{EnvLookup: env})
}
