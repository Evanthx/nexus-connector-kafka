package nexuscaller

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
)

// ClientOptions loads the standard Temporal client settings (TEMPORAL_* variables and the
// temporal CLI's profile file), defaulting the namespace to Namespace.
func ClientOptions() (client.Options, error) {
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		return client.Options{}, err
	}
	if opts.Namespace == "" {
		opts.Namespace = Namespace
	}
	return opts, nil
}
