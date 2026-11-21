package nexuscaller

import (
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/test/encryption" // ENCRYPTION (optional): delete with the block below
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
)

// ClientOptions loads the standard Temporal client settings (TEMPORAL_* variables and the
// temporal CLI's profile file), defaulting the namespace to Namespace. With nothing set, that
// is a dev server on localhost:7233. With DEMO_ENCRYPTION_KEY set, its payloads are encrypted (the
// block marked ENCRYPTION).
func ClientOptions() (client.Options, error) {
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		return client.Options{}, err
	}
	if opts.Namespace == "" {
		opts.Namespace = Namespace
	}
	// ======================= BEGIN ENCRYPTION (optional) =======================
	// Keep this block to encrypt every payload this program sends to Temporal, with the demo codec
	// under the passphrase in DEMO_ENCRYPTION_KEY; unset, it does nothing. Delete it, and the
	// import marked ENCRYPTION, to leave payloads unencrypted: nothing else depends on it.
	// The connector programs encrypt through a codec server instead: see docs/encryption.md.
	if key := encryption.Key(); key != "" {
		dc, err := encryption.DataConverter(key)
		if err != nil {
			return client.Options{}, err
		}
		opts.DataConverter = dc
		opts.FailureConverter = encryption.FailureConverter(dc) // so errors can be read too, and are encrypted
	}
	// ======================== END ENCRYPTION (optional) ========================
	return opts, nil
}
