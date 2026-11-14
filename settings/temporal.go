package settings

// Temporal describes how a program connects to Temporal: with the Temporal SDK's envconfig, as the
// temporal CLI does, from the TEMPORAL_* variables and the CLI's profile file, a variable that is
// set overriding the file. Its Section lists every variable envconfig reads, in
// go.temporal.io/sdk/contrib/envconfig's ClientConfigProfile.ApplyEnvVars and LoadClientConfig,
// except TEMPORAL_CODEC_ENDPOINT and TEMPORAL_CODEC_AUTH, which have no effect without
// LoadClientOptionsRequest.IncludeRemoteCodec. They have no check: envconfig reads them, through
// the Env the program hands it, and reports what is wrong when the program connects.
type Temporal struct {
	// Namespace is the program's namespace when neither TEMPORAL_NAMESPACE nor the profile sets
	// one, and NamespaceHelp explains TEMPORAL_NAMESPACE for the program.
	Namespace     string
	NamespaceHelp string
	// Docs is Markdown added to TEMPORAL_ADDRESS's help in the docs, such as a link to the repo's
	// section on connecting to Temporal Cloud.
	Docs string
}

// The variables that say which profile envconfig reads.
const (
	TemporalConfigFileEnvVar = "TEMPORAL_CONFIG_FILE"
	TemporalProfileEnvVar    = "TEMPORAL_PROFILE"
	// TemporalGRPCMetaPrefix starts each variable that sets a gRPC header on every call.
	TemporalGRPCMetaPrefix = "TEMPORAL_GRPC_META_"
)

// Section is the "Temporal" section of a program that connects with t.
func (t Temporal) Section() *Section {
	return &Section{
		Title: "Temporal",
		Variables: []*Variable{
			{
				Name:    "TEMPORAL_ADDRESS",
				Default: "localhost:7233",
				Help: "The Temporal frontend's `host:port`; for Temporal Cloud, the namespace's gRPC endpoint. " +
					"The Temporal SDK's `envconfig` reads this and every variable below as the `temporal` CLI does: " +
					"each one that is set overrides the profile file's value.",
				Docs: t.Docs,
			},
			{Name: "TEMPORAL_NAMESPACE", Default: t.Namespace, Help: t.NamespaceHelp},
			{
				Name: "TEMPORAL_API_KEY", Default: "none", Secret: true,
				Help: "An API key, for a Temporal Cloud namespace that uses API key authentication. Turns on TLS " +
					"unless `TEMPORAL_TLS` is `false`.",
			},
			{
				Name: TemporalProfileEnvVar, Default: "default",
				Help: "The profile to read from the profile file. If it is set and the file has no such profile, the " +
					"program can't connect; `default` may be missing.",
			},
			{
				Name: TemporalConfigFileEnvVar, Default: "temporalio/temporal.toml in the user's config directory",
				Help: "The profile file, which `temporal config set` writes. A missing file is fine: only the " +
					"variables apply. The user's config directory is `~/.config` on Linux (or `$XDG_CONFIG_HOME`) and " +
					"`~/Library/Application Support` on macOS.",
			},
			{
				Name: "TEMPORAL_TLS", Default: "on with an API key or any TLS setting, else off",
				Help: "`true` or `1` to connect with TLS, `false` or `0` not to. Any other value is ignored.",
			},
			{
				Name: "TEMPORAL_TLS_CLIENT_CERT_PATH", Default: "none",
				Help: "The path to the client certificate for mTLS, with `TEMPORAL_TLS_CLIENT_KEY_PATH`.",
			},
			{
				Name: "TEMPORAL_TLS_CLIENT_CERT_DATA", Default: "none",
				Help: "The client certificate for mTLS itself, in PEM, instead of a path, with `TEMPORAL_TLS_CLIENT_KEY_DATA`.",
			},
			{
				Name: "TEMPORAL_TLS_CLIENT_KEY_PATH", Default: "none",
				Help: "The path to the client certificate's private key, with `TEMPORAL_TLS_CLIENT_CERT_PATH`.",
			},
			{
				Name: "TEMPORAL_TLS_CLIENT_KEY_DATA", Default: "none", Secret: true,
				Help: "The client certificate's private key itself, in PEM, instead of a path, with `TEMPORAL_TLS_CLIENT_CERT_DATA`.",
			},
			{
				Name: "TEMPORAL_TLS_SERVER_CA_CERT_PATH", Default: "the system's",
				Help: "The path to the CA certificate to verify the server with, for a server whose certificate the system doesn't trust.",
			},
			{
				Name: "TEMPORAL_TLS_SERVER_CA_CERT_DATA", Default: "the system's",
				Help: "The CA certificate itself, in PEM, instead of a path.",
			},
			{
				Name: "TEMPORAL_TLS_SERVER_NAME", Default: "the address's host",
				Help: "The name to expect on the server's certificate, and to send as SNI.",
			},
			{
				Name: "TEMPORAL_TLS_DISABLE_HOST_VERIFICATION", Default: "false",
				Help: "`true` or `1` to accept a server certificate that doesn't name the host. For testing only.",
			},
			{
				Name: "TEMPORAL_CLIENT_AUTHORITY", Default: "the address",
				Help: "The `:authority` header to send, for a proxy or load balancer that routes on it.",
			},
		},
		Families: []*Family{{
			Prefix:  TemporalGRPCMetaPrefix,
			Members: []Member{{Label: "Value", Secret: true, Help: "The header's value."}},
			Help: "gRPC headers sent with every call to Temporal: `" + TemporalGRPCMetaPrefix + "<NAME>` sends header " +
				"`<name>`, lower case with `-` for `_`. Shown as secrets, since a header can hold credentials.",
			AddButton: "Add gRPC header",
			AddHelp: "Name the header in upper-case letters, digits and underscores; an underscore becomes a hyphen. " +
				"X_TENANT sends the header x-tenant.",
			Example: "X_TENANT",
		}},
	}
}
