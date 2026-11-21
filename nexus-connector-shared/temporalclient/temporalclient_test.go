package temporalclient

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/report"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

// The namespace defaults to connectors only when neither the variable nor the profile file sets
// one; any name, default included, is respected.
func TestOptions_Namespace(t *testing.T) {
	for _, tc := range []struct {
		name, env, file, want string
	}{
		{name: "unset", want: "connectors"},
		{name: "env default", env: "default", want: "default"},
		{name: "file default", file: "[profile.default]\nnamespace = \"default\"\n", want: "default"},
		{name: "env overrides file", env: "mine", file: "[profile.default]\nnamespace = \"default\"\n", want: "mine"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configFile := filepath.Join(t.TempDir(), "temporal.toml")
			if tc.file != "" {
				require.NoError(t, os.WriteFile(configFile, []byte(tc.file), 0o600))
			}
			env := settings.Map{"TEMPORAL_CONFIG_FILE": configFile}
			if tc.env != "" {
				env["TEMPORAL_NAMESPACE"] = tc.env
			}
			opts, err := Options(env)
			require.NoError(t, err)
			require.Equal(t, tc.want, opts.Namespace)
		})
	}
}

// The address comes from the environment; with no codec set, the SDK's converters apply.
func TestOptions_AddressAndNoCodec(t *testing.T) {
	opts, err := Options(settings.Map{"TEMPORAL_CONFIG_FILE": filepath.Join(t.TempDir(), "none.toml"), "TEMPORAL_ADDRESS": "temporal.example:7233"})
	require.NoError(t, err)
	require.Equal(t, "temporal.example:7233", opts.HostPort)
	require.Nil(t, opts.DataConverter)
	require.Nil(t, opts.FailureConverter)
}

// With a codec set, payloads and errors go through it.
func TestOptions_AppliesTheCodec(t *testing.T) {
	opts, err := Options(settings.Map{"TEMPORAL_CONFIG_FILE": filepath.Join(t.TempDir(), "none.toml"), "TEMPORAL_CODEC_ENDPOINT": "http://localhost:1"})
	require.NoError(t, err)
	require.NotNil(t, opts.DataConverter)
	require.NotNil(t, opts.FailureConverter)
}

// The report's Temporal section marks the defaults and never shows the codec's credential.
func TestSection(t *testing.T) {
	none := filepath.Join(t.TempDir(), "none.toml")
	env := settings.Map{"TEMPORAL_CONFIG_FILE": none, "TEMPORAL_CODEC_ENDPOINT": "http://codec.example", "TEMPORAL_CODEC_AUTH": "Bearer never_shown"}
	opts, err := Options(env)
	require.NoError(t, err)
	section := Section(env, opts)
	require.Equal(t, "Temporal", section.Title)
	require.Equal(t, report.Setting{Name: "namespace", Value: "connectors (default)"}, section.Settings[0])
	require.Equal(t, report.Setting{Name: "address", Value: "localhost:7233 (default)"}, section.Settings[1])
	require.Equal(t, "payload codec", section.Settings[2].Name)
	require.Contains(t, section.Settings[2].Value, "http://codec.example")
	require.NotContains(t, section.Settings[2].Value, "never_shown")

	env = settings.Map{"TEMPORAL_CONFIG_FILE": none, "TEMPORAL_NAMESPACE": "mine", "TEMPORAL_ADDRESS": "t.example:7233"}
	opts, err = Options(env)
	require.NoError(t, err)
	section = Section(env, opts)
	require.Equal(t, "mine", section.Settings[0].Value)
	require.Equal(t, "t.example:7233", section.Settings[1].Value)
}
