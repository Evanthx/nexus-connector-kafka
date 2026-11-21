// Package codectest is for tests only: helpers for the end-to-end tests that run a connector with
// the demo payload codec (test/encryption).
package codectest

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/test/encryption"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// Passphrase is the demo codec's passphrase in the encryption tests.
const Passphrase = "end-to-end test passphrase"

// Server serves the demo codec (test/encryption) under Passphrase as a remote codec
// server, and returns the environment that points a program at it.
func Server(t *testing.T) settings.Map {
	t.Helper()
	c, err := encryption.NewCodec(Passphrase)
	require.NoError(t, err)
	server := httptest.NewServer(converter.NewPayloadCodecHTTPHandler(c))
	t.Cleanup(server.Close)
	return settings.Map{"TEMPORAL_CODEC_ENDPOINT": server.URL, "TEMPORAL_CONFIG_FILE": t.TempDir() + "/none.toml"}
}

// WithDemoCodec is a client sharing c's connection whose payloads go through the demo codec under
// Passphrase, as a demo worker's with DEMO_ENCRYPTION_KEY set.
func WithDemoCodec(t *testing.T, c client.Client, namespace string) client.Client {
	t.Helper()
	dc, err := encryption.DataConverter(Passphrase)
	require.NoError(t, err)
	out, err := client.NewClientFromExisting(c, client.Options{Namespace: namespace, DataConverter: dc, FailureConverter: encryption.FailureConverter(dc)})
	require.NoError(t, err)
	t.Cleanup(out.Close)
	return out
}

// RequireEncrypted fails the test unless payload is the demo codec's ciphertext, without
// plaintext in it.
func RequireEncrypted(t *testing.T, payload *commonpb.Payload, plaintext string) {
	t.Helper()
	require.NotNil(t, payload)
	require.Equal(t, "binary/encrypted", string(payload.GetMetadata()["encoding"]), "the payload in Temporal is not encrypted")
	require.NotContains(t, string(payload.GetData()), plaintext)
}
