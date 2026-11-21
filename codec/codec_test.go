package codec

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/settings"
	"github.com/temporalio/nexus-connector-kafka/test/encryption"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

// codecServer serves the demo codec as a remote codec server. It answers 503 to the first
// failFirst requests, and records the last X-Namespace and Authorization headers.
type codecServer struct {
	*httptest.Server
	failFirst          int32
	calls              atomic.Int32
	mu                 sync.Mutex
	namespace, gotAuth string
}

func newCodecServer(t *testing.T, codecs ...converter.PayloadCodec) *codecServer {
	t.Helper()
	s := &codecServer{}
	handler := converter.NewPayloadCodecHTTPHandler(codecs...)
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.calls.Add(1) <= s.failFirst {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		s.mu.Lock()
		s.namespace, s.gotAuth = r.Header.Get("X-Namespace"), r.Header.Get("Authorization")
		s.mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func demoCodec(t *testing.T) converter.PayloadCodec {
	t.Helper()
	c, err := encryption.NewCodec("demo passphrase")
	require.NoError(t, err)
	return c
}

// env is an environment with no profile file and vars set.
func env(t *testing.T, vars settings.Map) settings.Map {
	t.Helper()
	vars["TEMPORAL_CONFIG_FILE"] = t.TempDir() + "/none.toml"
	return vars
}

func TestLoad(t *testing.T) {
	s, err := Load(env(t, settings.Map{}))
	require.NoError(t, err)
	require.False(t, s.Enabled())
	require.Contains(t, s.String(), "unencrypted")

	s, err = Load(env(t, settings.Map{EndpointEnvVar: "https://codec.example.com", AuthEnvVar: "Bearer s3cr3t"}))
	require.NoError(t, err)
	require.True(t, s.Enabled())
	require.Equal(t, "encoded with the remote codec at https://codec.example.com (TEMPORAL_CODEC_AUTH set)", s.String())
	require.NotContains(t, s.String(), "s3cr3t")

	_, err = Load(env(t, settings.Map{EndpointEnvVar: "codec.example.com"}))
	require.EqualError(t, err, `TEMPORAL_CODEC_ENDPOINT must be an http or https URL, such as https://codec.example.com, not "codec.example.com"`)
}

// With TEMPORAL_CODEC_ENDPOINT set, every payload goes through the remote codec, which gets the
// namespace and the auth header, and what would go to Temporal is encrypted.
func TestApply_RemoteCodecEncryptsPayloads(t *testing.T) {
	server := newCodecServer(t, demoCodec(t))
	opts := client.Options{Namespace: "connectors"}
	s, err := Apply(env(t, settings.Map{EndpointEnvVar: server.URL + "/", AuthEnvVar: "Bearer s3cr3t"}), &opts)
	require.NoError(t, err)
	require.True(t, s.Enabled())

	payload, err := opts.DataConverter.ToPayload("card 4242")
	require.NoError(t, err)
	require.Equal(t, "binary/encrypted", string(payload.GetMetadata()["encoding"]))
	require.NotContains(t, string(payload.GetData()), "4242")
	var got string
	require.NoError(t, opts.DataConverter.FromPayload(payload, &got))
	require.Equal(t, "card 4242", got)

	require.NoError(t, Check(opts.DataConverter))
	server.mu.Lock()
	defer server.mu.Unlock()
	require.Equal(t, "connectors", server.namespace)
	require.Equal(t, "Bearer s3cr3t", server.gotAuth)
}

// With nothing set, the data converter is left alone.
func TestApply_NoCodec(t *testing.T) {
	opts := client.Options{}
	s, err := Apply(env(t, settings.Map{}), &opts)
	require.NoError(t, err)
	require.False(t, s.Enabled())
	require.Nil(t, opts.DataConverter)
}

// Codecs compiled in apply with no variable set.
func TestApply_CompiledCodecs(t *testing.T) {
	old := PayloadCodecs
	PayloadCodecs = []converter.PayloadCodec{demoCodec(t)}
	t.Cleanup(func() { PayloadCodecs = old })

	opts := client.Options{}
	s, err := Apply(env(t, settings.Map{}), &opts)
	require.NoError(t, err)
	require.Equal(t, "encoded with 1 codec(s) compiled in", s.String())
	payload, err := opts.DataConverter.ToPayload("x")
	require.NoError(t, err)
	require.Equal(t, "binary/encrypted", string(payload.GetMetadata()["encoding"]))
}

// A codec server that does nothing would let payloads reach Temporal as they are.
func TestCheck_Unchanged(t *testing.T) {
	server := newCodecServer(t) // no codecs: it returns payloads unchanged
	opts := client.Options{}
	_, err := Apply(env(t, settings.Map{EndpointEnvVar: server.URL}), &opts)
	require.NoError(t, err)
	require.ErrorIs(t, Check(opts.DataConverter), ErrUnchanged)
}

// A codec server that refuses the request, such as for a wrong TEMPORAL_CODEC_AUTH, fails Check.
func TestCheck_Refused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()
	opts := client.Options{}
	_, err := Apply(env(t, settings.Map{EndpointEnvVar: server.URL}), &opts)
	require.NoError(t, err)
	require.ErrorContains(t, Check(opts.DataConverter), "Unauthorized")
}

// At startup, a codec server that isn't up yet is waited for, with a warning per attempt.
func TestWaitAtStartup_RetriesUntilTheCodecWorks(t *testing.T) {
	old := retryDelay
	retryDelay = 10 * time.Millisecond
	t.Cleanup(func() { retryDelay = old })

	server := newCodecServer(t, demoCodec(t))
	server.failFirst = 2
	opts := client.Options{}
	e := env(t, settings.Map{EndpointEnvVar: server.URL})
	_, err := Apply(e, &opts)
	require.NoError(t, err)

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	require.NoError(t, WaitAtStartup(context.Background(), e, opts.DataConverter, logger))
	require.Contains(t, buf.String(), "level=WARN msg=\"Can't use the payload codec yet")
	require.Contains(t, buf.String(), "level=INFO msg=\"Payload codec works")
}

func TestWaitAtStartup_StopsWithTheContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	opts := client.Options{}
	e := env(t, settings.Map{EndpointEnvVar: server.URL})
	_, err := Apply(e, &opts)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, WaitAtStartup(ctx, e, opts.DataConverter, slog.Default()), context.DeadlineExceeded)
}

// Errors go through the codec too: their details (which the SDK's default failure converter would
// send as they are), messages and stack traces. Their type and retryability stay readable, and the
// same converter reads them back.
func TestApply_ErrorsAreEncoded(t *testing.T) {
	server := newCodecServer(t, demoCodec(t))
	opts := client.Options{}
	_, err := Apply(env(t, settings.Map{EndpointEnvVar: server.URL}), &opts)
	require.NoError(t, err)
	require.NotNil(t, opts.FailureConverter)
	failure := opts.FailureConverter.ErrorToFailure(temporal.NewApplicationError("request for card 4242 failed", "HTTPError", "response body 4242"))
	details := failure.GetApplicationFailureInfo().GetDetails().GetPayloads()
	require.Len(t, details, 1)
	require.Equal(t, "binary/encrypted", string(details[0].GetMetadata()["encoding"]))
	require.NotContains(t, string(details[0].GetData()), "4242")
	require.Equal(t, "Encoded failure", failure.GetMessage())
	require.Equal(t, "binary/encrypted", string(failure.GetEncodedAttributes().GetMetadata()["encoding"]))
	require.NotContains(t, string(failure.GetEncodedAttributes().GetData()), "4242")
	require.Equal(t, "HTTPError", failure.GetApplicationFailureInfo().GetType(), "the type stays readable")

	var appErr *temporal.ApplicationError
	require.ErrorAs(t, opts.FailureConverter.FailureToError(failure), &appErr)
	require.Equal(t, "request for card 4242 failed", appErr.Message(), "the same converter reads the message back")
}
