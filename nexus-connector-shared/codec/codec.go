// Package codec encrypts (or otherwise encodes) every payload a connector program sends to
// Temporal, and decodes every payload it reads, so data can reach Temporal only in the form the
// user's codec makes, such as encrypted. It is the same in every connector repo; copy it, don't
// change it in one.
//
// Two ways in, which can be combined:
//
//   - A remote codec server, with no code: set TEMPORAL_CODEC_ENDPOINT (and TEMPORAL_CODEC_AUTH if
//     the server wants an Authorization header), or the codec section of the temporal CLI's
//     profile file. This is the codec server many teams already run for the Temporal UI. The
//     program holds no keys.
//   - Codecs compiled in: build your own binary with a file that appends to PayloadCodecs in an
//     init function. See docs/encryption.md.
//
// Apply adds them to a program's client options; WaitAtStartup checks they work before the
// program serves.
package codec

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

// The variables for the remote codec. envconfig defines them; Apply reads them.
const (
	EndpointEnvVar = "TEMPORAL_CODEC_ENDPOINT"
	AuthEnvVar     = "TEMPORAL_CODEC_AUTH"
)

// PayloadCodecs are codecs compiled into this program. They are empty in the program as shipped;
// to add one, build the program with a file of your own in package codec:
//
//	package codec
//
//	func init() { PayloadCodecs = append(PayloadCodecs, mycodec.New()) }
//
// On the way to Temporal, a remote codec (if set) runs first, then these, last to first; on the
// way back, the reverse.
//
// Add to it only in an init function: it is read whenever a client's options are loaded, and is
// never locked.
var PayloadCodecs []converter.PayloadCodec

// requestTimeout bounds each call to the remote codec server.
const requestTimeout = 10 * time.Second

// Setup is the codec configuration found in env and the profile file.
type Setup struct {
	Endpoint string // the remote codec server, or ""
	Auth     string // its Authorization header value, or ""
	Compiled int    // how many codecs are compiled in (PayloadCodecs)
}

// Load reads the remote codec's settings as envconfig does: TEMPORAL_CODEC_ENDPOINT and
// TEMPORAL_CODEC_AUTH from env, over the codec section of the profile file. It checks the
// endpoint is an http or https URL.
func Load(env settings.Env) (Setup, error) {
	prof, err := envconfig.LoadClientConfigProfile(envconfig.LoadClientConfigProfileOptions{EnvLookup: env})
	if err != nil {
		return Setup{}, err
	}
	s := Setup{Compiled: len(PayloadCodecs)}
	if prof.Codec != nil {
		s.Endpoint, s.Auth = prof.Codec.Endpoint, prof.Codec.Auth
	}
	if s.Endpoint != "" {
		if err := CheckEndpoint(s.Endpoint); err != nil {
			return Setup{}, err
		}
	}
	return s, nil
}

// CheckEndpoint checks a remote codec endpoint is an http or https URL with a host.
func CheckEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%s must be an http or https URL, such as https://codec.example.com, not %q", EndpointEnvVar, endpoint)
	}
	return nil
}

// Enabled reports whether any codec is set.
func (s Setup) Enabled() bool { return s.Endpoint != "" || s.Compiled > 0 }

// String says what happens to payloads, for the startup report. It never shows the auth value.
func (s Setup) String() string {
	var parts []string
	if s.Endpoint != "" {
		auth := "TEMPORAL_CODEC_AUTH not set"
		if s.Auth != "" {
			auth = "TEMPORAL_CODEC_AUTH set"
		}
		parts = append(parts, fmt.Sprintf("the remote codec at %s (%s)", s.Endpoint, auth))
	}
	if s.Compiled > 0 {
		parts = append(parts, fmt.Sprintf("%d codec(s) compiled in", s.Compiled))
	}
	if len(parts) == 0 {
		return "none: payloads go to Temporal as they are, unencrypted (set " + EndpointEnvVar + " to encrypt them)"
	}
	return "encoded with " + strings.Join(parts, ", then ")
}

// Apply loads the codec settings in env and, if any codec is set, wraps opts' data converter so
// every payload passes through it, and, unless opts has one, sets a failure converter that encodes
// errors' details, messages and stack traces with it too. Call it after the namespace is final: the remote codec gets it
// in the X-Namespace header, as the temporal CLI and UI send it.
func Apply(env settings.Env, opts *client.Options) (Setup, error) {
	s, err := Load(env)
	if err != nil || !s.Enabled() {
		return s, err
	}
	codecs := append([]converter.PayloadCodec(nil), PayloadCodecs...)
	if s.Endpoint != "" {
		namespace, auth := opts.Namespace, s.Auth
		codecs = append(codecs, converter.NewRemotePayloadCodec(converter.RemotePayloadCodecOptions{
			Endpoint: strings.TrimSuffix(s.Endpoint, "/"),
			ModifyRequest: func(req *http.Request) error {
				req.Header.Set("X-Namespace", namespace)
				if auth != "" {
					req.Header.Set("Authorization", auth)
				}
				return nil
			},
			Client: http.Client{Timeout: requestTimeout},
		}))
	}
	parent := opts.DataConverter
	if parent == nil {
		parent = converter.GetDefaultDataConverter()
	}
	opts.DataConverter = converter.NewCodecDataConverter(parent, codecs...)
	// Errors go through the codec too. The SDK's default failure converter doesn't use the client's
	// data converter, so without this an error's details (such as an HTTP response body) would reach
	// Temporal unencoded; EncodeCommonAttributes also moves each error's message and stack trace into
	// an encoded payload, leaving "Encoded failure" in their place. The error's type and whether it
	// is retryable stay readable. See docs/encryption.md.
	if opts.FailureConverter == nil {
		opts.FailureConverter = temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{
			DataConverter: opts.DataConverter, EncodeCommonAttributes: true,
		})
	}
	return s, nil
}

// checkValue is what Check encodes and decodes.
const checkValue = "nexus connector codec check"

// ErrUnchanged means the codec returned payloads unchanged, so they would reach Temporal as they
// are: a codec server that does nothing, or the wrong endpoint.
var ErrUnchanged = errors.New("the codec returned the payload unchanged, so payloads would reach Temporal unencrypted")

// Check encodes a payload with dc and decodes it again, as every payload will be: it fails if the
// remote codec can't be reached, refuses the request (such as a wrong TEMPORAL_CODEC_AUTH), can't
// decode what it encoded, or (ErrUnchanged) encodes nothing.
func Check(dc converter.DataConverter) error {
	payload, err := dc.ToPayload(checkValue)
	if err != nil {
		return fmt.Errorf("encode a test payload: %w", err)
	}
	var got string
	if err := dc.FromPayload(payload, &got); err != nil {
		return fmt.Errorf("decode the test payload it encoded: %w", err)
	}
	if got != checkValue {
		return fmt.Errorf("decoding the test payload gave %q, not %q: the codec's decode doesn't undo its encode", got, checkValue)
	}
	if plain, _ := converter.GetDefaultDataConverter().ToPayload(checkValue); samePayload(plain, payload) {
		return ErrUnchanged
	}
	return nil
}

func samePayload(a, b *commonpb.Payload) bool {
	return string(a.GetData()) == string(b.GetData()) &&
		string(a.GetMetadata()["encoding"]) == string(b.GetMetadata()["encoding"])
}

// retryDelay is how long WaitAtStartup waits between attempts.
var retryDelay = 5 * time.Second

// Describe says what happens to payloads with env's codec settings, for a startup report.
func Describe(env settings.Env) string {
	s, err := Load(env)
	if err != nil {
		return err.Error()
	}
	return s.String()
}

// WaitAtStartup checks the codec env sets, in dc (the data converter Apply made), with Check until
// it works or ctx ends, logging a warning for each failure, as programs wait for the systems they
// connect to. Call it after logging the startup report and before serving. It returns at once if
// env sets no codec. A codec that encodes nothing (ErrUnchanged) is logged as a warning, not
// waited for.
func WaitAtStartup(ctx context.Context, env settings.Env, dc converter.DataConverter, logger *slog.Logger) error {
	s, err := Load(env)
	if err != nil || !s.Enabled() {
		return err
	}
	for {
		err := Check(dc)
		switch {
		case err == nil:
			logger.Info("Payload codec works: payloads are " + s.String())
			return nil
		case errors.Is(err, ErrUnchanged):
			logger.Warn("The payload codec returned a test payload unchanged; payloads will reach Temporal as they are. "+
				"Check "+EndpointEnvVar+" points at the codec server you mean.", "codec", s.String())
			return nil
		}
		logger.Warn("Can't use the payload codec yet; retrying in "+retryDelay.String()+
			". Check the codec server is running and "+EndpointEnvVar+" and "+AuthEnvVar+" are right.", "codec", s.String(), "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
		}
	}
}
