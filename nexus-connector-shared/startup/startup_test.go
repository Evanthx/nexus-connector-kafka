package startup

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"go.temporal.io/sdk/client"
)

// env is an environment with no Temporal profile file and vars.
func env(t *testing.T, vars settings.Map) settings.Map {
	m := settings.Map{"TEMPORAL_CONFIG_FILE": filepath.Join(t.TempDir(), "none.toml")}
	for k, v := range vars {
		m[k] = v
	}
	return m
}

// Every problem is reported at once, each line of a joined error under its heading.
func TestProblems_ReportsEveryProblem(t *testing.T) {
	s := New("Outbound Connector", env(t, nil))
	s.Dial = func(client.Options) (client.Client, error) { return nil, errors.New("connection refused") }
	s.Config(errors.Join(errors.New("set TOKEN"), errors.New("SIZE must be a number")))
	s.Add("Failed to connect to Stripe", errors.New("bad key"))
	s.Add("Never shown", nil)
	opts, c := s.Temporal()
	require.Nil(t, c)
	require.Equal(t, "connectors", opts.Namespace, "the namespace default applies")
	require.EqualError(t, s.Problems(), "The Outbound Connector can't start:\n"+
		"Invalid Outbound Connector configuration:\n  - set TOKEN\n  - SIZE must be a number\n"+
		"Failed to connect to Stripe:\n  - bad key\n"+
		"Failed to connect to Temporal:\n  - connection refused")
}

func TestProblems_NoneIsNil(t *testing.T) {
	s := New("Outbound Connector", env(t, nil))
	s.Config(nil)
	require.NoError(t, s.Problems())
}

// Client options that don't load are reported, and Temporal doesn't dial.
func TestTemporal_BadClientConfiguration(t *testing.T) {
	s := New("Inbound Connector", env(t, settings.Map{"TEMPORAL_CODEC_ENDPOINT": "ftp://codec.example"}))
	s.Dial = func(client.Options) (client.Client, error) {
		require.Fail(t, "dialed with options that didn't load")
		return nil, nil
	}
	_, c := s.Temporal()
	require.Nil(t, c)
	require.ErrorContains(t, s.Problems(), "Failed to load Temporal client configuration:\n  - ")
}

// The client options have the payload codec: a program gets encryption from Temporal alone.
func TestTemporal_AppliesTheCodec(t *testing.T) {
	var dialed client.Options
	s := New("Outbound Connector", env(t, settings.Map{"TEMPORAL_CODEC_ENDPOINT": "http://localhost:1"}))
	s.Dial = func(opts client.Options) (client.Client, error) { dialed = opts; return nil, errors.New("not dialing") }
	opts, _ := s.Temporal()
	require.NotNil(t, opts.DataConverter)
	require.NotNil(t, opts.FailureConverter)
	require.NotNil(t, dialed.DataConverter, "it dials with the codec")
}

// Ready waits for the codec, then each wait in order, and stops on a wait's error.
func TestReady_WaitsInOrder(t *testing.T) {
	s := New("Outbound Connector", env(t, nil))
	var order []string
	wait := func(name string, err error) func(context.Context) error {
		return func(context.Context) error { order = append(order, name); return err }
	}
	require.NoError(t, s.Ready(context.Background(), wait("a", nil), wait("b", nil)))
	require.Equal(t, []string{"a", "b"}, order)

	order = nil
	require.EqualError(t, s.Ready(context.Background(), wait("a", errors.New("refused")), wait("b", nil)), "refused")
	require.Equal(t, []string{"a"}, order)
}

// A codec server that doesn't answer is waited for, logging why, before the program's own waits;
// interrupting the wait is ErrStopped, not a failure.
func TestReady_WaitsForTheCodecFirst(t *testing.T) {
	var logs bytes.Buffer
	s := New("Outbound Connector", env(t, settings.Map{"TEMPORAL_CODEC_ENDPOINT": "http://127.0.0.1:1"}))
	s.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	s.Dial = func(client.Options) (client.Client, error) { return nil, errors.New("not dialing") }
	s.Temporal()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	called := false
	err := s.Ready(ctx, func(context.Context) error { called = true; return nil })
	require.ErrorIs(t, err, ErrStopped)
	require.False(t, called, "the program's own wait runs only once the codec works")
	require.Contains(t, logs.String(), "Can't use the payload codec yet")
}

func TestReady_StoppedBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := New("Outbound Connector", env(t, nil)).Ready(ctx, func(ctx context.Context) error { return ctx.Err() })
	require.ErrorIs(t, err, ErrStopped)
}

// An empty program name still gives a readable message.
func TestProblems_EmptyProgram(t *testing.T) {
	s := New("", env(t, nil))
	s.Config(errors.New("bad"))
	require.EqualError(t, s.Problems(), "The program can't start:\nInvalid program configuration:\n  - bad")
}
