package report

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

// Settings line up across sections, defaults are marked, and the behaviour follows.
func TestFormat(t *testing.T) {
	env := settings.Map{"SET": "x"}
	got := Format("Outbound Connector", []Section{
		{Title: "A", Settings: []Setting{Env(env, "SET", "x"), Env(env, "UNSET", 3)}},
		{Title: "B", Settings: []Setting{{Name: "LONGER_NAME", Value: "y"}}},
	}, []string{"Do one thing.", "Do another."})
	require.Equal(t, `Outbound Connector configuration (values marked "default" come from variables that aren't set):
  A
    SET          x
    UNSET        3 (default)
  B
    LONGER_NAME  y
What the Outbound Connector will do:
  - Do one thing.
  - Do another.`, got)
}

// An empty program name doesn't crash the program at startup.
func TestFormat_EmptyProgram(t *testing.T) {
	require.Contains(t, Format("", nil, nil), "Program configuration")
}

func TestDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		10 * time.Minute: "10m", 24 * time.Hour: "24h", 90 * time.Minute: "1h30m", 5 * time.Second: "5s",
		90 * time.Second: "1m30s", 500 * time.Millisecond: "500ms", 0: "0s", time.Hour + time.Second: "1h0m1s",
	} {
		require.Equal(t, want, Duration(d), d.String())
	}
	require.Equal(t, Setting{Name: "D", Value: "10m (default)"}, Env(settings.Map{}, "D", 10*time.Minute))
}
