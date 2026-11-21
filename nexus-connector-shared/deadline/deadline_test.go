package deadline

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A call may take its limit, ending CallbackMargin before the caller's operation times out; a
// caller without time for an attempt after that is refused.
func TestScheduleToClose(t *testing.T) {
	for _, tc := range []struct {
		name    string
		header  string
		attempt time.Duration
		want    time.Duration
		wantErr string
	}{
		{"no caller timeout", "", 0, 10 * time.Minute, ""},
		{"caller shorter than the limit", "300000ms", 0, 5*time.Minute - CallbackMargin, ""},
		{"caller longer than the limit", "3600000ms", 0, 10 * time.Minute, ""},
		{"just over the margin", "10001ms", 0, time.Millisecond, ""},
		{"exactly the margin", "10s", 0, 0, "invalid operation timeout: the caller's Nexus operation times out in 10s, " +
			"which leaves no time to run: it needs more than 10s for the result to reach the caller; give the operation a longer timeout"},
		{"room for an attempt", "70s", time.Minute, time.Minute, ""},
		{"no room for an attempt", "69s", time.Minute, 0, "invalid operation timeout: the caller's Nexus operation times out in 1m9s, " +
			"which leaves no time for an attempt: it needs at least one attempt (1m0s) plus 10s for the result to reach the caller"},
		{"unreadable header", "soon", 0, 0, `invalid operation timeout: can't read the operation-timeout header "soon"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ScheduleToClose(tc.header, 10*time.Minute, tc.attempt)
			if tc.wantErr != "" {
				require.ErrorIs(t, err, ErrTooShort)
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
