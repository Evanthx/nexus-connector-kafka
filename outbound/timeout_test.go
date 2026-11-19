package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A write may take 10 minutes, or KAFKA_MAX_DURATION_MINUTES, ending 10 seconds before the
// caller's operation times out; a caller with no more than those 10 seconds left is refused.
func TestWriteScheduleToClose(t *testing.T) {
	for _, tc := range []struct {
		name        string
		maxDuration time.Duration
		header      string
		want        time.Duration
		wantErr     string
	}{
		{"no caller timeout", 0, "", 10 * time.Minute, ""},
		{"no caller timeout, maximum set", time.Hour, "", time.Hour, ""},
		{"caller shorter than the maximum", 0, "300000ms", 5*time.Minute - callbackMargin, ""},
		{"caller longer than the maximum", 0, "3600000ms", 10 * time.Minute, ""},
		{"caller longer than the default, maximum set", time.Hour, "1800000ms", 30*time.Minute - callbackMargin, ""},
		{"just over the margin", 0, "10001ms", time.Millisecond, ""},
		{"exactly the margin", 0, "10s", 0, "invalid operation timeout: the caller's Nexus operation times out in 10s, which leaves no time to run: it needs more than 10s for the result to reach the caller; give the operation a longer timeout"},
		{"unreadable header", 0, "soon", 0, `invalid operation timeout: can't read the operation-timeout header "soon"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := writeScheduleToClose(tc.maxDuration, tc.header)
			if tc.wantErr != "" {
				require.ErrorIs(t, err, errInvalidTimeout)
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestOptionsUseTotal(t *testing.T) {
	require.Equal(t, 4*time.Minute, writeOptions("w", 4*time.Minute).ScheduleToCloseTimeout)
}
