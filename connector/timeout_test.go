package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
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

// A read may take its time limit plus 5 minutes, or KAFKA_MAX_DURATION_MINUTES but never less than
// one attempt, ending 10 seconds before the caller's operation times out; a caller without time
// for the read's time limit plus those 10 seconds is refused.
func TestReadScheduleToClose(t *testing.T) {
	twoMinutes := kafkaapi.ReadInput{TimeLimitSeconds: ptr(int64(120))}
	for _, tc := range []struct {
		name        string
		in          kafkaapi.ReadInput
		maxDuration time.Duration
		header      string
		want        time.Duration
		wantErr     string
	}{
		{"no caller timeout", twoMinutes, 0, "", 7 * time.Minute, ""},
		{"no time limit, no caller timeout", kafkaapi.ReadInput{}, 0, "", 5 * time.Minute, ""},
		{"maximum set", twoMinutes, time.Hour, "", time.Hour, ""},
		{"maximum below one attempt", twoMinutes, time.Minute, "", 3 * time.Minute, ""},
		{"caller shorter than the maximum", twoMinutes, 0, "240000ms", 4*time.Minute - callbackMargin, ""},
		{"caller longer than the maximum", twoMinutes, 0, "3600000ms", 7 * time.Minute, ""},
		{"no time limit, a minute's caller", kafkaapi.ReadInput{}, 0, "60000ms", time.Minute - callbackMargin, ""},
		{"just over the time limit and margin", twoMinutes, 0, "130001ms", 2*time.Minute + time.Millisecond, ""},
		{"exactly the time limit and margin", twoMinutes, 0, "130s", 0,
			"the caller's Nexus operation times out in 2m10s, which leaves no time to run: it needs more than timeLimitSeconds (2m0s) plus 10s for the result to reach the caller; give the operation a longer timeout or lower timeLimitSeconds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := readScheduleToClose(tc.in, tc.maxDuration, tc.header)
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
	in := kafkaapi.ReadInput{TimeLimitSeconds: ptr(int64(120))}
	read := readOptions("r", "q", in, 4*time.Minute)
	require.Equal(t, 4*time.Minute, read.ScheduleToCloseTimeout)
	require.Equal(t, 3*time.Minute, read.StartToCloseTimeout, "one attempt: the time limit plus a minute")
	require.Equal(t, 4*time.Minute, writeOptions("w", 4*time.Minute).ScheduleToCloseTimeout)
}
