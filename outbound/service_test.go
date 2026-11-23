package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOptionsUseTotal(t *testing.T) {
	require.Equal(t, 4*time.Minute, writeOptions("w", 4*time.Minute).ScheduleToCloseTimeout)
}
