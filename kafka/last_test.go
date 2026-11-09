package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func readLast(t *testing.T, cfg Config) (string, int64, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rec, ok, err := ReadLast(ctx, cfg)
	require.NoError(t, err)
	return string(rec.Value), rec.Partition, ok
}

func TestReadLast_EmptyTopic(t *testing.T) {
	cfg := newCluster(t)
	_, _, ok := readLast(t, cfg)
	require.False(t, ok)
}

func TestReadLast_ReturnsTheNewestMessageEveryTime(t *testing.T) {
	cfg := newCluster(t)
	produce(t, cfg, 0, "p0 first", "p0 second")
	time.Sleep(5 * time.Millisecond) // later timestamps for the next writes
	produce(t, cfg, 2, "p2 only")
	time.Sleep(5 * time.Millisecond)
	produce(t, cfg, 1, "p1 first", "p1 newest")

	for range 3 {
		value, partition, ok := readLast(t, cfg)
		require.True(t, ok)
		require.Equal(t, "p1 newest", value)
		require.EqualValues(t, 1, partition)
	}

	// A newer message on another partition replaces it.
	time.Sleep(5 * time.Millisecond)
	produce(t, cfg, 0, "p0 newest")
	value, _, _ := readLast(t, cfg)
	require.Equal(t, "p0 newest", value)
}

func TestReadLast_UnknownTopic(t *testing.T) {
	cfg := newCluster(t)
	cfg.Topic = "missing"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, err := ReadLast(ctx, cfg)
	require.ErrorContains(t, err, "UNKNOWN_TOPIC_OR_PARTITION")
}
