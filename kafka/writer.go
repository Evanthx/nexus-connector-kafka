package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrRejected means Kafka refused a write for a reason that retrying won't change, such as a
// record larger than the broker allows or a topic the connector may not write to.
var ErrRejected = errors.New("kafka rejected the write")

// Writer writes records and waits for the brokers to acknowledge them.
type Writer struct {
	client *kgo.Client
}

// NewWriter connects to the brokers in cfg.
func NewWriter(cfg Config) (*Writer, error) {
	client, err := kgo.NewClient(cfg.clientOpts()...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	return &Writer{client: client}, nil
}

// Write writes records in order. An error wrapping ErrRejected won't succeed if retried.
func (w *Writer) Write(ctx context.Context, records ...*kgo.Record) error {
	err := w.client.ProduceSync(ctx, records...).FirstErr()
	if err == nil {
		return nil
	}
	var ke *kerr.Error
	if errors.As(err, &ke) && !ke.Retriable {
		return fmt.Errorf("%w: %w", ErrRejected, err)
	}
	return fmt.Errorf("write: %w", err)
}

// Close closes the writer's connections.
func (w *Writer) Close() {
	w.client.Close()
}
