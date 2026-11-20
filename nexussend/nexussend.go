// Package nexussend sends an Inbound Connector's batches to a Nexus endpoint, one standalone Nexus
// operation per batch. It knows nothing about the system the messages come from, so every
// Inbound Connector can use it: the connector reads, batches and names each operation, and sends
// it through a Sender.
//
// A Sender is built in layers, each a Sender wrapping the next:
//
//	sender := nexussend.NewRateLimited(nexussend.NewRetrying(nexus, backoff, logger), perMinute, burst)
//
// Nexus starts the operation. Retrying retries it until it starts, so a retry never counts
// against the rate limit again. RateLimited waits until the batch's messages fit in the rate.
package nexussend

import (
	"context"
	"errors"
)

// Sender sends one operation. It returns nil once the operation has started (or had already
// started), an error wrapping ErrTooLarge if the server refused its input as too large, and
// otherwise an error the caller may retry (or ctx's error).
type Sender interface {
	Send(ctx context.Context, op Operation) error
}

// Operation is one batch to send.
type Operation struct {
	// ID is the operation ID. Sending the same ID again starts nothing new unless the earlier
	// operation failed, so a batch delivered again after a crash isn't started twice.
	ID string
	// Input is the operation's input, such as the batch's messages.
	Input any
	// Messages is how many messages Input holds. The rate limit counts these.
	Messages int
	// Summary is shown with the operation in the Temporal UI and CLI, such as where the messages
	// came from.
	Summary string
}

// ErrFailed means the operation completed unsuccessfully and starting it again can't help (see
// Final), such as a receiver that failed it with a non-retryable error. Only a Nexus that waits
// returns it. The batch goes to the dead-letter destination.
var ErrFailed = errors.New("the operation failed and retrying can't help")

// ErrTooLarge means the server refused the operation's input as larger than it allows. No retry
// can deliver it: split the batch, or send a single message that is too large to the dead-letter
// destination.
var ErrTooLarge = errors.New("the server refused the operation's input as too large")

// SenderFunc is a function used as a Sender.
type SenderFunc func(ctx context.Context, op Operation) error

// Send calls f.
func (f SenderFunc) Send(ctx context.Context, op Operation) error { return f(ctx, op) }
