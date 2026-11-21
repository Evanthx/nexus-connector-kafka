package nexussend

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// Nexus starts each operation as a standalone Nexus operation on an endpoint. Without wait, Send
// returns once the operation has started, so the next can start while it runs. With wait, Send
// also waits for the operation to complete, so operations sent one after another run one after
// another: an Inbound Connector's ordered mode.
type Nexus struct {
	client    client.NexusClient
	operation string
	timeout   time.Duration
	wait      bool
}

// NewNexus returns a Nexus that starts operation of service through endpoint, each allowed timeout
// to complete, and with wait, waits for each to complete.
func NewNexus(c client.Client, endpoint, service, operation string, timeout time.Duration, wait bool) (*Nexus, error) {
	nc, err := c.NewNexusClient(client.NexusClientOptions{Endpoint: endpoint, Service: service})
	if err != nil {
		return nil, err
	}
	return &Nexus{client: nc, operation: operation, timeout: timeout, wait: wait}, nil
}

// Send starts the operation, and with wait, waits for it to complete. With these policies, a start
// with an ID already used starts nothing new: if its operation is running, the start attaches to
// it (and with wait, waits for it); if it completed, the server reports it already started, which
// counts as sent. Only an operation that failed, was cancelled, was terminated, or timed out is
// started again.
//
// With wait, an operation that completed unsuccessfully returns an error wrapping ErrFailed if
// retrying can't help (see Final), and otherwise an error to retry, which starts it again.
func (n *Nexus) Send(ctx context.Context, op Operation) error {
	handle, err := n.client.ExecuteOperation(ctx, n.operation, op.Input, client.StartNexusOperationOptions{
		ID:                     op.ID,
		ScheduleToCloseTimeout: n.timeout,
		IDConflictPolicy:       enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
		IDReusePolicy:          enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		Summary:                op.Summary,
	})
	var started *serviceerror.NexusOperationExecutionAlreadyStarted
	switch {
	case errors.As(err, &started):
		return nil // it completed successfully: a failed one would have started again
	case refusedAsTooLarge(err):
		return fmt.Errorf("%w: %w", ErrTooLarge, err)
	case err != nil:
		return err
	case !n.wait:
		return nil
	}
	err = handle.Get(ctx, nil)
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return ctx.Err()
	case Final(err):
		return fmt.Errorf("%w: operation %s: %w", ErrFailed, op.ID, err)
	default:
		return fmt.Errorf("operation %s: %w", op.ID, err)
	}
}

// Final reports whether an operation that completed unsuccessfully with err should not be
// started again: it failed with a non-retryable application error or handler error, or someone
// cancelled or terminated it. Anything else, such as a retryable failure, a timeout, or an error
// reaching Temporal while waiting, is retried. The outermost of those errors in err's chain
// decides, so a retryable failure caused by a cancelled activity is still retried.
func Final(err error) bool {
	final, _ := decide(err)
	return final
}

// decide walks err's chain, outermost first, including errors that wrap several, and reports
// whether the first error that decides is final, and whether one decided at all.
func decide(err error) (final, decided bool) {
	switch e := err.(type) {
	case nil:
		return false, false
	case *temporal.CanceledError, *temporal.TerminatedError:
		return true, true
	case *temporal.TimeoutError:
		return false, true
	case *temporal.ApplicationError:
		return e.NonRetryable(), true
	case *nexus.HandlerError:
		return !e.Retryable(), true
	case interface{ Unwrap() []error }:
		for _, inner := range e.Unwrap() {
			if final, decided := decide(inner); decided {
				return final, true
			}
		}
		return false, false
	default:
		return decide(errors.Unwrap(err))
	}
}

// refusedAsTooLarge reports whether the server refused an operation because its input is larger
// than the server allows: the one refusal that is about the messages, not the setup. Other
// refusals, including other InvalidArguments such as an operation timeout the namespace doesn't
// allow, would refuse every batch alike, so they are retried, and the Inbound Connector waits for
// the setup to be fixed rather than sending every message to the dead-letter destination.
func refusedAsTooLarge(err error) bool {
	var invalid *serviceerror.InvalidArgument
	return errors.As(err, &invalid) && strings.Contains(invalid.Error(), "exceeds size limit")
}
