package nexussend

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// Nexus starts each operation as a standalone Nexus operation on an endpoint. Without wait, Send
// returns once the operation has started, so the next can start while it runs. With wait, Send
// also waits for the operation to complete, so operations sent one after another run one after
// another: an Inbound Connector's ordered mode.
//
// An operation ID is delivered at most once: a start with an ID already used never runs the
// operation again, even if it failed. A failed operation was delivered, and is handled in
// Temporal, where its receiver's workflow can be fixed and reset.
type Nexus struct {
	client    client.Client
	nexus     client.NexusClient
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
	return &Nexus{client: c, nexus: nc, operation: operation, timeout: timeout, wait: wait}, nil
}

// Send starts the operation, and with wait, waits for it to complete. With these policies, a start
// with an ID already used starts nothing new: if its operation is running, the start attaches to
// it (and with wait, waits for it); if it has closed, whether it succeeded or failed, the server
// reports it already started, which counts as sent.
//
// With wait, an operation that completed unsuccessfully returns an error wrapping ErrFailed: it was
// delivered, and isn't started again. An error reaching Temporal while waiting is returned to be
// retried; the retry's start attaches to the operation or finds it closed.
func (n *Nexus) Send(ctx context.Context, op Operation) error {
	handle, err := n.nexus.ExecuteOperation(ctx, n.operation, op.Input, client.StartNexusOperationOptions{
		ID:                     op.ID,
		ScheduleToCloseTimeout: n.timeout,
		IDConflictPolicy:       enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
		IDReusePolicy:          enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE,
		Summary:                op.Summary,
	})
	var started *serviceerror.NexusOperationExecutionAlreadyStarted
	switch {
	case errors.As(err, &started) && !n.wait:
		return nil // delivered before
	case errors.As(err, &started):
		// Delivered before and closed: wait reports how it ended.
		handle = n.client.GetNexusOperationHandle(client.GetNexusOperationHandleOptions{OperationID: op.ID})
	case refusedAsTooLarge(err):
		return fmt.Errorf("%w: %w", ErrTooLarge, err)
	case err != nil:
		return err
	case !n.wait:
		return nil
	default: // started: wait for it
	}
	err = handle.Get(ctx, nil)
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return ctx.Err()
	case closed(ctx, handle):
		return fmt.Errorf("%w: operation %s: %w", ErrFailed, op.ID, err)
	default:
		return fmt.Errorf("operation %s: %w", op.ID, err)
	}
}

// closed reports whether handle's operation has closed, rather than Get having failed to reach
// Temporal. It is false if Temporal can't be asked either, so the caller retries.
func closed(ctx context.Context, handle client.NexusOperationHandle) bool {
	desc, err := handle.Describe(ctx, client.DescribeNexusOperationOptions{})
	if err != nil {
		return false
	}
	switch desc.Status {
	case enumspb.NEXUS_OPERATION_EXECUTION_STATUS_UNSPECIFIED, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING:
		return false
	default:
		return true
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
