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

// Nexus starts each operation as a standalone Nexus operation on an endpoint. It doesn't wait
// for the operation to finish: an Inbound Connector only sends messages in.
type Nexus struct {
	client    client.NexusClient
	operation string
	timeout   time.Duration
}

// NewNexus returns a Nexus that starts operation of service through endpoint, each allowed timeout
// to complete.
func NewNexus(c client.Client, endpoint, service, operation string, timeout time.Duration) (*Nexus, error) {
	nc, err := c.NewNexusClient(client.NexusClientOptions{Endpoint: endpoint, Service: service})
	if err != nil {
		return nil, err
	}
	return &Nexus{client: nc, operation: operation, timeout: timeout}, nil
}

// Send starts the operation. With these policies, a start with an ID already used starts nothing
// new: if its operation is running, the start attaches to it; if it completed, the server reports
// it already started, which counts as sent. Only an operation that failed, was cancelled, or timed
// out is started again.
func (n *Nexus) Send(ctx context.Context, op Operation) error {
	_, err := n.client.ExecuteOperation(ctx, n.operation, op.Input, client.StartNexusOperationOptions{
		ID:                     op.ID,
		ScheduleToCloseTimeout: n.timeout,
		IDConflictPolicy:       enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
		IDReusePolicy:          enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		Summary:                op.Summary,
	})
	var started *serviceerror.NexusOperationExecutionAlreadyStarted
	switch {
	case err == nil, errors.As(err, &started):
		return nil
	case refusedAsTooLarge(err):
		return fmt.Errorf("%w: %w", ErrTooLarge, err)
	default:
		return err
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
