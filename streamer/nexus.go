package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// nexusStarter starts each batch's operation as a standalone Nexus operation. It doesn't wait
// for the operation to finish: the streamer only sends messages in.
type nexusStarter struct {
	client    client.NexusClient
	operation string
	timeout   time.Duration
}

func newNexusStarter(c client.Client, cfg config) (*nexusStarter, error) {
	nc, err := c.NewNexusClient(client.NexusClientOptions{Endpoint: cfg.endpoint, Service: cfg.service})
	if err != nil {
		return nil, err
	}
	return &nexusStarter{client: nc, operation: cfg.operation, timeout: cfg.operationTimeout}, nil
}

// start starts the operation. With these policies, a start with an ID already used starts nothing
// new: if its operation is running, the start attaches to it; if it completed, the server reports
// it already started, which counts as delivered. Only an operation that failed, was cancelled, or
// timed out is started again.
func (n *nexusStarter) start(ctx context.Context, operationID string, input kafkaapi.ReceiveInput) error {
	first, last := input.Records[0], input.Records[len(input.Records)-1]
	_, err := n.client.ExecuteOperation(ctx, n.operation, input, client.StartNexusOperationOptions{
		ID:                     operationID,
		ScheduleToCloseTimeout: n.timeout,
		IDConflictPolicy:       enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
		IDReusePolicy:          enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		Summary:                fmt.Sprintf("Kafka %s/%d/%d-%d", first.Topic, first.Partition, first.Offset, last.Offset),
	})
	var started *serviceerror.NexusOperationExecutionAlreadyStarted
	switch {
	case err == nil, errors.As(err, &started):
		return nil
	case refusedAsTooLarge(err):
		return fmt.Errorf("%w: %w", errUndeliverable, err)
	default:
		return err
	}
}

// refusedAsTooLarge reports whether the server refused an operation because its input is larger
// than the server allows: the one refusal that is about the messages, not the setup. Other
// refusals, including other InvalidArguments such as an operation timeout the namespace doesn't
// allow, would refuse every batch alike, so they are retried, and the stream waits for the setup
// to be fixed rather than moving every message to the dead-letter topic.
func refusedAsTooLarge(err error) bool {
	var invalid *serviceerror.InvalidArgument
	return errors.As(err, &invalid) && strings.Contains(invalid.Error(), "exceeds size limit")
}
