package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
)

// defaultTaskQueue is the Outbound Connector's task queue unless NEXUS_TASK_QUEUE says otherwise.
const defaultTaskQueue = setup.DefaultTaskQueue

// replyMargin is how long before the Nexus request's deadline a write stops waiting for Kafka, so
// the handler can still reply with why it failed rather than let the request time out.
const replyMargin = time.Second

// newService builds the KafkaService Nexus service. Its one operation, write, is synchronous: it
// writes within the Nexus request and returns, with no activity or workflow behind it. Each
// attempt has until replyMargin before the request's deadline (10 seconds by default on the
// server). A failure retrying can fix is a retryable handler error, which the caller's server
// retries with its own backoff until the caller's Nexus operation times out; Rejected is final.
func newService(w *topicWriter) *nexus.Service {
	write := nexus.NewSyncOperation(kafkaapi.KafkaService.Write.Name(),
		func(ctx context.Context, in kafkaapi.WriteInput, opts nexus.StartOperationOptions) (kafkaapi.WriteOutput, error) {
			writeCtx := ctx
			if deadline, ok := ctx.Deadline(); ok {
				var cancel context.CancelFunc
				writeCtx, cancel = context.WithDeadline(ctx, deadline.Add(-replyMargin))
				defer cancel()
			}
			out, err := w.write(writeCtx, opts.RequestID, in)
			if err != nil {
				if writeCtx.Err() != nil && ctx.Err() == nil {
					err = fmt.Errorf("%w: Kafka didn't acknowledge the write in time; the Outbound Connector's log has the Kafka client's errors", err)
				}
				return kafkaapi.WriteOutput{}, handlerError(err)
			}
			temporalnexus.GetLogger(ctx).Info("Wrote messages", "count", out.Written)
			return out, nil
		})
	svc := nexus.NewService(kafkaapi.KafkaService.ServiceName)
	svc.MustRegister(write)
	return svc
}

// newWorker creates the Outbound Connector's worker. It polls taskQueue, which the Nexus endpoint
// targets, and serves the Nexus service.
func newWorker(c client.Client, taskQueue string, w *topicWriter) worker.Worker {
	wk := worker.New(c, taskQueue, worker.Options{})
	wk.RegisterNexusService(newService(w))
	return wk
}
