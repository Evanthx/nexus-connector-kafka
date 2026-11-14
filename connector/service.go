package main

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
)

// defaultTaskQueue is the connector's task queue unless NEXUS_TASK_QUEUE says otherwise.
const defaultTaskQueue = setup.DefaultTaskQueue

// writeOptions are the options for the activity behind one write, which may take total in all
// (from writeScheduleToClose). Its ID is also the prefix of each message's dedupe-id.
func writeOptions(requestID string, total time.Duration) client.StartActivityOptions {
	return client.StartActivityOptions{
		ID:                     "kafka-write-" + requestID,
		StartToCloseTimeout:    time.Minute,
		ScheduleToCloseTimeout: total,
	}
}

// newService builds the KafkaService Nexus service. Its one operation, write, starts a standalone
// activity and completes when it does. The activity stops trying shortly before the caller's
// operation times out, so the caller gets its last error; an operation whose caller has too little
// time left is refused before any activity starts. The activity ID comes from the Nexus request ID,
// so a retried start of the same operation reaches the same activity.
func newService(a *activities) *nexus.Service {
	write := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[kafkaapi.WriteInput, kafkaapi.WriteOutput]{
		Name: kafkaapi.KafkaService.Write.Name(),
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, in kafkaapi.WriteInput, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[kafkaapi.WriteOutput], error) {
			total, err := writeScheduleToClose(a.maxDuration, opts.Header.Get(nexus.HeaderOperationTimeout))
			if err != nil {
				return temporalnexus.TemporalOperationResult[kafkaapi.WriteOutput]{}, &nexus.HandlerError{
					Type:  nexus.HandlerErrorTypeBadRequest,
					Cause: applicationError(err),
				}
			}
			return temporalnexus.StartActivity(ctx, nc, writeOptions(opts.RequestID, total), a.Write, in)
		},
	})
	svc := nexus.NewService(kafkaapi.KafkaService.ServiceName)
	svc.MustRegister(write)
	return svc
}

// newWorker creates the connector's worker. It polls taskQueue, which the Nexus endpoint targets,
// serves the Nexus service, and runs writes.
func newWorker(c client.Client, taskQueue string, a *activities) worker.Worker {
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterNexusService(newService(a))
	w.RegisterActivity(a.Write)
	return w
}
