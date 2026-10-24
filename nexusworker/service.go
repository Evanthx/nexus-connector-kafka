package main

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
)

const (
	// taskQueue is the Nexus worker's task queue. The Nexus endpoint targets it, and write
	// activities run on it.
	taskQueue = "nexus-connector-kafka"
	// readTaskQueue is where read activities run, on a worker of their own. A read can wait a
	// minute or more for its batch, and reads are limited to as many at once as the reader has
	// consumers; on a queue of their own, waiting reads never hold up a write.
	readTaskQueue = "nexus-connector-kafka-reads"
)

// readOptions are the options for the activity behind one read. An attempt may take the read's
// time limit plus a minute; the whole read, with retries, the time limit plus five minutes. A
// worker that dies is noticed within the heartbeat timeout, twice the heartbeat interval, or at
// the start-to-close timeout if that comes first, as it does for a read of a minute or less.
func readOptions(requestID string, in kafkaapi.ReadInput) client.StartActivityOptions {
	timeLimit := time.Duration(in.TimeLimitSecondsOrDefault()) * time.Second
	return client.StartActivityOptions{
		ID:                     "kafka-read-" + requestID,
		TaskQueue:              readTaskQueue,
		StartToCloseTimeout:    timeLimit + time.Minute,
		ScheduleToCloseTimeout: timeLimit + 5*time.Minute,
		HeartbeatTimeout:       2 * heartbeatInterval,
	}
}

// writeOptions are the options for the activity behind one write. Its ID is also the prefix of
// each message's dedupe-id.
func writeOptions(requestID string) client.StartActivityOptions {
	return client.StartActivityOptions{
		ID:                     "kafka-write-" + requestID,
		StartToCloseTimeout:    time.Minute,
		ScheduleToCloseTimeout: 10 * time.Minute,
	}
}

// newService builds the KafkaService Nexus service. Each operation starts one standalone
// activity and completes when it does. The activity ID comes from the Nexus request ID, so a
// retried start of the same operation reaches the same activity.
func newService(a *activities) *nexus.Service {
	read := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[kafkaapi.ReadInput, kafkaapi.ReadOutput]{
		Name: kafkaapi.KafkaService.Read.Name(),
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, in kafkaapi.ReadInput, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[kafkaapi.ReadOutput], error) {
			if in.Ack != nil {
				if err := a.reader.CheckAck(in.Group, *in.Ack); err != nil {
					// Rejected before any activity starts.
					return temporalnexus.TemporalOperationResult[kafkaapi.ReadOutput]{}, &nexus.HandlerError{
						Type:  nexus.HandlerErrorTypeBadRequest,
						Cause: applicationError(err),
					}
				}
			}
			return temporalnexus.StartActivity(ctx, nc, readOptions(opts.RequestID, in), a.Read, in)
		},
	})
	write := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[kafkaapi.WriteInput, kafkaapi.WriteOutput]{
		Name: kafkaapi.KafkaService.Write.Name(),
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, in kafkaapi.WriteInput, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[kafkaapi.WriteOutput], error) {
			return temporalnexus.StartActivity(ctx, nc, writeOptions(opts.RequestID), a.Write, in)
		},
	})
	svc := nexus.NewService(kafkaapi.KafkaService.ServiceName)
	svc.MustRegister(read, write)
	return svc
}

// newWorkers creates the Nexus worker's two workers. The first serves the Nexus service and runs
// writes. The second runs reads, no more at once than the reader has consumers, so a read never
// waits for one.
func newWorkers(c client.Client, a *activities) (nexusAndWrites, reads worker.Worker) {
	nexusAndWrites = worker.New(c, taskQueue, worker.Options{})
	nexusAndWrites.RegisterNexusService(newService(a))
	nexusAndWrites.RegisterActivity(a.Write)
	reads = worker.New(c, readTaskQueue, worker.Options{MaxConcurrentActivityExecutionSize: kafka.MaxConcurrentReads})
	reads.RegisterActivity(a.Read)
	return nexusAndWrites, reads
}
