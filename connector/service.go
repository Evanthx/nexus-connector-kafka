package main

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
)

// defaultTaskQueue is the connector's task queue unless NEXUS_TASK_QUEUE says otherwise.
const defaultTaskQueue = setup.DefaultTaskQueue

// readQueue is where read activities run, on a worker of their own: the connector's task queue
// with "-reads" added. A read can wait a minute or more for its batch, and reads are limited to as
// many at once as the reader has consumers; on a queue of their own, waiting reads never hold up a
// write.
func readQueue(taskQueue string) string {
	return taskQueue + "-reads"
}

// readOptions are the options for the activity behind one read, on readTaskQueue. An attempt may
// take the read's time limit plus a minute; the whole read, with retries, total (from
// readScheduleToClose). A worker that dies is noticed within the heartbeat timeout, twice the heartbeat
// interval, or at the start-to-close timeout if that comes first, as it does for a read of a
// minute or less.
func readOptions(requestID, readTaskQueue string, in kafkaapi.ReadInput, total time.Duration) client.StartActivityOptions {
	return client.StartActivityOptions{
		ID:                     "kafka-read-" + requestID,
		TaskQueue:              readTaskQueue,
		StartToCloseTimeout:    readAttempt(in),
		ScheduleToCloseTimeout: total,
		HeartbeatTimeout:       2 * heartbeatInterval,
	}
}

// writeOptions are the options for the activity behind one write, which may take total in all
// (from writeScheduleToClose). Its ID is also the prefix of each message's dedupe-id.
func writeOptions(requestID string, total time.Duration) client.StartActivityOptions {
	return client.StartActivityOptions{
		ID:                     "kafka-write-" + requestID,
		StartToCloseTimeout:    time.Minute,
		ScheduleToCloseTimeout: total,
	}
}

// newService builds the KafkaService Nexus service. Each operation starts one standalone
// activity and completes when it does. The activity stops trying shortly before the caller's
// operation times out, so the caller gets its last error; an operation whose caller has too
// little time left is refused before any activity starts. The activity ID comes from the Nexus request ID, so a
// retried start of the same operation reaches the same activity. Reads run on readTaskQueue.
func newService(a *activities, readTaskQueue string) *nexus.Service {
	read := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[kafkaapi.ReadInput, kafkaapi.ReadOutput]{
		Name: kafkaapi.KafkaService.Read.Name(),
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, in kafkaapi.ReadInput, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[kafkaapi.ReadOutput], error) {
			// Check the consumer group and the ack before any activity starts, and refuse a bad
			// request at once rather than through a failed activity.
			group, err := a.groupFor(deref(in.Group))
			if err == nil && in.Ack != nil {
				err = a.reader.CheckAck(group, *in.Ack)
			}
			var total time.Duration
			if err == nil {
				total, err = readScheduleToClose(in, a.maxDuration, opts.Header.Get(nexus.HeaderOperationTimeout))
			}
			if err != nil {
				return temporalnexus.TemporalOperationResult[kafkaapi.ReadOutput]{}, &nexus.HandlerError{
					Type:  nexus.HandlerErrorTypeBadRequest,
					Cause: applicationError(err),
				}
			}
			in.Group = &group
			return temporalnexus.StartActivity(ctx, nc, readOptions(opts.RequestID, readTaskQueue, in, total), a.Read, in)
		},
	})
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
	svc.MustRegister(read, write)
	return svc
}

// newWorkers creates the connector's two workers. The first polls taskQueue, which the Nexus
// endpoint targets: it serves the Nexus service and runs writes. The second polls readQueue(taskQueue)
// and runs reads, no more at once than the reader has consumers, so a read never waits for one.
func newWorkers(c client.Client, taskQueue string, a *activities) (nexusAndWrites, reads worker.Worker) {
	nexusAndWrites = worker.New(c, taskQueue, worker.Options{})
	nexusAndWrites.RegisterNexusService(newService(a, readQueue(taskQueue)))
	nexusAndWrites.RegisterActivity(a.Write)
	reads = worker.New(c, readQueue(taskQueue), worker.Options{MaxConcurrentActivityExecutionSize: kafka.MaxConcurrentReads})
	reads.RegisterActivity(a.Read)
	return nexusAndWrites, reads
}
