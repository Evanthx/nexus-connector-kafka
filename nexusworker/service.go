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

// taskQueue is the Nexus worker's task queue. The Nexus endpoint targets it.
const taskQueue = "nexus-connector-kafka"

// readOptions are the options for the activity behind one read. An attempt may take the read's
// time limit plus a minute; the whole read, with retries, the time limit plus five minutes. The
// read heartbeats while it waits, so a worker that dies is noticed within the heartbeat timeout.
func readOptions(requestID string, in kafkaapi.ReadInput) client.StartActivityOptions {
	timeLimit := time.Duration(in.TimeLimitSecondsOrDefault()) * time.Second
	return client.StartActivityOptions{
		ID:                     "kafka-read-" + requestID,
		StartToCloseTimeout:    timeLimit + time.Minute,
		ScheduleToCloseTimeout: timeLimit + 5*time.Minute,
		HeartbeatTimeout:       30 * time.Second,
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

// workerOptions are the options for the worker that runs the activities. It runs no more at once
// than the reader has consumers, so a read never waits for one.
func workerOptions() worker.Options {
	return worker.Options{MaxConcurrentActivityExecutionSize: kafka.MaxConcurrentReads}
}

// registry is the part of a worker that register needs.
type registry interface {
	worker.ActivityRegistry
	worker.NexusServiceRegistry
}

// register registers the Nexus service and the activities it starts.
func register(w registry, a *activities) {
	w.RegisterNexusService(newService(a))
	w.RegisterActivity(a)
}
