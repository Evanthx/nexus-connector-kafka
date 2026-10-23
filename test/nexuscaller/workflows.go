// Package nexuscaller is for testing only. Its workflows run in their own namespace and call the
// nexusworker's KafkaService through a Nexus endpoint, the way a real caller would:
// ReaderWorkflow reads a consumer group's batches in a loop, and WriterWorkflow writes messages.
package nexuscaller

import (
	"context"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

const (
	// Namespace is the test programs' default namespace.
	Namespace = "kafka-test"
	// TaskQueue is the test caller worker's task queue.
	TaskQueue = "kafka-nexuscaller"
	// EndpointName is the Nexus endpoint that routes to the nexusworker.
	EndpointName = "kafka-endpoint"
)

// DefaultTimeLimitSeconds is how long ReaderWorkflow's reads wait for a batch to fill. A long poll
// costs much less than reading often: while the topic is idle, each 60-second wait costs two
// Actions (the read and its activity), where reading with no time limit and pausing 5 seconds
// after each empty batch costs about three (the read, its activity, and a timer) every 5 seconds.
// It is shorter than the nexusworker's 70-second heartbeat interval, so these reads send no
// heartbeats.
const DefaultTimeLimitSeconds = 60

// ReaderInput starts ReaderWorkflow. The starter's read flags fill it in.
type ReaderInput struct {
	// Group is the consumer group to read for. Its committed offsets are where reading resumes.
	Group string
	// BatchSize is the most messages each read returns. 0 means the read operation's default, 100.
	BatchSize int64
	// TimeLimitSeconds is how long each read waits for its batch to fill. 0 means
	// DefaultTimeLimitSeconds: the reader always long-polls.
	TimeLimitSeconds int64
	// Position is the position the previous run read up to, carried across continue-as-new. Its
	// first read acknowledges it.
	Position string
}

// shouldContinueAsNew reports whether the run should continue as new. Tests replace it.
var shouldContinueAsNew = func(ctx workflow.Context) bool {
	return workflow.GetInfo(ctx).GetContinueAsNewSuggested()
}

// ReaderWorkflow reads in.Group's batches until it is stopped. It processes each batch's records
// in parallel, one ProcessRecord activity each, and acknowledges the batch on its next read. When
// Temporal suggests it, it continues as new, passing on the position it has read up to.
//
// One activity per message is the expensive way to process: each activity is a billable Action,
// so processing costs one Action per message, many times what reading the batch costs. Passing
// the whole batch to one activity costs one Action per batch instead, but a message that fails
// retries the whole batch. This reader uses one per message to show messages processed in
// parallel.
//
// Run one per group: two readers of one group read the same batches.
func ReaderWorkflow(ctx workflow.Context, in ReaderInput) error {
	if in.TimeLimitSeconds == 0 {
		in.TimeLimitSeconds = DefaultTimeLimitSeconds
	}
	// A client for the nexusworker's KafkaService, reached through the Nexus endpoint. The
	// endpoint, not this workflow, knows which namespace and task queue serve it.
	kafkaService := workflow.NewNexusClient(EndpointName, kafkaapi.KafkaService.ServiceName)
	// ProcessRecord activities may each take up to a minute per attempt. With no retry policy
	// set, a failed attempt is retried with backoff until it succeeds.
	processCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	// position is where this reader has read up to. Passing it as the next read's ack commits it
	// to the group, so the group resumes after it.
	position := in.Position

	// The reading loop. Each pass reads one batch, processes it, and remembers its position. The
	// workflow history records every pass, so if the worker restarts, the loop carries on where it
	// was rather than starting over.
	for {
		// 1. Read the next batch. The ack (the previous batch's position) is committed before
		// reading, so acknowledging a batch costs no extra call. Without an ack, as on a new
		// reader's first pass, the read starts from the group's committed offsets.
		read := kafkaapi.ReadInput{Group: in.Group, TimeLimitSeconds: &in.TimeLimitSeconds}
		if in.BatchSize != 0 {
			read.BatchSize = &in.BatchSize // left unset, the operation uses its default, 100
		}
		if position != "" {
			read.Ack = &position
		}
		var batch kafkaapi.ReadOutput
		err := kafkaService.ExecuteOperation(ctx, kafkaapi.KafkaService.Read, read, workflow.NexusOperationOptions{
			// How long this workflow waits for the read, retries included. The read itself gives
			// up after its time limit plus five minutes; this allows ten, so the read's own
			// limit is what ends a failing read.
			ScheduleToCloseTimeout: time.Duration(in.TimeLimitSeconds)*time.Second + 10*time.Minute,
		}).Get(ctx, &batch)
		if err != nil {
			return err
		}

		// 2. Process the batch: start one ProcessRecord activity per message, all at once, then
		// wait for every one. Messages run in parallel, so their order isn't kept; to keep
		// partition order, process each partition's messages one after another instead.
		processed := make([]workflow.Future, len(batch.Records))
		for i, rec := range batch.Records {
			processed[i] = workflow.ExecuteActivity(processCtx, ProcessRecord, rec)
		}
		for _, f := range processed {
			if err := f.Get(ctx, nil); err != nil {
				return err
			}
		}

		// 3. The batch is processed, so its position is safe to acknowledge on the next read. An
		// empty batch's position moves past nothing unread, so acknowledging it is harmless.
		position = batch.Position

		// 4. A loop like this one grows the workflow's history with every pass. When Temporal
		// suggests it, start a fresh run with the same input and this position; its first read
		// acknowledges the batch just processed. This is not the loop ending: the new run picks
		// up the loop where this one left off.
		if shouldContinueAsNew(ctx) {
			in.Position = position
			return workflow.NewContinueAsNewError(ctx, ReaderWorkflow, in)
		}
	}
}

// ProcessRecord stands in for real work on one message: it logs it. Each call is a billable
// Action; see ReaderWorkflow.
func ProcessRecord(ctx context.Context, rec kafkaapi.Record) error {
	activity.GetLogger(ctx).Info("Processed message",
		"partition", rec.Partition, "offset", rec.Offset, "value", string(rec.Value))
	return nil
}

// WriterWorkflow writes messages to the topic and returns how many were written. It runs once
// and finishes: one write operation for all the messages, in order.
func WriterWorkflow(ctx workflow.Context, messages []string) (int64, error) {
	kafkaService := workflow.NewNexusClient(EndpointName, kafkaapi.KafkaService.ServiceName)
	in := kafkaapi.WriteInput{}
	for _, m := range messages {
		in.Messages = append(in.Messages, kafkaapi.Message{Value: []byte(m)})
	}
	var out kafkaapi.WriteOutput
	err := kafkaService.ExecuteOperation(ctx, kafkaapi.KafkaService.Write, in, workflow.NexusOperationOptions{
		// The write itself retries for up to ten minutes; this waits a little longer.
		ScheduleToCloseTimeout: 15 * time.Minute,
	}).Get(ctx, &out)
	if err != nil {
		return 0, err
	}
	return out.Written, nil
}
