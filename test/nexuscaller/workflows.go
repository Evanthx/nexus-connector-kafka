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

// emptyPause is how long ReaderWorkflow waits after an empty batch read with no time limit, so an
// idle topic doesn't make it loop without pause.
const emptyPause = 5 * time.Second

// ReaderInput starts ReaderWorkflow.
type ReaderInput struct {
	Group            string
	BatchSize        int64
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
// Run one per group: two readers of one group read the same batches.
func ReaderWorkflow(ctx workflow.Context, in ReaderInput) error {
	kafkaService := workflow.NewNexusClient(EndpointName, kafkaapi.KafkaService.ServiceName)
	processCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	position := in.Position
	for {
		read := kafkaapi.ReadInput{Group: in.Group, BatchSize: &in.BatchSize, TimeLimitSeconds: &in.TimeLimitSeconds}
		if position != "" {
			read.Ack = &position
		}
		var batch kafkaapi.ReadOutput
		err := kafkaService.ExecuteOperation(ctx, kafkaapi.KafkaService.Read, read, workflow.NexusOperationOptions{
			// More than the read's own limit: its time limit plus five minutes of retries.
			ScheduleToCloseTimeout: time.Duration(in.TimeLimitSeconds)*time.Second + 10*time.Minute,
		}).Get(ctx, &batch)
		if err != nil {
			return err
		}

		processed := make([]workflow.Future, len(batch.Records))
		for i, rec := range batch.Records {
			processed[i] = workflow.ExecuteActivity(processCtx, ProcessRecord, rec)
		}
		for _, f := range processed {
			if err := f.Get(ctx, nil); err != nil {
				return err
			}
		}
		position = batch.Position

		if shouldContinueAsNew(ctx) {
			in.Position = position
			return workflow.NewContinueAsNewError(ctx, ReaderWorkflow, in)
		}
		if len(batch.Records) == 0 && in.TimeLimitSeconds == 0 {
			if err := workflow.Sleep(ctx, emptyPause); err != nil {
				return err
			}
		}
	}
}

// ProcessRecord stands in for real work on one message: it logs it.
func ProcessRecord(ctx context.Context, rec kafkaapi.Record) error {
	activity.GetLogger(ctx).Info("Processed message",
		"partition", rec.Partition, "offset", rec.Offset, "value", string(rec.Value))
	return nil
}

// WriterWorkflow writes messages to the topic and returns how many were written.
func WriterWorkflow(ctx workflow.Context, messages []string) (int64, error) {
	kafkaService := workflow.NewNexusClient(EndpointName, kafkaapi.KafkaService.ServiceName)
	in := kafkaapi.WriteInput{}
	for _, m := range messages {
		in.Messages = append(in.Messages, kafkaapi.Message{Value: []byte(m)})
	}
	var out kafkaapi.WriteOutput
	err := kafkaService.ExecuteOperation(ctx, kafkaapi.KafkaService.Write, in, workflow.NexusOperationOptions{
		ScheduleToCloseTimeout: 15 * time.Minute,
	}).Get(ctx, &out)
	if err != nil {
		return 0, err
	}
	return out.Written, nil
}
