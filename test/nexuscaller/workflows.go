// Package nexuscaller is for testing only. Its workflows run in their own namespace and call the
// nexusworker's KafkaService through Nexus endpoints, the way a real caller would: ReaderWorkflow
// reads batches from one or more endpoints in a loop, and WriterWorkflow writes messages.
//
// Each Nexus worker serves one topic, so the endpoint a call goes to decides the topic. The
// walkthrough in the README runs two Nexus workers on two topics, behind CustomersEndpoint and
// OrdersEndpoint, and one ReaderWorkflow that reads from both.
package nexuscaller

import (
	"context"
	"fmt"
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
	// CustomersEndpoint routes to a Nexus worker serving the customers topic. That worker has no
	// consumer group of its own, so each read names one.
	CustomersEndpoint = "kafka-customers-endpoint"
	// OrdersEndpoint routes to a second Nexus worker, serving the orders topic. That worker sets its
	// own consumer group (KAFKA_GROUP), so reads leave the group empty.
	OrdersEndpoint = "kafka-orders-endpoint"
)

// DefaultTimeLimitSeconds is how long ReaderWorkflow's reads wait for a batch to fill. A long poll
// costs much less than reading often: while the topic is idle, each 60-second wait costs two
// Actions (the read and its activity), where reading with no time limit and pausing 5 seconds
// after each empty batch costs about three (the read, its activity, and a timer) every 5 seconds.
// It is shorter than the nexusworker's 70-second heartbeat interval, so these reads send no
// heartbeats.
const DefaultTimeLimitSeconds = 60

// Source is one place ReaderWorkflow reads from.
type Source struct {
	// Endpoint is the Nexus endpoint to read through. Its Nexus worker decides the topic.
	Endpoint string
	// Group is the consumer group to read for. Its committed offsets are where reading resumes.
	// Leave it empty when the endpoint's Nexus worker has a consumer group of its own.
	Group string
	// Position is where this source has been read up to. The next read from it acknowledges it.
	// It is carried across continue-as-new.
	Position string
}

// ReaderInput starts ReaderWorkflow. The starter's read flags fill it in.
type ReaderInput struct {
	// Sources are read from in turn: a batch from the first, then a batch from the next, and so on.
	Sources []Source
	// BatchSize is the most messages each read returns. 0 means the read operation's default, 100.
	BatchSize int64
	// TimeLimitSeconds is how long each read waits for its batch to fill. 0 means
	// DefaultTimeLimitSeconds: the reader always long-polls.
	TimeLimitSeconds int64
}

// shouldContinueAsNew reports whether the run should continue as new. Tests replace it.
var shouldContinueAsNew = func(ctx workflow.Context) bool {
	return workflow.GetInfo(ctx).GetContinueAsNewSuggested()
}

// ReaderWorkflow reads batches from each of in.Sources in turn until it is stopped. It processes
// each batch's records in parallel, one ProcessRecord activity each, and acknowledges the batch on
// its next read from that source. When Temporal suggests it, it continues as new, passing on the
// position it has read up to in each source.
//
// Reading the sources one after another keeps the loop simple. The cost is that a busy source
// waits while an idle one long-polls; a reader that needs every source read as fast as possible
// would read them in parallel, or run one ReaderWorkflow per source.
//
// One activity per message is the expensive way to process: each activity is a billable Action,
// so processing costs one Action per message, many times what reading the batch costs. Passing
// the whole batch to one activity costs one Action per batch instead, but a message that fails
// retries the whole batch. This reader uses one per message to show messages processed in
// parallel.
//
// Run one per consumer group: two readers of one consumer group read the same batches.
func ReaderWorkflow(ctx workflow.Context, in ReaderInput) error {
	if in.TimeLimitSeconds == 0 {
		in.TimeLimitSeconds = DefaultTimeLimitSeconds
	}

	// The reading loop. Each pass reads one batch from each source in turn. The workflow history
	// records every pass, so if the worker restarts, the loop carries on where it was rather than
	// starting over.
	for {
		for i := range in.Sources {
			if err := readBatch(ctx, &in.Sources[i], in.BatchSize, in.TimeLimitSeconds); err != nil {
				return err
			}
		}

		// A loop like this one grows the workflow's history with every pass. When Temporal
		// suggests it, start a fresh run with the same input, which now holds each source's
		// position; each source's first read in the new run acknowledges the batch just processed.
		// This is not the loop ending: the new run picks up where this one left off.
		if shouldContinueAsNew(ctx) {
			return workflow.NewContinueAsNewError(ctx, ReaderWorkflow, in)
		}
	}
}

// readBatch reads the next batch from source, processes it, and moves source.Position on.
func readBatch(ctx workflow.Context, source *Source, batchSize, timeLimitSeconds int64) error {
	// A client for the KafkaService behind source's endpoint. The endpoint, not this workflow,
	// knows which namespace and task queue serve it, and so which topic is read.
	kafkaService := workflow.NewNexusClient(source.Endpoint, kafkaapi.KafkaService.ServiceName)

	// 1. Read the next batch. The ack (the previous batch's position) is committed before
	// reading, so acknowledging a batch costs no extra call. Without an ack, as on a new reader's
	// first pass, the read starts from the consumer group's committed offsets.
	read := kafkaapi.ReadInput{TimeLimitSeconds: &timeLimitSeconds}
	if source.Group != "" {
		read.Group = &source.Group // left out, the endpoint's own consumer group is used
	}
	if batchSize != 0 {
		read.BatchSize = &batchSize // left out, the operation uses its default, 100
	}
	if source.Position != "" {
		read.Ack = &source.Position
	}
	logger := workflow.GetLogger(ctx)
	logger.Info(fmt.Sprintf("Reading through %s; the read waits up to %d seconds for its batch to fill",
		source.Endpoint, timeLimitSeconds))
	var batch kafkaapi.ReadOutput
	err := kafkaService.ExecuteOperation(ctx, kafkaapi.KafkaService.Read, read, workflow.NexusOperationOptions{
		// How long this workflow waits for the read, retries included. The read itself gives up
		// after its time limit plus five minutes; this allows ten, so the read's own limit is
		// what ends a failing read.
		ScheduleToCloseTimeout: time.Duration(timeLimitSeconds)*time.Second + 10*time.Minute,
	}).Get(ctx, &batch)
	if err != nil {
		return err
	}
	// The topic is in each record: the caller names only the endpoint.
	if len(batch.Records) == 0 {
		logger.Info(fmt.Sprintf("Read no new messages through %s", source.Endpoint))
	} else {
		logger.Info(fmt.Sprintf("Read %d message(s) from topic *** %s *** through %s",
			len(batch.Records), batch.Records[0].Topic, source.Endpoint))
	}

	// 2. Process the batch: start one ProcessRecord activity per message, all at once, then wait
	// for every one. Messages run in parallel, so their order isn't kept; to keep partition order,
	// process each partition's messages one after another instead. ProcessRecord activities may
	// each take up to a minute per attempt. With no retry policy set, a failed attempt is retried
	// with backoff until it succeeds.
	processCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	processed := make([]workflow.Future, len(batch.Records))
	for i, rec := range batch.Records {
		processed[i] = workflow.ExecuteActivity(processCtx, ProcessRecord, rec)
	}
	for _, f := range processed {
		if err := f.Get(ctx, nil); err != nil {
			return err
		}
	}

	// 3. The batch is processed, so its position is safe to acknowledge on the next read from this
	// source. An empty batch's position moves past nothing unread, so acknowledging it is harmless.
	source.Position = batch.Position
	return nil
}

// ProcessRecord stands in for real work on one message: it logs it. Each call is a billable
// Action; see ReaderWorkflow.
func ProcessRecord(ctx context.Context, rec kafkaapi.Record) error {
	activity.GetLogger(ctx).Info(fmt.Sprintf("Processed message from topic *** %s ***", rec.Topic),
		"partition", rec.Partition, "offset", rec.Offset, "value", string(rec.Value))
	return nil
}

// WriterInput starts WriterWorkflow.
type WriterInput struct {
	// Endpoint is the Nexus endpoint to write through. Its Nexus worker decides the topic.
	Endpoint string
	// Messages are the messages to write, in order.
	Messages []string
}

// WriterWorkflow writes messages to the topic behind in.Endpoint and returns how many were
// written. It runs once and finishes: one write operation for all the messages, in order.
func WriterWorkflow(ctx workflow.Context, in WriterInput) (int64, error) {
	kafkaService := workflow.NewNexusClient(in.Endpoint, kafkaapi.KafkaService.ServiceName)
	write := kafkaapi.WriteInput{}
	for _, m := range in.Messages {
		write.Messages = append(write.Messages, kafkaapi.Message{Value: []byte(m)})
	}
	var out kafkaapi.WriteOutput
	err := kafkaService.ExecuteOperation(ctx, kafkaapi.KafkaService.Write, write, workflow.NexusOperationOptions{
		// The write itself retries for up to ten minutes; this waits a little longer.
		ScheduleToCloseTimeout: 15 * time.Minute,
	}).Get(ctx, &out)
	if err != nil {
		return 0, err
	}
	return out.Written, nil
}
