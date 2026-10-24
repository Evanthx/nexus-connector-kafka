package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/test/nexuscaller"
	"github.com/temporalio/nexus-connector-kafka/test/testenv"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// readOnce calls read once, for tests that check one read at a time.
func readOnce(ctx workflow.Context, in kafkaapi.ReadInput) (kafkaapi.ReadOutput, error) {
	c := workflow.NewNexusClient(nexuscaller.EndpointName, kafkaapi.KafkaService.ServiceName)
	var out kafkaapi.ReadOutput
	err := c.ExecuteOperation(ctx, kafkaapi.KafkaService.Read, in, workflow.NexusOperationOptions{ScheduleToCloseTimeout: time.Minute}).Get(ctx, &out)
	return out, err
}

type harness struct {
	kafkaCfg kafka.Config
	test     client.Client
}

// start runs the nexusworker against an in-memory Kafka and a dev server, with a worker for the
// test caller in the test namespace. maxBatchBytes is the nexusworker's batch byte limit.
func start(t *testing.T, maxBatchBytes int) harness {
	t.Helper()
	kafkaCfg := testenv.Kafka(t)
	connector, test := testenv.Temporal(t)
	testenv.Endpoint(t, connector, nexuscaller.EndpointName, testenv.ConnectorNamespace, taskQueue)

	reader, err := kafka.NewGroupReader(kafkaCfg, recordBytes(maxBatchBytes))
	require.NoError(t, err)
	t.Cleanup(reader.Close)
	writer, err := kafka.NewWriter(kafkaCfg)
	require.NoError(t, err)
	t.Cleanup(writer.Close)
	w, reads := newWorkers(connector, &activities{reader: reader, writer: writer, topic: kafkaCfg.Topic, maxBatchBytes: maxBatchBytes})
	require.NoError(t, reads.Start())
	t.Cleanup(reads.Stop)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	caller := worker.New(test, nexuscaller.TaskQueue, worker.Options{})
	caller.RegisterWorkflow(readOnce)
	caller.RegisterWorkflow(nexuscaller.WriterWorkflow)
	require.NoError(t, caller.Start())
	t.Cleanup(caller.Stop)
	return harness{kafkaCfg: kafkaCfg, test: test}
}

func (h harness) read(t *testing.T, in kafkaapi.ReadInput) (kafkaapi.ReadOutput, error) {
	t.Helper()
	run, err := h.test.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{TaskQueue: nexuscaller.TaskQueue}, readOnce, in)
	require.NoError(t, err)
	var out kafkaapi.ReadOutput
	err = run.Get(context.Background(), &out)
	return out, err
}

func (h harness) write(t *testing.T, messages ...string) {
	t.Helper()
	run, err := h.test.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{TaskQueue: nexuscaller.TaskQueue}, nexuscaller.WriterWorkflow, messages)
	require.NoError(t, err)
	var written int64
	require.NoError(t, run.Get(context.Background(), &written))
	require.Equal(t, int64(len(messages)), written)
}

func ptr[T any](v T) *T { return &v }

func values(recs []kafkaapi.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.Value)
	}
	return out
}

func requireErrorType(t *testing.T, err error, errType string) {
	t.Helper()
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, errType, appErr.Type(), err.Error())
}

// A caller in another namespace writes messages, then reads them by consumer group: a read
// without an ack returns the same batch, a read with one moves on.
func TestEndToEnd_WriteThenReadByGroup(t *testing.T) {
	h := start(t, defaultMaxBatchBytes)
	h.write(t, "m0", "m1", "m2")

	first, err := h.read(t, kafkaapi.ReadInput{Group: "g", BatchSize: ptr(int64(2))})
	require.NoError(t, err)
	require.Len(t, first.Records, 2)
	again, err := h.read(t, kafkaapi.ReadInput{Group: "g", BatchSize: ptr(int64(2))})
	require.NoError(t, err)
	require.Equal(t, values(first.Records), values(again.Records))

	next, err := h.read(t, kafkaapi.ReadInput{Group: "g", BatchSize: ptr(int64(10)), Ack: &first.Position})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"m0", "m1", "m2"}, append(values(first.Records), values(next.Records)...))

	// Every written message has a dedupe-id from the write's activity ID.
	for _, rec := range append(first.Records, next.Records...) {
		require.Len(t, rec.Headers, 1)
		require.Equal(t, DedupeHeader, rec.Headers[0].Key)
		require.True(t, strings.HasPrefix(string(rec.Headers[0].Value), "kafka-write-"), string(rec.Headers[0].Value))
	}
}

// Reads waiting for their batches don't hold up a write, even when as many are waiting as the
// reader allows at once.
func TestEndToEnd_WaitingReadsDoNotBlockWrites(t *testing.T) {
	h := start(t, defaultMaxBatchBytes)
	for i := range kafka.MaxConcurrentReads {
		_, err := h.test.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{TaskQueue: nexuscaller.TaskQueue}, readOnce,
			kafkaapi.ReadInput{Group: fmt.Sprintf("idle-%d", i), TimeLimitSeconds: ptr(int64(30))})
		require.NoError(t, err)
	}
	<-time.After(5 * time.Second) // let the reads start and take their slots

	start := time.Now()
	h.write(t, "not held up")
	require.Less(t, time.Since(start), 15*time.Second)
}

// A read with a time limit waits for messages written after it started.
func TestEndToEnd_ReadWaitsForNewMessages(t *testing.T) {
	h := start(t, defaultMaxBatchBytes)
	// Write while the read below is waiting.
	written := make(chan error, 1)
	time.AfterFunc(2*time.Second, func() {
		run, err := h.test.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{TaskQueue: nexuscaller.TaskQueue}, nexuscaller.WriterWorkflow, []string{"late"})
		if err == nil {
			err = run.Get(context.Background(), nil)
		}
		written <- err
	})
	t.Cleanup(func() { require.NoError(t, <-written) })
	out, err := h.read(t, kafkaapi.ReadInput{Group: "g", BatchSize: ptr(int64(1)), TimeLimitSeconds: ptr(int64(60))})
	require.NoError(t, err)
	require.Equal(t, []string{"late"}, values(out.Records))
}

// An ack from another group is rejected before any activity starts.
func TestEndToEnd_InvalidAck(t *testing.T) {
	h := start(t, defaultMaxBatchBytes)
	h.write(t, "m0")
	a, err := h.read(t, kafkaapi.ReadInput{Group: "a"})
	require.NoError(t, err)

	_, err = h.read(t, kafkaapi.ReadInput{Group: "b", Ack: &a.Position})
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
	requireErrorType(t, err, ErrorTypeInvalidRequest)
}

// A group with a live member, such as a streamer's, can't be committed for, and the caller is told
// so at once rather than after retries.
func TestEndToEnd_GroupInUse(t *testing.T) {
	h := start(t, defaultMaxBatchBytes)
	h.write(t, "m0", "m1")
	member, err := kafka.NewGroupMember(h.kafkaCfg, "live")
	require.NoError(t, err)
	t.Cleanup(member.Close)
	b, err := member.Next(context.Background(), 1, 0)
	require.NoError(t, err)
	require.NoError(t, member.Commit(context.Background(), b))

	first, err := h.read(t, kafkaapi.ReadInput{Group: "live", BatchSize: ptr(int64(1))})
	require.NoError(t, err)
	_, err = h.read(t, kafkaapi.ReadInput{Group: "live", BatchSize: ptr(int64(0)), Ack: &first.Position})
	requireErrorType(t, err, ErrorTypeGroupInUse)
}

// A record that can't fit in a result on its own fails the read with RecordTooLarge.
func TestEndToEnd_RecordTooLarge(t *testing.T) {
	h := start(t, resultReserve+1000)
	h.write(t, strings.Repeat("x", 2000))
	_, err := h.read(t, kafkaapi.ReadInput{Group: "g"})
	requireErrorType(t, err, ErrorTypeRecordTooLarge)
}

// The test caller's ReaderWorkflow reads every message by group, across batches, and processes
// each.
func TestEndToEnd_ReaderWorkflow(t *testing.T) {
	h := start(t, defaultMaxBatchBytes)
	var messages []string
	for i := range 12 {
		messages = append(messages, fmt.Sprintf("m%d", i))
	}
	h.write(t, messages...)

	// The reader runs on a task queue of its own, with a ProcessRecord that reports each message.
	// It runs its activities on its own task queue.
	const readerQueue = "kafka-reader-test"
	processed := make(chan string, 100)
	readerWorker := worker.New(h.test, readerQueue, worker.Options{})
	readerWorker.RegisterWorkflow(nexuscaller.ReaderWorkflow)
	readerWorker.RegisterActivityWithOptions(func(_ context.Context, rec kafkaapi.Record) error {
		processed <- string(rec.Value)
		return nil
	}, activity.RegisterOptions{Name: "ProcessRecord"})
	require.NoError(t, readerWorker.Start())
	t.Cleanup(readerWorker.Stop)

	run, err := h.test.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{TaskQueue: readerQueue},
		nexuscaller.ReaderWorkflow, nexuscaller.ReaderInput{Group: "g", BatchSize: 5, TimeLimitSeconds: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.test.TerminateWorkflow(context.Background(), run.GetID(), "", "test done") })

	var got []string
	deadline := time.After(time.Minute)
	for len(got) < len(messages) {
		select {
		case v := <-processed:
			got = append(got, v)
		case <-deadline:
			t.Fatalf("processed %d of %d messages: %v", len(got), len(messages), got)
		}
	}
	require.ElementsMatch(t, messages, got)
}
