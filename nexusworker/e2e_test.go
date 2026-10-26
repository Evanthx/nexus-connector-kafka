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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// readOnce calls read through endpoint once, for tests that check one read at a time.
func readOnce(ctx workflow.Context, endpoint string, in kafkaapi.ReadInput) (kafkaapi.ReadOutput, error) {
	c := workflow.NewNexusClient(endpoint, kafkaapi.KafkaService.ServiceName)
	var out kafkaapi.ReadOutput
	err := c.ExecuteOperation(ctx, kafkaapi.KafkaService.Read, in, workflow.NexusOperationOptions{ScheduleToCloseTimeout: time.Minute}).Get(ctx, &out)
	return out, err
}

type harness struct {
	kafkaCfg  kafka.Config // for the customers topic
	connector client.Client
	test      client.Client
}

// newHarness starts an in-memory Kafka and a dev server, with a worker for the test caller in the
// test namespace. It serves no endpoint yet: see serve.
func newHarness(t *testing.T) harness {
	t.Helper()
	kafkaCfg := testenv.Kafka(t)
	connector, test := testenv.Temporal(t)
	caller := worker.New(test, nexuscaller.TaskQueue, worker.Options{})
	caller.RegisterWorkflow(readOnce)
	caller.RegisterWorkflow(nexuscaller.WriterWorkflow)
	require.NoError(t, caller.Start())
	t.Cleanup(caller.Stop)
	return harness{kafkaCfg: kafkaCfg, connector: connector, test: test}
}

// start is newHarness serving the customers topic through CustomersEndpoint, with no consumer group of
// its own. maxBatchBytes is the nexusworker's batch byte limit.
func start(t *testing.T, maxBatchBytes int) harness {
	t.Helper()
	h := newHarness(t)
	h.serve(t, nexuscaller.CustomersEndpoint, defaultTaskQueue, h.kafkaCfg, "", maxBatchBytes)
	return h
}

// serve runs a nexusworker for cfg's topic on taskQueue, behind a Nexus endpoint, as one process
// of main would, with group as its KAFKA_GROUP.
func (h harness) serve(t *testing.T, endpoint, taskQueue string, cfg kafka.Config, group string, maxBatchBytes int) {
	t.Helper()
	testenv.Endpoint(t, h.connector, endpoint, testenv.ConnectorNamespace, taskQueue)
	reader, err := kafka.NewGroupReader(cfg, recordBytes(maxBatchBytes))
	require.NoError(t, err)
	t.Cleanup(reader.Close)
	writer, err := kafka.NewWriter(cfg)
	require.NoError(t, err)
	t.Cleanup(writer.Close)
	a := &activities{reader: reader, writer: writer, topic: cfg.Topic, group: group, maxBatchBytes: maxBatchBytes}
	w, reads := newWorkers(h.connector, taskQueue, a)
	require.NoError(t, reads.Start())
	t.Cleanup(reads.Stop)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)
}

// read reads once through CustomersEndpoint.
func (h harness) read(t *testing.T, in kafkaapi.ReadInput) (kafkaapi.ReadOutput, error) {
	t.Helper()
	return h.readFrom(t, nexuscaller.CustomersEndpoint, in)
}

func (h harness) readFrom(t *testing.T, endpoint string, in kafkaapi.ReadInput) (kafkaapi.ReadOutput, error) {
	t.Helper()
	run, err := h.test.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{TaskQueue: nexuscaller.TaskQueue}, readOnce, endpoint, in)
	require.NoError(t, err)
	var out kafkaapi.ReadOutput
	err = run.Get(context.Background(), &out)
	return out, err
}

// write writes through CustomersEndpoint.
func (h harness) write(t *testing.T, messages ...string) {
	t.Helper()
	h.writeTo(t, nexuscaller.CustomersEndpoint, messages...)
}

func (h harness) writeTo(t *testing.T, endpoint string, messages ...string) {
	t.Helper()
	run, err := h.test.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{TaskQueue: nexuscaller.TaskQueue},
		nexuscaller.WriterWorkflow, nexuscaller.WriterInput{Endpoint: endpoint, Messages: messages})
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

	first, err := h.read(t, kafkaapi.ReadInput{Group: ptr("g"), BatchSize: ptr(int64(2))})
	require.NoError(t, err)
	require.Len(t, first.Records, 2)
	again, err := h.read(t, kafkaapi.ReadInput{Group: ptr("g"), BatchSize: ptr(int64(2))})
	require.NoError(t, err)
	require.Equal(t, values(first.Records), values(again.Records))

	next, err := h.read(t, kafkaapi.ReadInput{Group: ptr("g"), BatchSize: ptr(int64(10)), Ack: &first.Position})
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
			nexuscaller.CustomersEndpoint, kafkaapi.ReadInput{Group: ptr(fmt.Sprintf("idle-%d", i)), TimeLimitSeconds: ptr(int64(30))})
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
		run, err := h.test.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{TaskQueue: nexuscaller.TaskQueue},
			nexuscaller.WriterWorkflow, nexuscaller.WriterInput{Endpoint: nexuscaller.CustomersEndpoint, Messages: []string{"late"}})
		if err == nil {
			err = run.Get(context.Background(), nil)
		}
		written <- err
	})
	t.Cleanup(func() { require.NoError(t, <-written) })
	out, err := h.read(t, kafkaapi.ReadInput{Group: ptr("g"), BatchSize: ptr(int64(1)), TimeLimitSeconds: ptr(int64(60))})
	require.NoError(t, err)
	require.Equal(t, []string{"late"}, values(out.Records))
}

// An ack from another group is rejected before any activity starts.
func TestEndToEnd_InvalidAck(t *testing.T) {
	h := start(t, defaultMaxBatchBytes)
	h.write(t, "m0")
	a, err := h.read(t, kafkaapi.ReadInput{Group: ptr("a")})
	require.NoError(t, err)

	_, err = h.read(t, kafkaapi.ReadInput{Group: ptr("b"), Ack: &a.Position})
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

	first, err := h.read(t, kafkaapi.ReadInput{Group: ptr("live"), BatchSize: ptr(int64(1))})
	require.NoError(t, err)
	_, err = h.read(t, kafkaapi.ReadInput{Group: ptr("live"), BatchSize: ptr(int64(0)), Ack: &first.Position})
	requireErrorType(t, err, ErrorTypeGroupInUse)
}

// A record that can't fit in a result on its own fails the read with RecordTooLarge.
func TestEndToEnd_RecordTooLarge(t *testing.T) {
	h := start(t, resultReserve+1000)
	h.write(t, strings.Repeat("x", 2000))
	_, err := h.read(t, kafkaapi.ReadInput{Group: ptr("g")})
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
		nexuscaller.ReaderWorkflow, nexuscaller.ReaderInput{
			Sources:   []nexuscaller.Source{{Endpoint: nexuscaller.CustomersEndpoint, Group: "g"}},
			BatchSize: 5, TimeLimitSeconds: 1,
		})
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

// Two nexusworkers serve two topics through two endpoints, as two processes of main would, and one
// ReaderWorkflow reads each topic through its endpoint: the customers topic for a consumer group the
// reader names, and the orders topic for the consumer group its nexusworker sets.
func TestEndToEnd_TwoEndpointsTwoTopics(t *testing.T) {
	h := newHarness(t)
	h.serve(t, nexuscaller.CustomersEndpoint, defaultTaskQueue, h.kafkaCfg, "", defaultMaxBatchBytes)
	ordersCfg := h.kafkaCfg
	ordersCfg.Topic = testenv.OrdersTopic
	h.serve(t, nexuscaller.OrdersEndpoint, defaultTaskQueue+"-orders", ordersCfg, "orders-reader", defaultMaxBatchBytes)

	h.writeTo(t, nexuscaller.CustomersEndpoint, "event0", "event1")
	h.writeTo(t, nexuscaller.OrdersEndpoint, "order0")

	const readerQueue = "kafka-reader-test"
	processed := make(chan kafkaapi.Record, 100)
	readerWorker := worker.New(h.test, readerQueue, worker.Options{})
	readerWorker.RegisterWorkflow(nexuscaller.ReaderWorkflow)
	readerWorker.RegisterActivityWithOptions(func(_ context.Context, rec kafkaapi.Record) error {
		processed <- rec
		return nil
	}, activity.RegisterOptions{Name: "ProcessRecord"})
	require.NoError(t, readerWorker.Start())
	t.Cleanup(readerWorker.Stop)

	run, err := h.test.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{TaskQueue: readerQueue},
		nexuscaller.ReaderWorkflow, nexuscaller.ReaderInput{
			Sources: []nexuscaller.Source{
				{Endpoint: nexuscaller.CustomersEndpoint, Group: "customers-reader"},
				{Endpoint: nexuscaller.OrdersEndpoint},
			},
			TimeLimitSeconds: 1,
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.test.TerminateWorkflow(context.Background(), run.GetID(), "", "test done") })

	byTopic := map[string][]string{}
	deadline := time.After(time.Minute)
	for len(byTopic[testenv.Topic])+len(byTopic[testenv.OrdersTopic]) < 3 {
		select {
		case rec := <-processed:
			byTopic[rec.Topic] = append(byTopic[rec.Topic], string(rec.Value))
		case <-deadline:
			t.Fatalf("processed %v", byTopic)
		}
	}
	require.ElementsMatch(t, []string{"event0", "event1"}, byTopic[testenv.Topic])
	require.Equal(t, []string{"order0"}, byTopic[testenv.OrdersTopic])

	// Each endpoint read for its own consumer group.
	admin := kadm.NewClient(newKafkaClient(t, h.kafkaCfg))
	for group, topic := range map[string]string{"customers-reader": testenv.Topic, "orders-reader": testenv.OrdersTopic} {
		require.Eventually(t, func() bool {
			offsets, err := admin.FetchOffsets(context.Background(), group)
			return err == nil && len(offsets[topic]) > 0
		}, 30*time.Second, 200*time.Millisecond, "consumer group %s has offsets for %s", group, topic)
	}
}

// A nexusworker with a consumer group of its own serves only that consumer group: a read may leave
// group empty or name it, but naming another is refused at once. A nexusworker without one
// refuses a read that names none.
func TestEndToEnd_ConsumerGroupIsChecked(t *testing.T) {
	h := newHarness(t)
	h.serve(t, nexuscaller.CustomersEndpoint, defaultTaskQueue, h.kafkaCfg, "", defaultMaxBatchBytes)
	h.serve(t, nexuscaller.OrdersEndpoint, defaultTaskQueue+"-orders", h.kafkaCfg, "fixed", defaultMaxBatchBytes)

	_, err := h.readFrom(t, nexuscaller.OrdersEndpoint, kafkaapi.ReadInput{})
	require.NoError(t, err)
	_, err = h.readFrom(t, nexuscaller.OrdersEndpoint, kafkaapi.ReadInput{Group: ptr("fixed")})
	require.NoError(t, err)

	_, err = h.readFrom(t, nexuscaller.OrdersEndpoint, kafkaapi.ReadInput{Group: ptr("other")})
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
	requireErrorType(t, err, ErrorTypeInvalidRequest)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Contains(t, appErr.Error(), `reads only consumer group "fixed"`, "the caller sees why")

	_, err = h.readFrom(t, nexuscaller.CustomersEndpoint, kafkaapi.ReadInput{})
	requireErrorType(t, err, ErrorTypeInvalidRequest)
	require.ErrorAs(t, err, &appErr)
	require.Contains(t, appErr.Error(), "set group, the consumer group to read for")
}

func newKafkaClient(t *testing.T, cfg kafka.Config) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	return cl
}
