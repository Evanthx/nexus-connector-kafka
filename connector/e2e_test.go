package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/test/nexuscaller"
	"github.com/temporalio/nexus-connector-kafka/test/testenv"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// writeWithin calls write through endpoint once with the given Nexus operation timeout.
func writeWithin(ctx workflow.Context, endpoint string, in kafkaapi.WriteInput, timeout time.Duration) error {
	c := workflow.NewNexusClient(endpoint, kafkaapi.KafkaService.ServiceName)
	return c.ExecuteOperation(ctx, kafkaapi.KafkaService.Write, in, workflow.NexusOperationOptions{ScheduleToCloseTimeout: timeout}).Get(ctx, nil)
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
	caller.RegisterWorkflow(writeWithin)
	caller.RegisterWorkflow(nexuscaller.WriterWorkflow)
	require.NoError(t, caller.Start())
	t.Cleanup(caller.Stop)
	return harness{kafkaCfg: kafkaCfg, connector: connector, test: test}
}

// start is newHarness serving the customers topic through CustomersEndpoint.
func start(t *testing.T) harness {
	t.Helper()
	h := newHarness(t)
	h.serve(t, nexuscaller.CustomersEndpoint, defaultTaskQueue, h.kafkaCfg)
	return h
}

// serve runs a connector for cfg's topic on taskQueue, behind a Nexus endpoint, as one process
// of main would.
func (h harness) serve(t *testing.T, endpoint, taskQueue string, cfg kafka.Config) {
	t.Helper()
	testenv.Endpoint(t, h.connector, endpoint, testenv.ConnectorNamespace, taskQueue)
	writer, err := kafka.NewWriter(cfg)
	require.NoError(t, err)
	t.Cleanup(writer.Close)
	w := newWorker(h.connector, taskQueue, &activities{writer: writer, topic: cfg.Topic})
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)
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

// consume reads the first n messages in cfg's topic, from every partition, without a consumer
// group.
func consume(t *testing.T, cfg kafka.Config, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var recs []*kgo.Record
	for len(recs) < n {
		fetches := cl.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "read %d of %d messages", len(recs), n)
		recs = append(recs, fetches.Records()...)
	}
	return recs
}

func values(recs []*kgo.Record) []string {
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

// A caller in another namespace writes messages through the connector, and they reach the topic,
// each with a dedupe-id from the write's activity ID.
func TestEndToEnd_Write(t *testing.T) {
	h := start(t)
	h.write(t, "m0", "m1", "m2")

	recs := consume(t, h.kafkaCfg, 3)
	require.ElementsMatch(t, []string{"m0", "m1", "m2"}, values(recs))
	for _, rec := range recs {
		require.Len(t, rec.Headers, 1)
		require.Equal(t, DedupeHeader, rec.Headers[0].Key)
		require.True(t, strings.HasPrefix(string(rec.Headers[0].Value), "kafka-write-"), string(rec.Headers[0].Value))
	}
}

// A write whose caller's Nexus operation leaves 10 seconds or less to run it is refused before any
// activity starts.
func TestEndToEnd_CallerTimeoutTooShort(t *testing.T) {
	h := start(t)
	in := kafkaapi.WriteInput{Messages: []kafkaapi.Message{{Value: []byte("m")}}}
	run, err := h.test.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{TaskQueue: nexuscaller.TaskQueue},
		writeWithin, nexuscaller.CustomersEndpoint, in, 8*time.Second)
	require.NoError(t, err)
	err = run.Get(context.Background(), nil)
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
	requireErrorType(t, err, ErrorTypeInvalidRequest)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Contains(t, appErr.Message(), "it needs more than 10s")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, found, err := kafka.ReadLast(ctx, h.kafkaCfg)
	require.NoError(t, err)
	require.False(t, found, "the refused write wrote nothing")
}

// Two connectors serve two topics through two endpoints, as two processes of main would, and each
// endpoint writes to its own topic.
func TestEndToEnd_TwoEndpointsTwoTopics(t *testing.T) {
	h := newHarness(t)
	h.serve(t, nexuscaller.CustomersEndpoint, defaultTaskQueue, h.kafkaCfg)
	ordersCfg := h.kafkaCfg
	ordersCfg.Topic = testenv.OrdersTopic
	h.serve(t, nexuscaller.OrdersEndpoint, defaultTaskQueue+"-orders", ordersCfg)

	h.writeTo(t, nexuscaller.CustomersEndpoint, "event0", "event1")
	h.writeTo(t, nexuscaller.OrdersEndpoint, "order0")

	require.ElementsMatch(t, []string{"event0", "event1"}, values(consume(t, h.kafkaCfg, 2)))
	require.Equal(t, []string{"order0"}, values(consume(t, ordersCfg, 1)))
}
