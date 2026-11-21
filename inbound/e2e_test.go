package main

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/nexussend"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/temporalclient"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/test/codectest"
	"github.com/temporalio/nexus-connector-kafka/test/testenv"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	receiverEndpoint  = "kafka-receiver-endpoint"
	receiverTaskQueue = "kafka-receiver"
)

// receiver is a KafkaMessageReceiver in the test namespace. It records each message, and fails
// the operation for a batch whose first message failFirst picks, once.
type receiver struct {
	mu        sync.Mutex
	got       []string
	failed    map[string]bool
	failFirst func(value string) bool
	delay     time.Duration // how long each operation takes before it records its messages
}

func (r *receiver) values() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.got...)
}

func (r *receiver) waitFor(t *testing.T, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return len(r.values()) >= n }, 30*time.Second, 50*time.Millisecond, "received %v", r.values())
}

func (r *receiver) service() *nexus.Service {
	op := nexus.NewSyncOperation(kafkaapi.KafkaMessageReceiver.Receive.Name(), func(_ context.Context, input kafkaapi.ReceiveInput, _ nexus.StartOperationOptions) (kafkaapi.ReceiveOutput, error) {
		time.Sleep(r.delay) //nolint:forbidigo // a slow receiver
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, rec := range input.Records {
			r.got = append(r.got, string(rec.Value))
		}
		v := string(input.Records[0].Value)
		if r.failFirst != nil && r.failFirst(v) && !r.failed[v] {
			r.failed[v] = true
			return kafkaapi.ReceiveOutput{}, nexus.NewOperationFailedErrorf("failing %s once", v)
		}
		return kafkaapi.ReceiveOutput{}, nil
	})
	svc := nexus.NewService(kafkaapi.KafkaMessageReceiver.ServiceName)
	svc.MustRegister(op)
	return svc
}

type e2e struct {
	kafkaCfg        kafka.Config
	starter         *nexussend.Nexus // starts operations without waiting (INBOUND_ORDERING=none)
	waiter          *nexussend.Nexus // waits for each to complete (INBOUND_ORDERING=partition)
	receiver        *receiver
	connector, test client.Client
}

// startE2E runs a receiver in the test namespace, behind an endpoint, and returns a starter in the
// connectors namespace that delivers to it.
func startE2E(t *testing.T, rcv *receiver) e2e {
	t.Helper()
	kafkaCfg := testenv.Kafka(t)
	connector, test := testenv.Temporal(t)
	testenv.Endpoint(t, connector, receiverEndpoint, testenv.TestNamespace, receiverTaskQueue)
	rcv.failed = map[string]bool{}
	w := worker.New(test, receiverTaskQueue, worker.Options{})
	w.RegisterNexusService(rcv.service())
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	starter, err := nexussend.NewNexus(connector, receiverEndpoint, kafkaapi.KafkaMessageReceiver.ServiceName,
		kafkaapi.KafkaMessageReceiver.Receive.Name(), time.Hour, false)
	require.NoError(t, err)
	waiter, err := nexussend.NewNexus(connector, receiverEndpoint, kafkaapi.KafkaMessageReceiver.ServiceName,
		kafkaapi.KafkaMessageReceiver.Receive.Name(), time.Hour, true)
	require.NoError(t, err)
	return e2e{kafkaCfg: kafkaCfg, starter: starter, waiter: waiter, receiver: rcv, connector: connector, test: test}
}

// The Inbound Connector delivers the messages to a service in another namespace, through the
// endpoint.
func TestEndToEnd_InboundDeliversIntoAnotherNamespace(t *testing.T) {
	env := startE2E(t, &receiver{})
	produce(t, env.kafkaCfg, 0, "m0", "m1")
	produce(t, env.kafkaCfg, 2, "m2")
	startInbound(t, env.kafkaCfg, "g", env.starter, bigEnough)
	env.receiver.waitFor(t, 3)
	require.ElementsMatch(t, []string{"m0", "m1", "m2"}, env.receiver.values())
}

// A batch delivered again matches the completed operation and starts nothing new.
func TestEndToEnd_RedeliveryStartsNothing(t *testing.T) {
	env := startE2E(t, &receiver{})
	input := kafkaapi.ReceiveInput{Records: []kafkaapi.Record{{Topic: "events", Partition: 0, Offset: 7, Timestamp: "2026-10-22T00:00:00Z", Value: []byte("once")}}}
	id := operationID("g", input.Records)
	require.NoError(t, env.starter.Send(context.Background(), nexussend.Operation{ID: id, Input: input, Messages: len(input.Records)}))
	env.receiver.waitFor(t, 1)
	require.NoError(t, env.starter.Send(context.Background(), nexussend.Operation{ID: id, Input: input, Messages: len(input.Records)}))
	require.Never(t, func() bool { return len(env.receiver.values()) > 1 }, time.Second, 50*time.Millisecond)
}

// A batch whose operation failed is delivered again when it is redelivered.
func TestEndToEnd_FailedOperationRunsAgainOnRedelivery(t *testing.T) {
	env := startE2E(t, &receiver{failFirst: func(v string) bool { return v == "retry-me" }})
	input := kafkaapi.ReceiveInput{Records: []kafkaapi.Record{{Topic: "events", Partition: 0, Offset: 8, Timestamp: "2026-10-22T00:00:00Z", Value: []byte("retry-me")}}}
	id := operationID("g", input.Records)
	require.NoError(t, env.starter.Send(context.Background(), nexussend.Operation{ID: id, Input: input, Messages: len(input.Records)}))
	env.receiver.waitFor(t, 1)
	// While the failed operation is still closing, a start attaches to it; once it has closed, a
	// start runs it again.
	require.Eventually(t, func() bool {
		return env.starter.Send(context.Background(), nexussend.Operation{ID: id, Input: input, Messages: len(input.Records)}) == nil && len(env.receiver.values()) >= 2
	}, 30*time.Second, 200*time.Millisecond)
	require.Equal(t, []string{"retry-me", "retry-me"}, env.receiver.values())
}

// The server refuses an input over its size limit, and the sender reports it as too large,
// so it goes to the dead-letter topic rather than being retried.
func TestEndToEnd_ServerRefusalIsUndeliverable(t *testing.T) {
	env := startE2E(t, &receiver{})
	input := kafkaapi.ReceiveInput{Records: []kafkaapi.Record{{Topic: "events", Timestamp: "2026-10-22T00:00:00Z", Value: []byte(strings.Repeat("x", 1700*1024))}}}
	err := env.starter.Send(context.Background(), nexussend.Operation{ID: operationID("g", input.Records), Input: input, Messages: len(input.Records)})
	require.ErrorIs(t, err, nexussend.ErrTooLarge, "%v", err)
}

// In ordered mode, sending returns only once the receiver's operation has completed, so the next
// batch for the partition can't start before it.
func TestEndToEnd_OrderedWaitsForTheOperationToComplete(t *testing.T) {
	env := startE2E(t, &receiver{delay: time.Second})
	for i, v := range []string{"first", "second"} {
		input := kafkaapi.ReceiveInput{Records: []kafkaapi.Record{{Topic: "events", Offset: int64(i), Timestamp: "2026-10-22T00:00:00Z", Value: []byte(v)}}}
		require.NoError(t, env.waiter.Send(context.Background(), nexussend.Operation{ID: operationID("ordered", input.Records), Input: input, Messages: 1}))
		require.Contains(t, env.receiver.values(), v, "Send returned before the operation completed")
	}
	require.Equal(t, []string{"first", "second"}, env.receiver.values())
}

// attempts counts each run of the receiving workflow by its first message.
var attempts sync.Map

// flakyReceiverWorkflow fails its first run with a retryable error, unless its message is
// "bad", which always fails with a non-retryable one.
func flakyReceiverWorkflow(_ workflow.Context, input kafkaapi.ReceiveInput) (kafkaapi.ReceiveOutput, error) {
	v := string(input.Records[0].Value)
	n, _ := attempts.LoadOrStore(v, new(atomic.Int32))
	if v == "bad" {
		n.(*atomic.Int32).Add(1)
		return kafkaapi.ReceiveOutput{}, temporal.NewNonRetryableApplicationError("bad message", "Bad", nil)
	}
	if n.(*atomic.Int32).Add(1) == 1 {
		return kafkaapi.ReceiveOutput{}, temporal.NewApplicationError("try again later", "Transient")
	}
	return kafkaapi.ReceiveOutput{}, nil
}

// startWorkflowReceiver serves the receiver with flakyReceiverWorkflow behind its own endpoint,
// and returns an ordered sender that retries as the Inbound Connector does.
func startWorkflowReceiver(t *testing.T, env e2e) nexussend.Sender {
	t.Helper()
	const endpoint, taskQueue = "kafka-workflow-receiver-endpoint", "kafka-workflow-receiver"
	testenv.Endpoint(t, env.connector, endpoint, testenv.TestNamespace, taskQueue)
	op := temporalnexus.NewWorkflowRunOperation(kafkaapi.KafkaMessageReceiver.Receive.Name(), flakyReceiverWorkflow,
		func(_ context.Context, _ kafkaapi.ReceiveInput, o nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
			return client.StartWorkflowOptions{ID: o.RequestID}, nil
		})
	svc := nexus.NewService(kafkaapi.KafkaMessageReceiver.ServiceName)
	svc.MustRegister(op)
	w := worker.New(env.test, taskQueue, worker.Options{})
	w.RegisterNexusService(svc)
	w.RegisterWorkflow(flakyReceiverWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)
	waiter, err := nexussend.NewNexus(env.connector, endpoint, kafkaapi.KafkaMessageReceiver.ServiceName,
		kafkaapi.KafkaMessageReceiver.Receive.Name(), time.Hour, true)
	require.NoError(t, err)
	return nexussend.NewRetrying(waiter, func(int) time.Duration { return 100 * time.Millisecond }, slog.Default())
}

// In ordered mode, a receiver that fails with a retryable error is run again until it succeeds;
// one that fails with a non-retryable error is final, so its batch goes to the dead-letter topic.
func TestEndToEnd_OrderedRetriesOnlyRetryableFailures(t *testing.T) {
	env := startE2E(t, &receiver{})
	sender := startWorkflowReceiver(t, env)
	send := func(v string) error {
		input := kafkaapi.ReceiveInput{Records: []kafkaapi.Record{{Topic: "events", Timestamp: "2026-10-22T00:00:00Z", Value: []byte(v)}}}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		return sender.Send(ctx, nexussend.Operation{ID: "ordered-" + v, Input: input, Messages: 1})
	}

	require.NoError(t, send("flaky"))
	n, _ := attempts.Load("flaky")
	require.Equal(t, int32(2), n.(*atomic.Int32).Load(), "the retryable failure ran again")

	err := send("bad")
	require.ErrorIs(t, err, nexussend.ErrFailed)
	n, _ = attempts.Load("bad")
	require.Equal(t, int32(1), n.(*atomic.Int32).Load(), "a non-retryable failure isn't run again")
}

// With encryption, what the Inbound Connector sends is stored in Temporal only encrypted, and a
// receiver with the same key reads the messages.
func TestEndToEnd_EncryptedDelivery(t *testing.T) {
	connector, test := testenv.Temporal(t)
	const endpoint, taskQueue = "kafka-encrypted-receiver-endpoint", "kafka-encrypted-receiver"
	testenv.Endpoint(t, connector, endpoint, testenv.TestNamespace, taskQueue)
	rcv := &receiver{failed: map[string]bool{}}
	w := worker.New(codectest.WithDemoCodec(t, test, testenv.TestNamespace), taskQueue, worker.Options{})
	w.RegisterNexusService(rcv.service())
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	// The Inbound Connector's client, with the codec server set as it would be in its environment.
	// Built as the program builds them (temporalclient.Options), so the test fails if that stops
	// applying the codec. The namespace is the default, testenv.ConnectorNamespace.
	opts, err := temporalclient.Options(codectest.Server(t))
	require.NoError(t, err)
	encrypted, err := client.NewClientFromExisting(connector, opts)
	require.NoError(t, err)
	t.Cleanup(encrypted.Close)
	sender, err := nexussend.NewNexus(encrypted, endpoint, kafkaapi.KafkaMessageReceiver.ServiceName,
		kafkaapi.KafkaMessageReceiver.Receive.Name(), time.Hour, true)
	require.NoError(t, err)

	input := kafkaapi.ReceiveInput{Records: []kafkaapi.Record{{Topic: "events", Timestamp: "2026-10-22T00:00:00Z", Value: []byte("secret-2")}}}
	id := operationID("encrypted", input.Records)
	require.NoError(t, sender.Send(context.Background(), nexussend.Operation{ID: id, Input: input, Messages: 1, Summary: "Kafka events/0/0-0"}))
	require.Equal(t, []string{"secret-2"}, rcv.values())

	resp, err := connector.WorkflowService().DescribeNexusOperationExecution(context.Background(),
		&workflowservice.DescribeNexusOperationExecutionRequest{Namespace: testenv.ConnectorNamespace, OperationId: id, IncludeInput: true})
	require.NoError(t, err)
	codectest.RequireEncrypted(t, resp.GetInput(), "secret-2")
	codectest.RequireEncrypted(t, resp.GetInfo().GetUserMetadata().GetSummary(), "events")
}
