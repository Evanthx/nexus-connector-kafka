package main

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/test/testenv"
	"go.temporal.io/sdk/worker"
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
	kafkaCfg kafka.Config
	starter  *nexusStarter
	receiver *receiver
}

// startE2E runs a receiver in the test namespace, behind an endpoint, and returns a starter in the
// connector namespace that delivers to it.
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

	starter, err := newNexusStarter(connector, config{
		endpoint:         receiverEndpoint,
		service:          kafkaapi.KafkaMessageReceiver.ServiceName,
		operation:        kafkaapi.KafkaMessageReceiver.Receive.Name(),
		operationTimeout: time.Hour,
	})
	require.NoError(t, err)
	return e2e{kafkaCfg: kafkaCfg, starter: starter, receiver: rcv}
}

// The streamer delivers the messages to a service in another namespace, through the endpoint.
func TestEndToEnd_StreamsIntoAnotherNamespace(t *testing.T) {
	env := startE2E(t, &receiver{})
	produce(t, env.kafkaCfg, 0, "m0", "m1")
	produce(t, env.kafkaCfg, 2, "m2")
	stream(t, env.kafkaCfg, "g", env.starter, bigEnough)
	env.receiver.waitFor(t, 3)
	require.ElementsMatch(t, []string{"m0", "m1", "m2"}, env.receiver.values())
}

// A batch delivered again matches the completed operation and starts nothing new.
func TestEndToEnd_RedeliveryStartsNothing(t *testing.T) {
	env := startE2E(t, &receiver{})
	input := kafkaapi.ReceiveInput{Records: []kafkaapi.Record{{Topic: "events", Partition: 0, Offset: 7, Timestamp: "2026-10-22T00:00:00Z", Value: []byte("once")}}}
	id := operationID("g", input.Records)
	require.NoError(t, env.starter.start(context.Background(), id, input))
	env.receiver.waitFor(t, 1)
	require.NoError(t, env.starter.start(context.Background(), id, input))
	require.Never(t, func() bool { return len(env.receiver.values()) > 1 }, time.Second, 50*time.Millisecond)
}

// A batch whose operation failed is delivered again when it is redelivered.
func TestEndToEnd_FailedOperationRunsAgainOnRedelivery(t *testing.T) {
	env := startE2E(t, &receiver{failFirst: func(v string) bool { return v == "retry-me" }})
	input := kafkaapi.ReceiveInput{Records: []kafkaapi.Record{{Topic: "events", Partition: 0, Offset: 8, Timestamp: "2026-10-22T00:00:00Z", Value: []byte("retry-me")}}}
	id := operationID("g", input.Records)
	require.NoError(t, env.starter.start(context.Background(), id, input))
	env.receiver.waitFor(t, 1)
	// While the failed operation is still closing, a start attaches to it; once it has closed, a
	// start runs it again.
	require.Eventually(t, func() bool {
		return env.starter.start(context.Background(), id, input) == nil && len(env.receiver.values()) >= 2
	}, 30*time.Second, 200*time.Millisecond)
	require.Equal(t, []string{"retry-me", "retry-me"}, env.receiver.values())
}

// The server refuses an input over its size limit, and the starter reports it as undeliverable,
// so it goes to the dead-letter topic rather than being retried.
func TestEndToEnd_ServerRefusalIsUndeliverable(t *testing.T) {
	env := startE2E(t, &receiver{})
	input := kafkaapi.ReceiveInput{Records: []kafkaapi.Record{{Topic: "events", Timestamp: "2026-10-22T00:00:00Z", Value: []byte(strings.Repeat("x", 1700*1024))}}}
	err := env.starter.start(context.Background(), operationID("g", input.Records), input)
	require.ErrorIs(t, err, errUndeliverable, "%v", err)
}
