package nexuscaller

import (
	"context"
	"sync"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// fakeKafka serves read from a list of batches and records each read's consumer group and ack.
type fakeKafka struct {
	mu         sync.Mutex
	batches    []kafkaapi.ReadOutput
	groups     []string
	acks       []string
	timeLimits []int64
	batchSizes []int64
}

func (f *fakeKafka) service() *nexus.Service {
	read := nexus.NewSyncOperation(kafkaapi.KafkaService.Read.Name(), func(_ context.Context, in kafkaapi.ReadInput, _ nexus.StartOperationOptions) (kafkaapi.ReadOutput, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		ack := ""
		if in.Ack != nil {
			ack = *in.Ack
		}
		group := ""
		if in.Group != nil {
			group = *in.Group
		}
		f.groups = append(f.groups, group)
		f.acks = append(f.acks, ack)
		f.timeLimits = append(f.timeLimits, in.TimeLimitSecondsOrDefault())
		f.batchSizes = append(f.batchSizes, in.BatchSizeOrDefault())
		if len(f.batches) == 0 {
			return kafkaapi.ReadOutput{Records: []kafkaapi.Record{}, Position: ack}, nil
		}
		b := f.batches[0]
		f.batches = f.batches[1:]
		return b, nil
	})
	svc := nexus.NewService(kafkaapi.KafkaService.ServiceName)
	svc.MustRegister(read)
	return svc
}

func record(v string) kafkaapi.Record {
	return kafkaapi.Record{Topic: "t", Timestamp: "2026-10-22T00:00:00Z", Value: []byte(v)}
}

// The reader processes every record of each batch, acknowledges each batch on the next read,
// and when it continues as new, passes on the position after the last batch it processed.
func TestReaderWorkflow(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	fake := &fakeKafka{batches: []kafkaapi.ReadOutput{
		{Records: []kafkaapi.Record{record("a"), record("b")}, Position: "p1"},
		{Records: []kafkaapi.Record{record("c")}, Position: "p2"},
	}}
	env.RegisterNexusService(fake.service())

	var mu sync.Mutex
	var processed []string
	env.RegisterActivityWithOptions(func(_ context.Context, rec kafkaapi.Record) error {
		mu.Lock()
		defer mu.Unlock()
		processed = append(processed, string(rec.Value))
		return nil
	}, activity.RegisterOptions{Name: "ProcessRecord"})

	reads := 0
	original := shouldContinueAsNew
	shouldContinueAsNew = func(workflow.Context) bool { reads++; return reads == 2 }
	t.Cleanup(func() { shouldContinueAsNew = original })

	env.ExecuteWorkflow(ReaderWorkflow, ReaderInput{
		Sources:   []Source{{Endpoint: CustomersEndpoint, Group: "g", Position: "p0"}},
		BatchSize: 10, TimeLimitSeconds: 30,
	})

	require.True(t, env.IsWorkflowCompleted())
	var can *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &can)
	var next ReaderInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &next))
	require.Equal(t, ReaderInput{
		Sources:   []Source{{Endpoint: CustomersEndpoint, Group: "g", Position: "p2"}},
		BatchSize: 10, TimeLimitSeconds: 30,
	}, next)

	require.Equal(t, []string{"p0", "p1"}, fake.acks)
	require.ElementsMatch(t, []string{"a", "b", "c"}, processed)
}

// Without a time limit, the reader long-polls with DefaultTimeLimitSeconds rather than reading as
// fast as it can. Without a batch size, it leaves the read's own default (100) to apply, rather
// than asking for batches of 0, which would only acknowledge.
func TestReaderWorkflow_Defaults(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	fake := &fakeKafka{}
	env.RegisterNexusService(fake.service())
	env.RegisterActivity(ProcessRecord)

	original := shouldContinueAsNew
	shouldContinueAsNew = func(workflow.Context) bool { return true }
	t.Cleanup(func() { shouldContinueAsNew = original })

	env.ExecuteWorkflow(ReaderWorkflow, ReaderInput{Sources: []Source{{Endpoint: CustomersEndpoint, Group: "g"}}})
	var can *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &can)
	require.Equal(t, []int64{DefaultTimeLimitSeconds}, fake.timeLimits)
	require.Equal(t, []int64{100}, fake.batchSizes)
}

// With two sources, each pass reads a batch from one, then a batch from the other, each with its
// own consumer group and ack, and continue-as-new carries both positions. A source with no
// consumer group leaves it out, for the endpoint's own to apply.
func TestReaderWorkflow_ReadsEachSourceInTurn(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	fake := &fakeKafka{batches: []kafkaapi.ReadOutput{
		{Records: []kafkaapi.Record{record("event")}, Position: "customers-1"},
		{Records: []kafkaapi.Record{record("order")}, Position: "orders-1"},
	}}
	env.RegisterNexusService(fake.service())
	var mu sync.Mutex
	var processed []string
	env.RegisterActivityWithOptions(func(_ context.Context, rec kafkaapi.Record) error {
		mu.Lock()
		defer mu.Unlock()
		processed = append(processed, string(rec.Value))
		return nil
	}, activity.RegisterOptions{Name: "ProcessRecord"})

	original := shouldContinueAsNew
	shouldContinueAsNew = func(workflow.Context) bool { return true }
	t.Cleanup(func() { shouldContinueAsNew = original })

	env.ExecuteWorkflow(ReaderWorkflow, ReaderInput{Sources: []Source{
		{Endpoint: CustomersEndpoint, Group: "reader1", Position: "customers-0"},
		{Endpoint: OrdersEndpoint, Position: "orders-0"},
	}})
	var can *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &can)
	var next ReaderInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &next))
	require.Equal(t, []Source{
		{Endpoint: CustomersEndpoint, Group: "reader1", Position: "customers-1"},
		{Endpoint: OrdersEndpoint, Position: "orders-1"},
	}, next.Sources)

	require.Equal(t, []string{"reader1", ""}, fake.groups)
	require.Equal(t, []string{"customers-0", "orders-0"}, fake.acks)
	require.Equal(t, []string{"event", "order"}, processed)
}
