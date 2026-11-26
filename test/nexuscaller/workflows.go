// Package nexuscaller is for testing only. Its workflow, WriterWorkflow, runs in its own namespace
// and calls the Outbound Connector's KafkaService through a Nexus endpoint, the way a real caller
// would, to write messages.
//
// Each write names its topic, which must be one the Outbound Connector behind the endpoint allows.
// The walkthrough in docs/testing.md runs one Outbound Connector allowing two topics, customers and
// orders, behind Endpoint.
package nexuscaller

import (
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"go.temporal.io/sdk/workflow"
)

const (
	// Namespace is the test programs' default namespace.
	Namespace = "kafka-test"
	// TaskQueue is the test caller worker's task queue.
	TaskQueue = "kafka-nexuscaller"
	// Endpoint routes to the Outbound Connector, which allows the customers and orders topics.
	Endpoint = "kafka-outbound-endpoint"
)

// WriterInput starts WriterWorkflow.
type WriterInput struct {
	// Endpoint is the Nexus endpoint to write through.
	Endpoint string
	// Topic is the topic to write to: one the endpoint's Outbound Connector allows.
	Topic string
	// Messages are the messages to write, in order.
	Messages []string
}

// WriterWorkflow writes messages to in.Topic through in.Endpoint and returns how many were
// written. It runs once and finishes: one write operation for all the messages, in order.
func WriterWorkflow(ctx workflow.Context, in WriterInput) (int64, error) {
	kafkaService := workflow.NewNexusClient(in.Endpoint, kafkaapi.KafkaService.ServiceName)
	write := kafkaapi.WriteInput{Topic: in.Topic}
	for _, m := range in.Messages {
		write.Messages = append(write.Messages, kafkaapi.Message{Value: []byte(m)})
	}
	var out kafkaapi.WriteOutput
	err := kafkaService.ExecuteOperation(ctx, kafkaapi.KafkaService.Write, write, workflow.NexusOperationOptions{
		// A failed write is retried by this workflow's Temporal server until this timeout, and
		// nothing else stops the retries, so always set one. If it passes, the call fails with a
		// timeout error; the last attempt's failure, on the pending Nexus operation, has the reason.
		ScheduleToCloseTimeout: 15 * time.Minute,
	}).Get(ctx, &out)
	if err != nil {
		return 0, err
	}
	return out.Written, nil
}
