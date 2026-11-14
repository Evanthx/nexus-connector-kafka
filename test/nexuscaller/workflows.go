// Package nexuscaller is for testing only. Its workflow, WriterWorkflow, runs in its own namespace
// and calls the connector's KafkaService through a Nexus endpoint, the way a real caller would, to
// write messages.
//
// Each connector serves one topic, so the endpoint a call goes to decides the topic. The
// walkthrough in the README runs two connectors on two topics, behind CustomersEndpoint and
// OrdersEndpoint.
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
	// CustomersEndpoint routes to a connector serving the customers topic.
	CustomersEndpoint = "kafka-customers-endpoint"
	// OrdersEndpoint routes to a second connector, serving the orders topic.
	OrdersEndpoint = "kafka-orders-endpoint"
)

// WriterInput starts WriterWorkflow.
type WriterInput struct {
	// Endpoint is the Nexus endpoint to write through. Its connector decides the topic.
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
		// The write retries for up to ten minutes (or the connector's
		// KAFKA_MAX_DURATION_MINUTES), and stops 10 seconds before this if that comes first.
		ScheduleToCloseTimeout: 15 * time.Minute,
	}).Get(ctx, &out)
	if err != nil {
		return 0, err
	}
	return out.Written, nil
}
