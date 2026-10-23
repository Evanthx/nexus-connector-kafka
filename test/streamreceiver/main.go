// Command streamreceiver receives the streamer's messages. For testing only.
//
// It serves the KafkaMessageReceiver Nexus service. Each receive starts ReceiveWorkflow, which
// logs the batch's messages. It connects with the standard TEMPORAL_* variables and the temporal CLI's
// profile file; the namespace defaults to kafka-test.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	defaultNamespace = "kafka-test"
	// taskQueue is the receiver's task queue. The streamer's Nexus endpoint targets it.
	taskQueue = "kafka-receiver"
)

// ReceiveWorkflow stands in for real work on a batch: it logs each message.
func ReceiveWorkflow(ctx workflow.Context, input kafkaapi.ReceiveInput) (kafkaapi.ReceiveOutput, error) {
	for _, rec := range input.Records {
		workflow.GetLogger(ctx).Info("Received message",
			"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "value", string(rec.Value))
	}
	return kafkaapi.ReceiveOutput{}, nil
}

// receiveOperation starts ReceiveWorkflow for each batch. The workflow ID names the batch's
// partition and offsets.
func receiveOperation() nexus.Operation[kafkaapi.ReceiveInput, kafkaapi.ReceiveOutput] {
	return temporalnexus.NewWorkflowRunOperation(kafkaapi.KafkaMessageReceiver.Receive.Name(), ReceiveWorkflow,
		func(_ context.Context, input kafkaapi.ReceiveInput, _ nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
			first, last := input.Records[0], input.Records[len(input.Records)-1]
			return client.StartWorkflowOptions{ID: fmt.Sprintf("kafka-batch-%s-%d-%d-%d", first.Topic, first.Partition, first.Offset, last.Offset)}, nil
		})
}

func main() {
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		log.Fatalf("Temporal client settings: %v", err)
	}
	if opts.Namespace == "" {
		opts.Namespace = defaultNamespace
	}
	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalf("Failed to create Temporal client: %v", err)
	}
	defer c.Close()

	svc := nexus.NewService(kafkaapi.KafkaMessageReceiver.ServiceName)
	svc.MustRegister(receiveOperation())
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterNexusService(svc)
	w.RegisterWorkflow(ReceiveWorkflow)
	log.Printf("Receiving on namespace %q, task queue %q", opts.Namespace, taskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker stopped: %v", err)
	}
}
