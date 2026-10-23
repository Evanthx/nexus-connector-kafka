// Command worker runs the test caller's workflows and the ProcessRecord activity. For testing only.
//
// The starter only starts workflows; this worker runs them. It polls TaskQueue in the kafka-test
// namespace, and each ReaderWorkflow's loop (see ReaderWorkflow in test/nexuscaller/workflows.go)
// runs here, read after read, until the workflow is terminated. If the worker stops, the workflow
// doesn't: it carries on from its history when a worker polls again.
//
// It needs no Kafka settings. Its workflows reach Kafka only through the Nexus endpoint
// (EndpointName), which routes to the nexusworker in another namespace.
package main

import (
	"log"

	"github.com/temporalio/nexus-connector-kafka/test/nexuscaller"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	opts, err := nexuscaller.ClientOptions()
	if err != nil {
		log.Fatalf("Temporal client settings: %v", err)
	}
	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalf("Failed to create Temporal client: %v", err)
	}
	defer c.Close()

	// Default worker options: the SDK's limits on concurrent workflow and activity tasks are
	// plenty for a test caller.
	w := worker.New(c, nexuscaller.TaskQueue, worker.Options{})
	w.RegisterWorkflow(nexuscaller.ReaderWorkflow)
	w.RegisterWorkflow(nexuscaller.WriterWorkflow)
	w.RegisterActivity(nexuscaller.ProcessRecord)
	log.Printf("Starting worker on namespace %q, task queue %q", opts.Namespace, nexuscaller.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker stopped: %v", err)
	}
}
