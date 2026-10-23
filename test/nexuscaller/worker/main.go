// Command worker runs the test caller's workflows and the ProcessRecord activity. For testing only.
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

	w := worker.New(c, nexuscaller.TaskQueue, worker.Options{})
	w.RegisterWorkflow(nexuscaller.ReaderWorkflow)
	w.RegisterWorkflow(nexuscaller.WriterWorkflow)
	w.RegisterActivity(nexuscaller.ProcessRecord)
	log.Printf("Starting worker on namespace %q, task queue %q", opts.Namespace, nexuscaller.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker stopped: %v", err)
	}
}
