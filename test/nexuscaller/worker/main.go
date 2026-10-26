// Command worker runs the test caller's workflows and the ProcessRecord activity. For testing only.
//
// The starter only starts workflows; this worker runs them. It polls TaskQueue in the kafka-test
// namespace, and each ReaderWorkflow's loop (see ReaderWorkflow in test/nexuscaller/workflows.go)
// runs here, read after read, until the workflow is terminated. If the worker stops, the workflow
// doesn't: it carries on from its history when a worker polls again.
//
// It needs no Kafka settings. Its workflows reach Kafka only through the Nexus endpoints
// (CustomersEndpoint and OrdersEndpoint), which route to nexusworkers in another namespace.
package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/temporalio/nexus-connector-kafka/test/nexuscaller"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
)

func main() {
	opts, err := nexuscaller.ClientOptions()
	if err != nil {
		log.Fatalf("Temporal client settings: %v", err)
	}
	opts.Logger = demoLogger()
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
	log.Print("Waiting for workflows to run. Nothing more is logged until one starts: start one with " +
		"`go run ./test/nexuscaller/starter read` or `write`, and each read and message processed is logged here")
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker stopped: %v", err)
	}
}

// noisyKeys are the keys the SDK adds to every workflow and activity log line. Dropping them, and
// debug lines, leaves the demo's own lines short enough to follow.
var noisyKeys = map[string]bool{
	"Namespace": true, "TaskQueue": true, "WorkerID": true, "WorkflowType": true, "WorkflowID": true,
	"RunID": true, "Attempt": true, "ActivityID": true, "ActivityType": true,
}

// demoLogger logs at info level and above, as "15:04:05 INFO message key=value ...", without
// noisyKeys.
func demoLogger() sdklog.Logger {
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch {
			case a.Key == slog.TimeKey && len(groups) == 0:
				return slog.String(slog.TimeKey, a.Value.Time().Format("15:04:05"))
			case noisyKeys[a.Key]:
				return slog.Attr{} // an empty Attr is left out
			}
			return a
		},
	})
	return sdklog.NewStructuredLogger(slog.New(handler))
}
