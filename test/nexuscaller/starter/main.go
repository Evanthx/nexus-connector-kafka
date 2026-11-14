// Command starter starts the test caller's workflow. For testing only.
//
//	go run ./test/nexuscaller/starter write "first message" "second message"
//	go run ./test/nexuscaller/starter write -endpoint kafka-orders-endpoint "an order"
//
// write runs WriterWorkflow and waits for it. The starter only starts the workflow: it runs on the
// worker in test/nexuscaller/worker. Start that worker first, or the workflow waits in its task
// queue.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/temporalio/nexus-connector-kafka/test/nexuscaller"
	"go.temporal.io/sdk/client"
)

// errUsage means the command line is wrong; main prints how to use the starter.
var errUsage = errors.New("usage")

func main() {
	err := run(os.Args[1:])
	if errors.Is(err, errUsage) {
		fmt.Fprintln(os.Stderr, "usage: starter write [-endpoint endpoint] <message>...")
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	if len(args) == 0 || args[0] != "write" {
		return errUsage
	}
	opts, err := nexuscaller.ClientOptions()
	if err != nil {
		return fmt.Errorf("temporal client settings: %w", err)
	}
	c, err := client.Dial(opts)
	if err != nil {
		return fmt.Errorf("create Temporal client: %w", err)
	}
	defer c.Close()
	return write(c, args[1:])
}

// write runs a WriterWorkflow that writes each argument as one message, in order, and waits for
// it. -endpoint (default kafka-customers-endpoint) chooses the Nexus endpoint, and so the topic. The
// workflow calls that connector's write operation, which adds a dedupe-id header to each
// message so a consumer can drop a copy left by a retried write.
func write(c client.Client, args []string) error {
	flags := flag.NewFlagSet("write", flag.ContinueOnError)
	endpoint := flags.String("endpoint", nexuscaller.CustomersEndpoint, "the Nexus endpoint to write through")
	if err := flags.Parse(args); err != nil || flags.NArg() == 0 {
		return errUsage
	}
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		// No ID: the SDK generates one, so each write is its own workflow.
		TaskQueue: nexuscaller.TaskQueue,
	}, nexuscaller.WriterWorkflow, nexuscaller.WriterInput{Endpoint: *endpoint, Messages: flags.Args()})
	if err != nil {
		return fmt.Errorf("start the writer: %w", err)
	}
	// Wait for the workflow's result: how many messages Kafka acknowledged.
	var written int64
	if err := run.Get(context.Background(), &written); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}
	log.Printf("Wrote %d message(s) through %s", written, *endpoint)
	return nil
}
