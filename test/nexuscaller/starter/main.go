// Command starter starts the test caller's workflows. For testing only.
//
//	go run ./test/nexuscaller/starter read -group demo
//	go run ./test/nexuscaller/starter write "first message" "second message"
//
// read starts ReaderWorkflow for a group, with the workflow ID kafka-reader-<group>, so a second
// reader of the same group can't start while one is running. It returns once the workflow has
// started; the workflow runs until it is terminated. write runs WriterWorkflow and waits for it.
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
		fmt.Fprintln(os.Stderr, "usage: starter read -group <group> [-batch-size n] [-time-limit seconds]")
		fmt.Fprintln(os.Stderr, "       starter write <message>...")
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	if len(args) == 0 || (args[0] != "read" && args[0] != "write") {
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
	if args[0] == "read" {
		return read(c, args[1:])
	}
	return write(c, args[1:])
}

func read(c client.Client, args []string) error {
	flags := flag.NewFlagSet("read", flag.ContinueOnError)
	group := flags.String("group", "", "consumer group to read for (required)")
	batchSize := flags.Int64("batch-size", 10, "most messages per batch")
	timeLimit := flags.Int64("time-limit", 30, "seconds a read waits for its batch to fill")
	if err := flags.Parse(args); err != nil || *group == "" {
		return errUsage
	}
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        "kafka-reader-" + *group,
		TaskQueue: nexuscaller.TaskQueue,
		// One reader per group: fail rather than return the reader already running.
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, nexuscaller.ReaderWorkflow, nexuscaller.ReaderInput{Group: *group, BatchSize: *batchSize, TimeLimitSeconds: *timeLimit})
	if err != nil {
		return fmt.Errorf("start the reader: %w", err)
	}
	log.Printf("Started reader workflow %s for group %q", run.GetID(), *group)
	return nil
}

func write(c client.Client, messages []string) error {
	if len(messages) == 0 {
		return errUsage
	}
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		TaskQueue: nexuscaller.TaskQueue,
	}, nexuscaller.WriterWorkflow, messages)
	if err != nil {
		return fmt.Errorf("start the writer: %w", err)
	}
	var written int64
	if err := run.Get(context.Background(), &written); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}
	log.Printf("Wrote %d message(s)", written)
	return nil
}
