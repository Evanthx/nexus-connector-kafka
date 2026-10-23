// Command starter starts the test caller's workflows. For testing only.
//
//	go run ./test/nexuscaller/starter read -group demo
//	go run ./test/nexuscaller/starter write "first message" "second message"
//
// read starts ReaderWorkflow for a group, with the workflow ID kafka-reader-<group>, so a second
// reader of the same group can't start while one is running. It returns once the workflow has
// started; the workflow runs until it is terminated. write runs WriterWorkflow and waits for it.
//
// The starter only starts workflows. It doesn't read or loop itself: the reading loop is in
// ReaderWorkflow (test/nexuscaller/workflows.go), which runs on the worker in
// test/nexuscaller/worker. Start that worker first, or the workflow waits in its task queue.
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

// read starts a ReaderWorkflow. Its flags become the ReaderInput, and from there every read the
// workflow makes:
//
//   - -group (required): the Kafka consumer group to read for. The group's committed offsets are
//     where reading resumes, so a reader stopped and started again carries on where it left off.
//     A new group starts where the nexusworker's KAFKA_START_FROM says (oldest by default).
//   - -batch-size (default 10 here): the most messages one read returns. The read operation
//     itself defaults to 100 and allows up to 10,000, and a batch also stops at about 2 MiB
//     encoded (the nexusworker's KAFKA_MAX_BATCH_BYTES), Temporal's payload limit.
//   - -time-limit (default 60 seconds): how long a read waits for its batch to fill before
//     returning what it has. Reads are long polls, which is far cheaper on an idle topic than
//     reading often, and at 60 seconds or less a read sends no heartbeats. The operation allows
//     up to 600; 0 here means the default.
func read(c client.Client, args []string) error {
	flags := flag.NewFlagSet("read", flag.ContinueOnError)
	group := flags.String("group", "", "consumer group to read for (required)")
	batchSize := flags.Int64("batch-size", 10, "most messages per batch")
	timeLimit := flags.Int64("time-limit", nexuscaller.DefaultTimeLimitSeconds, "seconds a read waits for its batch to fill")
	if err := flags.Parse(args); err != nil || *group == "" {
		return errUsage
	}
	// ExecuteWorkflow returns once the workflow has started; it doesn't wait for it to finish,
	// and a reader never finishes on its own.
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		// The ID names the group, so there is at most one running reader per group: two readers
		// of one group would read the same batches.
		ID: "kafka-reader-" + *group,
		// The task queue the worker in test/nexuscaller/worker polls.
		TaskQueue: nexuscaller.TaskQueue,
		// By default, starting a workflow whose ID is already running returns the running one.
		// Fail instead, so a second reader for the group is reported rather than silently joined.
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, nexuscaller.ReaderWorkflow, nexuscaller.ReaderInput{Group: *group, BatchSize: *batchSize, TimeLimitSeconds: *timeLimit})
	if err != nil {
		return fmt.Errorf("start the reader: %w", err)
	}
	log.Printf("Started reader workflow %s for group %q", run.GetID(), *group)
	return nil
}

// write runs a WriterWorkflow that writes each argument as one message, in order, and waits for
// it. The workflow calls the nexusworker's write operation, which adds a dedupe-id header to each
// message so a consumer can drop a copy left by a retried write.
func write(c client.Client, messages []string) error {
	if len(messages) == 0 {
		return errUsage
	}
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		// No ID: the SDK generates one, so each write is its own workflow.
		TaskQueue: nexuscaller.TaskQueue,
	}, nexuscaller.WriterWorkflow, messages)
	if err != nil {
		return fmt.Errorf("start the writer: %w", err)
	}
	// Unlike read, wait for the workflow's result: how many messages Kafka acknowledged.
	var written int64
	if err := run.Get(context.Background(), &written); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}
	log.Printf("Wrote %d message(s)", written)
	return nil
}
