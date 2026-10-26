// Command starter starts the test caller's workflows. For testing only.
//
//	go run ./test/nexuscaller/starter read
//	go run ./test/nexuscaller/starter read -source kafka-customers-endpoint=demo -source kafka-orders-endpoint
//	go run ./test/nexuscaller/starter write "first message" "second message"
//	go run ./test/nexuscaller/starter write -endpoint kafka-orders-endpoint "an order"
//
// read starts one ReaderWorkflow that reads from each source in turn. Its workflow ID names the
// sources, so a second reader of the same sources can't start while one is running. It returns
// once the workflow has started; the workflow runs until it is terminated. write runs
// WriterWorkflow and waits for it.
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
	"strings"

	"github.com/temporalio/nexus-connector-kafka/test/nexuscaller"
	"go.temporal.io/sdk/client"
)

// errUsage means the command line is wrong; main prints how to use the starter.
var errUsage = errors.New("usage")

func main() {
	err := run(os.Args[1:])
	if errors.Is(err, errUsage) {
		fmt.Fprintln(os.Stderr, "usage: starter read [-source endpoint[=group]]... [-batch-size n] [-time-limit seconds]")
		fmt.Fprintln(os.Stderr, "       starter write [-endpoint endpoint] <message>...")
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

// sources is the -source flag: each use adds one source, written endpoint=group, or just endpoint
// when the endpoint's Nexus worker has a consumer group of its own.
type sources []nexuscaller.Source

func (s *sources) String() string { return fmt.Sprint(*s) }

func (s *sources) Set(v string) error {
	endpoint, group, _ := strings.Cut(v, "=")
	if endpoint == "" {
		return errors.New("a source needs an endpoint")
	}
	*s = append(*s, nexuscaller.Source{Endpoint: endpoint, Group: group})
	return nil
}

// defaultSources are the README walkthrough's two endpoints: the customers topic, read for the
// consumer group reader1, and the orders topic, whose Nexus worker sets its own consumer group.
var defaultSources = sources{
	{Endpoint: nexuscaller.CustomersEndpoint, Group: "reader1"},
	{Endpoint: nexuscaller.OrdersEndpoint},
}

// read starts a ReaderWorkflow. Its flags become the ReaderInput, and from there every read the
// workflow makes:
//
//   - -source (repeatable; default the walkthrough's two): where to read. Each is a Nexus endpoint,
//     which decides the topic, and a consumer group, written endpoint=group. Leave the group out
//     (just endpoint) when the endpoint's Nexus worker has a consumer group of its own
//     (KAFKA_GROUP); name one when it doesn't. The consumer group's committed offsets are where
//     reading resumes, so a reader stopped and started again carries on where it left off.
//   - -batch-size (default 10 here): the most messages one read returns. The read operation
//     itself defaults to 100 and allows up to 10,000, and a batch also stops at about 2 MiB
//     encoded (the nexusworker's KAFKA_MAX_BATCH_BYTES), Temporal's payload limit.
//   - -time-limit (default 60 seconds): how long a read waits for its batch to fill before
//     returning what it has. Reads are long polls, which is far cheaper on an idle topic than
//     reading often, and at 60 seconds or less a read sends no heartbeats. The operation allows
//     up to 600; 0 here means the default.
func read(c client.Client, args []string) error {
	flags := flag.NewFlagSet("read", flag.ContinueOnError)
	var from sources
	flags.Var(&from, "source", "endpoint=group, or endpoint alone if its Nexus worker sets the consumer group (repeatable)")
	batchSize := flags.Int64("batch-size", 10, "most messages per batch")
	timeLimit := flags.Int64("time-limit", nexuscaller.DefaultTimeLimitSeconds, "seconds a read waits for its batch to fill")
	if err := flags.Parse(args); err != nil {
		return errUsage
	}
	if len(from) == 0 {
		from = defaultSources
	}
	// ExecuteWorkflow returns once the workflow has started; it doesn't wait for it to finish,
	// and a reader never finishes on its own.
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		// The ID names the sources, so there is at most one running reader of them: two readers
		// of one consumer group would read the same batches.
		ID: readerID(from),
		// The task queue the worker in test/nexuscaller/worker polls.
		TaskQueue: nexuscaller.TaskQueue,
		// By default, starting a workflow whose ID is already running returns the running one.
		// Fail instead, so a second reader of the same sources is reported rather than silently
		// joined.
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, nexuscaller.ReaderWorkflow, nexuscaller.ReaderInput{Sources: from, BatchSize: *batchSize, TimeLimitSeconds: *timeLimit})
	if err != nil {
		return fmt.Errorf("start the reader: %w", err)
	}
	// The starter's work is done: the reading happens in the workflow, on the worker.
	log.Printf("Started reader workflow %s. It keeps running after this command exits: watch the "+
		"test caller's worker log (go run ./test/nexuscaller/worker) for each read and message processed, "+
		"or find the workflow in the Web UI, namespace %s", run.GetID(), nexuscaller.Namespace)
	return nil
}

// readerID is the workflow ID for a reader of from: kafka-reader-, then each source's endpoint
// and consumer group, such as kafka-reader-kafka-customers-endpoint/reader1+kafka-orders-endpoint.
func readerID(from sources) string {
	names := make([]string, len(from))
	for i, s := range from {
		names[i] = s.Endpoint
		if s.Group != "" {
			names[i] += "/" + s.Group
		}
	}
	return "kafka-reader-" + strings.Join(names, "+")
}

// write runs a WriterWorkflow that writes each argument as one message, in order, and waits for
// it. -endpoint (default kafka-customers-endpoint) chooses the Nexus endpoint, and so the topic. The
// workflow calls that Nexus worker's write operation, which adds a dedupe-id header to each
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
	// Unlike read, wait for the workflow's result: how many messages Kafka acknowledged.
	var written int64
	if err := run.Get(context.Background(), &written); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}
	log.Printf("Wrote %d message(s) through %s", written, *endpoint)
	return nil
}
