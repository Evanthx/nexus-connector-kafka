// Package testenv is for tests only. It starts what the end-to-end tests need: an in-memory Kafka
// and a Temporal dev server with the connector's and the test programs' namespaces.
package testenv

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/twmb/franz-go/pkg/kfake"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

const (
	// ConnectorNamespace is where the streamer and the nexusworker run.
	ConnectorNamespace = "kafka-connector"
	// TestNamespace is where the test programs run.
	TestNamespace = "kafka-test"
	// Topic is the topic Kafka starts with, with three partitions.
	Topic = "events"
	// DeadLetterTopic is a second topic, for the streamer's dead letters.
	DeadLetterTopic = "events-dead-letters"
)

// Kafka starts an in-memory Kafka with Topic and DeadLetterTopic and returns the settings to reach
// it.
func Kafka(t *testing.T) kafka.Config {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, Topic), kfake.SeedTopics(1, DeadLetterTopic))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return kafka.Config{Brokers: c.ListenAddrs(), Topic: Topic, StartFrom: kafka.StartFromOldest}
}

// Temporal starts a dev server with both namespaces and the dynamic config the connector needs,
// and returns a client for each namespace. It skips the test if the temporal CLI isn't on the
// PATH, or with -short.
func Temporal(t *testing.T) (connector, test client.Client) {
	t.Helper()
	if testing.Short() {
		t.Skip("starts a dev server")
	}
	cli, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not on the PATH")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath:  cli,
		ClientOptions: &client.Options{Namespace: ConnectorNamespace},
		ExtraArgs: []string{
			"--namespace", TestNamespace,
			// The nexusworker's operations complete through standalone activities' callbacks.
			"--dynamic-config-value", "activity.enableCallbacks=true",
			// The streamer starts standalone Nexus operations.
			"--dynamic-config-value", "nexusoperation.enableStandalone=true",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Stop() })

	test, err = client.Dial(client.Options{HostPort: server.FrontendHostPort(), Namespace: TestNamespace})
	require.NoError(t, err)
	t.Cleanup(test.Close)
	return server.Client(), test
}

// Endpoint creates a Nexus endpoint that routes to taskQueue in namespace.
func Endpoint(t *testing.T, c client.Client, name, namespace, taskQueue string) {
	t.Helper()
	_, err := c.OperatorService().CreateNexusEndpoint(context.Background(), &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: name,
			Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{
				Namespace: namespace,
				TaskQueue: taskQueue,
			}}},
		},
	})
	require.NoError(t, err)
}
