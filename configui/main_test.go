package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/configui"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	ct "github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/test/configuitest"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

// connectorClientMakers are the client constructors of the library this repo's programs call
// Kafka with, as "import path.Name", which configui must not call itself (see TestNoOwnClient):
// its checks go through the kafka package (Identify, ReadLast).
var connectorClientMakers = []string{
	"github.com/twmb/franz-go/pkg/kgo.NewClient",
	"github.com/twmb/franz-go/pkg/kadm.NewClient",
	"github.com/twmb/franz-go/pkg/kadm.NewOptClient",
}

// newCluster starts an in-memory Kafka with one-partition topics, writes values to the first, and
// returns its brokers.
func newCluster(t *testing.T, topics []string, values ...string) string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topics...))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	require.NoError(t, err)
	defer cl.Close()
	for i, v := range values {
		r := &kgo.Record{Topic: topics[0], Value: []byte(v)}
		if i == len(values)-1 {
			r.Key = []byte("k")
			r.Headers = []kgo.RecordHeader{{Key: "source", Value: []byte("test")}}
		}
		require.NoError(t, cl.ProduceSync(context.Background(), r).FirstErr())
	}
	return strings.Join(c.ListenAddrs(), ",")
}

// The page gets both programs with every variable in their settings lists, in order, and the
// values set now, but never a secret's value.
func TestSettings_JSON(t *testing.T) {
	h := configui.NewServer(pageTitle, ct.BaseEnv(t, settings.Map{
		"KAFKA_TOPIC":      "orders",
		"KAFKA_PASSWORD":   "hunter2_never_sent",
		"TEMPORAL_API_KEY": "temporal_key_never_sent",
		"https_proxy":      "http://user:proxy_pw_never_sent@proxy.example:8080",
	}), programs)
	rec := ct.Get(t, h, "/api/settings")
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotContains(t, rec.Body.String(), "never_sent")

	var page configui.Page
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
	require.Equal(t, pageTitle, page.Title)
	require.Len(t, page.Programs, 2)
	for i, want := range []*settings.Program{setup.Inbound, setup.Outbound} {
		p := page.Programs[i]
		require.Equal(t, want.ID, p.ID)
		require.Equal(t, want.Command, p.Command)
		var names, wantNames []string
		for _, s := range p.Sections {
			for _, v := range s.Variables {
				names = append(names, v.Name)
				require.NotEmpty(t, v.Help, v.Name)
			}
		}
		for _, v := range want.Variables() {
			wantNames = append(wantNames, v.Name)
		}
		require.Equal(t, wantNames, names, "every variable in the settings list, in its order")
		var titles []string
		for _, s := range p.Sections {
			titles = append(titles, s.Title)
		}
		require.Equal(t, []string{"Kafka", want.Sections[1].Title, "Temporal", "Network"}, titles)
		require.Equal(t, "gRPC", p.Sections[3].Variables[0].External)
		require.Equal(t, []string{"https_proxy"}, p.Sections[3].Variables[0].Aliases)
	}
	require.Equal(t, map[string]string{
		"KAFKA_TOPIC":          "orders",
		"TEMPORAL_CONFIG_FILE": filepath.Join(filepath.Dir(page.TemporalProfile.File), "none.toml"),
	}, page.Values)
	require.Equal(t, map[string]bool{
		"KAFKA_PASSWORD": true, "TEMPORAL_API_KEY": true, "TEMPORAL_CODEC_AUTH": false, "TEMPORAL_TLS_CLIENT_KEY_DATA": false,
	}, page.SecretsSet)
	// External variables: read-only, as gRPC reads them, masked, and never among the values sent.
	require.Equal(t, map[string]configui.PageExternal{
		"HTTPS_PROXY": {Set: true, From: "https_proxy", Value: "http://user:****@proxy.example:8080"},
		"NO_PROXY":    {},
	}, page.External)
}

// The Settings check reports exactly what the Inbound Connector would at startup, every problem at
// once, and the Kafka check waits for valid Kafka settings.
func TestTest_ReportsEverySettingsProblemAsStartupDoes(t *testing.T) {
	programs := ct.WithTemporalTimeout(programs, time.Second)
	values := map[string]string{
		"KAFKA_TOPIC":               "orders",
		"KAFKA_START_FROM":          "yesterday",
		"KAFKA_BATCH_SIZE":          "0",
		"INBOUND_OPERATION_TIMEOUT": "0s",
		"TEMPORAL_ADDRESS":          "127.0.0.1:1",
	}
	_, resp := ct.PostTest(t, configui.NewServer(pageTitle, ct.BaseEnv(t, nil), programs), configui.TestRequest{Program: "inbound", Values: values})
	require.False(t, resp.OK)

	_, startupErr := setup.Inbound.Load(settings.Map(values))
	require.Error(t, startupErr)
	problems := ct.CheckTitled(t, resp, "Settings").Problems
	require.Equal(t, strings.Split(startupErr.Error(), "\n"), problems)
	require.Equal(t, []string{
		"set KAFKA_BROKERS: the Kafka brokers, as comma-separated host:port",
		"set KAFKA_GROUP: the consumer group the Inbound Connector joins",
		`KAFKA_START_FROM must be oldest or newest, not "yesterday"`,
		`KAFKA_BATCH_SIZE must be a whole number, at least 1, not "0"`,
		"set INBOUND_ENDPOINT: the Nexus endpoint to deliver to",
		"INBOUND_OPERATION_TIMEOUT must be more than 0",
	}, problems)
	require.Contains(t, ct.CheckTitled(t, resp, "Temporal").Problems[0], "Failed to connect to Temporal at 127.0.0.1:1")
	require.Equal(t, "Fix the Kafka settings first.", ct.CheckTitled(t, resp, "Kafka").Skipped)
}

// The Test reads the topic's last message with the programs' Kafka code, the same one each time,
// using the page's values in place of those where configui runs.
func TestTest_ReadsTheLastMessage(t *testing.T) {
	programs := ct.WithTemporalTimeout(programs, time.Second)
	brokers := newCluster(t, []string{"orders"}, "first", "last order")
	h := configui.NewServer(pageTitle, ct.BaseEnv(t, settings.Map{"KAFKA_TOPIC": "replaced-by-the-page"}), programs)
	req := configui.TestRequest{Program: "outbound", Values: map[string]string{
		"KAFKA_BROKERS": brokers, "KAFKA_TOPIC": "orders", "KAFKA_PASSWORD": "typed_s3cret", "TEMPORAL_ADDRESS": "127.0.0.1:1",
	}}
	for range 2 {
		raw, resp := ct.PostTest(t, h, req)
		require.NotContains(t, raw, "s3cret")
		k := ct.CheckTitled(t, resp, "Kafka")
		require.Empty(t, k.Problems)
		require.Contains(t, k.Passed[1], "Read the topic's last message: partition 0, offset 1, timestamp ")
		require.Equal(t, []string{"Key: k", "Value: last order", "Header source: test"}, k.Passed[2:])
		require.Contains(t, k.Passed[0], `found the topic "orders"`)
	}
}

// An empty topic is fine: there is just nothing to show.
func TestTest_EmptyTopic(t *testing.T) {
	programs := ct.WithTemporalTimeout(programs, time.Second)
	brokers := newCluster(t, []string{"orders"})
	_, resp := ct.PostTest(t, configui.NewServer(pageTitle, ct.BaseEnv(t, nil), programs), configui.TestRequest{Program: "outbound", Values: map[string]string{
		"KAFKA_BROKERS": brokers, "KAFKA_TOPIC": "orders", "TEMPORAL_ADDRESS": "127.0.0.1:1",
	}})
	k := ct.CheckTitled(t, resp, "Kafka")
	require.Empty(t, k.Problems)
	require.Equal(t, "The topic has no messages to read.", k.Passed[1])
}

// A Kafka failure shows its whole error.
func TestTest_ShowsTheWholeError(t *testing.T) {
	programs := ct.WithTemporalTimeout(programs, time.Second)
	brokers := newCluster(t, []string{"orders"})
	_, resp := ct.PostTest(t, configui.NewServer(pageTitle, ct.BaseEnv(t, nil), programs), configui.TestRequest{Program: "outbound", Values: map[string]string{
		"KAFKA_BROKERS": brokers, "KAFKA_TOPIC": "missing", "TEMPORAL_ADDRESS": "127.0.0.1:1",
	}})
	require.False(t, resp.OK)
	require.Equal(t, []string{`Connect to Kafka at ` + brokers + ` and find the topic "missing": describe topic "missing": ` +
		`UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.`}, ct.CheckTitled(t, resp, "Kafka").Problems)
}

// The Inbound Connector's Kafka check finds its dead-letter topic, or says it is missing.
func TestTest_InboundDeadLetterTopic(t *testing.T) {
	programs := ct.WithTemporalTimeout(programs, time.Second)
	brokers := newCluster(t, []string{"orders", "orders-dlq"}, "m")
	h := configui.NewServer(pageTitle, ct.BaseEnv(t, nil), programs)
	req := configui.TestRequest{Program: "inbound", Values: map[string]string{
		"KAFKA_BROKERS": brokers, "KAFKA_TOPIC": "orders", "KAFKA_GROUP": "g", "INBOUND_ENDPOINT": "ep",
		"KAFKA_DEAD_LETTER_TOPIC": "orders-dlq", "TEMPORAL_ADDRESS": "127.0.0.1:1",
	}}
	_, resp := ct.PostTest(t, h, req)
	k := ct.CheckTitled(t, resp, "Kafka")
	require.Empty(t, k.Problems)
	require.Contains(t, k.Passed, `The dead-letter topic "orders-dlq" exists.`)

	req.Values["KAFKA_DEAD_LETTER_TOPIC"] = "nowhere"
	_, resp = ct.PostTest(t, h, req)
	require.Equal(t, []string{`Can't find the dead-letter topic "nowhere"; create it, or set DELETE_DEAD_LETTERS=true instead: ` +
		`describe topic "nowhere": UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.`},
		ct.CheckTitled(t, resp, "Kafka").Problems)
}

// Against a dev server: the namespace exists; the Outbound Connector's Test reports a missing
// endpoint targeting its task queue with the command to create one, and the Inbound Connector's
// that INBOUND_ENDPOINT doesn't exist, until each does.
func TestTest_Temporal(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a dev server")
	}
	cli, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not on the PATH")
	}
	programs := ct.WithTemporalTimeout(programs, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: cli, ClientOptions: &client.Options{Namespace: "connectors"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Stop() })
	createEndpoint := func(name, taskQueue string) {
		_, err := server.Client().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: name,
				Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{
					Namespace: "connectors", TaskQueue: taskQueue,
				}}},
			},
		})
		require.NoError(t, err)
	}

	base := ct.BaseEnv(t, nil)
	h := configui.NewServer(pageTitle, base, programs)
	connected := []string{"No Temporal profile file at " + base["TEMPORAL_CONFIG_FILE"] + ": only the TEMPORAL_* variables apply.",
		"Connected to Temporal at " + server.FrontendHostPort() + ".", `Namespace "connectors" exists.`}
	outbound := configui.TestRequest{Program: "outbound", Values: map[string]string{
		"NEXUS_TASK_QUEUE": "kafka-orders", "TEMPORAL_ADDRESS": server.FrontendHostPort(),
		"TEMPORAL_CONFIG_FILE": base["TEMPORAL_CONFIG_FILE"], // as the page sends every variable it shows
	}}
	_, resp := ct.PostTest(t, h, outbound)
	temporal := ct.CheckTitled(t, resp, "Temporal")
	require.Equal(t, connected, temporal.Passed)
	require.Len(t, temporal.Problems, 1)
	require.Contains(t, temporal.Problems[0], `No Nexus endpoint targets namespace "connectors" and task queue "kafka-orders"`)
	require.Contains(t, temporal.Problems[0], "temporal operator nexus endpoint create --name kafka-endpoint --target-namespace connectors "+
		"--target-task-queue kafka-orders --description-file ./endpoint-description.md")
	createEndpoint("orders-endpoint", "kafka-orders")
	_, resp = ct.PostTest(t, h, outbound)
	temporal = ct.CheckTitled(t, resp, "Temporal")
	require.Empty(t, temporal.Problems)
	require.Contains(t, temporal.Passed, `Nexus endpoint "orders-endpoint" targets namespace "connectors" and task queue "kafka-orders".`)

	inbound := configui.TestRequest{Program: "inbound", Values: map[string]string{
		"INBOUND_ENDPOINT": "receiver-endpoint", "TEMPORAL_ADDRESS": server.FrontendHostPort(),
		"TEMPORAL_CONFIG_FILE": base["TEMPORAL_CONFIG_FILE"],
	}}
	_, resp = ct.PostTest(t, h, inbound)
	temporal = ct.CheckTitled(t, resp, "Temporal")
	require.Equal(t, connected, temporal.Passed)
	require.Equal(t, []string{`Nexus endpoint "receiver-endpoint", which the program calls, doesn't exist. Create it, targeting ` +
		`the namespace and task queue of the worker that serves it (a dev server forgets endpoints when it restarts): ` +
		`temporal operator nexus endpoint create --name receiver-endpoint --target-namespace <namespace> --target-task-queue <task queue>`},
		temporal.Problems)
	createEndpoint("receiver-endpoint", "receiver")
	_, resp = ct.PostTest(t, h, inbound)
	temporal = ct.CheckTitled(t, resp, "Temporal")
	require.Empty(t, temporal.Problems)
	require.Equal(t, append(connected,
		`Nexus endpoint "receiver-endpoint", which the program calls, exists: it targets namespace "connectors" and task queue "receiver".`),
		temporal.Passed)

	inbound.Values["INBOUND_ENDPOINT"] = ""
	_, resp = ct.PostTest(t, h, inbound)
	require.Equal(t, connected, ct.CheckTitled(t, resp, "Temporal").Passed, "no endpoint to look for: Settings reports it")

	outbound.Values["TEMPORAL_NAMESPACE"] = "missing"
	_, resp = ct.PostTest(t, h, outbound)
	require.Contains(t, ct.CheckTitled(t, resp, "Temporal").Problems[0], `Namespace "missing" doesn't exist.`)
}

// The page's Kafka checks go through the kafka package, not a client of the page's own.
func TestNoOwnClient(t *testing.T) {
	ct.RequireNoOwnClient(t, ".", connectorClientMakers...)
}
