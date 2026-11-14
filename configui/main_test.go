package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/settings"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

// connectorClientMakers are the client constructors of the library this repo's connector calls
// Kafka with, as "import path.Name", which configui must not call itself (see TestNoOwnClient):
// its checks go through the kafka package (Identify, ReadLast, GroupMembers).
var connectorClientMakers = []string{
	"github.com/twmb/franz-go/pkg/kgo.NewClient",
	"github.com/twmb/franz-go/pkg/kadm.NewClient",
	"github.com/twmb/franz-go/pkg/kadm.NewOptClient",
}

// baseEnv is the environment configui was started in, for a test: nothing of the developer's own,
// and no Temporal profile file.
func baseEnv(t *testing.T, vars settings.Map) settings.Map {
	env := settings.Map{"TEMPORAL_CONFIG_FILE": filepath.Join(t.TempDir(), "none.toml")}
	for k, v := range vars {
		env[k] = v
	}
	return env
}

// setTemporalTimeout sets temporalTimeout for the test.
func setTemporalTimeout(t *testing.T, d time.Duration) {
	old := temporalTimeout
	temporalTimeout = d
	t.Cleanup(func() { temporalTimeout = old })
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

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://localhost:8090"+path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://localhost:8090/api/test", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// postTest posts req and returns the response, raw and decoded.
func postTest(t *testing.T, h http.Handler, req testRequest) (string, testResponse) {
	t.Helper()
	b, err := json.Marshal(req)
	require.NoError(t, err)
	rec := post(t, h, string(b))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp testResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	return rec.Body.String(), resp
}

func checkTitled(t *testing.T, resp testResponse, title string) check {
	t.Helper()
	for _, c := range resp.Checks {
		if c.Title == title {
			return c
		}
	}
	require.Failf(t, "no check", "%q in %+v", title, resp.Checks)
	return check{}
}

// The page gets both programs with every variable in their settings lists, in order, and the
// values set now, but never a secret's value.
func TestSettings_JSON(t *testing.T) {
	h := newServer(baseEnv(t, settings.Map{
		"KAFKA_TOPIC":      "orders",
		"KAFKA_PASSWORD":   "hunter2_never_sent",
		"TEMPORAL_API_KEY": "temporal_key_never_sent",
		"https_proxy":      "http://user:proxy_pw_never_sent@proxy.example:8080",
	}), programs)
	rec := get(t, h, "/api/settings")
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotContains(t, rec.Body.String(), "never_sent")

	var page pageJSON
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
	require.Equal(t, pageTitle, page.Title)
	require.Len(t, page.Programs, 2)
	for i, want := range []*settings.Program{setup.Relay, setup.Connector} {
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
		"KAFKA_PASSWORD": true, "TEMPORAL_API_KEY": true, "TEMPORAL_TLS_CLIENT_KEY_DATA": false,
	}, page.SecretsSet)
	// External variables: read-only, as gRPC reads them, masked, and never among the values sent.
	require.Equal(t, map[string]externalJSON{
		"HTTPS_PROXY": {Set: true, From: "https_proxy", Value: "http://user:****@proxy.example:8080"},
		"NO_PROXY":    {},
	}, page.External)
}

func TestLocalOnly(t *testing.T) {
	h := newServer(baseEnv(t, nil), programs)

	req := httptest.NewRequest(http.MethodGet, "http://evil.example:8090/api/settings", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, "a Host other than localhost is refused")

	req = httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8090/api/test", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "text/plain")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnsupportedMediaType, rec.Code, "a POST that isn't JSON is refused")

	require.Equal(t, http.StatusOK, get(t, h, "/").Code)
	require.Equal(t, http.StatusOK, get(t, h, "/api/settings").Code)
}

func TestTest_RefusesABadRequest(t *testing.T) {
	h := newServer(baseEnv(t, nil), programs)
	for body, want := range map[string]string{
		`{"program":`:       "bad request",
		`{"program":"web"}`: `unknown program "web"`,
		`{"program":"relay","values":{"A=B":"x"}}`:      `"A=B" isn't a variable name`,
		`{"program":"connector","values":{"1ABC":"x"}}`: `"1ABC" isn't a variable name`,
	} {
		rec := post(t, h, body)
		require.Equal(t, http.StatusBadRequest, rec.Code, body)
		require.Contains(t, rec.Body.String(), want, body)
	}
}

// The Settings check reports exactly what the relay would at startup, every problem at once, and
// the Kafka check waits for valid Kafka settings.
func TestTest_ReportsEverySettingsProblemAsStartupDoes(t *testing.T) {
	setTemporalTimeout(t, time.Second)
	values := map[string]string{
		"KAFKA_TOPIC":             "orders",
		"KAFKA_START_FROM":        "yesterday",
		"KAFKA_BATCH_SIZE":        "0",
		"RELAY_OPERATION_TIMEOUT": "0s",
		"TEMPORAL_ADDRESS":        "127.0.0.1:1",
	}
	_, resp := postTest(t, newServer(baseEnv(t, nil), programs), testRequest{Program: "relay", Values: values})
	require.False(t, resp.OK)

	_, startupErr := setup.Relay.Load(settings.Map(values))
	require.Error(t, startupErr)
	problems := checkTitled(t, resp, "Settings").Problems
	require.Equal(t, strings.Split(startupErr.Error(), "\n"), problems)
	require.Equal(t, []string{
		"set KAFKA_BROKERS: the Kafka brokers, as comma-separated host:port",
		`KAFKA_START_FROM must be oldest or newest, not "yesterday"`,
		"set KAFKA_GROUP: the consumer group the relay joins",
		`KAFKA_BATCH_SIZE must be a whole number, at least 1, not "0"`,
		"set RELAY_ENDPOINT: the Nexus endpoint to deliver to",
		"RELAY_OPERATION_TIMEOUT must be more than 0",
	}, problems)
	require.Contains(t, checkTitled(t, resp, "Temporal").Problems[0], "Failed to connect to Temporal at 127.0.0.1:1")
	require.Equal(t, "Fix the Kafka settings first.", checkTitled(t, resp, "Kafka").Skipped)
}

// The Test reads the topic's last message with the programs' Kafka code, the same one each time,
// using the page's values in place of those where configui runs.
func TestTest_ReadsTheLastMessage(t *testing.T) {
	setTemporalTimeout(t, time.Second)
	brokers := newCluster(t, []string{"orders"}, "first", "last order")
	h := newServer(baseEnv(t, settings.Map{"KAFKA_TOPIC": "replaced-by-the-page"}), programs)
	req := testRequest{Program: "connector", Values: map[string]string{
		"KAFKA_BROKERS": brokers, "KAFKA_TOPIC": "orders", "KAFKA_PASSWORD": "typed_s3cret", "TEMPORAL_ADDRESS": "127.0.0.1:1",
	}}
	for range 2 {
		raw, resp := postTest(t, h, req)
		require.NotContains(t, raw, "s3cret")
		k := checkTitled(t, resp, "Kafka")
		require.Empty(t, k.Problems)
		require.Contains(t, k.Passed[1], "Read the topic's last message: partition 0, offset 1, timestamp ")
		require.Equal(t, []string{"Key: k", "Value: last order", "Header source: test"}, k.Passed[2:])
		require.Contains(t, k.Passed[0], `found the topic "orders"`)
	}
}

// An empty topic is fine: there is just nothing to show.
func TestTest_EmptyTopic(t *testing.T) {
	setTemporalTimeout(t, time.Second)
	brokers := newCluster(t, []string{"orders"})
	_, resp := postTest(t, newServer(baseEnv(t, nil), programs), testRequest{Program: "connector", Values: map[string]string{
		"KAFKA_BROKERS": brokers, "KAFKA_TOPIC": "orders", "TEMPORAL_ADDRESS": "127.0.0.1:1",
	}})
	k := checkTitled(t, resp, "Kafka")
	require.Empty(t, k.Problems)
	require.Equal(t, "The topic has no messages to read.", k.Passed[1])
}

// A Kafka failure shows its whole error.
func TestTest_ShowsTheWholeError(t *testing.T) {
	setTemporalTimeout(t, time.Second)
	brokers := newCluster(t, []string{"orders"})
	_, resp := postTest(t, newServer(baseEnv(t, nil), programs), testRequest{Program: "connector", Values: map[string]string{
		"KAFKA_BROKERS": brokers, "KAFKA_TOPIC": "missing", "TEMPORAL_ADDRESS": "127.0.0.1:1",
	}})
	require.False(t, resp.OK)
	require.Equal(t, []string{`Connect to Kafka at ` + brokers + ` and find the topic "missing": describe topic "missing": ` +
		`UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.`}, checkTitled(t, resp, "Kafka").Problems)
}

// The relay's Kafka check finds its dead-letter topic, or says it is missing.
func TestTest_RelayDeadLetterTopic(t *testing.T) {
	setTemporalTimeout(t, time.Second)
	brokers := newCluster(t, []string{"orders", "orders-dlq"}, "m")
	h := newServer(baseEnv(t, nil), programs)
	req := testRequest{Program: "relay", Values: map[string]string{
		"KAFKA_BROKERS": brokers, "KAFKA_TOPIC": "orders", "KAFKA_GROUP": "g", "RELAY_ENDPOINT": "ep",
		"KAFKA_DEAD_LETTER_TOPIC": "orders-dlq", "TEMPORAL_ADDRESS": "127.0.0.1:1",
	}}
	_, resp := postTest(t, h, req)
	k := checkTitled(t, resp, "Kafka")
	require.Empty(t, k.Problems)
	require.Contains(t, k.Passed, `The dead-letter topic "orders-dlq" exists.`)

	req.Values["KAFKA_DEAD_LETTER_TOPIC"] = "nowhere"
	_, resp = postTest(t, h, req)
	require.Equal(t, []string{`Can't find the dead-letter topic "nowhere"; create it, or set DELETE_DEAD_LETTERS=true instead: ` +
		`describe topic "nowhere": UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.`},
		checkTitled(t, resp, "Kafka").Problems)
}

// A connector whose KAFKA_GROUP has live members, such as a relay, can't read: the Test says so.
func TestTest_ConnectorGroupInUse(t *testing.T) {
	setTemporalTimeout(t, time.Second)
	brokers := newCluster(t, []string{"orders"}, "m")
	member, err := kafka.NewGroupMember(kafka.Config{Brokers: strings.Split(brokers, ","), Topic: "orders", StartFrom: kafka.StartFromOldest}, "live")
	require.NoError(t, err)
	t.Cleanup(member.Close)
	_, err = member.Next(context.Background(), 1, 0)
	require.NoError(t, err)

	h := newServer(baseEnv(t, nil), programs)
	req := testRequest{Program: "connector", Values: map[string]string{
		"KAFKA_BROKERS": brokers, "KAFKA_TOPIC": "orders", "KAFKA_GROUP": "live", "TEMPORAL_ADDRESS": "127.0.0.1:1",
	}}
	_, resp := postTest(t, h, req)
	require.Equal(t, []string{`The consumer group "live" has 1 live member(s), such as a relay, so every read through this ` +
		`connector would fail with GroupInUse. Set KAFKA_GROUP to a consumer group nothing else uses.`}, checkTitled(t, resp, "Kafka").Problems)

	req.Values["KAFKA_GROUP"] = "free"
	_, resp = postTest(t, h, req)
	k := checkTitled(t, resp, "Kafka")
	require.Empty(t, k.Problems)
	require.Contains(t, k.Passed, `The consumer group "free" has no live members, so this connector can read for it.`)
}

// The page's values replace every variable the program owns; others pass through.
func TestPageEnv(t *testing.T) {
	base := settings.Map{
		"HOME":                          "/home/x",
		"HTTPS_PROXY":                   "http://proxy.example:8080", // external: the page can't change it
		"TEMPORAL_TLS_CLIENT_CERT_PATH": "/cert",                     // owned, not sent: unset
		"KAFKA_TOPIC":                   "old",
		"KAFKA_BATCH_SIZE":              "9",
		"KAFKA_PASSWORD":                "kept",
		"RELAY_ENDPOINT":                "not on the page",
	}
	env := pageEnv(base, setup.Relay, testRequest{
		Values: map[string]string{
			"KAFKA_TOPIC":      "new",
			"KAFKA_BATCH_SIZE": "  ",
			"KAFKA_PASSWORD":   "ignored: kept",
			"EXTRA":            "added",
		},
		KeepSecrets: map[string]bool{"KAFKA_PASSWORD": true, "HOME": true},
	})
	require.Equal(t, settings.Map{
		"HOME":           "/home/x",
		"HTTPS_PROXY":    "http://proxy.example:8080",
		"KAFKA_TOPIC":    "new",
		"KAFKA_PASSWORD": "kept",
		"EXTRA":          "added",
	}, env)
}

// Against a dev server: the namespace exists; the connector's Test reports a missing endpoint
// targeting its task queue with the command to create one, and the relay's that RELAY_ENDPOINT
// doesn't exist, until each does.
func TestTest_Temporal(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a dev server")
	}
	cli, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not on the PATH")
	}
	setTemporalTimeout(t, 10*time.Second)
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

	base := baseEnv(t, nil)
	h := newServer(base, programs)
	connected := []string{"No Temporal profile file at " + base["TEMPORAL_CONFIG_FILE"] + ": only the TEMPORAL_* variables apply.",
		"Connected to Temporal at " + server.FrontendHostPort() + ".", `Namespace "connectors" exists.`}
	connector := testRequest{Program: "connector", Values: map[string]string{
		"NEXUS_TASK_QUEUE": "kafka-orders", "TEMPORAL_ADDRESS": server.FrontendHostPort(),
		"TEMPORAL_CONFIG_FILE": base["TEMPORAL_CONFIG_FILE"], // as the page sends every variable it shows
	}}
	_, resp := postTest(t, h, connector)
	temporal := checkTitled(t, resp, "Temporal")
	require.Equal(t, connected, temporal.Passed)
	require.Len(t, temporal.Problems, 1)
	require.Contains(t, temporal.Problems[0], `No Nexus endpoint targets namespace "connectors" and task queue "kafka-orders"`)
	require.Contains(t, temporal.Problems[0], "temporal operator nexus endpoint create --name kafka-endpoint --target-namespace connectors "+
		"--target-task-queue kafka-orders --description-file ./endpoint-description.md")
	createEndpoint("orders-endpoint", "kafka-orders")
	_, resp = postTest(t, h, connector)
	temporal = checkTitled(t, resp, "Temporal")
	require.Empty(t, temporal.Problems)
	require.Contains(t, temporal.Passed, `Nexus endpoint "orders-endpoint" targets namespace "connectors" and task queue "kafka-orders".`)

	relay := testRequest{Program: "relay", Values: map[string]string{
		"RELAY_ENDPOINT": "receiver-endpoint", "TEMPORAL_ADDRESS": server.FrontendHostPort(),
		"TEMPORAL_CONFIG_FILE": base["TEMPORAL_CONFIG_FILE"],
	}}
	_, resp = postTest(t, h, relay)
	temporal = checkTitled(t, resp, "Temporal")
	require.Equal(t, connected, temporal.Passed)
	require.Equal(t, []string{`Nexus endpoint "receiver-endpoint", which the program calls, doesn't exist. Create it, targeting ` +
		`the namespace and task queue of the worker that serves it (a dev server forgets endpoints when it restarts): ` +
		`temporal operator nexus endpoint create --name receiver-endpoint --target-namespace <namespace> --target-task-queue <task queue>`},
		temporal.Problems)
	createEndpoint("receiver-endpoint", "receiver")
	_, resp = postTest(t, h, relay)
	temporal = checkTitled(t, resp, "Temporal")
	require.Empty(t, temporal.Problems)
	require.Equal(t, append(connected,
		`Nexus endpoint "receiver-endpoint", which the program calls, exists: it targets namespace "connectors" and task queue "receiver".`),
		temporal.Passed)

	relay.Values["RELAY_ENDPOINT"] = ""
	_, resp = postTest(t, h, relay)
	require.Equal(t, connected, checkTitled(t, resp, "Temporal").Passed, "no endpoint to look for: Settings reports it")

	connector.Values["TEMPORAL_NAMESPACE"] = "missing"
	_, resp = postTest(t, h, connector)
	require.Contains(t, checkTitled(t, resp, "Temporal").Problems[0], `Namespace "missing" doesn't exist.`)
}

func TestValidName(t *testing.T) {
	for _, ok := range []string{"A", "_A", "a1_B"} {
		require.True(t, validName(ok), ok)
	}
	for _, bad := range []string{"", "1A", "A-B", "A=B", "A B", "Ä"} {
		require.False(t, validName(bad), bad)
	}
}
