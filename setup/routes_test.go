package setup

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

func TestParseRoutes_DefaultsAndOverrides(t *testing.T) {
	routes, err := ParseRoutes([]byte(`{
		"defaults": {"deadLetterTopic": "dlq", "batchSize": 10},
		"routes": [
			{"topic": "orders", "endpoint": "orders-receiver"},
			{"topic": "customers", "endpoint": "c", "batchSize": 5, "group": "mine", "ordering": "partition"}
		]}`))
	require.NoError(t, err)
	require.Len(t, routes, 2)

	orders := routes[0]
	require.Equal(t, "orders", orders.Topic)
	require.Equal(t, "nexus-connector-kafka:orders:orders-receiver", orders.Group, "derived")
	require.Equal(t, "dlq", orders.DeadLetterTopic, "from defaults")
	require.Equal(t, 10, orders.BatchSize, "from defaults")
	require.Equal(t, "KafkaMessageReceiver", orders.Service, "built in")
	require.Equal(t, "receive", orders.Operation)
	require.Equal(t, kafka.StartFromOldest, orders.StartFrom)
	require.Equal(t, 24*time.Hour, orders.OperationTimeout)
	require.Equal(t, DefaultMaxInputBytes, orders.MaxInputBytes)
	require.False(t, orders.Ordered)
	require.Equal(t, FromDefaults, orders.Origin["batchSize"])
	require.Equal(t, FromRoute, orders.Origin["topic"])
	require.NotContains(t, orders.Origin, "service")

	customers := routes[1]
	require.Equal(t, 5, customers.BatchSize, "the route wins")
	require.Equal(t, FromRoute, customers.Origin["batchSize"])
	require.Equal(t, "mine", customers.Group, "used as is")
	require.True(t, customers.Ordered)
}

// An empty string in a route clears a default.
func TestParseRoutes_SyntaxErrorHasLineAndColumn(t *testing.T) {
	_, err := ParseRoutes([]byte("{\n  \"routes\": [\n    {\"topic\": \"t\",}\n  ]\n}"))
	require.EqualError(t, err, "the routes aren't valid JSON: line 3, column 19: invalid character '}' looking for beginning of object key string")

	_, err = ParseRoutes([]byte(`["not", "an", "object"]`))
	require.EqualError(t, err, "the routes must be a JSON object with defaults and routes")
}

// Strict parsing: an unknown key, at the top, in defaults or in a route, is an error; so is
// endpoint, topic or group in defaults, and a value of the wrong type. Every problem at once.
func TestParseRoutes_Strict(t *testing.T) {
	_, err := ParseRoutes([]byte(`{
		"version": 1,
		"defaults": {"endpoint": "e", "batchsize": 5, "batchSize": "5"},
		"routes": [{"topic": "t", "endpoint": "e", "deleteDeadLetters": "yes", "groupPrefix": "x", "batchTimeLimit": 5}]}`))
	require.EqualError(t, err, `unknown key "version": only defaults and routes are allowed`+"\n"+
		`defaults.batchSize must be a whole number, at least 1, not "5"`+"\n"+
		`defaults: unknown key "batchsize"`+"\n"+
		`defaults: unknown key "endpoint": set it on each route`+"\n"+
		`routes[0] (t): batchTimeLimit must be a duration as a string, such as "500ms" or "1h"`+"\n"+
		`routes[0] (t): deleteDeadLetters must be true or false`+"\n"+
		`routes[0] (t): unknown key "groupPrefix"`)
}

func TestParseRoutes_Rules(t *testing.T) {
	for _, tc := range []struct {
		name, json, want string
	}{
		{"no routes", `{"routes": []}`, "routes is empty: list at least one route, with its topic and endpoint"},
		{"no routes key", `{}`, "routes is empty: list at least one route, with its topic and endpoint"},
		{"topic and endpoint", `{"routes": [{"deleteDeadLetters": true}]}`,
			"routes[0]: set topic: the topic to stream\nroutes[0]: set endpoint: the Nexus endpoint to deliver to"},
		{"bad topic name", `{"routes": [{"topic": "a b", "endpoint": "e", "deleteDeadLetters": true}]}`,
			`routes[0] (a b): topic name "a b" has characters Kafka doesn't allow: use letters, digits, '.', '_' and '-'`},
		{"neither dead letters", `{"routes": [{"topic": "t", "endpoint": "e"}]}`,
			"routes[0] (t): set deadLetterTopic, the topic undeliverable messages go to, or deleteDeadLetters to true to delete them instead"},
		{"both dead letters", `{"routes": [{"topic": "t", "endpoint": "e", "deadLetterTopic": "d", "deleteDeadLetters": true}]}`,
			"routes[0] (t): set deadLetterTopic or deleteDeadLetters, not both: undeliverable messages either go to that topic or are deleted"},
		{"topic twice", `{"defaults": {"deleteDeadLetters": true}, "routes": [{"topic": "t", "endpoint": "a"}, {"topic": "t", "endpoint": "b"}]}`,
			"routes[1] (t): the topic is in routes[0] (t) too: each topic goes in one route"},
		{"same group", `{"defaults": {"deleteDeadLetters": true}, "routes": [{"topic": "a", "endpoint": "e", "group": "g"}, {"topic": "b", "endpoint": "e", "group": "g"}]}`,
			`routes[1] (b): the consumer group "g" is routes[0] (a)'s too: each route needs its own group`},
		{"group same as derived", `{"defaults": {"deleteDeadLetters": true}, "routes": [{"topic": "a", "endpoint": "e"}, {"topic": "b", "endpoint": "e", "group": "nexus-connector-kafka:a:e"}]}`,
			`routes[1] (b): the consumer group "nexus-connector-kafka:a:e" is routes[0] (a)'s too: each route needs its own group`},
		{"dead letters to a source", `{"defaults": {"deadLetterTopic": "b"}, "routes": [{"topic": "a", "endpoint": "e"}, {"topic": "b", "endpoint": "e", "deadLetterTopic": "x"}]}`,
			`routes[0] (a): deadLetterTopic "b" is the topic routes[1] (b) streams: dead letters must go to a topic no route reads`},
		{"dead letters to itself", `{"routes": [{"topic": "a", "endpoint": "e", "deadLetterTopic": "a"}]}`,
			`routes[0] (a): deadLetterTopic "a" is the topic routes[0] (a) streams: dead letters must go to a topic no route reads`},
		{"operation timeout", `{"routes": [{"topic": "t", "endpoint": "e", "deleteDeadLetters": true, "operationTimeout": "0s"}]}`,
			"routes[0] (t): operationTimeout must be more than 0"},
		{"rate below batch", `{"routes": [{"topic": "t", "endpoint": "e", "deleteDeadLetters": true, "batchSize": 100, "maxMessagesPerMinute": 50}]}`,
			"routes[0] (t): batchSize (100) is more than maxMessagesPerMinute (50), so a full batch could never be sent: set batchSize to 50 or less, or raise maxMessagesPerMinute"},
		{"ordering", `{"routes": [{"topic": "t", "endpoint": "e", "deleteDeadLetters": true, "ordering": "strict"}]}`,
			`routes[0] (t): ordering must be "none" or "partition", not "strict"`},
		{"startFrom", `{"routes": [{"topic": "t", "endpoint": "e", "deleteDeadLetters": true, "startFrom": "latest"}]}`,
			`routes[0] (t): startFrom must be "oldest" or "newest", not "latest"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseRoutes([]byte(tc.json))
			require.EqualError(t, err, tc.want)
		})
	}
}

// A topic starting with _ is allowed; Kafka's internal topics are refused at startup instead.
func TestParseRoutes_UnderscoreTopic(t *testing.T) {
	routes, err := ParseRoutes([]byte(`{"routes": [{"topic": "_private", "endpoint": "e", "deleteDeadLetters": true}]}`))
	require.NoError(t, err)
	require.Equal(t, "_private", routes[0].Topic)
}

// The example parses, its defaults list every field that can have a default, its first route only
// topic and endpoint, and its second every route field but deadLetterTopic.
func TestRoutesExample(t *testing.T) {
	routes, err := ParseRoutes([]byte(RoutesExample))
	require.NoError(t, err)
	require.Len(t, routes, 2)

	var ex struct {
		Defaults map[string]json.RawMessage
		Routes   []map[string]json.RawMessage
	}
	require.NoError(t, json.Unmarshal([]byte(RoutesExample), &ex))
	var all, defaultable []string
	for _, f := range RouteFields {
		all = append(all, f.Name)
		if !f.RouteOnly {
			defaultable = append(defaultable, f.Name)
		}
	}
	require.ElementsMatch(t, defaultable, slices.Collect(maps.Keys(ex.Defaults)))
	require.ElementsMatch(t, []string{"topic", "endpoint"}, slices.Collect(maps.Keys(ex.Routes[0])))
	require.ElementsMatch(t, slices.DeleteFunc(all, func(n string) bool { return n == "deadLetterTopic" }),
		slices.Collect(maps.Keys(ex.Routes[1])), "every field but deadLetterTopic: it deletes dead letters")

	// The defaults section shows the built-in defaults.
	builtInRoute, err := ParseRoutes([]byte(`{"routes": [{"topic": "x", "endpoint": "y", "deadLetterTopic": "dead-letters"}]}`))
	require.NoError(t, err)
	got, want := routes[0], builtInRoute[0]
	got.Topic, got.Endpoint, got.Group, got.Origin = want.Topic, want.Endpoint, want.Group, want.Origin
	require.Equal(t, want, got)
}

func TestLoadRoutes(t *testing.T) {
	const valid = `{"routes": [{"topic": "t", "endpoint": "e", "deleteDeadLetters": true}]}`

	_, _, err := LoadRoutes(settings.Map{})
	require.EqualError(t, err, "set INBOUND_CONFIG to the routes, as JSON, or INBOUND_CONFIG_FILE to a file holding them")

	path := filepath.Join(t.TempDir(), "routes.json")
	require.NoError(t, os.WriteFile(path, []byte(valid), 0o600))
	_, _, err = LoadRoutes(settings.Map{"INBOUND_CONFIG": valid, "INBOUND_CONFIG_FILE": path})
	require.EqualError(t, err, "set INBOUND_CONFIG or INBOUND_CONFIG_FILE, not both")

	routes, source, err := LoadRoutes(settings.Map{"INBOUND_CONFIG": valid})
	require.NoError(t, err)
	require.Equal(t, "INBOUND_CONFIG", source)
	require.Equal(t, "t", routes[0].Topic)

	routes, source, err = LoadRoutes(settings.Map{"INBOUND_CONFIG_FILE": path})
	require.NoError(t, err)
	require.Equal(t, "INBOUND_CONFIG_FILE ("+path+")", source)
	require.Equal(t, "t", routes[0].Topic)

	_, _, err = LoadRoutes(settings.Map{"INBOUND_CONFIG_FILE": filepath.Join(t.TempDir(), "missing.json")})
	require.ErrorContains(t, err, "INBOUND_CONFIG_FILE: read the routes:")

	// The program's Load runs the same checks, so the setup page's Test shows route errors.
	_, err = Inbound.Load(settings.Map{"KAFKA_BROKERS": "b:9092", "INBOUND_CONFIG": `{"routes": [{"topic": "t"}]}`})
	require.ErrorContains(t, err, "routes[0] (t): set endpoint")
}

// Derived groups can't run together: topic and endpoint names can't contain the separator, so
// topic "a-b" with endpoint "c" and topic "a" with endpoint "b-c" get different groups.
func TestParseRoutes_DerivedGroupsDontCollide(t *testing.T) {
	routes, err := ParseRoutes([]byte(`{"defaults": {"deleteDeadLetters": true}, "routes": [
		{"topic": "a-b", "endpoint": "c"},
		{"topic": "a", "endpoint": "b-c"}
	]}`))
	require.NoError(t, err)
	require.NotEqual(t, routes[0].Group, routes[1].Group)
}

// "" is the same as leaving a field out: the route takes the value from defaults, not the built-in.
func TestParseRoutes_EmptyIsUnset(t *testing.T) {
	routes, err := ParseRoutes([]byte(`{
		"defaults": {"service": "MyReceiver", "batchTimeLimit": "2s", "deadLetterTopic": "dlq", "startFrom": ""},
		"routes": [{"topic": "t", "endpoint": "e", "service": "", "batchTimeLimit": "", "deadLetterTopic": "", "ordering": ""}]}`))
	require.NoError(t, err)
	r := routes[0]
	require.Equal(t, "MyReceiver", r.Service)
	require.Equal(t, 2*time.Second, r.BatchTimeLimit)
	require.Equal(t, "dlq", r.DeadLetterTopic)
	require.Equal(t, kafka.StartFromOldest, r.StartFrom, "\"\" in defaults: the built-in")
	require.False(t, r.Ordered)
}

// A route picks dead-lettering or deleting whatever defaults says; both on one route is an error.
func TestParseRoutes_RouteOverridesDeadLetters(t *testing.T) {
	routes, err := ParseRoutes([]byte(`{"defaults": {"deadLetterTopic": "dlq"}, "routes": [
		{"topic": "a", "endpoint": "e", "deleteDeadLetters": true}]}`))
	require.NoError(t, err)
	require.Empty(t, routes[0].DeadLetterTopic)
	require.True(t, routes[0].DeleteDeadLetters)

	routes, err = ParseRoutes([]byte(`{"defaults": {"deleteDeadLetters": true}, "routes": [
		{"topic": "a", "endpoint": "e", "deadLetterTopic": "dlq"}]}`))
	require.NoError(t, err)
	require.Equal(t, "dlq", routes[0].DeadLetterTopic)
	require.False(t, routes[0].DeleteDeadLetters)

	_, err = ParseRoutes([]byte(`{"defaults": {"deadLetterTopic": "dlq", "deleteDeadLetters": true}, "routes": [
		{"topic": "a", "endpoint": "e"}]}`))
	require.ErrorContains(t, err, "not both")
}

// null is the same as "" and as leaving a field out, on every kind of field.
func TestParseRoutes_NullIsUnset(t *testing.T) {
	routes, err := ParseRoutes([]byte(`{
		"defaults": {"service": "MyReceiver", "batchSize": 7, "deleteDeadLetters": true, "startFrom": null},
		"routes": [{"topic": "t", "endpoint": "e", "service": null, "batchSize": null, "deadLetterTopic": null, "deleteDeadLetters": null}]}`))
	require.NoError(t, err)
	r := routes[0]
	require.Equal(t, "MyReceiver", r.Service)
	require.Equal(t, 7, r.BatchSize)
	require.True(t, r.DeleteDeadLetters, "null doesn't override defaults' true")
	require.Empty(t, r.DeadLetterTopic)
	require.Equal(t, kafka.StartFromOldest, r.StartFrom)
}

// A blank endpoint or group is refused when the routes are read, not when the endpoint is looked up.
func TestParseRoutes_BlankEndpointAndGroup(t *testing.T) {
	_, err := ParseRoutes([]byte(`{"defaults": {"deleteDeadLetters": true}, "routes": [
		{"topic": "a", "endpoint": "  "},
		{"topic": "b", "endpoint": "e", "group": " "}]}`))
	require.ErrorContains(t, err, "routes[0] (a): endpoint is blank")
	require.ErrorContains(t, err, "routes[1] (b): group is blank")
}
