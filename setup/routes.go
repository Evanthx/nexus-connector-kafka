package setup

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

// This file is the Inbound Connector's routes: the topics it streams, each with its endpoint and
// settings, as JSON in INBOUND_CONFIG or the file INBOUND_CONFIG_FILE names. RouteFields is the one
// list of fields: parsing, the docs table and the example all come from it.

// Route is one topic the Inbound Connector streams, with every setting resolved.
type Route struct {
	Topic                string
	Endpoint             string
	Group                string // the consumer group: the route's group, or nexus-connector-kafka:<topic>:<endpoint>
	Service              string
	Operation            string
	StartFrom            kafka.StartFrom
	DeadLetterTopic      string // "" when DeleteDeadLetters
	DeleteDeadLetters    bool
	BatchSize            int
	BatchTimeLimit       time.Duration
	OperationTimeout     time.Duration
	MaxInputBytes        int
	MaxMessagesPerMinute int  // 0 for no limit
	Ordered              bool // ordering "partition"
	// Origin says where each field's value came from: FromRoute or FromDefaults. A field not in it
	// has its built-in default (or, for group, the derived name).
	Origin map[string]Origin
}

// Origin is where a route's setting came from.
type Origin string

// Where a route's setting can come from besides the built-in default.
const (
	FromRoute    Origin = "route"
	FromDefaults Origin = "defaults"
)

// RouteField is one field of a route, in the JSON.
type RouteField struct {
	Name    string
	Help    string // Markdown, for the docs and the setup page
	Default string // for people: what applies when neither the route nor defaults sets it
	// RouteOnly fields are set on each route, never in defaults: topic, endpoint and group.
	RouteOnly bool
	// set parses raw, the field's JSON value, into r. An empty string means unset: the built-in
	// default.
	set func(r *Route, raw json.RawMessage) error
}

// DefaultMaxInputBytes is under Temporal's 2 MiB payload limit by 64 KiB: the limit applies to the
// whole payload, metadata included, and the size the Inbound Connector counts is an estimate of the
// encoded records.
const DefaultMaxInputBytes = 2*1024*1024 - 64*1024

// GroupPrefix starts the consumer group of a route that doesn't name one.
const GroupPrefix = "nexus-connector-kafka:"

// builtIn is a route with every built-in default.
func builtIn() Route {
	return Route{
		Service:          kafkaapi.KafkaMessageReceiver.ServiceName,
		Operation:        kafkaapi.KafkaMessageReceiver.Receive.Name(),
		StartFrom:        kafka.StartFromOldest,
		BatchSize:        100,
		OperationTimeout: 24 * time.Hour,
		MaxInputBytes:    DefaultMaxInputBytes,
	}
}

// RouteFields are every field of a route, in the order the docs and the example show them.
var RouteFields = []RouteField{
	{
		Name: "topic", RouteOnly: true, Default: "required",
		Help: "The topic to stream. Each topic is in one route only.",
		set:  text(func(r *Route, v string) { r.Topic = v }),
	},
	{
		Name: "endpoint", RouteOnly: true, Default: "required",
		Help: "The Nexus endpoint the route delivers batches to. It routes to the receiver you implement, " +
			"`KafkaMessageReceiver.receive`.",
		set: text(func(r *Route, v string) { r.Endpoint = v }),
	},
	{
		Name: "group", RouteOnly: true, Default: "`" + GroupPrefix + "<topic>:<endpoint>`",
		Help: "The consumer group the route joins, used as is. Inbound Connectors with the same group share the topic's " +
			"partitions. Don't share it with any other consumer; no two routes may use the same group.",
		set: text(func(r *Route, v string) { r.Group = v }),
	},
	{
		Name: "service", Default: "`" + kafkaapi.KafkaMessageReceiver.ServiceName + "`",
		Help: "The Nexus service the receiver serves.",
		set: text(func(r *Route, v string) {
			r.Service = cmp(v, kafkaapi.KafkaMessageReceiver.ServiceName)
		}),
	},
	{
		Name: "operation", Default: "`" + kafkaapi.KafkaMessageReceiver.Receive.Name() + "`",
		Help: "The operation started for each batch.",
		set: text(func(r *Route, v string) {
			r.Operation = cmp(v, kafkaapi.KafkaMessageReceiver.Receive.Name())
		}),
	},
	{
		Name: "startFrom", Default: "`oldest`",
		Help: "Where a consumer group with no committed offset starts reading a partition: `oldest`, every message " +
			"still in the topic, or `newest`, only messages written from then on. See " +
			"[How it works](how-it-works.md#where-reading-starts).",
		set: func(r *Route, raw json.RawMessage) error {
			v, err := str(raw)
			if err != nil {
				return err
			}
			switch s := kafka.StartFrom(v); s {
			case "":
				r.StartFrom = kafka.StartFromOldest
			case kafka.StartFromOldest, kafka.StartFromNewest:
				r.StartFrom = s
			default:
				return fmt.Errorf("must be %q or %q, not %q", kafka.StartFromOldest, kafka.StartFromNewest, v)
			}
			return nil
		},
	},
	{
		Name: "deadLetterTopic", Default: "none",
		Help: "An existing topic, not one any route streams, for messages too large to deliver. Set this or " +
			"`deleteDeadLetters`, not both. On a route, either one replaces the other from `defaults`.",
		set: text(func(r *Route, v string) { r.DeadLetterTopic = v }),
	},
	{
		Name: "deleteDeadLetters", Default: "`false`",
		Help: "`true` to log and skip messages too large to deliver, instead of writing them to `deadLetterTopic`.",
		set: func(r *Route, raw json.RawMessage) error {
			var b bool
			if err := json.Unmarshal(raw, &b); err != nil {
				return errors.New("must be true or false")
			}
			r.DeleteDeadLetters = b
			return nil
		},
	},
	{
		Name: "batchSize", Default: "`100`",
		Help: "The most messages in one batch, and so in one Nexus operation per partition. `1` for one operation per " +
			"message, which also rules out a message being delivered twice.",
		set: whole(1, func(r *Route, n int) { r.BatchSize = n }),
	},
	{
		Name: "batchTimeLimit", Default: "`\"0s\"` (deliver as soon as there are messages)",
		Help: "How long a batch waits to fill, such as `\"500ms\"` or `\"1s\"`. Waiting makes fewer, fuller batches on a " +
			"quiet topic, which costs fewer operations.",
		set: duration(func(r *Route, d time.Duration) { r.BatchTimeLimit = d }, 0),
	},
	{
		Name: "operationTimeout", Default: "`\"24h\"`",
		Help: "How long each operation may take, such as `\"30m\"` or `\"24h\"`. More than 0.",
		set:  duration(func(r *Route, d time.Duration) { r.OperationTimeout = d }, 24*time.Hour),
	},
	{
		Name: "maxInputBytes", Default: "`" + strconv.Itoa(DefaultMaxInputBytes) + "` (2 MiB less 64 KiB)",
		Help: "The largest encoded batch sent to one operation. Larger batches are split; a single larger message goes " +
			"to the dead-letter topic.",
		set: whole(1, func(r *Route, n int) { r.MaxInputBytes = n }),
	},
	{
		Name: "maxMessagesPerMinute", Default: "`0` (no limit)",
		Help: "The most messages this route sends to Temporal a minute, such as `600`, spread evenly: it waits before " +
			"each operation until the batch fits, and the waiting messages stay in Kafka. At least `batchSize`; " +
			"`0` for no limit. The limit is per route and per process: three Inbound Connectors with `600` send up to " +
			"1,800 a minute between them. Messages sent to the dead-letter topic don't count.",
		set: whole(0, func(r *Route, n int) { r.MaxMessagesPerMinute = n }),
	},
	{
		Name: "ordering", Default: "`none`",
		Help: "`none` to start each batch's operation without waiting for the one before, as fast as possible, but a " +
			"later batch can be processed before an earlier one. `partition` to process each partition's messages in " +
			"order: wait for each operation to complete before starting the next for that partition, and commit only " +
			"then. A failed operation isn't started again or dead-lettered: it is logged, handled in Temporal, and " +
			"the partition carries on. See [Ordering](how-it-works.md#ordering).",
		set: func(r *Route, raw json.RawMessage) error {
			v, err := str(raw)
			if err != nil {
				return err
			}
			switch v {
			case "", "none":
				r.Ordered = false
			case "partition":
				r.Ordered = true
			default:
				return fmt.Errorf(`must be "none" or "partition", not %q`, v)
			}
			return nil
		},
	},
}

// cmp is v, or def if v is empty.
func cmp(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func str(raw json.RawMessage) (string, error) {
	var v string
	if err := json.Unmarshal(raw, &v); err != nil {
		return "", errors.New("must be a string")
	}
	return v, nil
}

func text(set func(r *Route, v string)) func(*Route, json.RawMessage) error {
	return func(r *Route, raw json.RawMessage) error {
		v, err := str(raw)
		if err != nil {
			return err
		}
		set(r, v)
		return nil
	}
}

func whole(least int, set func(r *Route, n int)) func(*Route, json.RawMessage) error {
	return func(r *Route, raw json.RawMessage) error {
		var n int
		if err := json.Unmarshal(raw, &n); err != nil || n < least {
			return fmt.Errorf("must be a whole number, at least %d, not %s", least, raw)
		}
		set(r, n)
		return nil
	}
}

func duration(set func(r *Route, d time.Duration), def time.Duration) func(*Route, json.RawMessage) error {
	return func(r *Route, raw json.RawMessage) error {
		v, err := str(raw)
		if err != nil {
			return errors.New(`must be a duration as a string, such as "500ms" or "1h"`)
		}
		if v == "" {
			set(r, def)
			return nil
		}
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return fmt.Errorf(`must be a duration such as "500ms" or "1h", not %q`, v)
		}
		set(r, d)
		return nil
	}
}

func field(name string) (RouteField, bool) {
	i := slices.IndexFunc(RouteFields, func(f RouteField) bool { return f.Name == name })
	if i < 0 {
		return RouteField{}, false
	}
	return RouteFields[i], true
}

// ParseRoutes parses and checks the JSON routes in data, reporting every problem at once, and
// returns every route resolved: the route's own fields, then defaults, then built-in defaults.
func ParseRoutes(data []byte) ([]Route, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return nil, jsonError(data, err)
	}
	if top == nil {
		return nil, errors.New("the routes must be a JSON object with defaults and routes")
	}
	var errs []error
	for _, k := range slices.Sorted(maps.Keys(top)) {
		if k != "defaults" && k != "routes" {
			errs = append(errs, fmt.Errorf("unknown key %q: only defaults and routes are allowed", k))
		}
	}

	base := builtIn()
	base.Origin = map[string]Origin{}
	if raw, ok := top["defaults"]; ok {
		errs = append(errs, applyDefaults(&base, raw)...)
	}

	var raws []json.RawMessage
	if raw, ok := top["routes"]; ok {
		if err := json.Unmarshal(raw, &raws); err != nil {
			errs = append(errs, errors.New("routes must be a JSON array of routes"))
		}
	}
	if len(raws) == 0 && len(errs) == 0 {
		errs = append(errs, errors.New("routes is empty: list at least one route, with its topic and endpoint"))
	}
	routes := make([]Route, len(raws))
	for i, raw := range raws {
		var routeErrs []error
		routes[i], routeErrs = parseRoute(i, base, raw)
		errs = append(errs, routeErrs...)
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	if err := routeRules(routes); err != nil {
		return nil, err
	}
	return routes, nil
}

// applyDefaults sets the fields in raw, the defaults section, on base.
func applyDefaults(base *Route, raw json.RawMessage) []error {
	var errs []error
	var defaults map[string]json.RawMessage
	if err := json.Unmarshal(raw, &defaults); err != nil || defaults == nil {
		return []error{errors.New("defaults must be a JSON object")}
	}
	for _, k := range slices.Sorted(maps.Keys(defaults)) {
		f, ok := field(k)
		switch {
		case !ok:
			errs = append(errs, fmt.Errorf("defaults: unknown key %q", k))
		case f.RouteOnly:
			errs = append(errs, fmt.Errorf("defaults: unknown key %q: set it on each route", k))
		case isEmpty(defaults[k]):
			// "" or null is the same as leaving the field out
		default:
			if err := f.set(base, defaults[k]); err != nil {
				errs = append(errs, fmt.Errorf("defaults.%s %w", k, err))
				continue
			}
			base.Origin[k] = FromDefaults
		}
	}
	return errs
}

// parseRoute is route i, raw, over base, with its group derived if it names none.
func parseRoute(i int, base Route, raw json.RawMessage) (Route, []error) {
	r := base
	r.Origin = maps.Clone(base.Origin)
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return r, []error{fmt.Errorf("routes[%d] must be a JSON object", i)}
	}
	var fieldErrs []error
	for _, k := range slices.Sorted(maps.Keys(obj)) {
		f, ok := field(k)
		if !ok {
			fieldErrs = append(fieldErrs, fmt.Errorf("unknown key %q", k))
			continue
		}
		if isEmpty(obj[k]) {
			continue // "" or null is the same as leaving the field out
		}
		if err := f.set(&r, obj[k]); err != nil {
			fieldErrs = append(fieldErrs, fmt.Errorf("%s %w", k, err))
			continue // a field that didn't parse isn't set
		}
		r.Origin[k] = FromRoute
	}
	errs := make([]error, len(fieldErrs))
	for j, err := range fieldErrs {
		errs[j] = fmt.Errorf("%s: %w", routeLabel(i, r.Topic), err)
	}
	overrideDeadLetters(&r)
	if r.Group == "" {
		r.Group = GroupPrefix + r.Topic + ":" + r.Endpoint
	}
	return r, errs
}

// isEmpty reports whether raw is the empty string or null, which count as leaving the field out.
func isEmpty(raw json.RawMessage) bool {
	v := string(bytes.TrimSpace(raw))
	return v == `""` || v == "null"
}

// overrideDeadLetters lets a route choose between deadLetterTopic and deleteDeadLetters whatever
// defaults says: the one the route sets wins over the other from defaults. Both on the route is
// still an error (checkRoute).
func overrideDeadLetters(r *Route) {
	topicHere, deleteHere := r.Origin["deadLetterTopic"] == FromRoute, r.Origin["deleteDeadLetters"] == FromRoute
	switch {
	case deleteHere && !topicHere && r.DeleteDeadLetters:
		r.DeadLetterTopic = ""
	case topicHere && !deleteHere:
		r.DeleteDeadLetters = false
	default: // both or neither on the route: nothing to override
	}
}

func routeLabel(i int, topic string) string {
	if topic == "" {
		return fmt.Sprintf("routes[%d]", i)
	}
	return fmt.Sprintf("routes[%d] (%s)", i, topic)
}

// routeRules checks the resolved routes, each alone and against each other.
func routeRules(routes []Route) error {
	var errs []error
	sources := map[string]int{} // each topic's first route
	for i, r := range routes {
		if _, dup := sources[r.Topic]; r.Topic != "" && !dup {
			sources[r.Topic] = i
		}
	}
	groups := map[string]int{}
	for i, r := range routes {
		problems := checkRoute(r)
		if j, ok := sources[r.DeadLetterTopic]; ok && r.DeadLetterTopic != "" && !r.DeleteDeadLetters {
			problems = append(problems, fmt.Sprintf("deadLetterTopic %q is the topic %s streams: dead letters must go to a topic no route reads",
				r.DeadLetterTopic, routeLabel(j, r.DeadLetterTopic)))
		}
		if j := sources[r.Topic]; r.Topic != "" && j != i {
			problems = append(problems, fmt.Sprintf("the topic is in %s too: each topic goes in one route", routeLabel(j, r.Topic)))
		}
		if j, dup := groups[r.Group]; dup {
			problems = append(problems, fmt.Sprintf("the consumer group %q is %s's too: each route needs its own group", r.Group, routeLabel(j, routes[j].Topic)))
		} else {
			groups[r.Group] = i
		}
		for _, p := range problems {
			errs = append(errs, fmt.Errorf("%s: %s", routeLabel(i, r.Topic), p))
		}
	}
	return errors.Join(errs...)
}

// checkRoute is what is wrong with r on its own.
func checkRoute(r Route) []string {
	var problems []string
	if r.Topic == "" {
		problems = append(problems, "set topic: the topic to stream")
	} else if err := CheckTopicName(r.Topic); err != nil {
		problems = append(problems, err.Error())
	}
	switch {
	case r.Endpoint == "":
		problems = append(problems, "set endpoint: the Nexus endpoint to deliver to")
	case strings.TrimSpace(r.Endpoint) == "":
		problems = append(problems, "endpoint is blank: set the Nexus endpoint to deliver to")
	default:
	}
	if r.Group != "" && strings.TrimSpace(r.Group) == "" {
		problems = append(problems, "group is blank: set a consumer group, or leave group out to use the default")
	}
	switch {
	case r.DeadLetterTopic == "" && !r.DeleteDeadLetters:
		problems = append(problems, "set deadLetterTopic, the topic undeliverable messages go to, or deleteDeadLetters to true to delete them instead")
	case r.DeadLetterTopic != "" && r.DeleteDeadLetters:
		problems = append(problems, "set deadLetterTopic or deleteDeadLetters, not both: undeliverable messages either go to that topic or are deleted")
	case r.DeadLetterTopic != "":
		if err := CheckTopicName(r.DeadLetterTopic); err != nil {
			problems = append(problems, "deadLetterTopic: "+err.Error())
		}
	default: // deleteDeadLetters alone
	}
	if r.OperationTimeout <= 0 {
		problems = append(problems, "operationTimeout must be more than 0")
	}
	if r.MaxMessagesPerMinute > 0 && r.BatchSize > r.MaxMessagesPerMinute {
		problems = append(problems, fmt.Sprintf("batchSize (%d) is more than maxMessagesPerMinute (%d), so a full batch could never be sent: "+
			"set batchSize to %d or less, or raise maxMessagesPerMinute", r.BatchSize, r.MaxMessagesPerMinute, r.MaxMessagesPerMinute))
	}
	return problems
}

// jsonError explains a JSON decoding error, with the line and column where it is.
func jsonError(data []byte, err error) error {
	var syntax *json.SyntaxError
	var typ *json.UnmarshalTypeError
	switch {
	case errors.As(err, &syntax):
		line, col := position(data, syntax.Offset)
		return fmt.Errorf("the routes aren't valid JSON: line %d, column %d: %v", line, col, syntax)
	case errors.As(err, &typ):
		return errors.New("the routes must be a JSON object with defaults and routes")
	}
	return fmt.Errorf("the routes aren't valid JSON: %w", err)
}

// position is the line and column, from 1, of the byte before offset in data: where the decoder
// stopped.
func position(data []byte, offset int64) (line, col int) {
	offset = max(min(offset, int64(len(data))), 1)
	before := data[:offset-1]
	line = bytes.Count(before, []byte("\n")) + 1
	col = int(offset) - (bytes.LastIndexByte(before, '\n') + 1)
	return line, col
}

// Topics are every topic the routes read or write: each source, then each dead-letter topic.
func Topics(routes []Route) []string {
	var out []string
	for _, r := range routes {
		out = append(out, r.Topic)
	}
	for _, r := range routes {
		if r.DeadLetterTopic != "" && !slices.Contains(out, r.DeadLetterTopic) {
			out = append(out, r.DeadLetterTopic)
		}
	}
	return out
}

// The variables that hold the routes. Exactly one is set.
var (
	InboundConfig = settings.Define(settings.Variable{
		Name:      "INBOUND_CONFIG",
		Required:  "unless INBOUND_CONFIG_FILE is set",
		Multiline: true,
		Example:   RoutesExample,
		HelpURL:   RoutesHelpURL,
		Help: "The routes, as JSON: a `defaults` section, then `routes`, one per topic, each with its `topic` and " +
			"`endpoint` and any setting that differs from `defaults`. Set this or `INBOUND_CONFIG_FILE`, not both.",
		Docs: "See [Inbound routes](#inbound-routes).",
	}, settings.Text)

	InboundConfigFile = settings.Define(settings.Variable{
		Name:     "INBOUND_CONFIG_FILE",
		Required: "unless INBOUND_CONFIG is set",
		Help:     "The path to a file holding the routes, as `INBOUND_CONFIG` would. Set this or `INBOUND_CONFIG`, not both.",
	}, settings.Text)
)

// RoutesHelpURL explains the routes at length.
const RoutesHelpURL = "https://github.com/Evanthx/nexus-connector-kafka/blob/main/docs/setup.md#inbound-routes"

// LoadRoutes reads the routes from INBOUND_CONFIG or the file INBOUND_CONFIG_FILE names, and
// parses them. source says where they came from, for the startup report.
func LoadRoutes(env settings.Env) (routes []Route, source string, err error) {
	inline, path := settings.Get(env, InboundConfig.Name), settings.Get(env, InboundConfigFile.Name)
	var data []byte
	switch {
	case inline == "" && path == "":
		return nil, "", errors.New("set INBOUND_CONFIG to the routes, as JSON, or INBOUND_CONFIG_FILE to a file holding them")
	case inline != "" && path != "":
		return nil, "", errors.New("set INBOUND_CONFIG or INBOUND_CONFIG_FILE, not both")
	case inline != "":
		data, source = []byte(inline), "INBOUND_CONFIG"
	default:
		data, err = os.ReadFile(path)
		if err != nil {
			return nil, "", fmt.Errorf("INBOUND_CONFIG_FILE: read the routes: %w", err)
		}
		source = "INBOUND_CONFIG_FILE (" + path + ")"
	}
	routes, err = ParseRoutes(data)
	if err != nil {
		return nil, "", err
	}
	return routes, source, nil
}

// routesRule checks that exactly one of INBOUND_CONFIG and INBOUND_CONFIG_FILE is set, and that the
// routes in it parse and pass every rule.
func routesRule(env settings.Env) error {
	_, _, err := LoadRoutes(env)
	return err
}

// RoutesMarkdown is the docs table of the route fields.
func RoutesMarkdown() string {
	var b strings.Builder
	b.WriteString("| Field | In `defaults` | Default | Meaning |\n|---|---|---|---|\n")
	for _, f := range RouteFields {
		where := "yes"
		if f.RouteOnly {
			where = "no: on each route"
		}
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s |\n", f.Name, where, f.Default, strings.ReplaceAll(f.Help, "|", `\|`))
	}
	return b.String()
}

// RoutesExample is a configuration with two routes: defaults lists every field that can have a
// default, the first route only what can't (topic and endpoint), and the second every route field
// but deadLetterTopic, since it deletes dead letters instead. TestRoutesExample fails if a field is
// added without updating it, or if its defaults stop matching the built-in ones.
const RoutesExample = `{
  "defaults": {
    "service": "KafkaMessageReceiver",
    "operation": "receive",
    "startFrom": "oldest",
    "deadLetterTopic": "dead-letters",
    "deleteDeadLetters": false,
    "batchSize": 100,
    "batchTimeLimit": "0s",
    "operationTimeout": "24h",
    "maxInputBytes": 2031616,
    "maxMessagesPerMinute": 0,
    "ordering": "none"
  },
  "routes": [
    { "topic": "orders", "endpoint": "orders-receiver" },
    {
      "topic": "customers",
      "endpoint": "customers-receiver",
      "group": "customers-reader",
      "service": "KafkaMessageReceiver",
      "operation": "receive",
      "startFrom": "newest",
      "deleteDeadLetters": true,
      "batchSize": 1,
      "batchTimeLimit": "1s",
      "operationTimeout": "1h",
      "maxInputBytes": 1048576,
      "maxMessagesPerMinute": 600,
      "ordering": "partition"
    }
  ]
}
`
