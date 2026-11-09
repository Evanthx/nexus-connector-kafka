package main

// variable is one environment variable the page shows: what it does and its default, for a
// program. Keep these in step with README's Settings table and the programs' config code.
type variable struct {
	Name     string `json:"name"`
	Section  string `json:"section"`
	Default  string `json:"default"`  // what the program uses when it is unset; empty if nothing
	Required string `json:"required"` // "yes", or when it is required; empty if optional
	Help     string `json:"help"`
	Secret   bool   `json:"secret"` // shown as asterisks, never sent to the page
}

// program is a way to run the connector, and the variables it reads.
type program struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Command   string     `json:"command"`
	Variables []variable `json:"variables"`
}

var kafkaVariables = []variable{
	{Name: "KAFKA_BROKERS", Section: "Kafka", Required: "yes",
		Help: "The Kafka brokers to connect to, as a comma-separated list of host:port, such as broker1:9092,broker2:9092. One reachable broker is enough to find the rest."},
	{Name: "KAFKA_TOPIC", Section: "Kafka", Required: "yes",
		Help: "The topic this program serves. One program serves one topic; run one per topic."},
	{Name: "KAFKA_USERNAME", Section: "Kafka", Default: "none: no authentication",
		Help: "The SASL/SCRAM-SHA-256 username. Leave it and KAFKA_PASSWORD blank if Kafka needs no login. TLS and other SASL mechanisms are not supported."},
	{Name: "KAFKA_PASSWORD", Section: "Kafka", Default: "none", Secret: true,
		Help: "The password for KAFKA_USERNAME."},
	{Name: "KAFKA_START_FROM", Section: "Kafka", Default: "oldest",
		Help: "Where a consumer group with no committed position starts reading a partition: oldest (every message still in the topic) or newest (only messages written from then on)."},
}

var temporalVariables = []variable{
	{Name: "TEMPORAL_ADDRESS", Section: "Temporal", Default: "localhost:7233",
		Help: "The Temporal server's host:port. For Temporal Cloud, your namespace's gRPC endpoint. TLS settings (TEMPORAL_TLS_*) and the temporal CLI's profile file also work, as for the temporal CLI; this page doesn't show them."},
	{Name: "TEMPORAL_NAMESPACE", Section: "Temporal", Default: "kafka-connector",
		Help: "The Temporal namespace the program connects to."},
	{Name: "TEMPORAL_API_KEY", Section: "Temporal", Default: "none", Secret: true,
		Help: "A Temporal Cloud API key, if the namespace uses API key authentication."},
}

var programs = []program{
	{
		ID: "streamer", Name: "Streamer", Command: "go run ./streamer",
		Variables: concat(kafkaVariables, []variable{
			{Name: "KAFKA_GROUP", Section: "Streamer", Required: "yes",
				Help: "The consumer group the streamer joins. Streamers with the same group share the topic's partitions. Don't share it with a Nexus worker reader or any other consumer."},
			{Name: "KAFKA_DEAD_LETTER_TOPIC", Section: "Streamer", Required: "unless DELETE_DEAD_LETTERS is true",
				Help: "An existing topic, other than KAFKA_TOPIC, for messages too large to deliver. Set this or DELETE_DEAD_LETTERS=true, not both."},
			{Name: "DELETE_DEAD_LETTERS", Section: "Streamer", Default: "false",
				Help: "true to log and skip messages too large to deliver, instead of writing them to KAFKA_DEAD_LETTER_TOPIC."},
			{Name: "KAFKA_BATCH_SIZE", Section: "Streamer", Default: "100",
				Help: "The most messages in one batch, and so in one Nexus operation per partition. Set 1 for one operation per message, which also rules out a message being delivered twice."},
			{Name: "KAFKA_BATCH_TIME_LIMIT", Section: "Streamer", Default: "0 (deliver as soon as there are messages)",
				Help: "How long a batch waits to fill, such as 500ms or 1s. Waiting makes fewer, fuller batches on a quiet topic, which costs fewer operations."},
			{Name: "STREAM_ENDPOINT", Section: "Streamer", Required: "yes",
				Help: "The Nexus endpoint the streamer delivers batches to. It routes to the receiver you implement."},
			{Name: "STREAM_SERVICE", Section: "Streamer", Default: "KafkaMessageReceiver",
				Help: "The Nexus service the receiver serves."},
			{Name: "STREAM_OPERATION", Section: "Streamer", Default: "receive",
				Help: "The operation started for each batch."},
			{Name: "STREAM_OPERATION_TIMEOUT", Section: "Streamer", Default: "24h",
				Help: "How long each operation may take, such as 30m or 24h. More than 0."},
			{Name: "STREAM_MAX_INPUT_BYTES", Section: "Streamer", Default: "2031616 (2 MiB less 64 KiB)",
				Help: "The largest encoded batch sent to one operation. Larger batches are split; a single larger message goes to the dead-letter topic."},
		}, temporalVariables),
	},
	{
		ID: "nexusworker", Name: "Nexus worker", Command: "go run ./nexusworker",
		Variables: concat(kafkaVariables, []variable{
			{Name: "KAFKA_GROUP", Section: "Nexus worker", Default: "none: each read names its consumer group",
				Help: "Set it to make every read through this worker use this one consumer group. Leave it blank to let each read name its own."},
			{Name: "NEXUS_TASK_QUEUE", Section: "Nexus worker", Default: "nexus-connector-kafka",
				Help: "The task queue the Nexus endpoint targets. Reads run on this name with -reads added. Give each Nexus worker for another topic or consumer group its own."},
			{Name: "KAFKA_MAX_BATCH_BYTES", Section: "Nexus worker", Default: "2097152 (2 MiB)",
				Help: "The most bytes a read's result may encode to. Temporal's payload limit is 2 MiB by default."},
		}, temporalVariables),
	},
}

func concat(lists ...[]variable) []variable {
	var out []variable
	for _, l := range lists {
		out = append(out, l...)
	}
	return out
}

// allVariables is every variable any program reads, once each.
func allVariables() map[string]variable {
	all := map[string]variable{}
	for _, p := range programs {
		for _, v := range p.Variables {
			all[v.Name] = v
		}
	}
	return all
}
