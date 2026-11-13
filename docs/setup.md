# Setup

How to configure and run the Kafka connector's two programs: the relay (Nexus Connector Relay)
and the connector (Nexus Connector). For a guided walkthrough with a local dev server and Docker
Kafka, see [Testing](testing.md).

## Prerequisites

- **Go 1.26** or later.
- **Temporal CLI 1.9.1** or later (it includes Temporal Server 1.32). Check with
  `temporal --version`.
- **Docker**, for the local Kafka (if using the demo setup).

## Run the relay

The relay streams every message from a Kafka topic into Temporal as it arrives, by starting
one standalone Nexus operation per partition batch through a Nexus endpoint.

### 1. Implement the receiver

Implement `KafkaMessageReceiver.receive` — a Nexus service you write, in any Temporal SDK. Its
input is `ReceiveInput{records: [Record...]}`: messages from one partition in offset order
(schema in `kafka.nexusrpc.yaml`; Go types in `generated/kafkaapi`; bytes are base64 in JSON).
Its output is empty. Back it with a workflow so the work is durable.
`test/relayreceiver/main.go` is a working Go example to copy.

Make the receiver idempotent. Delivery is at least once, and batches can overlap when
redelivered. Drop messages already handled using `topic` + `partition` + `offset`.

### 2. Create a dead-letter topic

Create a topic in Kafka for undeliverable messages, or plan to set `DELETE_DEAD_LETTERS=true`.
The relay won't start with neither, or both.

### 3. Create the Nexus endpoint

```bash
temporal operator nexus endpoint create \
  --name <endpoint> \
  --target-namespace <receiver namespace> \
  --target-task-queue <receiver task queue>
```

On Temporal Cloud, create it in the Cloud UI or with `tcld`, and allow the relay's namespace to
call it.

### 4. Set environment variables and run

| Variable | Set it to |
|---|---|
| `KAFKA_BROKERS` | Your Kafka brokers, comma-separated `host:port`. |
| `KAFKA_TOPIC` | The topic to stream. |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | A SASL/SCRAM-SHA-256 login, if your Kafka needs one. |
| `KAFKA_GROUP` | The consumer group the relay joins. Relays with the same consumer group share the topic's partitions. |
| `KAFKA_DEAD_LETTER_TOPIC` | An existing topic for messages that can't be delivered. Or leave it unset and set `DELETE_DEAD_LETTERS=true` to delete them instead. |
| `RELAY_ENDPOINT` | The Nexus endpoint to deliver to. It routes to the service that receives the messages, `KafkaMessageReceiver.receive`. |
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` | Your Temporal server and namespace (default `connectors`), plus `TEMPORAL_API_KEY` for Temporal Cloud. The namespace needs standalone Nexus operations. |

Then run:

```bash
go run ./relay
# or build first: go build -o bin/ ./relay && ./bin/relay
```

Scale out by running more relays with the same `KAFKA_GROUP`, up to one per partition.

## Run the connector

The connector serves the topic as `KafkaService` with `read` and `write` operations.

### 1. Set environment variables and run

| Variable | Set it to |
|---|---|
| `KAFKA_BROKERS` | Your Kafka brokers, comma-separated `host:port`. |
| `KAFKA_TOPIC` | The topic this connector reads and writes. Callers can't choose another. |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | A SASL/SCRAM-SHA-256 login, if your Kafka needs one. |
| `KAFKA_GROUP` | Optional. The only consumer group reads may use. Leave it unset to let each read name its own. |
| `NEXUS_TASK_QUEUE` | The task queue to serve (default `nexus-connector-kafka`). Give each connector for a different topic or consumer group its own. |
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` | Your Temporal server and namespace (default `connectors`), plus `TEMPORAL_API_KEY` for Temporal Cloud. The namespace needs standalone activities with completion callbacks. |

Then run:

```bash
go run ./connector
# or build first: go build -o bin/ ./connector && ./bin/connector
```

### 2. Create the Nexus endpoint

Target the connector's namespace and `NEXUS_TASK_QUEUE` exactly. Reads run on a second queue,
`<NEXUS_TASK_QUEUE>-reads`; never target that one.

```bash
temporal operator nexus endpoint create \
  --name <endpoint> \
  --target-namespace connectors \
  --target-task-queue nexus-connector-kafka \
  --description-file ./endpoint-description.md
```

## Starting Temporal

The namespace the connector and the relay run in needs two dynamic config values:

```bash
temporal server start-dev \
  --dynamic-config-value activity.enableCallbacks=true \
  --dynamic-config-value nexusoperation.enableStandalone=true \
  --namespace connectors \
  --namespace kafka-test
```

The `connectors` namespace is where the relay and the connector run. `kafka-test` is where test
programs run. On Temporal Cloud, confirm with Temporal that both features are enabled for your
namespace.

## Work out your settings in a browser

`configui` is a page for working out the variables before you run either program:

- It shows every variable the relay or the connector reads, with what it does and its
  default. Fields start with the values set in the terminal you started it from.
- **Test** sets the variables in the page's own process, then reads the topic's last message
  using the programs' own Kafka code. Nothing is committed and no consumer group is joined.
- **Variables to set** lists the `export` lines to use where the program runs.

```bash
go run ./configui               # http://localhost:8090; -addr localhost:9000 for another port
```

The page listens on localhost only, since it handles credentials. To try it against a local Kafka,
run `test/kafka/scripts/configui.sh` from the repository root (requires Docker): it starts the
local Kafka, seeds it, writes one more message, and opens the page with the local Kafka's settings
pre-filled. See [Testing](testing.md) for the full demo.

## Connecting to Temporal Cloud

Both programs connect to Temporal the way the `temporal` CLI does: set `TEMPORAL_ADDRESS` to your
namespace's gRPC endpoint, `TEMPORAL_NAMESPACE` to your namespace name, and `TEMPORAL_API_KEY`
to an API key. TLS settings (`TEMPORAL_TLS_*`) and the CLI's profile file also work.

The namespace defaults to `connectors`. To use another, including `default`, set
`TEMPORAL_NAMESPACE` or the profile file's `namespace`; the variable wins if both are set.

All Nexus connectors default to the same `connectors` namespace. They are kept apart by task
queue: each connector serves its own (`NEXUS_TASK_QUEUE`, default `nexus-connector-kafka`). Two
Kafka connectors in one namespace, such as one per topic, need different `NEXUS_TASK_QUEUE`
values.

The namespace the connector and the relay run in needs:
- **Standalone Nexus operations** (for the relay)
- **Standalone activities with completion callbacks** (for the connector)

Confirm with Temporal that both are enabled for your namespace before running.

## Full settings reference

Both programs connect to Temporal with `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` (default
`connectors`), `TEMPORAL_API_KEY`, `TEMPORAL_TLS_*`, and the CLI's profile file.

| Variable | Used by | Meaning |
|---|---|---|
| `KAFKA_BROKERS` | both | Comma-separated `host:port` list. Required. |
| `KAFKA_TOPIC` | both | The topic. Required. |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | both | SASL/SCRAM-SHA-256 login. Leave unset for no authentication. |
| `KAFKA_START_FROM` | both | `oldest` (default) or `newest`; see [How it works](how-it-works.md#where-reading-starts). |
| `KAFKA_MAX_BATCH_BYTES` | connector | Most bytes a read's result encodes to. Default 2097152. |
| `KAFKA_MAX_DURATION_MINUTES` | connector | Longest a read or write retries, in whole minutes, at least 1. Default: 10 for a write, a read's time limit plus 5 for a read. Retries also stop 10 seconds before the caller's timeout. |
| `KAFKA_GROUP` | relay | The consumer group to join. Required. |
| `KAFKA_GROUP` | connector | The only consumer group reads may use; see [How it works](how-it-works.md#the-connector). Unset, each read names one. |
| `NEXUS_TASK_QUEUE` | connector | The task queue its Nexus endpoint targets. Reads run on this name with `-reads` added. Default `nexus-connector-kafka`. |
| `KAFKA_DEAD_LETTER_TOPIC` | relay | Where undeliverable messages go. Must already exist. Required unless `DELETE_DEAD_LETTERS` is `true`. |
| `DELETE_DEAD_LETTERS` | relay | `true` to delete undeliverable messages instead of writing them to a topic; then leave `KAFKA_DEAD_LETTER_TOPIC` unset. Default `false`. |
| `KAFKA_BATCH_SIZE` | relay | Most messages per batch. Default 100. Set 1 to rule out duplicates. |
| `KAFKA_BATCH_TIME_LIMIT` | relay | How long a batch waits to fill, such as `500ms`. Default 0. |
| `RELAY_ENDPOINT` | relay | The Nexus endpoint to deliver to. Required. |
| `RELAY_SERVICE`, `RELAY_OPERATION` | relay | The service and operation. Default `KafkaMessageReceiver` and `receive`. |
| `RELAY_OPERATION_TIMEOUT` | relay | How long each operation may take. Default `24h`. |
| `RELAY_MAX_INPUT_BYTES` | relay | The largest encoded input sent to an operation. Default 2031616 (2 MiB less 64 KiB). |
