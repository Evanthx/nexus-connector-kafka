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

To work them out and test them in a browser first, use the
[setup page](#work-out-your-settings-in-a-browser). Every variable the relay reads is in
[Environment variables](#environment-variables).

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

To work them out and test them in a browser first, use the
[setup page](#work-out-your-settings-in-a-browser). Every variable the connector reads is in
[Environment variables](#environment-variables).

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

To work out the variables in a form and test them before starting either program, run:

```bash
make configui    # or: go run ./configui --addr localhost:8090
```

and open <http://localhost:8090>. Choose the relay or the connector at the top. The page shows
every variable in [Environment variables](#environment-variables) with its help and default, and
the value set in the terminal you started it from. Secrets (`KAFKA_PASSWORD`, `TEMPORAL_API_KEY`)
show only as set or not set; a secret you type is used for the test only, never shown again or
saved. **Test** checks:

- **Settings**: every variable, as the program checks them at startup, listing every problem at
  once.
- **Temporal**: it connects as the program would, then checks that the namespace exists. For the
  connector, it checks that a Nexus endpoint targets the namespace and `NEXUS_TASK_QUEUE`, and
  shows the command to create one if none does. For the relay, it checks that the endpoint
  `RELAY_ENDPOINT` names exists, and shows where it leads.
- **Kafka**: it connects with the programs' own Kafka code, finds the topic, and reads its last
  message, without joining a consumer group or committing anything. A failure shows Kafka's whole
  error. For the relay, it also checks that `KAFKA_DEAD_LETTER_TOPIC` exists; for a connector with
  `KAFKA_GROUP`, that no live consumer, such as a relay, is in that consumer group, which would
  make every read fail with `GroupInUse`.

The page ends with the `export` lines to set, with a `<set NAME>` placeholder for each secret. It
never changes the program's environment, and it only answers on localhost, since it handles
credentials.

To try it against a local Kafka, run `test/kafka/scripts/configui.sh` from the repository root
(requires Docker): it starts the local Kafka, seeds it, writes one more message to be the topic's
last, and runs the page with the local Kafka's settings filled in. See [Testing](testing.md) for the
full demo.

## Connecting to Temporal Cloud

Both programs connect to Temporal the way the `temporal` CLI does: set `TEMPORAL_ADDRESS` to your
namespace's gRPC endpoint, `TEMPORAL_NAMESPACE` to your namespace name, and `TEMPORAL_API_KEY`
to an API key. The CLI's profile file and the other `TEMPORAL_*` variables in the
[Temporal tables](#environment-variables) also work.

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

## Environment variables

Set these where each program runs. The [setup page](#work-out-your-settings-in-a-browser) shows
the same lists and tests your values. The tables below are generated from `setup/settings.go` and,
for the Kafka variables both programs share, `kafka/settings.go`, with `make docs`: change a
variable there, not here.

<!-- settings:start -->
**Nexus Connector Relay** (`go run ./relay`):

### Kafka

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `KAFKA_BROKERS` | yes |  | The Kafka brokers to connect to, as a comma-separated list of `host:port`, such as `broker1:9092,broker2:9092`. One reachable broker is enough to find the rest. |
| `KAFKA_TOPIC` | yes |  | The topic the program serves. One process serves one topic; run one per topic. |
| `KAFKA_USERNAME` |  | none: no authentication | The SASL/SCRAM-SHA-256 username. Leave it and `KAFKA_PASSWORD` unset if Kafka needs no login. TLS and other SASL mechanisms are not supported. |
| `KAFKA_PASSWORD` (secret) |  | none | The password for `KAFKA_USERNAME`. The startup report shows only whether it is set. |
| `KAFKA_START_FROM` |  | oldest | Where a consumer group with no committed offset starts reading a partition: `oldest`, every message still in the topic, or `newest`, only messages written from then on. See [How it works](how-it-works.md#where-reading-starts). |

### Relay

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `KAFKA_GROUP` | yes |  | The consumer group the relay joins. Relays with the same consumer group share the topic's partitions. Don't share it with a connector reader or any other consumer. |
| `KAFKA_DEAD_LETTER_TOPIC` | unless DELETE_DEAD_LETTERS is true |  | An existing topic, other than `KAFKA_TOPIC`, for messages too large to deliver. Set this or `DELETE_DEAD_LETTERS=true`, not both. |
| `DELETE_DEAD_LETTERS` |  | false | `true` to log and skip messages too large to deliver, instead of writing them to `KAFKA_DEAD_LETTER_TOPIC`; then leave that unset. |
| `KAFKA_BATCH_SIZE` |  | 100 | The most messages in one batch, and so in one Nexus operation per partition. `1` for one operation per message, which also rules out a message being delivered twice. |
| `KAFKA_BATCH_TIME_LIMIT` |  | 0 (deliver as soon as there are messages) | How long a batch waits to fill, such as `500ms` or `1s`. Waiting makes fewer, fuller batches on a quiet topic, which costs fewer operations. |
| `RELAY_ENDPOINT` | yes |  | The Nexus endpoint the relay delivers batches to. It routes to the receiver you implement, `KafkaMessageReceiver.receive`. |
| `RELAY_SERVICE` |  | KafkaMessageReceiver | The Nexus service the receiver serves. |
| `RELAY_OPERATION` |  | receive | The operation started for each batch. |
| `RELAY_OPERATION_TIMEOUT` |  | 24h | How long each operation may take, such as `30m` or `24h`. More than 0. |
| `RELAY_MAX_INPUT_BYTES` |  | 2031616 (2 MiB less 64 KiB) | The largest encoded batch sent to one operation. Larger batches are split; a single larger message goes to the dead-letter topic. |

### Temporal

gRPC headers sent with every call to Temporal: `TEMPORAL_GRPC_META_<NAME>` sends header `<name>`, lower case with `-` for `_`. Shown as secrets, since a header can hold credentials.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `TEMPORAL_ADDRESS` |  | localhost:7233 | The Temporal frontend's `host:port`; for Temporal Cloud, the namespace's gRPC endpoint. The Temporal SDK's `envconfig` reads this and every variable below as the `temporal` CLI does: each one that is set overrides the profile file's value. See [Connecting to Temporal Cloud](#connecting-to-temporal-cloud). |
| `TEMPORAL_NAMESPACE` |  | connectors | The namespace the program runs in. The relay needs standalone Nexus operations enabled in it; the connector, standalone activities with completion callbacks. All connectors default to `connectors` and are kept apart by task queue. |
| `TEMPORAL_API_KEY` (secret) |  | none | An API key, for a Temporal Cloud namespace that uses API key authentication. Turns on TLS unless `TEMPORAL_TLS` is `false`. |
| `TEMPORAL_PROFILE` |  | default | The profile to read from the profile file. If it is set and the file has no such profile, the program can't connect; `default` may be missing. |
| `TEMPORAL_CONFIG_FILE` |  | temporalio/temporal.toml in the user's config directory | The profile file, which `temporal config set` writes. A missing file is fine: only the variables apply. The user's config directory is `~/.config` on Linux (or `$XDG_CONFIG_HOME`) and `~/Library/Application Support` on macOS. |
| `TEMPORAL_TLS` |  | on with an API key or any TLS setting, else off | `true` or `1` to connect with TLS, `false` or `0` not to. Any other value is ignored. |
| `TEMPORAL_TLS_CLIENT_CERT_PATH` |  | none | The path to the client certificate for mTLS, with `TEMPORAL_TLS_CLIENT_KEY_PATH`. |
| `TEMPORAL_TLS_CLIENT_CERT_DATA` |  | none | The client certificate for mTLS itself, in PEM, instead of a path, with `TEMPORAL_TLS_CLIENT_KEY_DATA`. |
| `TEMPORAL_TLS_CLIENT_KEY_PATH` |  | none | The path to the client certificate's private key, with `TEMPORAL_TLS_CLIENT_CERT_PATH`. |
| `TEMPORAL_TLS_CLIENT_KEY_DATA` (secret) |  | none | The client certificate's private key itself, in PEM, instead of a path, with `TEMPORAL_TLS_CLIENT_CERT_DATA`. |
| `TEMPORAL_TLS_SERVER_CA_CERT_PATH` |  | the system's | The path to the CA certificate to verify the server with, for a server whose certificate the system doesn't trust. |
| `TEMPORAL_TLS_SERVER_CA_CERT_DATA` |  | the system's | The CA certificate itself, in PEM, instead of a path. |
| `TEMPORAL_TLS_SERVER_NAME` |  | the address's host | The name to expect on the server's certificate, and to send as SNI. |
| `TEMPORAL_TLS_DISABLE_HOST_VERIFICATION` |  | false | `true` or `1` to accept a server certificate that doesn't name the host. For testing only. |
| `TEMPORAL_CLIENT_AUTHORITY` |  | the address | The `:authority` header to send, for a proxy or load balancer that routes on it. |
| `TEMPORAL_GRPC_META_<NAME>` (secret) |  |  | The header's value. |

### Network

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `HTTPS_PROXY` |  | none | *Read by gRPC from the process's environment; also as `https_proxy` when unset or empty.* The proxy for the program's connection to Temporal, such as `http://proxy.example:8080`, or `http://user:password@proxy.example:8080` for a proxy that asks for a password. Kafka brokers are always reached directly, never through it. Connections to `localhost` and loopback addresses never use a proxy. |
| `NO_PROXY` |  | none | *Read by gRPC from the process's environment; also as `no_proxy` when unset or empty.* Hosts the connection to Temporal reaches without `HTTPS_PROXY`: a comma-separated list of domains (each with its subdomains; with a leading `.`, subdomains only), IP addresses or CIDR ranges, each optionally with a port, or `*` for every host. Kafka doesn't use it. |

**Nexus Connector** (`go run ./connector`):

### Kafka

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `KAFKA_BROKERS` | yes |  | The Kafka brokers to connect to, as a comma-separated list of `host:port`, such as `broker1:9092,broker2:9092`. One reachable broker is enough to find the rest. |
| `KAFKA_TOPIC` | yes |  | The topic the program serves. One process serves one topic; run one per topic. |
| `KAFKA_USERNAME` |  | none: no authentication | The SASL/SCRAM-SHA-256 username. Leave it and `KAFKA_PASSWORD` unset if Kafka needs no login. TLS and other SASL mechanisms are not supported. |
| `KAFKA_PASSWORD` (secret) |  | none | The password for `KAFKA_USERNAME`. The startup report shows only whether it is set. |
| `KAFKA_START_FROM` |  | oldest | Where a consumer group with no committed offset starts reading a partition: `oldest`, every message still in the topic, or `newest`, only messages written from then on. See [How it works](how-it-works.md#where-reading-starts). |

### Connector

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `KAFKA_GROUP` |  | none: each read names its consumer group | The only consumer group reads through this connector may use. Unset, each read names its own. Never one a relay or another live consumer uses: reads then fail with `GroupInUse`. See [How it works](how-it-works.md#the-connector). |
| `NEXUS_TASK_QUEUE` |  | nexus-connector-kafka | The task queue the connector's Nexus endpoint targets. Reads run on this name with `-reads` added; never target that one. Give each connector for another topic or consumer group its own. |
| `KAFKA_MAX_BATCH_BYTES` |  | 2097152 (2 MiB) | The most bytes a read's result may encode to, more than 65536. Temporal's payload limit is 2 MiB by default. |
| `KAFKA_MAX_DURATION_MINUTES` |  | 10 for a write; a read's time limit plus 5 for a read | The longest a read or write keeps retrying, in whole minutes, at least 1; a read never less than its time limit plus 1. Either way it stops 10 seconds before the caller's Nexus operation times out. |

### Temporal

gRPC headers sent with every call to Temporal: `TEMPORAL_GRPC_META_<NAME>` sends header `<name>`, lower case with `-` for `_`. Shown as secrets, since a header can hold credentials.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `TEMPORAL_ADDRESS` |  | localhost:7233 | The Temporal frontend's `host:port`; for Temporal Cloud, the namespace's gRPC endpoint. The Temporal SDK's `envconfig` reads this and every variable below as the `temporal` CLI does: each one that is set overrides the profile file's value. See [Connecting to Temporal Cloud](#connecting-to-temporal-cloud). |
| `TEMPORAL_NAMESPACE` |  | connectors | The namespace the program runs in. The relay needs standalone Nexus operations enabled in it; the connector, standalone activities with completion callbacks. All connectors default to `connectors` and are kept apart by task queue. |
| `TEMPORAL_API_KEY` (secret) |  | none | An API key, for a Temporal Cloud namespace that uses API key authentication. Turns on TLS unless `TEMPORAL_TLS` is `false`. |
| `TEMPORAL_PROFILE` |  | default | The profile to read from the profile file. If it is set and the file has no such profile, the program can't connect; `default` may be missing. |
| `TEMPORAL_CONFIG_FILE` |  | temporalio/temporal.toml in the user's config directory | The profile file, which `temporal config set` writes. A missing file is fine: only the variables apply. The user's config directory is `~/.config` on Linux (or `$XDG_CONFIG_HOME`) and `~/Library/Application Support` on macOS. |
| `TEMPORAL_TLS` |  | on with an API key or any TLS setting, else off | `true` or `1` to connect with TLS, `false` or `0` not to. Any other value is ignored. |
| `TEMPORAL_TLS_CLIENT_CERT_PATH` |  | none | The path to the client certificate for mTLS, with `TEMPORAL_TLS_CLIENT_KEY_PATH`. |
| `TEMPORAL_TLS_CLIENT_CERT_DATA` |  | none | The client certificate for mTLS itself, in PEM, instead of a path, with `TEMPORAL_TLS_CLIENT_KEY_DATA`. |
| `TEMPORAL_TLS_CLIENT_KEY_PATH` |  | none | The path to the client certificate's private key, with `TEMPORAL_TLS_CLIENT_CERT_PATH`. |
| `TEMPORAL_TLS_CLIENT_KEY_DATA` (secret) |  | none | The client certificate's private key itself, in PEM, instead of a path, with `TEMPORAL_TLS_CLIENT_CERT_DATA`. |
| `TEMPORAL_TLS_SERVER_CA_CERT_PATH` |  | the system's | The path to the CA certificate to verify the server with, for a server whose certificate the system doesn't trust. |
| `TEMPORAL_TLS_SERVER_CA_CERT_DATA` |  | the system's | The CA certificate itself, in PEM, instead of a path. |
| `TEMPORAL_TLS_SERVER_NAME` |  | the address's host | The name to expect on the server's certificate, and to send as SNI. |
| `TEMPORAL_TLS_DISABLE_HOST_VERIFICATION` |  | false | `true` or `1` to accept a server certificate that doesn't name the host. For testing only. |
| `TEMPORAL_CLIENT_AUTHORITY` |  | the address | The `:authority` header to send, for a proxy or load balancer that routes on it. |
| `TEMPORAL_GRPC_META_<NAME>` (secret) |  |  | The header's value. |

### Network

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `HTTPS_PROXY` |  | none | *Read by gRPC from the process's environment; also as `https_proxy` when unset or empty.* The proxy for the program's connection to Temporal, such as `http://proxy.example:8080`, or `http://user:password@proxy.example:8080` for a proxy that asks for a password. Kafka brokers are always reached directly, never through it. Connections to `localhost` and loopback addresses never use a proxy. |
| `NO_PROXY` |  | none | *Read by gRPC from the process's environment; also as `no_proxy` when unset or empty.* Hosts the connection to Temporal reaches without `HTTPS_PROXY`: a comma-separated list of domains (each with its subdomains; with a leading `.`, subdomains only), IP addresses or CIDR ranges, each optionally with a port, or `*` for every host. Kafka doesn't use it. |

<!-- settings:end -->
