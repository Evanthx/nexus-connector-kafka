# Setup

How to configure and run the Kafka connector's two programs: the Inbound Connector and the Outbound
Connector. For a guided walkthrough with a local dev server and Docker Kafka, see
[Testing](testing.md).

## Prerequisites

- **Go 1.26** or later.
- **Temporal CLI 1.9.1** or later (it includes Temporal Server 1.32). Check with
  `temporal --version`.
- **Docker**, for the local Kafka (if using the demo setup).

## Run the Inbound Connector

The Inbound Connector streams every message from one or more Kafka topics into Temporal as it
arrives. Each topic is a **route**: for each, it joins a consumer group and starts one standalone
Nexus operation per partition batch through the route's Nexus endpoint.

### 1. Implement the receiver

Implement `KafkaMessageReceiver.receive` — a Nexus service you write, in any Temporal SDK. Its
input is `ReceiveInput{records: [Record...]}`: messages from one partition in offset order
(schema in `kafka.nexusrpc.yaml`; Go types in `generated/kafkaapi`; bytes are base64 in JSON).
Its output is empty. Back it with a workflow so the work is durable.
`test/inboundreceiver/main.go` is a working Go example to copy. One receiver can serve several
routes (each record names its `topic`), or each route can have its own.

Make the receiver idempotent. Delivery is at least once, and batches can overlap when
redelivered. Drop messages already handled using `topic` + `partition` + `offset`.

### 2. Create the dead-letter topics

For each route, create a topic in Kafka for undeliverable messages, or plan to set
`deleteDeadLetters` to `true`. A route needs one or the other, not both. Routes can share a
dead-letter topic, but it can't be a topic any route streams.

### 3. Create the Nexus endpoints

One per receiver:

```bash
temporal operator nexus endpoint create \
  --name <endpoint> \
  --target-namespace <receiver namespace> \
  --target-task-queue <receiver task queue>
```

On Temporal Cloud, create it in the Cloud UI or with `tcld`, and allow the Inbound Connector's
namespace to call it.

### 4. Write the routes, set environment variables and run

The routes are JSON, in `INBOUND_CONFIG` or in a file named by `INBOUND_CONFIG_FILE`. See
[Inbound routes](#inbound-routes). The smallest has one route:

```json
{"routes": [{"topic": "orders", "endpoint": "orders-receiver", "deadLetterTopic": "orders-dlq"}]}
```

| Variable | Set it to |
|---|---|
| `KAFKA_BROKERS` | Your Kafka brokers, comma-separated `host:port`. |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | Your Kafka username and password, if your Kafka needs a login. See [Connecting to Kafka](#connecting-to-kafka). |
| `KAFKA_TLS` | `true` if your Kafka needs encrypted connections, as hosted Kafka services do. |
| `INBOUND_CONFIG` or `INBOUND_CONFIG_FILE` | The routes, as JSON, or the path of a file holding them. Exactly one. |
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` | Your Temporal server and namespace (default `connectors`), plus `TEMPORAL_API_KEY` for Temporal Cloud. The namespace needs standalone Nexus operations. |

To work them out and test them in a browser first, use the
[setup page](#work-out-your-settings-in-a-browser). Every variable the Inbound Connector reads is in
[Environment variables](#environment-variables).

Then run:

```bash
go run ./inbound
# or build first: go build -o bin/ ./inbound && ./bin/inbound
```

The Inbound Connector waits until Kafka has every route's topic and dead-letter topic before it
starts any route. If one route stops on an error, the others stop too, every consumer group is
left, and the process exits with an error, for your process manager to restart. Changing the routes
takes a restart.

Scale out by running more Inbound Connectors with the same routes, up to one per partition of the
busiest topic. Every process runs every route; Inbound Connectors with the same route (topic and
endpoint, or `group`) share its consumer group. To give a busy topic processes of its own, run it
in a separate deployment with its own routes.

## Run the Outbound Connector

The Outbound Connector serves `KafkaService` with a `write` operation. Each write names its topic,
which must be one the Outbound Connector allows (`KAFKA_TOPICS`). To consume a topic, use the
Inbound Connector.

### 1. Set environment variables and run

| Variable | Set it to |
|---|---|
| `KAFKA_BROKERS` | Your Kafka brokers, comma-separated `host:port`. |
| `KAFKA_TOPICS` | The topics callers may write to, comma-separated exact names. A write to any other topic is `Rejected` and writes nothing. |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | Your Kafka username and password, if your Kafka needs a login. See [Connecting to Kafka](#connecting-to-kafka). |
| `KAFKA_TLS` | `true` if your Kafka needs encrypted connections, as hosted Kafka services do. |
| `NEXUS_TASK_QUEUE` | The task queue to serve (default `nexus-connector-kafka`). Give each Outbound Connector in a namespace its own. |
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` | Your Temporal server and namespace (default `connectors`), plus `TEMPORAL_API_KEY` for Temporal Cloud. The Outbound Connector needs no special server feature. |

Everyone who can call the endpoint can write to every topic on the list. For callers with
different access, run separate Outbound Connectors, each with its own `KAFKA_TOPICS` and
`NEXUS_TASK_QUEUE`, behind endpoints of their own. Every listed topic must exist when the Outbound
Connector starts; topics Kafka marks internal are refused. Changing the list takes a restart.

To work them out and test them in a browser first, use the [setup
page](#work-out-your-settings-in-a-browser). Every variable the Outbound Connector reads is in
[Environment variables](#environment-variables).

Then run:

```bash
go run ./outbound
# or build first: go build -o bin/ ./outbound && ./bin/outbound
```

### 2. Create the Nexus endpoint

Target the Outbound Connector's namespace and `NEXUS_TASK_QUEUE` exactly.

```bash
temporal operator nexus endpoint create \
  --name <endpoint> \
  --target-namespace connectors \
  --target-task-queue nexus-connector-kafka \
  --description-file ./endpoint-description.md
```

## Starting Temporal

The Inbound Connector's namespace needs standalone Nexus operations, a dynamic config value on a
dev server. The Outbound Connector needs no special server feature.

```bash
temporal server start-dev \
  --dynamic-config-value nexusoperation.enableStandalone=true \
  --namespace connectors \
  --namespace kafka-test
```

The `connectors` namespace is where the Inbound Connector and the Outbound Connector run.
`kafka-test` is where test programs run. On Temporal Cloud, confirm with Temporal that standalone
Nexus operations are enabled for the Inbound Connector's namespace.

## Work out your settings in a browser

To work out the variables in a form and test them before starting either program, run:

```bash
make configui    # or: go run ./configui --addr localhost:8090
```

and open <http://localhost:8090>. Choose the Inbound Connector or the Outbound Connector at the top.
The page shows every variable in [Environment variables](#environment-variables) with its help and
default, and the value set in the terminal you started it from. Secrets (`KAFKA_PASSWORD`,
`TEMPORAL_API_KEY`) show only as set or not set; a secret you type is used for the test only, never
shown again or saved. **Test** checks:

- **Settings**: every variable, as the program checks them at startup, listing every problem at
  once.
- **Temporal**: it connects as the program would, then checks that the namespace exists. For the
  Outbound Connector, it checks that a Nexus endpoint targets the namespace and `NEXUS_TASK_QUEUE`,
  and shows the command to create one if none does.
- **Kafka** (Outbound Connector): for each topic in `KAFKA_TOPICS`, it connects with the programs'
  own Kafka code, finds the topic, and reads its last message, without joining a consumer group or
  committing anything. A failure shows Kafka's whole error.
- **Routes** (Inbound Connector), once the routes are valid: every route as resolved, with its
  defaults filled in; that each route's endpoint exists, and where it leads; then, for each route,
  its topic and last message, as above, and that its dead-letter topic exists.

`INBOUND_CONFIG` is a text area with an **Insert example** button, which fills in an example with
two routes, and a link to [Inbound routes](#inbound-routes).

The page ends with the `export` lines to set, with a `<set NAME>` placeholder for each secret. It
never changes the program's environment, and it only answers on localhost, since it handles
credentials.

To try it against a local Kafka, run `test/kafka/scripts/configui.sh` from the repository root
(requires Docker): it starts the local Kafka, seeds it, writes one more message to be the topic's
last, and runs the page with the local Kafka's settings filled in. See [Testing](testing.md) for the
full demo.

## Connecting to Kafka

Kafka has two separate settings, and your Kafka may need both: whether the connection is
encrypted (TLS), and how the program logs in. Whoever runs your Kafka can tell you which it uses,
or look at the settings of a client that already connects (`security.protocol` and
`sasl.mechanism` in a Java client's properties).

**Encryption (TLS).** Set `KAFKA_TLS=true` if your Kafka needs it, as hosted Kafka services such as
Confluent Cloud, AWS MSK, Aiven and Azure Event Hubs do. If your brokers' certificates come from
your company's own certificate authority rather than a public one, set
`KAFKA_TLS_SERVER_CA_CERT_PATH` to its certificate; this turns TLS on by itself.

**Logging in.** Choose the one your Kafka uses:

| Your Kafka logs in with | Set |
|---|---|
| Nothing | Nothing. |
| A username and password | `KAFKA_USERNAME`, `KAFKA_PASSWORD`, and `KAFKA_SASL_MECHANISM` to how Kafka checks them: `SCRAM-SHA-256` (the default), `SCRAM-SHA-512` (AWS MSK, Strimzi), or `PLAIN` (Confluent Cloud API keys, Azure Event Hubs). |
| A client certificate (mTLS) | `KAFKA_TLS_CLIENT_CERT_PATH` and `KAFKA_TLS_CLIENT_KEY_PATH`, the certificate and its private key, as PEM files. This turns TLS on by itself. |

`PLAIN` sends the password as it is, so use it only with TLS.

For example, Confluent Cloud:

```bash
export KAFKA_BROKERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
export KAFKA_TLS=true
export KAFKA_SASL_MECHANISM=PLAIN
export KAFKA_USERNAME=<API key>
export KAFKA_PASSWORD=<API secret>
```

The startup report shows the login method and whether TLS is on. If Kafka refuses the connection,
see [Troubleshooting](troubleshooting.md#reading-startup-logs).

Not supported yet: AWS IAM (MSK's own login), OAuth, and Kerberos.

## Connecting to Temporal Cloud

Both programs connect to Temporal the way the `temporal` CLI does: set `TEMPORAL_ADDRESS` to your
namespace's gRPC endpoint, `TEMPORAL_NAMESPACE` to your namespace name, and `TEMPORAL_API_KEY`
to an API key. The CLI's profile file and the other `TEMPORAL_*` variables in the
[Temporal tables](#environment-variables) also work.

The namespace defaults to `connectors`. To use another, including `default`, set
`TEMPORAL_NAMESPACE` or the profile file's `namespace`; the variable wins if both are set.

All Nexus connectors default to the same `connectors` namespace. They are kept apart by task
queue: each serves its own (here `NEXUS_TASK_QUEUE`, default `nexus-connector-kafka`). Two
Outbound Connectors in one namespace, such as one per group of callers, need different `NEXUS_TASK_QUEUE` values.

The Inbound Connector's namespace needs **standalone Nexus operations**. Confirm with Temporal
that they are enabled for your namespace before running it. The Outbound Connector needs no special
server feature.

## Inbound routes

The Inbound Connector's topics, and how each is delivered, are JSON: in `INBOUND_CONFIG`, or in a
file named by `INBOUND_CONFIG_FILE` (set exactly one). A file is easier to keep in version control
and to mount from a secret or config map; the variable is handy for a one-off.

The JSON has a `defaults` section, then `routes`, one per topic. A route takes each setting from
itself, else from `defaults`, else from the built-in default in the table below. `topic`,
`endpoint` and `group` go on each route, never in `defaults`. An empty string (`""`) or `null` is the same as
leaving the field out. A route that sets `deadLetterTopic` or `deleteDeadLetters` replaces the other
from `defaults`, so one route can delete its dead letters while the rest send them to a topic.
Parsing is strict: an unknown key, or a value of the wrong type, is an error, and every problem is
reported at once, each naming its route (`routes[1] (customers): ...`). Durations are strings, such
as `"1s"` or `"24h"`.

**Consumer groups.** A route without `group` joins `nexus-connector-kafka:<topic>:<endpoint>`. So
Inbound Connectors delivering the same topic to the same endpoint share a group and split its
partitions, and any other pair is kept apart. A `group` is used as it is. No two routes may end up
with the same group, and no topic may be in two routes.

This example is also the setup page's **Insert example**: `defaults` lists every field that can
have a default, with its built-in value; the first route has only what can't come from `defaults`;
the second sets every field, clearing the default dead-letter topic so that `deleteDeadLetters` can
be on.

```json
{
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
      "deadLetterTopic": "",
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
```

The fields (generated from `setup/routes.go` with `make docs`):

<!-- routes:start -->
| Field | In `defaults` | Default | Meaning |
|---|---|---|---|
| `topic` | no: on each route | required | The topic to stream. Each topic is in one route only. |
| `endpoint` | no: on each route | required | The Nexus endpoint the route delivers batches to. It routes to the receiver you implement, `KafkaMessageReceiver.receive`. |
| `group` | no: on each route | `nexus-connector-kafka:<topic>:<endpoint>` | The consumer group the route joins, used as is. Inbound Connectors with the same group share the topic's partitions. Don't share it with any other consumer; no two routes may use the same group. |
| `service` | yes | `KafkaMessageReceiver` | The Nexus service the receiver serves. |
| `operation` | yes | `receive` | The operation started for each batch. |
| `startFrom` | yes | `oldest` | Where a consumer group with no committed offset starts reading a partition: `oldest`, every message still in the topic, or `newest`, only messages written from then on. See [How it works](how-it-works.md#where-reading-starts). |
| `deadLetterTopic` | yes | none | An existing topic, not one any route streams, for messages too large to deliver. Set this or `deleteDeadLetters`, not both. On a route, either one replaces the other from `defaults`. |
| `deleteDeadLetters` | yes | `false` | `true` to log and skip messages too large to deliver, instead of writing them to `deadLetterTopic`. |
| `batchSize` | yes | `100` | The most messages in one batch, and so in one Nexus operation per partition. `1` for one operation per message, which also rules out a message being delivered twice. |
| `batchTimeLimit` | yes | `"0s"` (deliver as soon as there are messages) | How long a batch waits to fill, such as `"500ms"` or `"1s"`. Waiting makes fewer, fuller batches on a quiet topic, which costs fewer operations. |
| `operationTimeout` | yes | `"24h"` | How long each operation may take, such as `"30m"` or `"24h"`. More than 0. |
| `maxInputBytes` | yes | `2031616` (2 MiB less 64 KiB) | The largest encoded batch sent to one operation. Larger batches are split; a single larger message goes to the dead-letter topic. |
| `maxMessagesPerMinute` | yes | `0` (no limit) | The most messages this route sends to Temporal a minute, such as `600`, spread evenly: it waits before each operation until the batch fits, and the waiting messages stay in Kafka. At least `batchSize`; `0` for no limit. The limit is per route and per process: three Inbound Connectors with `600` send up to 1,800 a minute between them. Messages sent to the dead-letter topic don't count. |
| `ordering` | yes | `none` | `none` to start each batch's operation without waiting for the one before, as fast as possible, but a later batch can be processed before an earlier one. `partition` to process each partition's messages in order: wait for each operation to complete before starting the next for that partition, and commit only then. A failed operation isn't started again or dead-lettered: it is logged, handled in Temporal, and the partition carries on. See [Ordering](how-it-works.md#ordering). |
<!-- routes:end -->

## Environment variables

Set these where each program runs. The [setup page](#work-out-your-settings-in-a-browser) shows
the same lists and tests your values. The tables below are generated from `setup/settings.go` and,
for the Kafka variables both programs share, `setup/kafka.go`, with `make docs`: change a variable
there, not here.

<!-- settings:start -->
**Inbound Connector** (`go run ./inbound`):

### Kafka

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `KAFKA_BROKERS` | yes |  | The Kafka brokers to connect to, as a comma-separated list of `host:port`, such as `broker1:9092,broker2:9092`. One reachable broker is enough to find the rest. |
| `KAFKA_USERNAME` |  | none: no login | Your Kafka username, if Kafka needs a login. Leave it and `KAFKA_PASSWORD` unset if it doesn't. See [Connecting to Kafka](#connecting-to-kafka). |
| `KAFKA_PASSWORD` (secret) |  | none | The password for `KAFKA_USERNAME`. The startup report shows only whether it is set. |
| `KAFKA_SASL_MECHANISM` |  | SCRAM-SHA-256 | How Kafka checks `KAFKA_USERNAME` and `KAFKA_PASSWORD`: `SCRAM-SHA-256`, `SCRAM-SHA-512` (AWS MSK), or `PLAIN` (Confluent Cloud, Azure Event Hubs). `PLAIN` sends the password as it is, so use it with TLS. |
| `KAFKA_TLS` |  | on if any KAFKA_TLS_*_PATH is set, else off | `true` to connect to Kafka with TLS (encrypted), as most hosted Kafka services require. The system's certificate authorities check the brokers' certificates unless `KAFKA_TLS_SERVER_CA_CERT_PATH` is set. |
| `KAFKA_TLS_SERVER_CA_CERT_PATH` |  | the system's | The path to the certificate (PEM) of the authority that signed the brokers' certificates, if it isn't a public one. Turns on TLS. |
| `KAFKA_TLS_CLIENT_CERT_PATH` |  | none | For mTLS: the path to the client certificate (PEM) the program logs in with, with `KAFKA_TLS_CLIENT_KEY_PATH`. Turns on TLS. |
| `KAFKA_TLS_CLIENT_KEY_PATH` |  | none | For mTLS: the path to the client certificate's private key (PEM), with `KAFKA_TLS_CLIENT_CERT_PATH`. |
| `KAFKA_CLIENT_LOG_LEVEL` |  | warn | How much the Kafka client library (franz-go) logs, in the program's log; at `warn`, failed connections to brokers, failed logins and consumer group errors, which show why the program stopped making progress: `error`, `warn`, `info` or `debug`; `info` and `debug` log much more, for chasing a problem. |

### Inbound Connector

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `INBOUND_CONFIG` | unless INBOUND_CONFIG_FILE is set |  | The routes, as JSON: a `defaults` section, then `routes`, one per topic, each with its `topic` and `endpoint` and any setting that differs from `defaults`. Set this or `INBOUND_CONFIG_FILE`, not both. See [Inbound routes](#inbound-routes). |
| `INBOUND_CONFIG_FILE` | unless INBOUND_CONFIG is set |  | The path to a file holding the routes, as `INBOUND_CONFIG` would. Set this or `INBOUND_CONFIG`, not both. |
| `INBOUND_STATUS_INTERVAL` |  | 5m | When nothing has been delivered for this long, such as `5m` or `1h`, the Inbound Connector logs a status line for each route saying which partitions it has and when it last delivered, and again each interval after, so a quiet topic can be told apart from a stalled Inbound Connector. `0` to turn it off. |

### Temporal

gRPC headers sent with every call to Temporal: `TEMPORAL_GRPC_META_<NAME>` sends header `<name>`, lower case with `-` for `_`. Shown as secrets, since a header can hold credentials.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `TEMPORAL_ADDRESS` |  | localhost:7233 | The Temporal frontend's `host:port`; for Temporal Cloud, the namespace's gRPC endpoint. The Temporal SDK's `envconfig` reads this and every variable below as the `temporal` CLI does: each one that is set overrides the profile file's value. See [Connecting to Temporal Cloud](#connecting-to-temporal-cloud). |
| `TEMPORAL_NAMESPACE` |  | connectors | The namespace the program runs in. The Inbound Connector needs standalone Nexus operations enabled in it; the Outbound Connector needs nothing extra. All connectors default to `connectors` and are kept apart by task queue. |
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
| `TEMPORAL_CODEC_ENDPOINT` |  | none: payloads are not encoded | A payload codec server, such as `https://codec.example.com`, to encrypt every payload the program sends to Temporal and decrypt every payload it reads, so data reaches Temporal only encrypted. The same codec server the Temporal UI uses works. The program checks it at startup and waits for it. See [Encryption](encryption.md). |
| `TEMPORAL_CODEC_AUTH` (secret) |  | none | The `Authorization` header value to send to `TEMPORAL_CODEC_ENDPOINT`, such as `Bearer <token>`. |
| `TEMPORAL_GRPC_META_<NAME>` (secret) |  |  | The header's value. |

### Network

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `HTTPS_PROXY` |  | none | *Read by gRPC from the process's environment; also as `https_proxy` when unset or empty.* The proxy for the program's connection to Temporal, such as `http://proxy.example:8080`, or `http://user:password@proxy.example:8080` for a proxy that asks for a password. Kafka brokers are always reached directly, never through it. Connections to `localhost` and loopback addresses never use a proxy. |
| `NO_PROXY` |  | none | *Read by gRPC from the process's environment; also as `no_proxy` when unset or empty.* Hosts the connection to Temporal reaches without `HTTPS_PROXY`: a comma-separated list of domains (each with its subdomains; with a leading `.`, subdomains only), IP addresses or CIDR ranges, each optionally with a port, or `*` for every host. Kafka doesn't use it. |

**Outbound Connector** (`go run ./outbound`):

### Kafka

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `KAFKA_BROKERS` | yes |  | The Kafka brokers to connect to, as a comma-separated list of `host:port`, such as `broker1:9092,broker2:9092`. One reachable broker is enough to find the rest. |
| `KAFKA_USERNAME` |  | none: no login | Your Kafka username, if Kafka needs a login. Leave it and `KAFKA_PASSWORD` unset if it doesn't. See [Connecting to Kafka](#connecting-to-kafka). |
| `KAFKA_PASSWORD` (secret) |  | none | The password for `KAFKA_USERNAME`. The startup report shows only whether it is set. |
| `KAFKA_SASL_MECHANISM` |  | SCRAM-SHA-256 | How Kafka checks `KAFKA_USERNAME` and `KAFKA_PASSWORD`: `SCRAM-SHA-256`, `SCRAM-SHA-512` (AWS MSK), or `PLAIN` (Confluent Cloud, Azure Event Hubs). `PLAIN` sends the password as it is, so use it with TLS. |
| `KAFKA_TLS` |  | on if any KAFKA_TLS_*_PATH is set, else off | `true` to connect to Kafka with TLS (encrypted), as most hosted Kafka services require. The system's certificate authorities check the brokers' certificates unless `KAFKA_TLS_SERVER_CA_CERT_PATH` is set. |
| `KAFKA_TLS_SERVER_CA_CERT_PATH` |  | the system's | The path to the certificate (PEM) of the authority that signed the brokers' certificates, if it isn't a public one. Turns on TLS. |
| `KAFKA_TLS_CLIENT_CERT_PATH` |  | none | For mTLS: the path to the client certificate (PEM) the program logs in with, with `KAFKA_TLS_CLIENT_KEY_PATH`. Turns on TLS. |
| `KAFKA_TLS_CLIENT_KEY_PATH` |  | none | For mTLS: the path to the client certificate's private key (PEM), with `KAFKA_TLS_CLIENT_CERT_PATH`. |
| `KAFKA_CLIENT_LOG_LEVEL` |  | warn | How much the Kafka client library (franz-go) logs, in the program's log; at `warn`, failed connections to brokers, failed logins and consumer group errors, which show why the program stopped making progress: `error`, `warn`, `info` or `debug`; `info` and `debug` log much more, for chasing a problem. |

### Outbound Connector

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `KAFKA_TOPICS` | yes |  | The topics callers may write to, as a comma-separated list of exact names, such as `orders,billing`. A write names its topic; one not on this list is refused (`Rejected`) and nothing is written. Every topic must exist when the Outbound Connector starts. Topics Kafka marks internal are refused. |
| `NEXUS_TASK_QUEUE` |  | nexus-connector-kafka | The task queue the Outbound Connector's Nexus endpoint targets. Give each Outbound Connector in a namespace its own. |

### Temporal

gRPC headers sent with every call to Temporal: `TEMPORAL_GRPC_META_<NAME>` sends header `<name>`, lower case with `-` for `_`. Shown as secrets, since a header can hold credentials.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `TEMPORAL_ADDRESS` |  | localhost:7233 | The Temporal frontend's `host:port`; for Temporal Cloud, the namespace's gRPC endpoint. The Temporal SDK's `envconfig` reads this and every variable below as the `temporal` CLI does: each one that is set overrides the profile file's value. See [Connecting to Temporal Cloud](#connecting-to-temporal-cloud). |
| `TEMPORAL_NAMESPACE` |  | connectors | The namespace the program runs in. The Inbound Connector needs standalone Nexus operations enabled in it; the Outbound Connector needs nothing extra. All connectors default to `connectors` and are kept apart by task queue. |
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
| `TEMPORAL_CODEC_ENDPOINT` |  | none: payloads are not encoded | A payload codec server, such as `https://codec.example.com`, to encrypt every payload the program sends to Temporal and decrypt every payload it reads, so data reaches Temporal only encrypted. The same codec server the Temporal UI uses works. The program checks it at startup and waits for it. See [Encryption](encryption.md). |
| `TEMPORAL_CODEC_AUTH` (secret) |  | none | The `Authorization` header value to send to `TEMPORAL_CODEC_ENDPOINT`, such as `Bearer <token>`. |
| `TEMPORAL_GRPC_META_<NAME>` (secret) |  |  | The header's value. |

### Network

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `HTTPS_PROXY` |  | none | *Read by gRPC from the process's environment; also as `https_proxy` when unset or empty.* The proxy for the program's connection to Temporal, such as `http://proxy.example:8080`, or `http://user:password@proxy.example:8080` for a proxy that asks for a password. Kafka brokers are always reached directly, never through it. Connections to `localhost` and loopback addresses never use a proxy. |
| `NO_PROXY` |  | none | *Read by gRPC from the process's environment; also as `no_proxy` when unset or empty.* Hosts the connection to Temporal reaches without `HTTPS_PROXY`: a comma-separated list of domains (each with its subdomains; with a leading `.`, subdomains only), IP addresses or CIDR ranges, each optionally with a port, or `*` for every host. Kafka doesn't use it. |

<!-- settings:end -->
