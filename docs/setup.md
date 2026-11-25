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

The Inbound Connector streams every message from a Kafka topic into Temporal as it arrives, by
starting one standalone Nexus operation per partition batch through a Nexus endpoint.

### 1. Implement the receiver

Implement `KafkaMessageReceiver.receive` — a Nexus service you write, in any Temporal SDK. Its
input is `ReceiveInput{records: [Record...]}`: messages from one partition in offset order
(schema in `kafka.nexusrpc.yaml`; Go types in `generated/kafkaapi`; bytes are base64 in JSON).
Its output is empty. Back it with a workflow so the work is durable.
`test/inboundreceiver/main.go` is a working Go example to copy.

Make the receiver idempotent. Delivery is at least once, and batches can overlap when
redelivered. Drop messages already handled using `topic` + `partition` + `offset`.

### 2. Create a dead-letter topic

Create a topic in Kafka for undeliverable messages, or plan to set `DELETE_DEAD_LETTERS=true`.
The Inbound Connector won't start with neither, or both.

### 3. Create the Nexus endpoint

```bash
temporal operator nexus endpoint create \
  --name <endpoint> \
  --target-namespace <receiver namespace> \
  --target-task-queue <receiver task queue>
```

On Temporal Cloud, create it in the Cloud UI or with `tcld`, and allow the Inbound Connector's
namespace to call it.

### 4. Set environment variables and run

| Variable | Set it to |
|---|---|
| `KAFKA_BROKERS` | Your Kafka brokers, comma-separated `host:port`. |
| `KAFKA_TOPIC` | The topic to stream. |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | Your Kafka username and password, if your Kafka needs a login. See [Connecting to Kafka](#connecting-to-kafka). |
| `KAFKA_TLS` | `true` if your Kafka needs encrypted connections, as hosted Kafka services do. |
| `KAFKA_GROUP` | The consumer group the Inbound Connector joins. Inbound Connectors with the same consumer group share the topic's partitions. |
| `KAFKA_DEAD_LETTER_TOPIC` | An existing topic for messages that can't be delivered. Or leave it unset and set `DELETE_DEAD_LETTERS=true` to delete them instead. |
| `INBOUND_ENDPOINT` | The Nexus endpoint to deliver to. It routes to the service that receives the messages, `KafkaMessageReceiver.receive`. |
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` | Your Temporal server and namespace (default `connectors`), plus `TEMPORAL_API_KEY` for Temporal Cloud. The namespace needs standalone Nexus operations. |

To work them out and test them in a browser first, use the
[setup page](#work-out-your-settings-in-a-browser). Every variable the Inbound Connector reads is in
[Environment variables](#environment-variables).

Then run:

```bash
go run ./inbound
# or build first: go build -o bin/ ./inbound && ./bin/inbound
```

Scale out by running more Inbound Connectors with the same `KAFKA_GROUP`, up to one per partition.

## Run the Outbound Connector

The Outbound Connector serves the topic as `KafkaService` with a `write` operation. To consume the
topic, use the Inbound Connector.

### 1. Set environment variables and run

| Variable | Set it to |
|---|---|
| `KAFKA_BROKERS` | Your Kafka brokers, comma-separated `host:port`. |
| `KAFKA_TOPIC` | The topic this Outbound Connector writes to. Callers can't choose another. |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | Your Kafka username and password, if your Kafka needs a login. See [Connecting to Kafka](#connecting-to-kafka). |
| `KAFKA_TLS` | `true` if your Kafka needs encrypted connections, as hosted Kafka services do. |
| `NEXUS_TASK_QUEUE` | The task queue to serve (default `nexus-connector-kafka`). Give each Outbound Connector for a different topic its own. |
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` | Your Temporal server and namespace (default `connectors`), plus `TEMPORAL_API_KEY` for Temporal Cloud. The Outbound Connector needs no special server feature. |

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
  and shows the command to create one if none does. For the Inbound Connector, it checks that the endpoint
  `INBOUND_ENDPOINT` names exists, and shows where it leads.
- **Kafka**: it connects with the programs' own Kafka code, finds the topic, and reads its last
  message, without joining a consumer group or committing anything. A failure shows Kafka's whole
  error. For the Inbound Connector, it also checks that `KAFKA_DEAD_LETTER_TOPIC` exists.

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
Outbound Connectors in one namespace, such as one per topic, need different `NEXUS_TASK_QUEUE` values.

The Inbound Connector's namespace needs **standalone Nexus operations**. Confirm with Temporal
that they are enabled for your namespace before running it. The Outbound Connector needs no special
server feature.

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
| `KAFKA_TOPIC` | yes |  | The topic the program serves. One process serves one topic; run one per topic. |
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
| `KAFKA_GROUP` | yes |  | The consumer group the Inbound Connector joins. Inbound Connectors with the same consumer group share the topic's partitions. Don't share it with any other consumer. |
| `KAFKA_START_FROM` |  | oldest | Where a consumer group with no committed offset starts reading a partition: `oldest`, every message still in the topic, or `newest`, only messages written from then on. See [How it works](how-it-works.md#where-reading-starts). |
| `KAFKA_DEAD_LETTER_TOPIC` | unless DELETE_DEAD_LETTERS is true |  | An existing topic, other than `KAFKA_TOPIC`, for messages too large to deliver. Set this or `DELETE_DEAD_LETTERS=true`, not both. |
| `DELETE_DEAD_LETTERS` |  | false | `true` to log and skip messages too large to deliver, instead of writing them to `KAFKA_DEAD_LETTER_TOPIC`; then leave that unset. |
| `KAFKA_BATCH_SIZE` |  | 100 | The most messages in one batch, and so in one Nexus operation per partition. `1` for one operation per message, which also rules out a message being delivered twice. |
| `KAFKA_BATCH_TIME_LIMIT` |  | 0 (deliver as soon as there are messages) | How long a batch waits to fill, such as `500ms` or `1s`. Waiting makes fewer, fuller batches on a quiet topic, which costs fewer operations. |
| `INBOUND_ENDPOINT` | yes |  | The Nexus endpoint the Inbound Connector delivers batches to. It routes to the receiver you implement, `KafkaMessageReceiver.receive`. |
| `INBOUND_SERVICE` |  | KafkaMessageReceiver | The Nexus service the receiver serves. |
| `INBOUND_OPERATION` |  | receive | The operation started for each batch. |
| `INBOUND_OPERATION_TIMEOUT` |  | 24h | How long each operation may take, such as `30m` or `24h`. More than 0. |
| `INBOUND_MAX_INPUT_BYTES` |  | 2031616 (2 MiB less 64 KiB) | The largest encoded batch sent to one operation. Larger batches are split; a single larger message goes to the dead-letter topic. |
| `INBOUND_MAX_MESSAGES_PER_MINUTE` |  | no limit | The most messages this Inbound Connector sends to Temporal a minute, such as `600`, spread evenly: it waits before each operation until the batch fits, and the waiting messages stay in Kafka. A whole number, at least `KAFKA_BATCH_SIZE`; a full batch can go at once, so one minute can see up to this plus one batch. The limit is per process: three Inbound Connectors with `600` send up to 1,800 a minute between them. Messages sent to the dead-letter topic don't count. |
| `INBOUND_ORDERING` |  | none | `none` to start each batch's operation without waiting for the one before, as fast as possible and using every worker your receiver has, but a later batch can be processed before an earlier one. `partition` to process each partition's messages in order: wait for each operation to complete before starting the next for that partition, and commit only then. At most one operation per partition runs at a time, so parallelism is the number of partitions. A failed operation isn't started again or dead-lettered: it is logged, handled in Temporal, and the partition carries on. See [Ordering](how-it-works.md#ordering). |
| `INBOUND_STATUS_INTERVAL` |  | 5m | When nothing has been delivered for this long, such as `5m` or `1h`, the Inbound Connector logs a status line saying which partitions it has and when it last delivered, and again each interval after, so a quiet topic can be told apart from a stalled Inbound Connector. `0` to turn it off. |

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
| `KAFKA_TOPIC` | yes |  | The topic the program serves. One process serves one topic; run one per topic. |
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
| `NEXUS_TASK_QUEUE` |  | nexus-connector-kafka | The task queue the Outbound Connector's Nexus endpoint targets. Give each Outbound Connector for another topic its own. |

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
