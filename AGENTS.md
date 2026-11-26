# AGENTS.md

Instructions for AI agents that set up and run the Kafka connector. The `docs/` directory has the
full user-facing documentation; this file has what you need to act, and the mistakes to avoid.

| Doc | What is in it |
|---|---|
| [`docs/setup.md`](docs/setup.md) | Prerequisites, creating endpoints, running the programs, the setup page (`make configui`), and every environment variable (tables generated from the settings files). |
| [`docs/testing.md`](docs/testing.md) | Demo walkthrough with a local dev server and Docker Kafka, unit tests. |
| [`docs/clients.md`](docs/clients.md) | The contract, and generating typed clients for Go, Python, TypeScript, and Java. |
| [`docs/how-it-works.md`](docs/how-it-works.md) | Folders, the contract, the Outbound Connector and the Inbound Connector in depth, cost, possible future changes. |
| [`docs/encryption.md`](docs/encryption.md) | Encrypting every payload with the user's codec (codec server or compiled in), what is and isn't encrypted, the encrypted demo. |
| [`docs/troubleshooting.md`](docs/troubleshooting.md) | Startup diagnostics, error types, consumer group rules, known limitations. |

## What this is

A Temporal connector for **Kafka topics** (Go). It ships as two programs: the Inbound
Connector and the Outbound Connector.

| Program | Use it to | How it reaches Temporal |
|---|---|---|
| `inbound/` | Move every message from one or more topics into Temporal as it arrives. | For each route (a topic and its endpoint, in JSON), joins a Kafka consumer group and starts one standalone Nexus operation per partition of each batch, through the route's Nexus endpoint, on a service you implement: `KafkaMessageReceiver.receive`. One-way: it never receives results. |
| `outbound/` | Let workflows write to Kafka. | Serves `KafkaService.write` as a Nexus operation behind a Nexus endpoint. Workflows call it, naming a topic on its allowlist (`KAFKA_TOPICS`). |

Choose by the user's need. To consume a topic, use the Inbound Connector: the Outbound Connector
only writes. To write to Kafka, use the Outbound Connector. They can run side by side on the same
topics.

The Inbound Connector streams every topic in its routes; the Outbound Connector writes to every
topic in `KAFKA_TOPICS`. No wildcards: exact names only. Topics Kafka marks internal are refused;
names starting with `_` are fine. Changing topics takes a restart.

## Requirements

- Go 1.26 or later. Build with `go build -o bin/ ./inbound ./outbound`, or run with `go run`.
- Temporal Server 1.32 or later (Temporal CLI 1.9.1 or later for a local dev server), or Temporal
  Cloud.
  - The Inbound Connector's namespace needs **standalone Nexus operations**. Dev server:
    `--dynamic-config-value nexusoperation.enableStandalone=true`. On Temporal Cloud, ask the user
    to confirm with Temporal that they are enabled for the namespace. Don't assume.
  - The Outbound Connector needs no special server feature.
- Kafka reachable from the process. Supported: plaintext or TLS (`KAFKA_TLS`, optionally
  `KAFKA_TLS_SERVER_CA_CERT_PATH`), and no login, a username and password (`KAFKA_SASL_MECHANISM`:
  `SCRAM-SHA-256` default, `SCRAM-SHA-512`, `PLAIN`), or mTLS (`KAFKA_TLS_CLIENT_CERT_PATH`,
  `KAFKA_TLS_CLIENT_KEY_PATH`). AWS IAM, OAuth and Kerberos are not supported: tell the user
  instead of working around it. `docs/setup.md#connecting-to-kafka` maps hosted services to settings.

Both programs connect to Temporal like the `temporal` CLI: `TEMPORAL_ADDRESS`,
`TEMPORAL_NAMESPACE` (default `connectors`), `TEMPORAL_API_KEY`, `TEMPORAL_TLS_*`, and the
CLI's profile file. `connectors` applies only when neither the variable nor the profile sets a
namespace; any name works, including `default`. Nexus connectors share the `connectors`
namespace and are kept apart by task queue, so two Outbound Connectors in one namespace, such as
one per group of callers with different access, need different `NEXUS_TASK_QUEUE` values.

The setup page shows External variables (`HTTPS_PROXY`, `NO_PROXY`, read by gRPC for the Temporal
connection only, never by Kafka) read-only and masked from its own environment, and shows what the
selected Temporal profile file sets beside each `TEMPORAL_*` field.

## Run the Inbound Connector

1. **Implement the receiver.** `KafkaMessageReceiver.receive` is a Nexus service the user writes,
   in any Temporal SDK with Nexus handlers. Its input is `ReceiveInput{records: [Record...]}`:
   messages from one partition, in offset order (schema in `kafka.nexusrpc.yaml`; Go types in
   `generated/kafkaapi`; bytes are base64 in JSON). Its output is empty. Back it with a workflow,
   so the work is durable and isn't bound by a synchronous handler's deadline (under 10 seconds).
   `test/inboundreceiver/main.go` is a working Go example to copy. One receiver can serve several
   routes (each record names its `topic`).
2. **Make the receiver idempotent.** Delivery is at least once, and batches can overlap when
   redelivered. Drop messages already handled using `topic` + `partition` + `offset`. If
   duplicates are unacceptable, set the route's `batchSize` to 1.
3. **Create a Nexus endpoint** per receiver, targeting its namespace and task queue:

   ```bash
   temporal operator nexus endpoint create --name <endpoint> \
     --target-namespace <receiver namespace> --target-task-queue <receiver task queue>
   ```

   On Temporal Cloud, create it in the Cloud UI or with `tcld`, and allow the Inbound Connector's
   namespace to call it.
4. **Create the dead-letter topics** in Kafka, or plan to set `deleteDeadLetters` to `true`. Each
   route needs one or the other, not both. A dead-letter topic can't be a topic any route streams.
5. **Write the routes** as JSON, in `INBOUND_CONFIG` or a file named by `INBOUND_CONFIG_FILE`
   (exactly one). A `defaults` section, then `routes`; each route sets `topic` and `endpoint`, and
   overrides any default:

   ```json
   {
     "defaults": { "deadLetterTopic": "dead-letters", "batchSize": 100 },
     "routes": [
       { "topic": "orders", "endpoint": "orders-receiver" },
       { "topic": "customers", "endpoint": "customers-receiver", "ordering": "partition" }
     ]
   }
   ```

   Fields (built-in default): `topic`, `endpoint`, `group` (routes only; group defaults to
   `nexus-connector-kafka:<topic>:<endpoint>`), `service` (`KafkaMessageReceiver`), `operation`
   (`receive`), `startFrom` (`oldest` or `newest`), `deadLetterTopic`, `deleteDeadLetters`
   (`false`), `batchSize` (100), `batchTimeLimit` (`"0s"`; `"1s"` lets batches fill on a quiet
   topic, which costs fewer operations), `operationTimeout` (`"24h"`), `maxInputBytes`,
   `maxMessagesPerMinute` (0, no limit; per route and per process, at least `batchSize`),
   `ordering` (`none`, or `partition` to process each partition in order; see
   `docs/how-it-works.md#ordering`). Strict: unknown keys are errors. `""` or `null` is the same as
   leaving a field out. On a route, `deadLetterTopic` or `deleteDeadLetters` replaces the other
   from `defaults`. No two routes may share a topic or a consumer group. `docs/setup.md#inbound-routes`
   has the full table and example; `make configui`'s **Insert example** fills one in.
6. **Set the environment and run** `inbound`:

   | Variable | Value |
   |---|---|
   | `KAFKA_BROKERS` | Comma-separated `host:port`. |
   | `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_SASL_MECHANISM`, `KAFKA_TLS*` | Kafka login and TLS, if needed (see Requirements). |
   | `INBOUND_CONFIG` or `INBOUND_CONFIG_FILE` | The routes from step 5. |

   Optional: `INBOUND_STATUS_INTERVAL` (`5m`), `KAFKA_CLIENT_LOG_LEVEL` (`warn`). `docs/setup.md`
   lists every variable. It starts no route until Kafka has every route's topic and dead-letter
   topic.
7. **Scale out** by running more Inbound Connectors with the same routes, up to one per partition.
   Every process runs every route; split a busy topic into its own deployment.

## Run the Outbound Connector

1. **Set the environment and run** `outbound`:

   | Variable | Value |
   |---|---|
   | `KAFKA_BROKERS` | As for the Inbound Connector. |
   | `KAFKA_TOPICS` | The topics callers may write to, comma-separated exact names. Each must exist at startup. |
   | `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_SASL_MECHANISM`, `KAFKA_TLS*` | If needed. |
   | `NEXUS_TASK_QUEUE` | Default `nexus-connector-kafka`. Give each Outbound Connector in a namespace its own. |

   Everyone who can call the endpoint can write to every listed topic. For callers with different
   access, run separate Outbound Connectors with their own `KAFKA_TOPICS`, `NEXUS_TASK_QUEUE` and
   endpoint.

2. **Create a Nexus endpoint** that targets the Outbound Connector's namespace and
   `NEXUS_TASK_QUEUE` exactly.

   ```bash
   temporal operator nexus endpoint create --name <endpoint> \
     --target-namespace connectors --target-task-queue nexus-connector-kafka \
     --description-file ./endpoint-description.md
   ```

3. **Call it from a workflow** with a Nexus client for `KafkaService` on that endpoint.
   `test/nexuscaller/workflows.go` (`WriterWorkflow`) is a working Go example to copy. `write`
   takes `topic` (required; one in `KAFKA_TOPICS`) and `messages: [{key?, value, headers?}]` and
   returns `written`. Each message gets a `dedupe-id` header that stays the same when the write is
   retried. **Give the call a timeout** (`ScheduleToCloseTimeout`): the caller's Temporal server
   retries a failed write until it, and nothing else stops the retries.

## Encryption

To keep data from reaching Temporal unencrypted, set `TEMPORAL_CODEC_ENDPOINT` (and
`TEMPORAL_CODEC_AUTH` if needed) on both programs to the user's payload codec server, or build them
with a codec appended to `codec.PayloadCodecs` in an `init`. The user's own workers (receiver,
callers) must use the same codec and keys. Each program checks the codec at startup and waits for
it. Errors are encrypted too (details, message, stack trace; the type stays readable), so the user's
workers need the same failure converter (`EncodeCommonAttributes: true`) to read them. Kafka
messages are not encrypted by it. `docs/encryption.md` has the setup, what is and isn't
encrypted, and the demo (`DEMO_ENCRYPTION_KEY`, `go run ./nexus-connector-shared/test/encryption/codecserver`).

## Rules that break things when ignored

- **Never share the Inbound Connector's consumer group** with any other live Kafka consumer. Each
  consumer group gets every message, so separate groups can read the same topic.
- **Process a batch, not a message, when cost matters.** One activity or workflow per message
  costs one billable Action per message.
- **The Inbound Connector won't dead-letter on outages.** Only messages too large for an
  operation's input go to the dead-letter topic. Every other error, such as Temporal unreachable or
  an endpoint that doesn't exist, is retried forever, and the stream waits. Read the Inbound
  Connector's log, not the dead-letter topic, to diagnose a stalled stream.
- **Batches can be processed out of order** unless the route's `ordering` is `partition`. With it, each
  partition's operations run one at a time, in order, at the cost of parallelism (one operation
  per partition at a time); a failed operation is logged and the partition moves on. Order
  covers only what the receiver's operation waits for: back it with a workflow. Either way, make the
  receiver idempotent per message (a workflow or activity ID from topic, partition and offset).
- **An operation is never started twice, even after it fails.** Its messages were delivered; the
  failure is handled in Temporal, never by the Inbound Connector running it again or
  dead-lettering it. Don't add a retry or a dead letter for a failed operation. With the default
  `ordering` `none`, the Inbound Connector doesn't even see failures in the receiver: a batch
  is committed once Temporal accepts its operation. Find failed ones with:

  ```bash
  temporal nexus operation list --namespace <Inbound Connector's namespace> \
    --query 'ExecutionStatus IN ("Failed", "TimedOut", "Canceled", "Terminated")'
  ```

  `docs/troubleshooting.md` has the recovery steps: fix the receiver, then reset its workflow.

## Check it is working

Both programs check their configuration and connect to Temporal at startup, before waiting for
Kafka. If anything fails, the program reports every problem in one message and exits:

```
The Outbound Connector can't start:
Invalid Outbound Connector configuration:
  - set KAFKA_TOPICS: the topics callers may write to, comma-separated
Failed to connect to Temporal:
  - failed reaching server: ... connection refused
```

If configuration and Temporal are OK, both programs log:

1. A configuration report: every setting, defaults marked, the password shown only as set or not,
   then what the program will do. Read it back to the user to confirm the setup.
2. `Connected to Kafka brokers=... topics=... cluster=...`, once Kafka has every topic (the
   Outbound Connector's `KAFKA_TOPICS`; every route's topic and dead-letter topic). Until then, a
   `WARN Can't reach Kafka or see these topics yet; retrying` every 5 seconds, naming the missing
   `topics=`, with the reason:
   - `connection refused` or a dial error means the brokers are wrong or unreachable.
   - `SASL_AUTHENTICATION_FAILED` means the username, password or `KAFKA_SASL_MECHANISM` is wrong.
   - `is TLS missing?` or `plaintext connection speaking to a tls endpoint` means Kafka needs
     `KAFKA_TLS=true`; `certificate is not trusted` means it
     needs `KAFKA_TLS_SERVER_CA_CERT_PATH`; `certificate required` means it needs a client
     certificate.
   - `UNKNOWN_TOPIC_OR_PARTITION` means a topic doesn't exist.
   - `is a topic Kafka keeps for itself (internal)` stops the program at once: remove that topic.

   Fix the cause; the program carries on by itself once Kafka answers.

   After startup, `Kafka client:` lines (`KAFKA_CLIENT_LOG_LEVEL`, warn by default), the consumer
   group's partition lines, and each route's `Waiting for messages` status line (every route's lines
   carry `topic=`) show why a stream stopped moving. One route stalled (endpoint missing, Temporal
   unreachable) doesn't hold up the others; a route that fails for good stops them all, and the
   process exits non-zero. `docs/troubleshooting.md` explains each.

Then:
- **Inbound Connector:** `INFO Delivered batch topic=... records=N` per batch. `temporal nexus operation list
  --namespace <Inbound Connector's namespace>` shows one operation per partition batch, with IDs
  `kafka-<source>-<partition>-<first offset>-<last offset>`.
- **Outbound Connector:** a caller's `write` returns `written`. A caller workflow that never
  progresses usually means no worker is polling the caller's own task queue.

## Errors from `write`

`write` names its topic (`WriteInput.topic`, required) and is a synchronous Nexus operation: each attempt runs within one Nexus request, which the
caller's Temporal server gives a deadline (10 seconds by default, server dynamic config
`component.nexusoperations.request.timeout`). The write stops waiting for Kafka 1 second before it,
to reply with the reason.

- **Some or all messages not written, for good or after others were written**: the operation
  fails at once, not retried, with Kafka's message and an `ApplicationError` as the cause (find it
  with `errors.As`) of type:
  - `Rejected`: Kafka refused a message for good (such as one over the broker's size limit, or
    `TOPIC_AUTHORIZATION_FAILED`), or the write named a topic not in `KAFKA_TOPICS` (message
    `topic "x" isn't one this Outbound Connector writes to`, nothing written, every message listed;
    the allowlist is never shown to callers). Fix it before resending.
  - `PartlyWritten`: some messages were written and the rest failed for a reason that may pass.
    Resend them as they are. Retrying the whole write would write the others twice.

  Its details are a `kafkaapi.WriteFailures`: every message not written, by `index` in
  `messages`, with Kafka's `reason`. **Every message not listed was written**: resend only the
  listed ones. Kafka accepts or refuses a partition's share of a batch as a whole, so a listed
  message may have nothing wrong with it.
- **Before giving up, a write tries again within the same attempt** the messages that failed with
  an error kgo marks final but that a moment later can pass (`UNKNOWN_SERVER_ERROR`, the
  idempotent producer's sequence and epoch errors): once at once, then once more halfway to the
  attempt's deadline if that is still ahead. Only the failed messages are sent again. Within a
  partition, a message sent again lands after the ones that were written, so order within a
  partition can change.

  ```go
  var appErr *temporal.ApplicationError
  if errors.As(err, &appErr) && appErr.Type() == "Rejected" {
      var failed kafkaapi.WriteFailures
      _ = appErr.Details(&failed) // failed.Failures[i].Index, .Reason
  }
  ```
- **Anything else** (Kafka unreachable, slow, or not acknowledging): a retryable `UNAVAILABLE`
  handler error. The caller's Temporal server retries it with its own backoff (1 second, growing to
  at most 1 hour between attempts; `component.nexusoperations.retryPolicy.*`) until the caller's
  Nexus operation times out. The caller then gets a timeout error (`operation timed out (type:
  ScheduleToClose)`), not the Kafka error. While it retries, the reason is the last attempt's
  failure on the caller workflow's pending Nexus operation (`temporal workflow describe`), and the
  Outbound Connector's log has the Kafka client's errors.

## Work out settings with the user

Run `make configui` (or `go run ./configui --addr localhost:8090`) with the user's variables
exported, and have them open <http://localhost:8090> (localhost only). `test/kafka/scripts/configui.sh`
does the same against the local Docker Kafka, with its settings filled in. The page has both
programs; it shows each variable with its help and default, secrets only as set or not set. Its
**Test**, for the chosen program:

- **Settings**: every variable, checked exactly as the program checks them at startup.
- **Temporal**: connects as the program would and checks the namespace exists. For the Outbound
  Connector, that an endpoint targets the namespace and `NEXUS_TASK_QUEUE` (it shows the create
  command if not).
- **Kafka** (Outbound Connector): for each topic in `KAFKA_TOPICS`, `kafka.Identify`, then
  `kafka.ReadLast` (the topic's last message, no consumer group, nothing committed) with the
  programs' own Kafka code, showing Kafka's whole error on failure.
- **Routes** (Inbound Connector, once the routes are valid): every route resolved; that each
  route's endpoint exists; then each route's topic as above, and that its dead-letter topic exists.

`INBOUND_CONFIG` is a text area with **Insert example** (`setup.RoutesExample`) and a link to
docs/setup.md#inbound-routes.

It changes no environment; it lists the `export` lines to set where the program runs.

## Try it locally first

`docs/testing.md` has the full demo walkthrough with a local dev server and Docker Kafka
(`test/kafka/`): the Inbound Connector with two routes (`test/kafka/routes.json`) and the example
receiver, and one Outbound Connector allowing two topics with the example caller. Run it top to bottom without stopping programs in between.

## Changing the code

### Commands

```bash
make test              # go test -race; end-to-end tests need `temporal` on PATH (no Docker: Kafka is kfake)
go test -short ./...   # skip the end-to-end (dev server) tests
go test ./kafka -run TestGroupMember_CommitIsWhereTheGroupResumes -v
make clients           # generate Go, Python, TypeScript and Java callers into .build/clients/ (not committed)
make generate          # regenerate generated/kafkaapi from kafka.nexusrpc.yaml (downloads nexgen to .build/)
make docs              # regenerate docs/setup.md's tables of environment variables from the settings files
make configui          # serve the setup page on http://localhost:8090
make lint              # golangci-lint
make ci                # vet, workflowcheck, lint, test
```

Needs Go SDK v1.50 or later (the standalone Nexus client).

### Rules for changes

- The contract is `kafka.nexusrpc.yaml`. After editing it, run `make generate` and commit
  `generated/kafkaapi`; never edit generated code by hand. Optional fields become pointers.
- `kafka/` has no Temporal imports; keep it that way. It returns sentinel errors
  (`ErrRejected`) that the Outbound Connector maps to an `ApplicationError`. Both programs share it.
- **A new environment variable goes in the settings file only**: a `settings.Define` with its
  help, default, required and check, in a section of `setup.Inbound` and/or `setup.Outbound` in
  `setup/settings.go`, or, for a Kafka variable both programs read, in `setup.KafkaSection` in
  `setup/kafka.go`. Read it with `.From(vals)` in `configFrom` (`inbound/config.go`,
  `outbound/config.go`), or `setup.Kafka`. Use the shared parse functions (`settings.Text`,
  `settings.PositiveInt`, `settings.Minutes`, ...) where one fits. Then run `make docs`; a test fails while
  docs/setup.md is stale. A rule between variables, such as "`INBOUND_CONFIG` or
  `INBOUND_CONFIG_FILE`, not both", goes in the `Program`'s `Rules`.
- **A new per-route setting of the Inbound Connector goes in `setup.RouteFields`** (`setup/routes.go`):
  name, help, default and parse, a field on `setup.Route`, the built-in default in `builtIn`, the
  example (`RoutesExample`: in `defaults` and the second route), any rule in `routeRules`, the
  report (`routeSection`, `routeBehavior` in `inbound/config.go`) and `make docs` (the routes table
  between `routes:start` and `routes:end`). Never read the environment directly
  (`os.Getenv`, `os.LookupEnv`, `os.Environ`, ...): only `nexus-connector-shared/settings/env.go`
  may, and its `nobypass_test.go` fails otherwise.
- Add a new setting to the program's startup report too (`config.report` in `inbound/config.go`,
  `config.report` in `outbound/config.go`, built with `report.Format` and `report.Env`). The
  report never shows the password.
- For a test that guards a fix, revert the fix and confirm the test fails before calling it done.
  Existing guard tests were checked this way.
- **`nexus-connector-shared/` is the same in every Go connector repo** (apart from the module path).
  Change it only for something every connector needs, then copy it to the other repos with the
  create-nexus-connector skill's `scripts/sync-shared.sh` and run `make ci` in each. Code about
  Kafka never goes there. Its [README](nexus-connector-shared/README.md) lists the packages.

### How the code works

**`kafka/`**
- `GroupMember` (Inbound Connector) is a kgo consumer group with `DisableAutoCommit` and
  `BlockRebalanceOnPoll`.
  - `chooseStarts` (`AdjustFetchOffsetsFn`) resolves each assigned partition's start with
    `startOffset` and commits it before consuming. A release or stop before the first
    commit then can't let the next member choose `newest` again.
  - `Commit`, `Release`, and `Close` must call `AllowRebalance`, or leaving the group hangs.
  - `OnPartitionsCallbackBlocked` closes `RebalanceWanted()`. `Next` then returns early, and the
    Inbound Connector `Release`s an undelivered batch: `SetOffsets` back to its first records, then
    `AllowRebalance`.
- `Identify` reads the cluster ID and topic ID with a plain client, since a new group client would
  join the group. `GroupMember.Identity` does the same through the member.
- `auth.go`: `Config.authOpts` adds `kgo.DialTLSConfig` when `Config.TLS` is set and the SASL
  mechanism (`Mechanism`, "" = SCRAM-SHA-256) when `Username` is. `TLSFiles.Load` builds the
  `tls.Config` from the CA, client certificate and key paths. `setup/kafka.go` reads the variables;
  `KafkaRules` (in both programs' `Rules`) checks them together and that the files load, and
  `setup.Kafka` returns an error since it reads the files. Tests use kfake's `EnableSASL`,
  `Superuser` and `TLS` with certificates from `test/testcerts`.
- `clientOpts` gives every kgo client `kgo.WithLogger(clientLogger)` (`kafka/logger.go`): franz-go
  logs nothing without one, so connection, login and group errors after startup were invisible.
  Its level is `KAFKA_CLIENT_LOG_LEVEL`, warn by default (`Config.ClientLogLevel`, nil = warn).
- `GroupMember` logs partitions assigned and revoked (INFO) and lost (WARN), and keeps them for
  `Partitions()`.
- `fetchErrors` logs `ErrDataLoss` and `ErrGroupSession`, which kgo recovers from, rather than
  failing.
- `Record.tombstone` marks a null value; dead letters keep the null.
- `batch` and `pollUntil` are the Inbound Connector's batching rule: count and time limit. The
  Inbound Connector then splits each partition's records by encoded size
  (the route's `maxInputBytes`).
- `startOffset` and kgo's `ConsumeResetOffset` implement a route's `startFrom` for the Inbound
  Connector. The topic is an argument everywhere (`Identify`, `ReadLast`, `NewGroupMember`), not
  part of `Config`, since one process uses several. `TopicIdentity.Internal` marks a topic Kafka
  keeps for itself. `listBounds` serves both `GroupMember` and `ReadLast`.
- `ConnectAtStartup(ctx, cfg, topics)` wraps `WaitForTopics`: `Identify` every topic every 5
  seconds, 10 seconds per attempt, a WARN per failed attempt naming the missing topics, INFO once
  all are there. An internal topic ends it at once with `ErrInternalTopic`. Both programs call it after the startup report and before
  dialing Temporal, because kgo connects lazily and would otherwise wait silently.

**`inbound/`**
- `main.run` builds one `reader` per route (`newReader`: its own `GroupMember`, sender from
  `layers`, and logger `With("topic", ...)`; one dead-letter `Writer` shared) after `Ready` waits
  for every route's topics, then `runReaders` runs them under `errgroup.WithContext`: the first
  error cancels the rest, and each reader closes its member (leaving its group) before
  `runReaders` returns.
- `reader.run` loops: `Next`, then `deliver`, then `Commit`.
  - `deliver` handles partitions in parallel. Each partition's records are split into as few
    `ReceiveInput`s as fit the route's `maxInputBytes`, started in order.
  - `Commit` uses a context that survives shutdown.
- `reader` sends each operation through a `nexussend.Sender` built by `layers` in `main.go`:
  `RateLimited` (only with the route's `maxMessagesPerMinute`) around `Retrying` around `Nexus`.
  The rate limit is outside the retries so a batch counts once, and per route.
- Only two things go to the dead-letter topic: an `InvalidArgument` saying the input exceeds the
  size limit (`refusedAsTooLarge`; a part of several records is first split in half and retried),
  and single messages over the route's `maxInputBytes`. The size check is local because gRPC's own
  too-large error is an internal SDK type. Every other error retries forever, so an outage stalls
  the stream instead of draining it into dead letters. A route's `deleteDeadLetters`
  makes `deadLetter` log and skip instead.
- `reportStatus` (`status.go`) logs, per route, `Waiting for messages` with the member's partitions every
  `INBOUND_STATUS_INTERVAL` (5m; 0 off) in which nothing was delivered.
- The operation ID (`operationid.go`) is `kafka-<source>-<partition>-<first>-<last>`. `sourceKey` hashes cluster ID,
  topic ID, topic, and group as a JSON array, which can't be ambiguous, and the identity is
  fetched for every batch. A redelivered batch can end at another offset and get a new ID, so
  batches can duplicate messages; `batchSize` 1 rules that out.

**`outbound/`**
- One process serves the topics in `KAFKA_TOPICS` behind one endpoint. `topicWriter` holds them as
  a set; `write` refuses any other topic with `topicNotAllowed` before sending anything, which
  `handlerError` maps to `Rejected` with every message in `WriteFailures`. Its message never lists
  the allowed topics.
- `write` is a synchronous Nexus operation (`nexus.NewSyncOperation` in `service.go`), with no
  activity or workflow. One worker on `NEXUS_TASK_QUEUE` serves the Nexus service; nothing is
  registered as an activity. It writes with a deadline `replyMargin` (1 second) before the Nexus
  request's, so it can reply with the reason.
- `topicWriter.records` (`write.go`) adds the header `dedupe-id: kafka-write-<Nexus request ID>-<i>`.
  The caller's server stores the request ID with the operation, so it is the same on every attempt.
- `kafka.Writer.Write` returns a `*kafka.WriteError` listing each record not written (index, Kafka's
  error, whether retrying could fix it); it wraps `ErrRejected` if any record was rejected. Every
  record not listed was acknowledged. A record is `Rejected` when kgo marks its error not
  retriable, except `retriedSoon` (`UNKNOWN_SERVER_ERROR` and the idempotent producer's sequence
  and epoch errors), which are `RetrySoon` instead. `NewWriter` sets kgo's `MetadataMinAge` to
  250ms: after a retryable error kgo refreshes metadata and checks the context only then, and at
  its 5-second default a write overran its deadline and the Nexus request with it.
- `topicWriter.writeAll` (`write.go`) makes up to three tries within the attempt, sending again
  only the `RetrySoon` records (at once, then at halfway to the deadline), and maps each try's
  failures back to the original indexes (`merge`); `finalize` makes any left over `Rejected`.
- `handlerError` (`errors.go`) maps `kafka.ErrRejected` to a non-retryable handler error with a
  `Rejected` `ApplicationError` cause, a write that wrote some messages but not all to
  `PartlyWritten` (both final, details a `kafkaapi.WriteFailures` from `writeFailures`), and a
  write that wrote nothing to a retryable `UNAVAILABLE` one, which the caller's server retries
  until the caller's operation times out.

**`setup/`**
- `setup/settings.go` declares `Inbound` and `Outbound`, the `settings.Program` of each: every
  variable it reads, in sections, with its check, plus the Inbound Connector's routes rule.
  `setup/topics.go` is `KAFKA_TOPICS` and `CheckTopicName`. `setup/routes.go` is the routes:
  `RouteFields` (one table for parsing, docs and example), `ParseRoutes` (strict JSON, every problem
  at once, `routes[i] (topic)` labels, line and column for syntax errors), `routeRules`,
  `LoadRoutes` (`INBOUND_CONFIG` or the file `INBOUND_CONFIG_FILE` names; a file, not the
  environment), `Topics`, `RoutesMarkdown` and `RoutesExample`. The
  Kafka section is `KafkaSection` (`setup/kafka.go`), shared by both; the Temporal section
  lists the variables envconfig reads, unchecked. They live in a package of their own because
  `inbound` and `outbound` are main packages, which configui can't import, and `kafka/` has no
  Temporal imports.
- Each program's `configFrom(env)` calls `Load`, which reports every problem at once (rules only
  once every variable is valid), then reads each value with `.From(vals)`; `main` passes
  `settings.OS()`.

**`nexus-connector-shared/`** (the same in every Go connector repo; see its README)
- `startup`: each `main` is `startup.New`, `Config(err)` for its settings, `Temporal()` (client
  options from `temporalclient.Options`, then the dial), `Problems()` (every problem in one error,
  exit), the startup report, then `Ready(ctx, waits...)`: the payload codec first, then
  `kafka.ConnectAtStartup`. `ErrStopped` means interrupted while waiting: stop quietly.
- `temporalclient.Options(env)`: envconfig from env (TEMPORAL_* and the profile file), namespace
  defaulting to `connectors`, then `codec.Apply`. `Section` is the report's Temporal section.
- `report`: `Format`, `Section`, `Setting`, `Env` (marks defaults) for the startup reports.
- `deadline`: a call's time limit, for Outbound Connectors whose calls run as standalone
  activities. This repo's Outbound Connector doesn't use it.
- `settings`: `Program`, `Load(env)`, `Define`/`Typed`, `Env` (`OS()` or `Map`), and `Markdown` for
  the docs tables.

**`nexus-connector-shared/nexussend`** (no Kafka code)
- `Nexus` starts operations with `USE_EXISTING` and `REJECT_DUPLICATE`: an ID is never run twice,
  even if its operation failed. `NexusOperationExecutionAlreadyStarted` counts as sent;
  `refusedAsTooLarge` maps the server's size refusal to `ErrTooLarge`.
- With `wait` (a route's `ordering` `partition`), `Nexus.Send` also waits on the handle's `Get` (for an
  already-started ID, a handle from `GetNexusOperationHandle`). If `Get` fails and `Describe` says
  the operation has closed, it returns `ErrFailed`, whatever the failure; `reader` logs an ERROR and
  commits past it, with no dead letter. Otherwise (Temporal unreachable while waiting) it returns
  the error to be retried, and the retry's start attaches to the operation or finds it closed.
- `Retrying` retries every error but `ErrTooLarge` and `ErrFailed` forever, with `Backoff`, until ctx
  ends. A retry never runs a closed operation again.
- `RateLimited` is a token bucket (`golang.org/x/time/rate`): `perMinute`, burst = the batch size.
  It reserves and waits itself, so a wait ends only when ctx does, and cancels the reservation if
  it does, so unsent messages aren't counted.

**`nexus-connector-shared/codec`**
- `Apply(env, &opts)`, called by `temporalclient.Options` after the namespace is final, wraps the data
  converter in `PayloadCodecs` (compiled in) and a remote codec for `TEMPORAL_CODEC_ENDPOINT` or
  the profile's codec section, with `X-Namespace` and `TEMPORAL_CODEC_AUTH`, and sets a failure
  converter on it with `EncodeCommonAttributes` (the SDK's default ignores the data converter). Not envconfig's
  `IncludeRemoteCodec`, which sends the namespace before the `connectors` default applies.
- `WaitAtStartup` round-trips a test payload (`Check`) until it works, after the startup report;
  `Describe` is the report's `payload codec` line. `ErrUnchanged`: a codec that encodes nothing.

**`configui/`**
- The setup page, never imported by either program. The page is `nexus-connector-shared/configui`
  (it renders only from `/api/settings`; its own tests use made-up programs); this repo's
  `main.go` runs it and `connector.go` lists the two programs and their Kafka checks (`checkKafka`, `checkRoutes`). A test runs against a `settings.Map` built from the process's environment
  with the page's values in place of every variable the program owns; nothing calls `os.Setenv`.
- `GET /api/settings`: the programs and the environment's values. Secrets are sent only as set or
  not set. `POST /api/test`: the Settings, Temporal and Kafka checks; secret values are redacted
  from every message.
- `connector.go`: the Outbound Connector's Check runs `checkKafka` per allowed topic; the Inbound
  Connector's is `checkRoutes` (resolved routes, each endpoint looked up with its own Temporal
  client from `temporalclient.Options`, then Kafka per route), so the shared page's API is
  unchanged. The shared page renders `settings.Variable`'s `Multiline`, `Example` and `HelpURL`.
- `localOnly` refuses a non-localhost `Host` and non-JSON POSTs, so other sites can't use it
  through the browser.
- `kafka.ReadLast` (`kafka/last.go`) reads each partition's last offset with no consumer group,
  and keeps the message with the latest timestamp; on a tie, the higher partition.
- `configui/gendocs` writes docs/setup.md's tables between the `settings:start` and `settings:end`
  markers (`make docs`); its test fails if they are stale.

**`test/`** (test only)
- `testenv`: kfake plus a dev server with both namespaces.
- `nexuscaller`: `WriterWorkflow` writes messages to a topic through the Outbound Connector's
  endpoint (`Endpoint`); the starter's `write` command (`-topic`, `-endpoint`) starts it.
- `inboundreceiver`: the example receiver.
- `nexus-connector-shared/test/encryption`: the demo AES-GCM codec (`DEMO_ENCRYPTION_KEY`) and `codecserver`. The caller and the
  receiver use it in blocks marked `BEGIN ENCRYPTION (optional)` / `END ENCRYPTION (optional)`, plus
  an import marked `ENCRYPTION (optional)`; deleting them leaves working code.
- `kafka/`: docker-compose, `.env`, `routes.json` (the demo's two inbound routes), plus seed,
  produce, consume, and configui scripts. Seed creates `customers`, `orders` and `dead-letters`, and
  writes 10 messages to `customers` and 3 to `orders`.

### Testing notes

- kfake sometimes answers a fetch only when the client's 5-second fetch wait ends, even with
  records available. Tests that expect a partial batch at a deadline use `partialWait` (6
  seconds). Tests that expect records use a batch size equal to the records expected, so they
  return as soon as those arrive.
