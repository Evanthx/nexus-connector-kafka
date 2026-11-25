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

A Temporal connector for **one Kafka topic** (Go). It ships as two programs: the Inbound
Connector and the Outbound Connector.

| Program | Use it to | How it reaches Temporal |
|---|---|---|
| `inbound/` | Move every message from Kafka into Temporal as it arrives. | Joins a Kafka consumer group and starts one standalone Nexus operation per partition of each batch, through a Nexus endpoint, on a service you implement: `KafkaMessageReceiver.receive`. One-way: it never receives results. |
| `outbound/` | Let workflows write to Kafka. | Serves `KafkaService.write` as a Nexus operation behind a Nexus endpoint. Workflows call it. |

Choose by the user's need. To consume a topic, use the Inbound Connector: the Outbound Connector
only writes. To write to Kafka, use the Outbound Connector. They can run side by side on one topic.

Each process serves one topic. For several topics, run one process per topic.

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
one per topic, need different `NEXUS_TASK_QUEUE` values.

The setup page shows External variables (`HTTPS_PROXY`, `NO_PROXY`, read by gRPC for the Temporal
connection only, never by Kafka) read-only and masked from its own environment, and shows what the
selected Temporal profile file sets beside each `TEMPORAL_*` field.

## Run the Inbound Connector

1. **Implement the receiver.** `KafkaMessageReceiver.receive` is a Nexus service the user writes,
   in any Temporal SDK with Nexus handlers. Its input is `ReceiveInput{records: [Record...]}`:
   messages from one partition, in offset order (schema in `kafka.nexusrpc.yaml`; Go types in
   `generated/kafkaapi`; bytes are base64 in JSON). Its output is empty. Back it with a workflow,
   so the work is durable and isn't bound by a synchronous handler's deadline (under 10 seconds).
   `test/inboundreceiver/main.go` is a working Go example to copy.
2. **Make the receiver idempotent.** Delivery is at least once, and batches can overlap when
   redelivered. Drop messages already handled using `topic` + `partition` + `offset`. If
   duplicates are unacceptable, set `KAFKA_BATCH_SIZE=1`.
3. **Create a Nexus endpoint** that targets the receiver's namespace and task queue:

   ```bash
   temporal operator nexus endpoint create --name <endpoint> \
     --target-namespace <receiver namespace> --target-task-queue <receiver task queue>
   ```

   On Temporal Cloud, create it in the Cloud UI or with `tcld`, and allow the Inbound Connector's
   namespace to call it.
4. **Create the dead-letter topic** in Kafka, or plan to set `DELETE_DEAD_LETTERS=true`. The
   Inbound Connector won't start with neither, or both.
5. **Set the environment and run** `inbound`:

   | Variable | Value |
   |---|---|
   | `KAFKA_BROKERS` | Comma-separated `host:port`. |
   | `KAFKA_TOPIC` | The topic to stream. |
   | `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_SASL_MECHANISM`, `KAFKA_TLS*` | Kafka login and TLS, if needed (see Requirements). |
   | `KAFKA_GROUP` | The Inbound Connector's consumer group. Inbound Connectors with the same consumer group share the partitions. |
   | `KAFKA_DEAD_LETTER_TOPIC` | An existing topic, different from `KAFKA_TOPIC`. Or `DELETE_DEAD_LETTERS=true` instead. |
   | `INBOUND_ENDPOINT` | The endpoint from step 3. |

   Optional: `KAFKA_START_FROM` (`oldest`, the default, or `newest`; where a new consumer group
   starts), `KAFKA_BATCH_SIZE` (100),
   `KAFKA_BATCH_TIME_LIMIT` (0; `1s` lets batches fill on a quiet topic, which costs fewer
   operations), `INBOUND_OPERATION_TIMEOUT` (`24h`), `INBOUND_MAX_MESSAGES_PER_MINUTE` (no
   limit; per process, spread evenly, at least `KAFKA_BATCH_SIZE`), `INBOUND_ORDERING` (`none`, or
   `partition` to process each partition in order; see `docs/how-it-works.md#ordering`). `docs/setup.md` lists every
   variable.
6. **Scale out** by running more Inbound Connectors with the same `KAFKA_GROUP`, up to one per
   partition.

## Run the Outbound Connector

1. **Set the environment and run** `outbound`:

   | Variable | Value |
   |---|---|
   | `KAFKA_BROKERS`, `KAFKA_TOPIC` | As for the Inbound Connector. Callers can't choose another topic. |
   | `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_SASL_MECHANISM`, `KAFKA_TLS*` | If needed. |
   | `NEXUS_TASK_QUEUE` | Default `nexus-connector-kafka`. Give each Outbound Connector for a different topic its own. |

2. **Create a Nexus endpoint** that targets the Outbound Connector's namespace and
   `NEXUS_TASK_QUEUE` exactly.

   ```bash
   temporal operator nexus endpoint create --name <endpoint> \
     --target-namespace connectors --target-task-queue nexus-connector-kafka \
     --description-file ./endpoint-description.md
   ```

3. **Call it from a workflow** with a Nexus client for `KafkaService` on that endpoint.
   `test/nexuscaller/workflows.go` (`WriterWorkflow`) is a working Go example to copy. `write`
   takes `messages: [{key?, value, headers?}]` and returns `written`. Each message gets a
   `dedupe-id` header that stays the same when the write is retried. **Give the call a timeout**
   (`ScheduleToCloseTimeout`): the caller's Temporal server retries a failed write until it, and
   nothing else stops the retries.

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
- **Batches can be processed out of order** unless `INBOUND_ORDERING=partition`. With it, each
  partition's operations run one at a time, in order, at the cost of parallelism (one operation
  per partition at a time); a failed operation is logged and the partition moves on. Order
  covers only what the receiver's operation waits for: back it with a workflow. Either way, make the
  receiver idempotent per message (a workflow or activity ID from topic, partition and offset).
- **An operation is never started twice, even after it fails.** Its messages were delivered; the
  failure is handled in Temporal, never by the Inbound Connector running it again or
  dead-lettering it. Don't add a retry or a dead letter for a failed operation. With the default
  `INBOUND_ORDERING=none`, the Inbound Connector doesn't even see failures in the receiver: a batch
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
  - set KAFKA_TOPIC: the topic to serve
Failed to connect to Temporal:
  - failed reaching server: ... connection refused
```

If configuration and Temporal are OK, both programs log:

1. A configuration report: every setting, defaults marked, the password shown only as set or not,
   then what the program will do. Read it back to the user to confirm the setup.
2. `Connected to Kafka brokers=... topic=... cluster=...`. Until then, a `WARN Can't reach Kafka or
   see the topic yet; retrying` every 5 seconds with the reason:
   - `connection refused` or a dial error means the brokers are wrong or unreachable.
   - `SASL_AUTHENTICATION_FAILED` means the username, password or `KAFKA_SASL_MECHANISM` is wrong.
   - `is TLS missing?` or `plaintext connection speaking to a tls endpoint` means Kafka needs
     `KAFKA_TLS=true`; `certificate is not trusted` means it
     needs `KAFKA_TLS_SERVER_CA_CERT_PATH`; `certificate required` means it needs a client
     certificate.
   - `UNKNOWN_TOPIC_OR_PARTITION` means the topic doesn't exist.

   Fix the cause; the program carries on by itself once Kafka answers.

   After startup, `Kafka client:` lines (`KAFKA_CLIENT_LOG_LEVEL`, warn by default), the consumer
   group's partition lines, and the Inbound Connector's `Waiting for messages` status line show why
   a stream stopped moving. `docs/troubleshooting.md` explains each.

Then:
- **Inbound Connector:** `INFO Delivered batch records=N` per batch. `temporal nexus operation list
  --namespace <Inbound Connector's namespace>` shows one operation per partition batch, with IDs
  `kafka-<source>-<partition>-<first offset>-<last offset>`.
- **Outbound Connector:** a caller's `write` returns `written`. A caller workflow that never
  progresses usually means no worker is polling the caller's own task queue.

## Errors from `write`

`write` is a synchronous Nexus operation: each attempt runs within one Nexus request, which the
caller's Temporal server gives a deadline (10 seconds by default, server dynamic config
`component.nexusoperations.request.timeout`). The write stops waiting for Kafka 1 second before it,
to reply with the reason.

- **Some or all messages not written, for good or after others were written**: the operation
  fails at once, not retried, with Kafka's message and an `ApplicationError` as the cause (find it
  with `errors.As`) of type:
  - `Rejected`: Kafka refused a message for good (such as one over the broker's size limit, or
    `TOPIC_AUTHORIZATION_FAILED`). Fix it before resending.
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
  command if not); for the Inbound Connector, that the endpoint `INBOUND_ENDPOINT` names exists.
- **Kafka**: `kafka.Identify`, then `kafka.ReadLast` (the topic's last message, no consumer group,
  nothing committed) with the programs' own Kafka code, showing Kafka's whole error on failure. For
  the Inbound Connector, that `KAFKA_DEAD_LETTER_TOPIC` exists.

It changes no environment; it lists the `export` lines to set where the program runs.

## Try it locally first

`docs/testing.md` has the full demo walkthrough with a local dev server and Docker Kafka
(`test/kafka/`): the Inbound Connector with the example receiver, and two Outbound Connectors
writing to two topics with the example caller. Run it top to bottom without stopping programs in between.

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
  docs/setup.md is stale. A rule between variables, such as "`KAFKA_DEAD_LETTER_TOPIC` or
  `DELETE_DEAD_LETTERS=true`, not both", goes in the `Program`'s `Rules`. Never read the environment directly
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
  (`INBOUND_MAX_INPUT_BYTES`).
- `startOffset` and kgo's `ConsumeResetOffset` implement `KAFKA_START_FROM` for the Inbound
  Connector. `listBounds` serves both `GroupMember` and `ReadLast`.
- `ConnectAtStartup` wraps `WaitForTopic`: `Identify` every 5 seconds, 10 seconds per attempt, a
  WARN per failure, INFO on success. Both programs call it after the startup report and before
  dialing Temporal, because kgo connects lazily and would otherwise wait silently.

**`inbound/`**
- `reader.run` loops: `Next`, then `deliver`, then `Commit`.
  - `deliver` handles partitions in parallel. Each partition's records are split into as few
    `ReceiveInput`s as fit `INBOUND_MAX_INPUT_BYTES`, started in order.
  - `Commit` uses a context that survives shutdown.
- `reader` sends each operation through a `nexussend.Sender` built by `layers` in `main.go`:
  `RateLimited` (only with `INBOUND_MAX_MESSAGES_PER_MINUTE`) around `Retrying` around `Nexus`. The
  rate limit is outside the retries so a batch counts once.
- Only two things go to the dead-letter topic: an `InvalidArgument` saying the input exceeds the
  size limit (`refusedAsTooLarge`; a part of several records is first split in half and retried),
  and single messages over `INBOUND_MAX_INPUT_BYTES`. The size check is local because gRPC's own
  too-large error is an internal SDK type. Every other error retries forever, so an outage stalls
  the stream instead of draining it into dead letters. `DELETE_DEAD_LETTERS=true` makes
  `deadLetter` log and skip instead.
- `reportStatus` (`status.go`) logs `Waiting for messages` with the member's partitions every
  `INBOUND_STATUS_INTERVAL` (5m; 0 off) in which nothing was delivered.
- The operation ID (`operationid.go`) is `kafka-<source>-<partition>-<first>-<last>`. `sourceKey` hashes cluster ID,
  topic ID, topic, and group as a JSON array, which can't be ambiguous, and the identity is
  fetched for every batch. A redelivered batch can end at another offset and get a new ID, so
  batches can duplicate messages; `KAFKA_BATCH_SIZE=1` rules that out.

**`outbound/`**
- One process serves one topic (`KAFKA_TOPIC`) behind one endpoint.
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
  variable it reads, in sections, with its check, plus the Inbound Connector's dead-letter rule. The
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
- With `wait` (`INBOUND_ORDERING=partition`), `Nexus.Send` also waits on the handle's `Get` (for an
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
  `main.go` runs it and `connector.go` lists the two programs and their Kafka checks (`checkKafka`, `inboundChecks`). A test runs against a `settings.Map` built from the process's environment
  with the page's values in place of every variable the program owns; nothing calls `os.Setenv`.
- `GET /api/settings`: the programs and the environment's values. Secrets are sent only as set or
  not set. `POST /api/test`: the Settings, Temporal and Kafka checks; secret values are redacted
  from every message.
- `localOnly` refuses a non-localhost `Host` and non-JSON POSTs, so other sites can't use it
  through the browser.
- `kafka.ReadLast` (`kafka/last.go`) reads each partition's last offset with no consumer group,
  and keeps the message with the latest timestamp; on a tie, the higher partition.
- `configui/gendocs` writes docs/setup.md's tables between the `settings:start` and `settings:end`
  markers (`make docs`); its test fails if they are stale.

**`test/`** (test only)
- `testenv`: kfake plus a dev server with both namespaces.
- `nexuscaller`: `WriterWorkflow` writes messages through an Outbound Connector's endpoint; the
  starter's `write` command starts it.
- `inboundreceiver`: the example receiver.
- `nexus-connector-shared/test/encryption`: the demo AES-GCM codec (`DEMO_ENCRYPTION_KEY`) and `codecserver`. The caller and the
  receiver use it in blocks marked `BEGIN ENCRYPTION (optional)` / `END ENCRYPTION (optional)`, plus
  an import marked `ENCRYPTION (optional)`; deleting them leaves working code.
- `kafka/`: docker-compose plus seed, produce, consume, and configui scripts. Seed writes 10 messages to
  `customers` and 3 to `orders`.

### Testing notes

- kfake sometimes answers a fetch only when the client's 5-second fetch wait ends, even with
  records available. Tests that expect a partial batch at a deadline use `partialWait` (6
  seconds). Tests that expect records use a batch size equal to the records expected, so they
  return as soon as those arrive.
