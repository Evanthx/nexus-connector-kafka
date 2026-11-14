# AGENTS.md

Instructions for AI agents that set up and run the Kafka connector. The `docs/` directory has the
full user-facing documentation; this file has what you need to act, and the mistakes to avoid.

| Doc | What is in it |
|---|---|
| [`docs/setup.md`](docs/setup.md) | Prerequisites, creating endpoints, running the programs, the setup page (`make configui`), and every environment variable (tables generated from the settings files). |
| [`docs/testing.md`](docs/testing.md) | Demo walkthrough with a local dev server and Docker Kafka, unit tests. |
| [`docs/clients.md`](docs/clients.md) | The contract, and generating typed clients for Go, Python, TypeScript, and Java. |
| [`docs/how-it-works.md`](docs/how-it-works.md) | Folders, the contract, the connector and the relay in depth, cost, possible future changes. |
| [`docs/troubleshooting.md`](docs/troubleshooting.md) | Startup diagnostics, error types, consumer group rules, known limitations. |

## What this is

A Temporal connector for **one Kafka topic** (Go). It ships as two programs: the Nexus Connector,
called the connector here, and the Nexus Connector Relay, called the relay.

| Program | Use it to | How it reaches Temporal |
|---|---|---|
| `relay/` | Move every message from Kafka into Temporal as it arrives. | Joins a Kafka consumer group and starts one standalone Nexus operation per partition of each batch, through a Nexus endpoint, on a service you implement: `KafkaMessageReceiver.receive`. One-way: it never receives results. |
| `connector/` | Let workflows write to Kafka. | Serves `KafkaService.write` as a Nexus operation behind a Nexus endpoint. Workflows call it. |

Choose by the user's need. To consume a topic, use the relay: it is the only way to read. To
write to Kafka, use the connector. They can run side by side on one topic.

Each process serves one topic. For several topics, run one process per topic.

## Requirements

- Go 1.26 or later. Build with `go build -o bin/ ./relay ./connector`, or run with `go run`.
- Temporal Server 1.32 or later (Temporal CLI 1.9.1 or later for a local dev server), or Temporal
  Cloud. The namespace the connector and the relay run in needs:
  - **Standalone Nexus operations**, for the relay. Dev server: `--dynamic-config-value
    nexusoperation.enableStandalone=true`.
  - **Standalone activities with completion callbacks**, for the connector. Dev server:
    `--dynamic-config-value activity.enableCallbacks=true`.
  - On Temporal Cloud, ask the user to confirm with Temporal that both are enabled for the
    namespace. Don't assume.
- Kafka reachable from the process, with no authentication or SASL/SCRAM-SHA-256. TLS and other
  SASL mechanisms are not supported: tell the user instead of working around it.

Both programs connect to Temporal like the `temporal` CLI: `TEMPORAL_ADDRESS`,
`TEMPORAL_NAMESPACE` (default `connectors`), `TEMPORAL_API_KEY`, `TEMPORAL_TLS_*`, and the
CLI's profile file. `connectors` applies only when neither the variable nor the profile sets a
namespace; any name works, including `default`. Nexus connectors share the `connectors`
namespace and are kept apart by task queue, so two Kafka connectors in one namespace need
different `NEXUS_TASK_QUEUE` values.

The setup page shows External variables (`HTTPS_PROXY`, `NO_PROXY`, read by gRPC for the Temporal
connection only, never by Kafka) read-only and masked from its own environment, and shows what the
selected Temporal profile file sets beside each `TEMPORAL_*` field.

## Run the relay

1. **Implement the receiver.** `KafkaMessageReceiver.receive` is a Nexus service the user writes,
   in any Temporal SDK with Nexus handlers. Its input is `ReceiveInput{records: [Record...]}`:
   messages from one partition, in offset order (schema in `kafka.nexusrpc.yaml`; Go types in
   `generated/kafkaapi`; bytes are base64 in JSON). Its output is empty. Back it with a workflow,
   so the work is durable and isn't bound by a synchronous handler's deadline (under 10 seconds).
   `test/relayreceiver/main.go` is a working Go example to copy.
2. **Make the receiver idempotent.** Delivery is at least once, and batches can overlap when
   redelivered. Drop messages already handled using `topic` + `partition` + `offset`. If
   duplicates are unacceptable, set `KAFKA_BATCH_SIZE=1`.
3. **Create a Nexus endpoint** that targets the receiver's namespace and task queue:

   ```bash
   temporal operator nexus endpoint create --name <endpoint> \
     --target-namespace <receiver namespace> --target-task-queue <receiver task queue>
   ```

   On Temporal Cloud, create it in the Cloud UI or with `tcld`, and allow the relay's
   namespace to call it.
4. **Create the dead-letter topic** in Kafka, or plan to set `DELETE_DEAD_LETTERS=true`. The
   relay won't start with neither, or both.
5. **Set the environment and run** `relay`:

   | Variable | Value |
   |---|---|
   | `KAFKA_BROKERS` | Comma-separated `host:port`. |
   | `KAFKA_TOPIC` | The topic to stream. |
   | `KAFKA_USERNAME`, `KAFKA_PASSWORD` | SASL/SCRAM-SHA-256 login, if needed. |
   | `KAFKA_GROUP` | The relay's consumer group. Relays with the same consumer group share the partitions. |
   | `KAFKA_DEAD_LETTER_TOPIC` | An existing topic, different from `KAFKA_TOPIC`. Or `DELETE_DEAD_LETTERS=true` instead. |
   | `RELAY_ENDPOINT` | The endpoint from step 3. |

   Optional: `KAFKA_START_FROM` (`oldest`, the default, or `newest`; where a new consumer group
   starts), `KAFKA_BATCH_SIZE` (100),
   `KAFKA_BATCH_TIME_LIMIT` (0; `1s` lets batches fill on a quiet topic, which costs fewer
   operations), `RELAY_OPERATION_TIMEOUT` (`24h`). `docs/setup.md` lists every variable.
6. **Scale out** by running more relays with the same `KAFKA_GROUP`, up to one per partition.

## Run the connector

1. **Set the environment and run** `connector`:

   | Variable | Value |
   |---|---|
   | `KAFKA_BROKERS`, `KAFKA_TOPIC` | As for the relay. Callers can't choose another topic. |
   | `KAFKA_USERNAME`, `KAFKA_PASSWORD` | If needed. |
   | `NEXUS_TASK_QUEUE` | Default `nexus-connector-kafka`. Give each connector for a different topic its own. |
   | `KAFKA_MAX_DURATION_MINUTES` | Optional. Longest a write retries, in whole minutes. Default 10. |

2. **Create a Nexus endpoint** that targets the connector's namespace and `NEXUS_TASK_QUEUE`
   exactly.

   ```bash
   temporal operator nexus endpoint create --name <endpoint> \
     --target-namespace connectors --target-task-queue nexus-connector-kafka \
     --description-file ./endpoint-description.md
   ```

3. **Call it from a workflow** with a Nexus client for `KafkaService` on that endpoint.
   `test/nexuscaller/workflows.go` (`WriterWorkflow`) is a working Go example to copy. `write`
   takes `messages: [{key?, value, headers?}]` and returns `written`. Each message gets a
   `dedupe-id` header that stays the same when the write is retried.

## Rules that break things when ignored

- **Never share the relay's consumer group** with any other live Kafka consumer. Each consumer
  group gets every message, so separate groups can read the same topic.
- **Process a batch, not a message, when cost matters.** One activity or workflow per message
  costs one billable Action per message.
- **The relay won't dead-letter on outages.** Only messages too large for an operation's input
  go to the dead-letter topic. Every other error, such as Temporal unreachable or an endpoint that
  doesn't exist, is retried forever, and the stream waits. Read the relay's log, not the
  dead-letter topic, to diagnose a stalled stream.
- **The relay doesn't see failures in the receiver.** A batch is committed once Temporal
  accepts its operation. Find failed ones with:

  ```bash
  temporal nexus operation list --namespace <relay namespace> \
    --query 'ExecutionStatus IN ("Failed", "TimedOut", "Canceled", "Terminated")'
  ```

  `docs/troubleshooting.md` has the replay steps.

## Check it is working

Both programs check their configuration and connect to Temporal at startup, before waiting for
Kafka. If anything fails, the program reports every problem in one message and exits:

```
The connector can't start:
Invalid connector configuration:
  - KAFKA_MAX_DURATION_MINUTES must be a whole number of minutes, at least 1, not "0"
Failed to connect to Temporal:
  - failed reaching server: ... connection refused
```

If configuration and Temporal are OK, both programs log:

1. A configuration report: every setting, defaults marked, the password shown only as set or not,
   then what the program will do. Read it back to the user to confirm the setup.
2. `Connected to Kafka brokers=... topic=... cluster=...`. Until then, a `WARN Can't reach Kafka or
   see the topic yet; retrying` every 5 seconds with the reason:
   - `connection refused` or a dial error means the brokers are wrong or unreachable.
   - `SASL_AUTHENTICATION_FAILED` means the username or password is wrong.
   - `UNKNOWN_TOPIC_OR_PARTITION` means the topic doesn't exist.

   Fix the cause; the program carries on by itself once Kafka answers.

Then:
- **Relay:** `INFO Delivered batch records=N` per batch. `temporal nexus operation list
  --namespace <relay namespace>` shows one operation per partition batch, with IDs
  `kafka-<source>-<partition>-<first offset>-<last offset>`.
- **Connector:** a caller's `write` returns `written`. A caller workflow that never progresses
  usually means no worker is polling the caller's own task queue.

## Errors from `write`

Failures that retrying can't fix are `ApplicationError`s:

| Type | Cause | Fix |
|---|---|---|
| `InvalidRequest` | The caller's Nexus operation timeout is 10 seconds or less, too short to run a write. | Give the call a longer timeout. |
| `Rejected` | Kafka refused a write, such as a message over the broker's size limit. | Fix the message. |

Other failures are retried for 10 minutes, or `KAFKA_MAX_DURATION_MINUTES`. Retries stop 10
seconds before the caller's Nexus operation times out, if that comes first, so the caller gets the
last error rather than a timeout.

## Work out settings with the user

Run `make configui` (or `go run ./configui --addr localhost:8090`) with the user's variables
exported, and have them open <http://localhost:8090> (localhost only). `test/kafka/scripts/configui.sh`
does the same against the local Docker Kafka, with its settings filled in. The page has both
programs; it shows each variable with its help and default, secrets only as set or not set. Its
**Test**, for the chosen program:

- **Settings**: every variable, checked exactly as the program checks them at startup.
- **Temporal**: connects as the program would and checks the namespace exists. For the connector,
  that an endpoint targets the namespace and `NEXUS_TASK_QUEUE` (it shows the create command if
  not); for the relay, that the endpoint `RELAY_ENDPOINT` names exists.
- **Kafka**: `kafka.Identify`, then `kafka.ReadLast` (the topic's last message, no consumer group,
  nothing committed) with the programs' own Kafka code, showing Kafka's whole error on failure. For
  the relay, that `KAFKA_DEAD_LETTER_TOPIC` exists.

It changes no environment; it lists the `export` lines to set where the program runs.

## Try it locally first

`docs/testing.md` has the full demo walkthrough with a local dev server and Docker Kafka
(`test/kafka/`): the relay with the example receiver, and two connectors writing to two topics
with the example caller. Run it top to bottom without stopping programs in between.

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

Needs Go SDK v1.50 or later (`temporalnexus.StartActivity`, the standalone Nexus client).

### Rules for changes

- The contract is `kafka.nexusrpc.yaml`. After editing it, run `make generate` and commit
  `generated/kafkaapi`; never edit generated code by hand. Optional fields become pointers.
- `kafka/` has no Temporal imports; keep it that way. It returns sentinel errors
  (`ErrRejected`) that the connector maps to an `ApplicationError`. Both programs share it.
- **A new environment variable goes in the settings file only**: a `settings.Define` with its
  help, default, required and check, in a section of `setup.Relay` and/or `setup.Connector` in
  `setup/settings.go`, or, for a Kafka variable both programs read, in `kafka.SettingsSection` in
  `kafka/settings.go`. Read it with `.From(vals)` in `configFrom` (`relay/config.go`,
  `connector/main.go`), or `kafka.ConfigFrom`. Then run `make docs`; a test fails while
  docs/setup.md is stale. A rule between variables, such as "`KAFKA_DEAD_LETTER_TOPIC` or
  `DELETE_DEAD_LETTERS=true`, not both", goes in the `Program`'s `Rules`. Never read the environment directly
  (`os.Getenv`, `os.LookupEnv`, `os.Environ`, ...): only `settings/env.go` may, and
  `settings/nobypass_test.go` fails otherwise.
- Add a new setting to the program's startup report too (`config.report` in `relay/config.go`,
  `config.report` in `connector/main.go`, built with `kafka.Report` and `kafka.EnvSetting`). The
  report never shows the password.
- For a test that guards a fix, revert the fix and confirm the test fails before calling it done.
  Existing guard tests were checked this way.

### How the code works

**`kafka/`**
- `GroupMember` (relay) is a kgo consumer group with `DisableAutoCommit` and
  `BlockRebalanceOnPoll`.
  - `chooseStarts` (`AdjustFetchOffsetsFn`) resolves each assigned partition's start with
    `startOffset` and commits it before consuming. A release or stop before the first
    commit then can't let the next member choose `newest` again.
  - `Commit`, `Release`, and `Close` must call `AllowRebalance`, or leaving the group hangs.
  - `OnPartitionsCallbackBlocked` closes `RebalanceWanted()`. `Next` then returns early, and the
    relay `Release`s an undelivered batch: `SetOffsets` back to its first records, then
    `AllowRebalance`.
- `Identify` reads the cluster ID and topic ID with a plain client, since a new group client would
  join the group. `GroupMember.Identity` does the same through the member.
- `fetchErrors` logs `ErrDataLoss` and `ErrGroupSession`, which kgo recovers from, rather than
  failing.
- `Record.tombstone` marks a null value; dead letters keep the null.
- `batch` and `pollUntil` are the relay's batching rule: count and time limit. The relay then
  splits each partition's records by encoded size (`RELAY_MAX_INPUT_BYTES`).
- `startOffset` and kgo's `ConsumeResetOffset` implement `KAFKA_START_FROM` for the relay.
  `listBounds` serves both `GroupMember` and `ReadLast`.
- `ConnectAtStartup` wraps `WaitForTopic`: `Identify` every 5 seconds, 10 seconds per attempt, a
  WARN per failure, INFO on success. Both programs call it after the startup report and before
  dialing Temporal, because kgo connects lazily and would otherwise wait silently.

**`relay/`**
- `relay.run` loops: `Next`, then `deliver`, then `Commit`.
  - `deliver` handles partitions in parallel. Each partition's records are split into as few
    `ReceiveInput`s as fit `RELAY_MAX_INPUT_BYTES`, started in order.
  - `Commit` uses a context that survives shutdown.
- `nexusStarter` starts operations with `USE_EXISTING` and `ALLOW_DUPLICATE_FAILED_ONLY`;
  `NexusOperationExecutionAlreadyStarted` counts as delivered.
- Only two things go to the dead-letter topic: an `InvalidArgument` saying the input exceeds the
  size limit (`refusedAsTooLarge`; a part of several records is first split in half and retried),
  and single messages over `RELAY_MAX_INPUT_BYTES`. The size check is local because gRPC's own
  too-large error is an internal SDK type. Every other error retries forever, so an outage stalls
  the stream instead of draining it into dead letters. `DELETE_DEAD_LETTERS=true` makes
  `deadLetter` log and skip instead.
- The operation ID is `kafka-<source>-<partition>-<first>-<last>`. `sourceKey` hashes cluster ID,
  topic ID, topic, and group as a JSON array, which can't be ambiguous, and the identity is
  fetched for every batch. A redelivered batch can end at another offset and get a new ID, so
  batches can duplicate messages; `KAFKA_BATCH_SIZE=1` rules that out.

**`connector/`**
- One process serves one topic (`KAFKA_TOPIC`) behind one endpoint.
- `write`'s `Start` starts `activities.Write` as a standalone activity. One worker on
  `NEXUS_TASK_QUEUE` runs the Nexus operation and the activity.
- `Write` adds the header `dedupe-id: <activityID>-<i>`.

**`setup/`, `settings/`**
- `setup/settings.go` declares `Relay` and `Connector`, the `settings.Program` of each: every
  variable it reads, in sections, with its check, plus the relay's dead-letter rule. The Kafka
  section is `kafka.SettingsSection` (`kafka/settings.go`), shared by both; the Temporal section
  lists the variables envconfig reads, unchecked. They live in a package of their own because
  `relay` and `connector` are main packages, which configui can't import, and `kafka/` has no
  Temporal imports. `ClientOptions(env)` loads the Temporal client options with envconfig from the
  same env, namespace defaulting to `connectors`.
- Each program's `configFrom(env)` calls `Load`, which reports every problem at once (rules only
  once every variable is valid), then reads each value with `.From(vals)`; `main` passes
  `settings.OS()`.
- `settings/` is the same in every Go connector repo: `Program`, `Load(env)`, `Define`/`Typed`,
  `Env` (`OS()` or `Map`), and `Markdown` for the docs tables.

**`configui/`**
- The setup page, never imported by either program. `main.go`, `temporal.go` and `index.html` are
  the same in every Go connector repo (the page renders only from `/api/settings`);
  `connector.go` lists the two programs and their Kafka checks (`checkKafka`, `relayChecks`,
  `connectorChecks`). A test runs against a `settings.Map` built from the process's environment
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
- `nexuscaller`: `WriterWorkflow` writes messages through a connector's endpoint; the starter's
  `write` command starts it.
- `relayreceiver`: the example receiver.
- `kafka/`: docker-compose plus seed, produce, consume, and configui scripts. Seed writes 10 messages to
  `customers` and 3 to `orders`.

### Testing notes

- kfake sometimes answers a fetch only when the client's 5-second fetch wait ends, even with
  records available. Tests that expect a partial batch at a deadline use `partialWait` (6
  seconds). Tests that expect records use a batch size equal to the records expected, so they
  return as soon as those arrive.
