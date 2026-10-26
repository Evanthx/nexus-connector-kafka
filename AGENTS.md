# AGENTS.md

Instructions for AI agents that set up and run this connector. README.md has the full explanations;
this file has what you need to act, and the mistakes to avoid.

## What this is

A Temporal connector for **one Kafka topic** (Go). It runs in one of two ways:

| Program | Use it to | How it reaches Temporal |
|---|---|---|
| `streamer/` | Move every message from Kafka into Temporal as it arrives. | Joins a Kafka consumer group and starts one standalone Nexus operation per partition of each batch, through a Nexus endpoint, on a service you implement: `KafkaMessageReceiver.receive`. One-way: it never receives results. |
| `nexusworker/` | Let workflows write to Kafka, or pull messages at their own pace. | Serves `KafkaService.read` and `KafkaService.write` as Nexus operations behind a Nexus endpoint. Workflows call them. |

Choose by the user's need. To trigger Temporal work from every message, use the streamer. To write
to Kafka, or to read from a workflow loop, use the Nexus worker. They can run side by side on one
topic, with different consumer groups.

Each process serves one topic. For several topics, run one process per topic.

## Requirements

- Go 1.26 or later. Build with `go build -o bin/ ./streamer ./nexusworker`, or run with `go run`.
- Temporal Server 1.32 or later (Temporal CLI 1.9.1 or later for a local dev server), or Temporal
  Cloud. The connector's namespace needs:
  - **Standalone Nexus operations**, for the streamer. Dev server: `--dynamic-config-value
    nexusoperation.enableStandalone=true`.
  - **Standalone activities with completion callbacks**, for the Nexus worker. Dev server:
    `--dynamic-config-value activity.enableCallbacks=true`.
  - On Temporal Cloud, ask the user to confirm with Temporal that both are enabled for the
    namespace. Don't assume.
- Kafka reachable from the process, with no authentication or SASL/SCRAM-SHA-256. TLS and other
  SASL mechanisms are not supported: tell the user instead of working around it.

Both programs connect to Temporal like the `temporal` CLI: `TEMPORAL_ADDRESS`,
`TEMPORAL_NAMESPACE` (default `kafka-connector`), `TEMPORAL_API_KEY`, `TEMPORAL_TLS_*`, and the
CLI's profile file.

## Run the streamer

1. **Implement the receiver.** `KafkaMessageReceiver.receive` is a Nexus service the user writes,
   in any Temporal SDK with Nexus handlers. Its input is `ReceiveInput{records: [Record...]}`:
   messages from one partition, in offset order (schema in `kafka.nexusrpc.yaml`; Go types in
   `generated/kafkaapi`; bytes are base64 in JSON). Its output is empty. Back it with a workflow,
   so the work is durable and isn't bound by a synchronous handler's deadline (under 10 seconds).
   `test/streamreceiver/main.go` is a working Go example to copy.
2. **Make the receiver idempotent.** Delivery is at least once, and batches can overlap when
   redelivered. Drop messages already handled using `topic` + `partition` + `offset`. If
   duplicates are unacceptable, set `KAFKA_BATCH_SIZE=1`.
3. **Create a Nexus endpoint** that targets the receiver's namespace and task queue:

   ```bash
   temporal operator nexus endpoint create --name <endpoint> \
     --target-namespace <receiver namespace> --target-task-queue <receiver task queue>
   ```

   On Temporal Cloud, create it in the Cloud UI or with `tcld`, and allow the connector's
   namespace to call it.
4. **Create the dead-letter topic** in Kafka, or plan to set `DELETE_DEAD_LETTERS=true`. The
   streamer won't start with neither, or both.
5. **Set the environment and run** `streamer`:

   | Variable | Value |
   |---|---|
   | `KAFKA_BROKERS` | Comma-separated `host:port`. |
   | `KAFKA_TOPIC` | The topic to stream. |
   | `KAFKA_USERNAME`, `KAFKA_PASSWORD` | SASL/SCRAM-SHA-256 login, if needed. |
   | `KAFKA_GROUP` | The streamer's consumer group. Streamers with the same consumer group share the partitions. |
   | `KAFKA_DEAD_LETTER_TOPIC` | An existing topic, different from `KAFKA_TOPIC`. Or `DELETE_DEAD_LETTERS=true` instead. |
   | `STREAM_ENDPOINT` | The endpoint from step 3. |

   Optional: `KAFKA_START_FROM` (`oldest`, the default, or `newest`), `KAFKA_BATCH_SIZE` (100),
   `KAFKA_BATCH_TIME_LIMIT` (0; `1s` lets batches fill on a quiet topic, which costs fewer
   operations), `STREAM_OPERATION_TIMEOUT` (`24h`). The README's Settings table has the rest.
6. **Scale out** by running more streamers with the same `KAFKA_GROUP`, up to one per partition.

## Run the Nexus worker

1. **Set the environment and run** `nexusworker`:

   | Variable | Value |
   |---|---|
   | `KAFKA_BROKERS`, `KAFKA_TOPIC` | As for the streamer. Callers can't choose another topic. |
   | `KAFKA_USERNAME`, `KAFKA_PASSWORD` | If needed. |
   | `KAFKA_GROUP` | Optional. Set it to pin every read to one consumer group. Leave it unset to make each read name its own. |
   | `NEXUS_TASK_QUEUE` | Default `nexus-connector-kafka`. Give each Nexus worker for a different topic or consumer group its own. |

2. **Create a Nexus endpoint** that targets the connector's namespace and `NEXUS_TASK_QUEUE`
   exactly. Reads run on a second queue, `<NEXUS_TASK_QUEUE>-reads`; never target that one.

   ```bash
   temporal operator nexus endpoint create --name <endpoint> \
     --target-namespace kafka-connector --target-task-queue nexus-connector-kafka \
     --description-file ./endpoint-description.md
   ```

3. **Call it from a workflow** with a Nexus client for `KafkaService` on that endpoint.
   `test/nexuscaller/workflows.go` (`ReaderWorkflow`, `WriterWorkflow`) is a working Go example
   to copy. The read loop:
   - Call `read` with `group` (unless the worker set `KAFKA_GROUP`), `batchSize`, and
     `timeLimitSeconds`. The first read has no `ack`; it starts from the consumer group's
     committed offsets.
   - Process `records`, then pass the returned `position` as `ack` on the next read. The ack is
     committed before reading, so acknowledging costs no extra call. Never build or edit a
     position: it is opaque.
   - To acknowledge the final batch without reading more, call `read` with the `ack` and
     `batchSize: 0`.
   - Use a `timeLimitSeconds` of 60 or less: reads long-poll, and reads over 70 seconds start
     sending billable heartbeats.
   - Continue as new when Temporal suggests it, carrying the position.
   - `write` takes `messages: [{key?, value, headers?}]` and returns `written`. Each message
     gets a `dedupe-id` header that stays the same when the write is retried.

## Rules that break things when ignored

- **One reader per consumer group.** Two readers of one group read the same batches. Make the
  caller's workflow ID name its endpoint and consumer group, so a second can't start (the test
  caller does this).
- **Never share a consumer group between the streamer and a Nexus worker reader**, or with any
  live Kafka consumer. Reads then fail with `GroupInUse`. Each consumer group gets every message,
  so separate groups can read the same topic.
- **Process a batch, not a message, when cost matters.** One activity or workflow per message
  costs one billable Action per message. The test caller uses one per message only to show
  parallel processing.
- **The streamer won't dead-letter on outages.** Only messages too large for an operation's input
  go to the dead-letter topic. Every other error, such as Temporal unreachable or an endpoint that
  doesn't exist, is retried forever, and the stream waits. Read the streamer's log, not the
  dead-letter topic, to diagnose a stalled stream.
- **The streamer doesn't see failures in the receiver.** A batch is committed once Temporal
  accepts its operation. Find failed ones with:

  ```bash
  temporal nexus operation list --namespace <connector namespace> \
    --query 'ExecutionStatus IN ("Failed", "TimedOut", "Canceled", "Terminated")'
  ```

  README "When an operation fails" says how to replay them.

## Check it is working

Both programs log, at startup:

1. A configuration report: every setting, defaults marked, the password shown only as set or not,
   then what the program will do. Read it back to the user to confirm the setup.
2. `Connected to Kafka brokers=... topic=... cluster=...`. Until then, a `WARN Can't reach Kafka or
   see the topic yet; retrying` every 5 seconds with the reason:
   - `connection refused` or a dial error means the brokers are wrong or unreachable.
   - `SASL_AUTHENTICATION_FAILED` means the username or password is wrong.
   - `UNKNOWN_TOPIC_OR_PARTITION` means the topic doesn't exist.

   Fix the cause; the program carries on by itself once Kafka answers.
3. Only after that does it connect to Temporal. A Temporal connection error exits the program.

Then:
- **Streamer:** `INFO Delivered batch records=N` per batch. `temporal nexus operation list
  --namespace <connector namespace>` shows one operation per partition batch, with IDs
  `kafka-<source>-<partition>-<first offset>-<last offset>`.
- **Nexus worker:** a caller's `read` returns records. A caller workflow that never progresses
  usually means no worker is polling the caller's own task queue.

## Errors from `read` and `write`

Failures that retrying can't fix are `ApplicationError`s:

| Type | Cause | Fix |
|---|---|---|
| `InvalidRequest` | No consumer group named when the worker has none; a different one named when it has `KAFKA_GROUP`; a malformed `ack`, or one from a topic since deleted and recreated. | Fix the request. For a recreated topic, read without `ack`. |
| `GroupInUse` | The consumer group has live members, such as a streamer. | Use another consumer group. |
| `RecordTooLarge` | The next message doesn't fit in a read's result (`KAFKA_MAX_BATCH_BYTES`, 2 MiB). | It blocks that consumer group: tell the user. |
| `Rejected` | Kafka refused a write, such as a message over the broker's size limit. | Fix the message. |

Other failures are retried: a read for its time limit plus 5 minutes, a write for 10 minutes.

## Try it locally first

README "Demo" runs everything with a local dev server and Docker Kafka (`test/kafka/`): both
methods, two Nexus workers on two topics, and the example caller and receiver. Run it top to
bottom without stopping programs in between.

## Changing the code

### Commands

```bash
make test              # go test -race; end-to-end tests need `temporal` on PATH (no Docker: Kafka is kfake)
go test -short ./...   # skip the end-to-end (dev server) tests
go test ./kafka -run TestGroupReader_AckCommits -v
make generate          # regenerate generated/kafkaapi from kafka.nexusrpc.yaml (downloads nexgen to .build/)
make lint              # golangci-lint
make ci                # vet, workflowcheck, lint, test
```

Needs Go SDK v1.50 or later (`temporalnexus.StartActivity`, the standalone Nexus client).

### Rules for changes

- The contract is `kafka.nexusrpc.yaml`. After editing it, run `make generate` and commit
  `generated/kafkaapi`; never edit generated code by hand. Optional fields become pointers.
- `kafka/` has no Temporal imports; keep it that way. It returns sentinel errors
  (`ErrInvalidPosition`, `ErrGroupInUse`, `ErrRecordTooLarge`, `ErrRejected`) that the programs
  map to `ApplicationError`s. Both programs share it, so a change to reading affects both.
- A new setting goes in the program's startup report too (`config.report` in `streamer/config.go`,
  `settings.report` in `nexusworker/main.go`, built with `kafka.Report` and `kafka.EnvSetting`)
  and in README's Settings table. The report never shows the password.
- For a test that guards a fix, revert the fix and confirm the test fails before calling it done.
  Existing guard tests were checked this way.

### How the code works

**`kafka/`**
- `GroupReader` (Nexus worker). Per read:
  1. Commit the ack. Kafka rejects commits from non-members while the group has live members,
     which becomes `ErrGroupInUse`. Batch size 0 stops here.
  2. Fetch the group's committed offsets and plan start and end offsets.
  3. Commit any start `KAFKA_START_FROM` chose, so a retried first read with `newest` doesn't
     skip what arrived meanwhile.
  4. Read through a pooled consumer client (`MaxConcurrentReads`; the Nexus worker runs no more
     read activities than that).

  A read drops what it fetched beyond its batch, so `NewGroupReader`'s `fetchMaxBytes` caps each
  fetch at the batch byte limit. With no time limit a partition ends at its last stable offset;
  with one, partitions have no end and the read waits for new records.
- Positions are opaque, unsigned base64 JSON naming group, topic, cluster, topic ID, and offsets.
  Callers are trusted, but an ack from an earlier incarnation of the topic is refused. `identify`
  sends an uncached `kmsg.MetadataRequest` on every read, because kadm's `Metadata` is cached for
  up to 5 seconds. Kafka deletes a group's offsets with its topic (checked on 4.3.1), so committed
  offsets need no such check.
- `GroupMember` (streamer) is a kgo consumer group with `DisableAutoCommit` and
  `BlockRebalanceOnPoll`.
  - `chooseStarts` (`AdjustFetchOffsetsFn`) resolves each assigned partition's start with the
    shared `startOffset` and commits it before consuming. A release or stop before the first
    commit then can't let the next member choose `newest` again.
  - `Commit`, `Release`, and `Close` must call `AllowRebalance`, or leaving the group hangs.
  - `OnPartitionsCallbackBlocked` closes `RebalanceWanted()`. `Next` then returns early, and the
    streamer `Release`s an undelivered batch: `SetOffsets` back to its first records, then
    `AllowRebalance`.
- `Identify` reads the cluster ID and topic ID with a plain client, since a new group client would
  join the group. `GroupMember.Identity` does the same through the member.
- `fetchErrors` logs `ErrDataLoss` and `ErrGroupSession`, which kgo recovers from, rather than
  failing.
- `Record.tombstone` marks a null value; dead letters keep the null.
- `batch` and `pollUntil` are the shared batching rule: count, encoded bytes, and time limit. A
  batch never takes a record after one that didn't fit, which would move the partition's position
  past it.
- `startOffset` and kgo's `ConsumeResetOffset` implement `KAFKA_START_FROM` the same way for both
  programs.
- `ConnectAtStartup` wraps `WaitForTopic`: `Identify` every 5 seconds, 10 seconds per attempt, a
  WARN per failure, INFO on success. Both programs call it after the startup report and before
  dialing Temporal, because kgo connects lazily and would otherwise wait silently.

**`streamer/`**
- `streamer.run` loops: `Next`, then `deliver`, then `Commit`.
  - `deliver` handles partitions in parallel. Each partition's records are split into as few
    `ReceiveInput`s as fit `STREAM_MAX_INPUT_BYTES`, started in order.
  - `Commit` uses a context that survives shutdown.
- `nexusStarter` starts operations with `USE_EXISTING` and `ALLOW_DUPLICATE_FAILED_ONLY`;
  `NexusOperationExecutionAlreadyStarted` counts as delivered.
- Only two things go to the dead-letter topic: an `InvalidArgument` saying the input exceeds the
  size limit (`refusedAsTooLarge`; a part of several records is first split in half and retried),
  and single messages over `STREAM_MAX_INPUT_BYTES`. The size check is local because gRPC's own
  too-large error is an internal SDK type. Every other error retries forever, so an outage stalls
  the stream instead of draining it into dead letters. `DELETE_DEAD_LETTERS=true` makes
  `deadLetter` log and skip instead.
- The operation ID is `kafka-<source>-<partition>-<first>-<last>`. `sourceKey` hashes cluster ID,
  topic ID, topic, and group as a JSON array, which can't be ambiguous, and the identity is
  fetched for every batch. A redelivered batch can end at another offset and get a new ID, so
  batches can duplicate messages; `KAFKA_BATCH_SIZE=1` rules that out.

**`nexusworker/`**
- One process serves one topic (`KAFKA_TOPIC`) behind one endpoint.
- `read`'s `Start` resolves the consumer group with `activities.groupFor` and validates the ack,
  refusing either as `BadRequest` before any activity starts, then starts `activities.Read` or
  `activities.Write`. With `KAFKA_GROUP` set, a worker serves only that group, and an empty
  request means it; without it, the request must name one.
- `newWorkers` makes two workers. One runs Nexus and writes on the task queue; the other runs
  reads on `readQueue(taskQueue)` (`<taskQueue>-reads`), capped at `MaxConcurrentReads`, so
  waiting reads never block writes.
- `Read` heartbeats from a goroutine every `heartbeatInterval` (70 seconds), starting one
  interval in, with a heartbeat timeout of twice that. Each heartbeat is a billable Action, so
  reads of a minute or less send none; 70 seconds is also above the SDK's 60-second maximum
  throttle, so heartbeats aren't delayed.
- `Write` adds the header `dedupe-id: <activityID>-<i>`.

**`test/`** (test only)
- `testenv`: kfake plus a dev server with both namespaces.
- `nexuscaller`: `ReaderWorkflow` reads a list of `Source`s, one batch from each in turn, with ack
  and continue-as-new carrying every source's position. It long-polls with
  `DefaultTimeLimitSeconds` (60, under the heartbeat interval). One activity per record is
  deliberate for the demo. It logs each read and processed message with the topic between `***`,
  because showing which topic is read is the point of the README demo.
  `shouldContinueAsNew` is a variable so tests can replace it.
- `streamreceiver`: the example receiver.
- `kafka/`: docker-compose plus seed, produce, and consume scripts. Seed writes 10 messages to
  `customers` and 3 to `orders`.

### Testing notes

- kfake sometimes answers a fetch only when the client's 5-second fetch wait ends, even with
  records available. Tests that expect a partial batch at a deadline use `partialWait` (6
  seconds). Tests that expect records use a batch size equal to the records expected, so they
  return as soon as those arrive.
