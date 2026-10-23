# nexus-connector-kafka

A Temporal connector for one Kafka topic. It runs in two ways, which share their Kafka code:

- **The streamer** streams every message into Temporal, like the Python prototype. It joins a
  consumer group and starts one standalone Nexus operation per batch, through a Nexus endpoint.
  It only sends messages in: nothing comes back.
- **The Nexus worker** serves the topic as a Nexus service with two operations. `read` returns the
  next batch for a consumer group. `write` writes messages. Each call runs as a standalone
  activity.

```
 streamer                                    namespace: kafka-test
┌──────────────────────────────┐  Nexus     ┌─────────────────────────────────┐
│ consumer group member        │──────────▶ │ KafkaMessageReceiver.receive    │
│  one operation per batch     │  endpoint  │  (your service; one call per    │
└──────────────────────────────┘            │   batch)                        │
                                            └─────────────────────────────────┘

 namespace: kafka-test                       namespace: kafka-connector
┌──────────────────────────────┐  Nexus     ┌─────────────────────────────────┐
│ your workflow                │──────────▶ │ nexusworker: KafkaService       │
│  read(group, ack) in a loop  │  endpoint  │  read  → standalone activity    │
│  write(messages)             │ ◀───────── │  write → standalone activity    │
└──────────────────────────────┘            └─────────────────────────────────┘
```

## Set up

### Prerequisites

- **Go 1.26** or later.
- **Temporal CLI 1.9.1** or later (it includes Temporal Server 1.32). Check with
  `temporal --version`.
- **Docker**, for the local Kafka.

Run every command from the repository root. Commands that keep running need their own terminal.

### 1. Start Temporal (terminal 1)

```bash
temporal server start-dev \
  --dynamic-config-value activity.enableCallbacks=true \
  --dynamic-config-value nexusoperation.enableStandalone=true \
  --namespace kafka-connector \
  --namespace kafka-test
```

`kafka-connector` is where the streamer and the Nexus worker run, and `kafka-test` is where the
test programs run. `nexusoperation.enableStandalone` lets the streamer start standalone Nexus
operations. `activity.enableCallbacks` lets the Nexus worker's operations complete when their
activities do. The Web UI is at <http://localhost:8233>.

### 2. Start Kafka and seed it

```bash
docker compose -f test/kafka/docker-compose.yml up -d --wait
test/kafka/scripts/seed.sh
```

This starts a single-node Kafka on `localhost:9092` that requires a SASL/SCRAM login. The seed
script creates the topic `events` (3 partitions) and its dead-letter topic
`events-dead-letters`, and writes 10 messages to `events`. The settings are in
[`test/kafka/.env`](test/kafka/.env).

## Way 1: stream messages into Temporal

### 1. Create the endpoint and start the receiver (terminal 2)

The streamer delivers each message to a Nexus endpoint. Here it routes to the test receiver in
`kafka-test`:

```bash
temporal operator nexus endpoint create \
  --name kafka-receiver-endpoint \
  --target-namespace kafka-test \
  --target-task-queue kafka-receiver
go run ./test/streamreceiver
```

### 2. Start the streamer (terminal 3)

```bash
set -a; source test/kafka/.env; set +a
export KAFKA_GROUP=streamer KAFKA_DEAD_LETTER_TOPIC=events-dead-letters STREAM_ENDPOINT=kafka-receiver-endpoint
go run ./streamer
```

It joins the group `streamer`, which has no committed offsets yet, so it starts at the oldest
message. Terminal 2 logs each of the 10 seeded messages. The streamer delivers them as one batch,
one operation for each of the 3 partitions, so each `ReceiveWorkflow` logs a partition's messages:

```
INFO  Received message ... WorkflowID kafka-batch-events-0-0-2 ... topic events partition 0 offset 0 value seed message 1 of 10
INFO  Received message ... WorkflowID kafka-batch-events-0-0-2 ... topic events partition 0 offset 1 value seed message 4 of 10
```

### 3. Write a message and watch it arrive

```bash
test/kafka/scripts/produce.sh "hello from the console producer"
```

Terminal 2 logs it within a second or two. Stop the streamer with Ctrl-C and start it again: it
delivers nothing it already delivered, because it committed each batch.

In the Web UI, `kafka-connector` shows one standalone Nexus operation per batch, under Nexus,
and `kafka-test` shows the `ReceiveWorkflow` each one started.

## Way 2: read and write through the Nexus worker

### 1. Create the endpoint and start the Nexus worker (terminal 4)

```bash
temporal operator nexus endpoint create \
  --name kafka-endpoint \
  --target-namespace kafka-connector \
  --target-task-queue nexus-connector-kafka \
  --description-file ./endpoint-description.md
set -a; source test/kafka/.env; set +a
go run ./nexusworker
```

### 2. Start the test caller's worker (terminal 5)

```bash
go run ./test/nexuscaller/worker
```

It runs in `kafka-test` and needs no Kafka settings: it reaches Kafka only through the endpoint.

### 3. Read by consumer group

```bash
go run ./test/nexuscaller/starter read -group reader1
```

This starts `ReaderWorkflow` for the group `reader1`. It reads a batch, processes each message
(terminal 5 logs `Processed message`), acknowledges the batch on its next read, and repeats. A
read waits up to 30 seconds for its batch to fill, then returns what it has. The reader runs until
you stop it:

```bash
temporal workflow terminate --namespace kafka-test --workflow-id kafka-reader-reader1
```

Start it again and it continues after the last batch it acknowledged. Its position is kept in
Kafka, as the group's committed offsets. While it runs, starting a second reader for the same group
fails with `Workflow execution is already running`, because the workflow ID names the group.

### 4. Write

```bash
go run ./test/nexuscaller/starter write "first" "second"
```

It prints `Wrote 2 message(s)`, and the running reader picks them up, as does the streamer if it
is still running. Each written message has a `dedupe-id` header; see them with
`test/kafka/scripts/consume.sh`:

```
Partition:0	Offset:3	dedupe-id:kafka-write-c1e0ee92-bd58-4dfb-8bbd-6f4ada7c5033-0	first
```

## Tear down

Stop the programs with Ctrl-C, then:

```bash
docker compose -f test/kafka/docker-compose.yml down -v
temporal operator nexus endpoint delete --name kafka-receiver-endpoint
temporal operator nexus endpoint delete --name kafka-endpoint
```

Stopping the dev server discards everything else, including the endpoints.

## Folders

| Folder | What is in it |
|---|---|
| `kafka/` | Kafka code both ways share: connecting, reading in batches, writing, dead letters. No Temporal code. |
| `streamer/` | Way 1: the program that streams each message into Temporal. |
| `nexusworker/` | Way 2: the program that serves `read` and `write` as Nexus operations. |
| `generated/kafkaapi/` | Go types for the contract, generated from `kafka.nexusrpc.yaml`. |
| `test/` | Testing only: a receiver for the streamer, a caller for the Nexus worker, shared end-to-end test setup, and a local Kafka. |

At the top level, `kafka.nexusrpc.yaml` is the contract and `endpoint-description.md` is the Nexus
worker endpoint's description.

## How it works

### The contract

[`kafka.nexusrpc.yaml`](kafka.nexusrpc.yaml) defines two Nexus services:

- **`KafkaService`**, served by the Nexus worker: `read` and `write`.
- **`KafkaMessageReceiver`**, which the streamer calls: `receive`, one call per batch, with the
  batch's messages from one partition in offset order. You implement it to receive the topic's
  messages.

Messages are delivered as a `Record`: `topic`, `partition`, `offset`, `timestamp`, `key`, `value`,
`tombstone`, and `headers`. Keys, values, and header values are bytes, base64 in JSON. `tombstone`
is true for a message with no value at all (null in Kafka), which on a compacted topic deletes its
key. Its `value` is empty, but an empty `value` alone isn't a tombstone. Topic, partition, and
offset together identify a message, so a receiver can use them to drop a message it has already
handled.

`make generate` turns the contract into `generated/kafkaapi` with
[nexgen](https://github.com/temporalio/nexgen). The generated types check their input when they
are decoded, so out-of-range values are rejected before any code here runs.

### Reading in batches

Both ways read in batches, with the same rule. Given a batch size and a time limit, a read returns
as soon as it has the batch size. Otherwise it returns what it has when the time limit passes. With
no time limit, it returns what Kafka has right away:

- **The Nexus worker** returns the messages already in the topic, up to the batch size, or an
  empty batch if there are none.
- **The streamer** waits until Kafka has messages, then returns the first ones Kafka sends, up to
  the batch size. A streamer has nothing else to do, so an empty batch would only make it ask
  again.

Messages from one partition are always in order.

### Where reading starts

A group that has committed offsets continues after them. For a partition the group has no
committed offset for (a new group, or a partition added to the topic), `KAFKA_START_FROM` decides:
`oldest`, the default, starts at the oldest message still in the topic, and `newest` reads only
messages written from then on. The same rule applies when a committed offset is no longer in the
topic, because retention deleted it or the topic was recreated. The Nexus worker commits a start
it chose this way before it reads, so a retried read starts at the same place even with `newest`.

### The streamer

The streamer is a live member of its consumer group (`KAFKA_GROUP`). Kafka splits the topic's
partitions among the group's members, so running more streamers spreads the load, up to one per
partition. When one stops, Kafka gives its partitions to the others. For a high-throughput topic,
use the streamer.

For each batch, it starts one standalone Nexus operation per partition, through `STREAM_ENDPOINT`,
with that partition's messages in offset order. If they are larger together than an operation's
input can be (`STREAM_MAX_INPUT_BYTES`), it splits them over as few operations as fit, in order.
Partitions are delivered in parallel. It doesn't wait for the operations to finish. Once every
message in the batch has an operation, it commits the batch, so the group continues after it.

While a batch is being delivered, the group doesn't rebalance, so its partitions stay with this
streamer until it is committed. A rebalance can't wait for long: Kafka removes a member that holds
one up past its rebalance timeout (a minute). So when a rebalance is waiting, because a streamer
joined or left, a streamer that hasn't finished its batch (for example, because Temporal is
unreachable) releases it without committing. The rebalance goes ahead, and whichever streamer gets
each partition delivers the batch again.

**Nothing comes back.** The streamer only sends messages in. It doesn't receive the operations'
results, and it can't send replies to Kafka. A service that needs to send something back to
Kafka uses the Nexus worker's `write`.

**Delivery is at least once.** If the streamer stops after starting a batch's operations but
before committing, the group delivers the batch again. Each operation's ID is built from its
messages: `kafka-<source>-<partition>-<first offset>-<last offset>`. `<source>` is 32 hex
characters, a hash of the Kafka cluster's ID, the topic's name and ID, and the group. Hashing them
keeps names apart that would otherwise run together, such as topic `a-b` with group `c` and topic
`a` with group `b-c`. The topic's ID changes if the topic is deleted and created again, so a new
topic's offsets don't reuse the old one's IDs. The operation's summary shows the topic, partition,
and offsets. The start uses two policies:

- `USE_EXISTING`: if the operation is still running, the start attaches to it.
- `ALLOW_DUPLICATE_FAILED_ONLY`: if it completed, the server reports it already started, which
  counts as delivered. If it failed, was cancelled, or timed out, it runs again.

So a batch delivered again with the same messages starts nothing new while the server still
remembers its operation. Two groups streaming the same topic get different IDs, so each delivers
every message. This dedupes deliveries of the same Kafka messages, not copies of a message written
twice: those have different offsets.

**Duplicates are possible when streaming in batches.** A batch delivered again starts at the same
offset, but it can end at a different one: more messages may have arrived since, or the time
limit may cut it at a different point. A batch that ends somewhere else has a different ID, so it
starts a new operation, and the messages the two batches share are received twice. This happens
only when the streamer stops between starting a batch's operations and committing them, the
commit fails, or the streamer releases a batch for a rebalance. Receivers should drop messages they have already handled, using topic, partition,
and offset. If duplicates aren't acceptable, set `KAFKA_BATCH_SIZE=1`: each operation then holds
one message, so its ID never changes, at the cost of one operation per message.

**The dead-letter topic.** A message goes to `KAFKA_DEAD_LETTER_TOPIC` when no retry could deliver
it: it is larger on its own than an operation's input can be (`STREAM_MAX_INPUT_BYTES`, 2 MiB by
default, measured as encoded), or the server rejects its operation as invalid, which sends every
message in that operation. A dead letter has the message's key, value (null for a tombstone), and
headers, plus `dead-letter-reason`, `dead-letter-source` (`topic/partition/offset`), and
`dead-letter-group`. Every other error, such as Temporal being unreachable, an endpoint that
doesn't exist, or standalone Nexus operations not being enabled, is retried with backoff (1
second, doubling, up to 30 seconds) until it works. The stream waits rather than move a working
topic into the dead-letter topic. The streamer logs every retry.

**When an operation fails.** Once Temporal accepts an operation, it is durable: Temporal runs it
to completion, retrying as the receiving service's code says, and the streamer commits the batch.
From then on, Kafka won't deliver those messages again. An operation still ends unsuccessfully if
the receiving service fails it (for example, its workflow fails), it is cancelled or terminated,
or it runs past `STREAM_OPERATION_TIMEOUT`. Nothing in the connector notices. To find these, list
the streamer's operations that ended any other way than completing:

```bash
temporal nexus operation list --namespace kafka-connector \
  --query 'ExecutionStatus IN ("Failed", "TimedOut", "Canceled", "Terminated")'
```

Each ID names the partition and offsets, so you know which messages it held. `describe` shows why
it failed, but not its input. To run it again:

- **If the receiving service still has the work,** retry it there. For a workflow-backed
  `receive`, the workflow's history holds the messages, so you can reset the workflow.
- **Otherwise, deliver the messages again from Kafka.** Stop every streamer in the group, move the
  group's offset for that partition back to the operation's first offset, and start the streamers
  again:

  ```bash
  kafka-consumer-groups.sh --bootstrap-server <brokers> --command-config <client config> \
    --group <group> --topic <topic>:<partition> --reset-offsets --to-offset <first offset> --execute
  ```

  Every message after that offset in the partition is delivered again. With `KAFKA_BATCH_SIZE=1`,
  each message's ID is the same as before, so messages whose operations completed start nothing,
  and only the failed ones run again. With larger batches, IDs can differ, so some messages that
  were handled can be received again.

### The Nexus worker

**`read(group, batchSize, timeLimitSeconds, ack)`** returns a batch of records and a `position`.
The position is an opaque string; pass it back as `ack` on your next read. A read with an `ack`
commits it first, then reads from there. A read without one starts from the group's committed
offsets, so it returns the oldest batch the group hasn't acknowledged.

The Nexus worker never joins the group. It uses the group only to store offsets, so any worker
process can serve any read, and a worker can restart between reads without losing anything.

- **Starting:** name a group. The caller never sees offsets. A new group needs no setup: it
  starts where `KAFKA_START_FROM` says, and Kafka creates it on the first commit.
- **Reading in a loop:** pass each read's `position` as the next read's `ack`. There is one call
  per batch.
- **The last batch:** a run that ends without acknowledging its last batch leaves it for the next
  reader to read again. To acknowledge it, end with a read that has the `ack` and `batchSize` 0.
  That read only commits: it doesn't ask Kafka for anything.
- **A new run or a new workflow:** call `read` without an `ack`. It continues after the group's
  last acknowledged batch, so nothing has to be passed between runs.

**One reader per group.** Every read starts from what Kafka has stored for the group, so two
readers of one group read the same batches. The test caller's starter uses the workflow ID
`kafka-reader-<group>` to keep it to one. Run as many Nexus worker processes as you like: they
hold no state.

**Don't use a group that has live members,** such as the streamer's. Kafka won't let a non-member
commit for such a group, so a read fails with `GroupInUse` when it has an `ack` to commit, or a
start to commit for a partition the group has no offset for.

**Positions are trusted.** A position is the group, topic, and offsets, encoded. The worker checks
that an `ack` is well formed and names the group and topic being read, but not that a read
returned it. A caller that makes up or edits a position can commit past messages it never read.
Only let callers you trust reach the endpoint.

**Kafka forgets an idle group's offsets.** Kafka keeps an empty group's committed offsets for
`offsets.retention.minutes` (7 days by default) after its last commit. A reader that stops for
longer starts again where `KAFKA_START_FROM` says.

**Batch size in bytes.** A read's result is at most `KAFKA_MAX_BATCH_BYTES` (2 MiB, Temporal's
payload limit, by default), measured as encoded. It keeps 64 KiB of that for the position and
the rest of the result. A batch stops before the message that would go over, and the next read
starts with that message. A message too large to fit on its own fails the read with
`RecordTooLarge`. It stays the group's next message, so the group can't read past it.

**Fetches are sized to a batch.** Each read borrows one of the worker's Kafka consumers, fetches
from the group's offsets, and returns the consumer when it's done. That drops whatever it fetched
beyond its batch, because the next read for the group may go to another worker process, so the
next read fetches that part again. To keep that waste small, one fetch asks Kafka for no more
than `KAFKA_MAX_BATCH_BYTES` minus the 64 KiB reserve, instead of the Kafka client's default of 1
MiB per partition and 50 MiB in all. Records encode larger in a result than Kafka sends them, so
one fetch still holds a full batch. Small batches (`batchSize` much smaller than what fits in the
byte limit) against a large backlog still fetch more than they return. See
[Possible future changes](#possible-future-changes).

**Each read heartbeats** every 10 seconds, from when it starts until it returns, so a worker that
dies is noticed within the 30-second heartbeat timeout. A worker process runs at most 8 activities
at once: the number of Kafka consumers it keeps for reads, so a read never waits for one.

**`write(messages)`** writes messages in order and waits for Kafka to acknowledge them. A retried
write can write a message twice. Each message gets a `dedupe-id` header,
`<activity ID>-<index>`, which is the same on every attempt, so a consumer can drop the copies.

**Errors.** A failure retrying can't fix is an `ApplicationError` with one of these types:

| Type | When |
|---|---|
| `InvalidRequest` | The `ack` isn't a well-formed position for this group and topic. Rejected before any activity starts, as a `BAD_REQUEST` Nexus error. |
| `GroupInUse` | The group has live members. |
| `RecordTooLarge` | The next message doesn't fit in a result on its own. |
| `Rejected` | Kafka refused the write, for example because a message is larger than the broker allows. |

Other failures are retried. A read retries for its time limit plus 5 minutes, and a write for 10
minutes. Retrying a read is safe: it commits the same `ack` again and reads the same batch again.

### The test caller

[`test/nexuscaller`](test/nexuscaller/workflows.go) shows how to read in a loop. `ReaderWorkflow`
reads a batch, processes its messages in parallel (one activity each), then acknowledges the batch
on its next read. When Temporal suggests it, because the history is getting long, it continues as
new and passes its position to the new run, whose first read acknowledges it. After an empty batch
read with no time limit, it waits 5 seconds before reading again.

### Settings

Both programs connect to Temporal the way the `temporal` CLI does: the `TEMPORAL_*` environment
variables (`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, `TEMPORAL_TLS_*`) and the
CLI's profile file. The namespace defaults to `kafka-connector`.

| Variable | Used by | Meaning |
|---|---|---|
| `KAFKA_BROKERS` | both | Comma-separated `host:port` list. Required. |
| `KAFKA_TOPIC` | both | The topic. Required. |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | both | SASL/SCRAM-SHA-256 login. Leave unset for no authentication. |
| `KAFKA_START_FROM` | both | `oldest` (default) or `newest`; see [Where reading starts](#where-reading-starts). |
| `KAFKA_MAX_BATCH_BYTES` | Nexus worker | Most bytes a read's result encodes to. Default 2097152. |
| `KAFKA_GROUP` | streamer | The consumer group to join. Required. |
| `KAFKA_DEAD_LETTER_TOPIC` | streamer | Where undeliverable messages go. Required; must already exist. |
| `KAFKA_BATCH_SIZE` | streamer | Most messages per batch. Default 100. Set 1 to rule out [duplicates](#the-streamer). |
| `KAFKA_BATCH_TIME_LIMIT` | streamer | How long a batch waits to fill, such as `500ms`. Default 0. |
| `STREAM_ENDPOINT` | streamer | The Nexus endpoint to deliver to. Required. |
| `STREAM_SERVICE`, `STREAM_OPERATION` | streamer | The service and operation. Default `KafkaMessageReceiver` and `receive`. |
| `STREAM_OPERATION_TIMEOUT` | streamer | How long each operation may take. Default `24h`. |
| `STREAM_MAX_INPUT_BYTES` | streamer | The largest encoded input sent to an operation. Default 2097152. |

On Temporal Cloud, or a server you run, the connector's namespace needs standalone Nexus operations
(for the streamer) and standalone activities with completion callbacks (for the Nexus worker).
Check with Temporal that both are available for your namespace.

### Tests

```bash
make test              # all tests, with the race detector
go test -short ./...   # skip the end-to-end tests
```

The tests need no Docker. They run Kafka in memory with franz-go's `kfake`.

- `kafka/` tests reading for a group (acks, ack-only reads, retried first reads, batch and byte
  limits, time limits, where a new group starts, groups in use), reading as a group member
  (commits, releasing a batch, noticing a waiting rebalance, where it starts), and tombstones.
- `streamer/` tests delivery order, splitting batches to fit an input, commits, redelivery with the
  same operation IDs, the duplicate when a redelivered batch ends elsewhere, releasing a stuck
  batch for a rebalance, operation IDs' source key, retries, and the dead-letter topic. Its end-to-end tests start a dev server with the `temporal` CLI and deliver
  to a receiver in another namespace. They show that a batch delivered again starts nothing new
  once its operation completed, and runs again if it failed.
- `nexusworker/` tests that a read heartbeats before it reaches Kafka. Its end-to-end tests write
  and read across namespaces through the endpoint, and run the test caller's `ReaderWorkflow`.
- `test/nexuscaller/` tests that `ReaderWorkflow` acknowledges every batch and carries its
  position across continue-as-new.

The end-to-end tests are skipped if `temporal` isn't on the `PATH`. `make ci` runs vet,
workflowcheck, lint, and the tests, as CI does.

### Changing the contract

Edit `kafka.nexusrpc.yaml`, then run `make generate` and commit `generated/kafkaapi`. It
downloads nexgen into `.build/` the first time. CI fails if the generated code is out of date.

### Known limitations

- **One topic** per connector, set by `KAFKA_TOPIC`.
- **SASL/SCRAM-SHA-256 or no authentication.** TLS and other SASL mechanisms aren't supported yet.
- **A message too large for a read's result blocks its group** in the Nexus worker; see
  [The Nexus worker](#the-nexus-worker).
- **The dead-letter topic is only for messages Temporal refuses.** The streamer doesn't see
  failures inside the operations it starts; see [When an operation fails](#the-streamer).
- **`write` can't write a tombstone.** A message's `value` is always bytes, so `write` has no way
  to write a null value.

### Possible future changes

- **Keep a group's consumer between reads.** A Nexus worker process could keep the consumer a read
  used assigned to the group's partitions, with what it fetched beyond the batch still buffered,
  and give it to the next read for that group if that read's `ack` is the position it stopped at.
  That read would then need no new fetch for what is already buffered, and could skip asking Kafka
  for the group's offsets. It helps only when reads for a group return to the same process, which
  is guaranteed with one worker process but not with several. It needs limits on how many groups
  are kept and how long, and memory for their buffers. Worth doing if measurements show reads
  fetching much more than they return: compare bytes fetched from Kafka with bytes returned, at
  real batch sizes and backlogs.
