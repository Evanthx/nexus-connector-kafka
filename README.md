# nexus-connector-kafka

- [Configure the streamer](#configure-the-streamer)
- [Configure the Nexus worker](#configure-the-nexus-worker)
- [Demo](#demo), a local Kafka and Temporal to try it with
- [Method 1: stream messages into Temporal](#method-1-stream-messages-into-temporal)
- [Method 2: read and write through the Nexus worker](#method-2-read-and-write-through-the-nexus-worker)
- [Tear down](#tear-down)
- [Folders](#folders)
- [How it works](#how-it-works)
  - [The contract](#the-contract)
  - [Reading in batches](#reading-in-batches)
  - [Where reading starts](#where-reading-starts)
  - [The streamer](#the-streamer)
  - [The Nexus worker](#the-nexus-worker)
  - [The test caller](#the-test-caller)
  - [Cost](#cost)
  - [Settings](#settings)
  - [Tests](#tests)
  - [Changing the contract](#changing-the-contract)
  - [Known limitations](#known-limitations)
  - [Possible future changes](#possible-future-changes)

AI agents: see [AGENTS.md](AGENTS.md) for how to set up and run this connector, and how the code
works.

A Temporal connector for one Kafka topic. It has two methods of running, which share their Kafka code:

- **The streamer** streams every message into Temporal. It joins a consumer group and starts one
  standalone Nexus operation per batch, through a Nexus endpoint. It only sends messages in: it
  doesn't wait for results or write replies to Kafka. This will change once completion callbacks 
  are implemented in the Go SDK, at that point replies could be written to Kafka - but only replies, 
  you will not be able to originate a message and send it to the streamer to write to Kafka.
- **The Nexus worker** serves the topic as a Nexus service with two operations. `read` returns the
  next batch for a consumer group. `write` writes messages. Each call runs as a standalone
  activity.

Use the streamer to move messages from Kafka into Temporal: it delivers as soon as messages
arrive, scales out across partitions, and costs nothing while the topic is idle. 
Use the Nexus worker to write to Kafka, or to read when a workflow should pull messages at its
own pace or for ad hoc reads. Each consumer group can have only one reader at a time.

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

## Configure the streamer

Set these environment variables, then run `go run ./streamer` (or a binary built from it):

| Variable | Set it to |
|---|---|
| `KAFKA_BROKERS` | Your Kafka brokers, comma-separated `host:port`. |
| `KAFKA_TOPIC` | The topic to stream. |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | A SASL/SCRAM-SHA-256 login, if your Kafka needs one. |
| `KAFKA_GROUP` | The consumer group the streamer joins. Streamers with the same consumer group share the topic's partitions. |
| `KAFKA_DEAD_LETTER_TOPIC` | An existing topic for messages that can't be delivered. Or leave it unset and set `DELETE_DEAD_LETTERS=true` to delete them instead. |
| `STREAM_ENDPOINT` | The Nexus endpoint to deliver to. It routes to the service that receives the messages, `KafkaMessageReceiver.receive`. |
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` | Your Temporal server and namespace (default `kafka-connector`), plus `TEMPORAL_API_KEY` for Temporal Cloud. The namespace needs standalone Nexus operations. |

Everything else has a default; see [Settings](#settings). When it starts, the streamer logs every
setting, marking the defaults, and what it will do with them: check it to confirm the setup. It
then asks Kafka for the topic, and until Kafka answers it logs a warning every 5 seconds with the
reason (unreachable brokers, a failed login, a missing topic), then `Connected to Kafka`. Only then
does it connect to Temporal and start streaming.

## Configure the Nexus worker

Set these environment variables, then run `go run ./nexusworker` (or a binary built from it):

| Variable | Set it to |
|---|---|
| `KAFKA_BROKERS` | Your Kafka brokers, comma-separated `host:port`. |
| `KAFKA_TOPIC` | The topic this Nexus worker reads and writes. Callers can't choose another. |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | A SASL/SCRAM-SHA-256 login, if your Kafka needs one. |
| `KAFKA_GROUP` | Optional. The only consumer group reads may use. Leave it unset to let each read name its own. |
| `NEXUS_TASK_QUEUE` | The task queue to serve (default `nexus-connector-kafka`). Give each Nexus worker for a different topic or consumer group its own. |
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` | Your Temporal server and namespace (default `kafka-connector`), plus `TEMPORAL_API_KEY` for Temporal Cloud. The namespace needs standalone activities with completion callbacks. |

Then create a Nexus endpoint that targets the Nexus worker's namespace and task queue. Callers
reach the topic through that endpoint. Everything else has a default; see [Settings](#settings).
When it starts, the Nexus worker logs every setting, marking the defaults, and what it will do
with them, including the task queue to point its endpoint at. Like the streamer, it then waits
for Kafka to answer, logging a warning every 5 seconds until it does, before it takes requests.
If Kafka goes away later, each read fails with the reason and is retried until its time limit
plus 5 minutes run out.

## Demo

The demo is one run-through, top to bottom. Steps 1 and 2 start Temporal and Kafka, which both
methods use. Steps 3 to 5 try Method 1, and steps 6 to 10 try Method 2 alongside it. **Leave every
program running until [Tear down](#tear-down)**: Method 2 needs the Temporal and Kafka from steps 1
and 2, and the streamer from Method 1 shows Method 2's writes arriving. By the end you have seven
terminals open.

### Prerequisites

- **Go 1.26** or later.
- **Temporal CLI 1.9.1** or later (it includes Temporal Server 1.32). Check with
  `temporal --version`.
- **Docker**, for the local Kafka.

Run every command from the repository root.

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

### 2. Start Kafka and seed it (terminal 2)

```bash
docker compose -f test/kafka/docker-compose.yml up -d --wait
test/kafka/scripts/seed.sh
```

This starts a single-node Kafka on `localhost:9092` that requires a SASL/SCRAM login, and returns
when it is up. The seed script creates two topics with 3 partitions each, `customers` and `orders`,
plus `customers-dead-letters` for the streamer. It writes 10 messages to `customers` and 3 to `orders`.
The settings are in [`test/kafka/.env`](test/kafka/.env).

## Method 1: stream messages into Temporal

### 3. Create the endpoint and start the receiver (terminal 2)

The streamer delivers each message to a Nexus endpoint. Here it routes to the test receiver in
`kafka-test`:

```bash
temporal operator nexus endpoint create \
  --name kafka-receiver-endpoint \
  --target-namespace kafka-test \
  --target-task-queue kafka-receiver
go run ./test/streamreceiver
```

### 4. Start the streamer (terminal 3)

```bash
set -a; source test/kafka/.env; set +a
export KAFKA_GROUP=streamer KAFKA_DEAD_LETTER_TOPIC=customers-dead-letters STREAM_ENDPOINT=kafka-receiver-endpoint
go run ./streamer
```

The streamer streams one topic, `customers`, from `.env`. It joins the consumer group `streamer`,
which has no committed offsets yet, so it starts at the oldest message. Terminal 2 logs each of the
10 seeded messages. The streamer delivers them as one batch, one operation for each of the 3
partitions, so each `ReceiveWorkflow` logs a partition's messages:

```
INFO  Received message ... WorkflowID kafka-batch-customers-0-0-2 ... topic customers partition 0 offset 0 value seed message 1 of 10
INFO  Received message ... WorkflowID kafka-batch-customers-0-0-2 ... topic customers partition 0 offset 1 value seed message 4 of 10
```

### 5. Write a message and watch it arrive (terminal 7)

Open another terminal; steps 9 and 10 use it too.

```bash
test/kafka/scripts/produce.sh "hello from the console producer"
```

Terminal 2 logs it within a second or two. If you like, stop the streamer with Ctrl-C and start it
again: it delivers nothing it already delivered, because it committed each batch. **Leave it
running** before you go on.

In the Web UI, `kafka-connector` shows one standalone Nexus operation per batch, under Nexus,
and `kafka-test` shows the `ReceiveWorkflow` each one started.

## Method 2: read and write through the Nexus worker

Keep everything from steps 1 to 5 running. Each Nexus worker serves one topic, behind a Nexus
endpoint of its own, so the endpoint a caller uses decides which topic it reads and writes. Here
two Nexus workers serve the two topics: `kafka-customers-endpoint` is the `customers` topic, and
`kafka-orders-endpoint` is the `orders` topic. One reader workflow reads both.

### 6. Start a Nexus worker for the customers topic (terminal 4)

```bash
temporal operator nexus endpoint create \
  --name kafka-customers-endpoint \
  --target-namespace kafka-connector \
  --target-task-queue nexus-connector-kafka \
  --description-file ./endpoint-description.md
set -a; source test/kafka/.env; set +a
go run ./nexusworker
```

It serves the topic `customers`, from `.env`. `KAFKA_GROUP` isn't set, so this Nexus worker has no
consumer group of its own: each read must name the consumer group it reads for, and a read that
doesn't fails with `InvalidRequest`. Different callers can share this endpoint with consumer groups
of their own, each reading the whole topic at its own pace. The reader in step 9 names `reader1`.
Step 7 shows the other choice.

### 7. Start a second Nexus worker, for the orders topic (terminal 5)

```bash
temporal operator nexus endpoint create \
  --name kafka-orders-endpoint \
  --target-namespace kafka-connector \
  --target-task-queue nexus-connector-kafka-orders \
  --description-file ./endpoint-description.md
set -a; source test/kafka/.env; set +a
export KAFKA_TOPIC=orders KAFKA_GROUP=orders-reader NEXUS_TASK_QUEUE=nexus-connector-kafka-orders
go run ./nexusworker
```

It serves the topic `orders` on a task queue of its own, which its endpoint targets. Unlike step 6,
it sets `KAFKA_GROUP`, so this Nexus worker has its own consumer group, `orders-reader`, and every
read through this endpoint uses it. A read may leave the group out or name `orders-reader`; a read
that names another consumer group fails with `InvalidRequest`.

### 8. Start the test caller's worker (terminal 6)

```bash
go run ./test/nexuscaller/worker
```

The test caller stands in for your own application: Temporal workflows, in `kafka-test`, that
read and write Kafka through the two endpoints. A Temporal worker is the process that runs
workflow and activity code. It polls a task queue (here `kafka-nexuscaller`) and runs whatever
Temporal gives it: this one runs `ReaderWorkflow`, `WriterWorkflow`, and the `ProcessRecord`
activity. The commands in steps 9 and 10 only ask Temporal to start a workflow; this worker is
what runs it, so terminal 6 is where you watch reading and processing happen.

Once started, it logs that it is waiting for workflows, and then nothing more: **this is
expected**. It isn't waiting for messages; nothing has asked it to run a workflow yet. Step 9
starts one, and the reading begins.

It is not a Nexus worker. The Nexus workers in terminals 4 and 5 serve the Kafka operations; this
worker calls them. It needs no Kafka settings, because it reaches Kafka only through the endpoints.

### 9. Read from both topics (terminal 7)

```bash
go run ./test/nexuscaller/starter read
```

The command prints the workflow it started and exits at once. That is all it does: it asks
Temporal to start a `ReaderWorkflow`, which keeps running on the test caller's worker after the
command exits. **Watch terminal 6** for what the reader does; the Web UI also shows the workflow,
in the `kafka-test` namespace, with each read and activity in its history.

The reader reads from both endpoints, and so from both topics: `kafka-customers-endpoint`
(`customers`) for the consumer group `reader1`, and `kafka-orders-endpoint` (`orders`) for that
endpoint's own consumer group, `orders-reader`. It reads a batch of up to 10 messages from
`customers` and processes each message, then does the same with `orders`, and repeats.
Terminal 6 logs each read, with the topic between `***`, and each message processed:

```
time=16:06:07 level=INFO msg="Reading through kafka-customers-endpoint; the read waits up to 60 seconds for its batch to fill"
time=16:06:07 level=INFO msg="Read 10 message(s) from topic *** customers *** through kafka-customers-endpoint"
time=16:06:07 level=INFO msg="Processed message from topic *** customers ***" partition=0 offset=0 value="seed message 1 of 10"
...
time=16:06:07 level=INFO msg="Reading through kafka-orders-endpoint; the read waits up to 60 seconds for its batch to fill"
time=16:07:07 level=INFO msg="Read 3 message(s) from topic *** orders *** through kafka-orders-endpoint"
time=16:07:07 level=INFO msg="Processed message from topic *** orders ***" partition=0 offset=0 value="seed order 1 of 3"
```

The messages in a batch are processed in parallel, so they are logged in no particular order.

Expect pauses. A read returns as soon as it has 10 messages, but with fewer it waits up to 60
seconds for more, then returns what it has. So the `customers` batch arrives at once, while the
`orders` read, which has only 3 messages, takes about a minute; after that, each quiet topic takes
a minute per read. `reader1` is a different consumer group from the streamer's, so the reader reads
the `customers` messages from the start, even though the streamer has already delivered them.

The reader runs until you stop it:

```bash
temporal workflow terminate --namespace kafka-test \
  --workflow-id kafka-reader-kafka-customers-endpoint/reader1+kafka-orders-endpoint
```

Start it again and it continues after the last batch it acknowledged from each topic. Its
positions are kept in Kafka, as each consumer group's committed offsets. While it runs, starting a
second reader of the same endpoints and consumer groups fails with `Workflow execution is already
running`, because the workflow ID names them. To read other endpoints or consumer groups, pass
`-source endpoint=group`, repeated, or `-source endpoint` for an endpoint that sets its own.

### 10. Write to each topic (terminal 7)

```bash
go run ./test/nexuscaller/starter write -endpoint kafka-customers-endpoint "customer Ada" "customer Grace"
go run ./test/nexuscaller/starter write -endpoint kafka-orders-endpoint "first order"
```

The two commands are the same apart from the endpoint, which chooses the topic: the first writes
two messages to `customers`, and the second writes one to `orders`. Each argument after the endpoint
is one message's text. (`-endpoint` defaults to `kafka-customers-endpoint`, so the first command works
without it too.)

Each command starts a `WriterWorkflow` on the test caller's worker (terminal 6) and waits for it.
The workflow calls `write` on the endpoint; the Nexus worker behind it (terminal 4 or 5) writes the
messages to its topic, in order, and the command prints how many were written:

```
Wrote 2 message(s) through kafka-customers-endpoint
Wrote 1 message(s) through kafka-orders-endpoint
```

Then watch them arrive. The reader in terminal 6 logs them on its next read of each topic, within
a minute or two. The streamer delivers the two `customers` messages too, so terminal 2 logs them
within a second or two: the streamer and the reader use different consumer groups, so each reads
every message. Nothing streams `orders`, so only the reader sees `first order`.

Each written message has a `dedupe-id` header; see them with
`test/kafka/scripts/consume.sh [topic]`:

```
Partition:0	Offset:3	dedupe-id:kafka-write-c1e0ee92-bd58-4dfb-8bbd-6f4ada7c5033-0	customer Ada
```

## Tear down

When you are done with both methods, stop the programs with Ctrl-C, then:

```bash
docker compose -f test/kafka/docker-compose.yml down -v
temporal operator nexus endpoint delete --name kafka-receiver-endpoint
temporal operator nexus endpoint delete --name kafka-customers-endpoint
temporal operator nexus endpoint delete --name kafka-orders-endpoint
```

Stopping the dev server discards everything else, including the endpoints.

## Folders

| Folder | What is in it |
|---|---|
| `kafka/` | Kafka code both methods share: connecting, reading in batches, writing, dead letters. No Temporal code. |
| `streamer/` | Method 1: the program that streams each message into Temporal. |
| `nexusworker/` | Method 2: the program that serves `read` and `write` as Nexus operations. |
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

Both methods read in batches, with the same rule. Given a batch size and a time limit, a read returns
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
topic, because retention deleted it or the topic was recreated. Both methods commit a start chosen
this way before reading from it, so a read that is retried, or a partition that moves to another
streamer before its first commit, starts at the same place even with `newest`. If retention
removes messages before they are read, Kafka's client logs it and carries on from the oldest
message left.

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
`a` with group `b-c`. The topic's ID changes if the topic is deleted and created again, and the
streamer asks for it with every batch, so a new topic's offsets don't reuse the old one's IDs. The operation's summary shows the topic, partition,
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
it, or is deleted, if `DELETE_DEAD_LETTERS` is `true` instead: it is larger on its own than an operation's input can be (`STREAM_MAX_INPUT_BYTES`, 2 MiB less
a 64 KiB reserve by default, measured as encoded), or the server refuses it alone as too large.
When the server refuses an operation of several messages as too large, the streamer splits it in
two and tries each half, so only a message refused on its own is dead-lettered. A dead letter has
the message's key, value (null for a tombstone), and headers, plus `dead-letter-reason`,
`dead-letter-source` (`topic/partition/offset`), and `dead-letter-group`. A deleted message is
logged with its topic, partition, offset, and the reason, then skipped: the streamer commits past
it, so it is never delivered. It stays in Kafka until retention removes it. Every other error, such as Temporal being unreachable, an endpoint that
doesn't exist, standalone Nexus operations not being enabled, or any other refusal (which would
refuse every batch alike), is retried with backoff (1 second, doubling, up to 30 seconds) until it
works. The stream waits rather than move a working
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

**One topic per Nexus worker.** A Nexus worker serves the topic in `KAFKA_TOPIC`; callers can't
choose it. To serve several topics, run a Nexus worker for each, every one with its own
`NEXUS_TASK_QUEUE` and a Nexus endpoint that targets it. Callers then choose the topic by choosing
the endpoint. Two Nexus workers can also serve the same topic for different consumer groups.

**The consumer group.** A Nexus worker started with `KAFKA_GROUP` serves only that consumer group:
a read may leave `group` empty or name that consumer group, and a read that names another fails
with `InvalidRequest`. A Nexus worker without `KAFKA_GROUP` serves any consumer group, and a read
must name one; a read without one fails with `InvalidRequest`. Both are refused before any activity
starts, with a message saying what to change.

**`read(group, batchSize, timeLimitSeconds, ack)`** returns a batch of records and a `position`.
The position is an opaque string; pass it back as `ack` on your next read. A read with an `ack`
commits it first, then reads from there. A read without one starts from the group's committed
offsets, so it returns the oldest batch the group hasn't acknowledged.

The Nexus worker never joins the group. It uses the group only to store offsets, so any worker
process can serve any read, and a worker can restart between reads without losing anything.

- **Starting:** name a consumer group, unless the Nexus worker has its own. The caller never sees
  offsets. A new group needs no setup: it
  starts where `KAFKA_START_FROM` says, and Kafka creates it on the first commit.
- **Reading in a loop:** pass each read's `position` as the next read's `ack`. There is one call
  per batch.
- **The last batch:** a run that ends without acknowledging its last batch leaves it for the next
  reader to read again. To acknowledge it, end with a read that has the `ack` and `batchSize` 0.
  That read only checks the topic and commits.
- **A new run or a new workflow:** call `read` without an `ack`. It continues after the group's
  last acknowledged batch, so nothing has to be passed between runs.

**One reader per group.** Every read starts from what Kafka has stored for the group, so two
readers of one group read the same batches. The test caller's starter names the reader's sources
in its workflow ID to keep it to one. Run as many Nexus worker processes for one topic as you like:
they hold no state.

**Don't use a group that has live members,** such as the streamer's. Kafka won't let a non-member
commit for such a group, so a read fails with `GroupInUse` when it has an `ack` to commit, or a
start to commit for a partition the group has no offset for.

In particular, the streamer and the Nexus worker can't share a consumer group. The streamer
commits a start for every partition when it joins, so a reader's first read on its consumer group
succeeds and returns whatever the streamer hasn't committed yet. Its next read, which acknowledges
that batch, fails with `GroupInUse`. Give each its own consumer group. Each consumer group gets
every message, so the streamer and a reader can both read the same topic.

**Positions are trusted.** A position is the group, topic, cluster, topic ID, and offsets,
encoded. The worker checks that an `ack` is well formed and names the group and topic being read,
but not that a read returned it. A caller that makes up or edits a position can commit past
messages it never read. Only let callers you trust reach the endpoint.

**A topic deleted and created again.** Its offsets start over and mean different messages, so an
`ack` returned before then fails with `InvalidRequest` rather than being committed. Read without an
`ack` to start over: Kafka deleted the group's offsets with the old topic, so reading starts where
`KAFKA_START_FROM` says.

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

**Each read heartbeats** every 70 seconds until it returns, starting 70 seconds in, and its
heartbeat timeout is 140 seconds. A read that returns sooner, such as the test caller's 60-second
reads, sends no heartbeat: each heartbeat that reaches Temporal is a billable Action. A worker that
dies during such a read is noticed at its start-to-close timeout (time limit plus a minute)
instead. A longer read is noticed within 140 seconds.

**Reads and writes run on separate task queues.** The endpoint targets `NEXUS_TASK_QUEUE`
(`nexus-connector-kafka` by default), where the Nexus operations and writes run. Reads run on the
same name with `-reads` added, on a second worker in the same process that runs at most 8 at once: the number of Kafka consumers it keeps
for reads, so a read never waits for one. Reads waiting out their time limits never hold up a
write.

**`write(messages)`** writes messages in order and waits for Kafka to acknowledge them. A retried
write can write a message twice. Each message gets a `dedupe-id` header,
`<activity ID>-<index>`, which is the same on every attempt, so a consumer can drop the copies.

**Errors.** A failure retrying can't fix is an `ApplicationError` with one of these types:

| Type | When |
|---|---|
| `InvalidRequest` | The read names no consumer group when the Nexus worker has none of its own, or names a different one when it has; or the `ack` isn't a well-formed position for this group and topic. These are rejected before any activity starts, as a `BAD_REQUEST` Nexus error. Also an `ack` from before the topic was deleted and created again. |
| `GroupInUse` | The group has live members. |
| `RecordTooLarge` | The next message doesn't fit in a result on its own. |
| `Rejected` | Kafka refused the write, for example because a message is larger than the broker allows. |

Other failures are retried. A read retries for its time limit plus 5 minutes, and a write for 10
minutes. Retrying a read is safe: it commits the same `ack` again and reads the same batch again.

### The test caller

[`test/nexuscaller`](test/nexuscaller/workflows.go) shows how to read in a loop, from one or more
Nexus endpoints. `ReaderWorkflow` takes a list of sources, each an endpoint and a consumer group
(left empty when the endpoint's Nexus worker has its own). Each pass reads a batch from each source
in turn, processes its messages in parallel (one activity each), and acknowledges the batch on its
next read from that source. Reading the sources one after another keeps the loop simple; the cost
is that a busy source waits while an idle one long-polls. When Temporal suggests it, because the
history is getting long, it continues as new and passes each source's position to the new run,
whose first read from that source acknowledges it. Its reads always
long-poll: `timeLimitSeconds` is 60 unless the starter's `-time-limit` says otherwise.

One activity per message shows messages processed in parallel, but it is the expensive choice:
see [Cost](#cost).

### Cost

Temporal Cloud bills by [Action](https://docs.temporal.io/evaluate/cloud/actions). What the
connector itself costs:

- **The streamer:** one standalone Nexus operation per partition in each batch, plus whatever the
  receiving service starts, such as one workflow per operation. It costs nothing while the topic
  is idle: it waits in Kafka.
- **The Nexus worker:** each `read` called from a workflow is one Action for the Nexus operation
  and one for the activity behind it, plus one per heartbeat (none for a read that returns within
  70 seconds; one every 70 seconds after that). `write` is the same without heartbeats.

These figures assume a standalone Nexus operation costs one Action to start, as a Nexus operation
started from a workflow does; Temporal's pricing pages don't state it for standalone ones yet.

With full batches, the connector's share is small: a few Actions per batch either way.
What decides the bill is the rest:

- **Process a batch, not a message.** One activity or workflow per message costs one Action per
  message, many times what reading the batch costs. Passing a batch to one activity costs one
  Action per batch, but a message that fails retries the whole batch.
- **Long-poll with `read`.** Give reads a time limit, such as the test caller's 60 seconds,
  rather than reading with no time limit and pausing after an empty batch. While the topic is idle,
  a 60-second read costs two Actions per wait (the read and its activity), about 120 an hour.
  Reading with no time limit and pausing 5 seconds after each empty batch costs about three (the
  read, its activity, and a timer) every 5 seconds, about 2,200 an hour. Keep the time limit under
  70 seconds to avoid heartbeats.
- **Let the streamer's batches fill.** With `KAFKA_BATCH_TIME_LIMIT` 0, a quiet topic's batches
  hold a message or two, so each message costs about an operation. A time limit such as `1s` lets
  batches fill at the cost of that much delay.

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
| `KAFKA_GROUP` | Nexus worker | The only consumer group reads may use; see [The Nexus worker](#the-nexus-worker). Unset, each read names one. |
| `NEXUS_TASK_QUEUE` | Nexus worker | The task queue its Nexus endpoint targets. Reads run on this name with `-reads` added. Default `nexus-connector-kafka`. |
| `KAFKA_DEAD_LETTER_TOPIC` | streamer | Where undeliverable messages go. Must already exist. Required unless `DELETE_DEAD_LETTERS` is `true`. |
| `DELETE_DEAD_LETTERS` | streamer | `true` to delete undeliverable messages instead of writing them to a topic; then leave `KAFKA_DEAD_LETTER_TOPIC` unset. Default `false`. |
| `KAFKA_BATCH_SIZE` | streamer | Most messages per batch. Default 100. Set 1 to rule out [duplicates](#the-streamer). |
| `KAFKA_BATCH_TIME_LIMIT` | streamer | How long a batch waits to fill, such as `500ms`. Default 0. |
| `STREAM_ENDPOINT` | streamer | The Nexus endpoint to deliver to. Required. |
| `STREAM_SERVICE`, `STREAM_OPERATION` | streamer | The service and operation. Default `KafkaMessageReceiver` and `receive`. |
| `STREAM_OPERATION_TIMEOUT` | streamer | How long each operation may take. Default `24h`. |
| `STREAM_MAX_INPUT_BYTES` | streamer | The largest encoded input sent to an operation. Default 2031616 (2 MiB less 64 KiB). |

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
  limits, time limits, where a new group starts, groups in use, a topic created again), reading
  as a group member (commits, releasing a batch, a released first batch surviving a handoff,
  noticing a waiting rebalance, where it starts), fetch errors Kafka's client recovers from, and
  tombstones.
- `streamer/` tests delivery order, splitting batches to fit an input, commits, redelivery with the
  same operation IDs, the duplicate when a redelivered batch ends elsewhere, releasing a stuck
  batch for a rebalance, operation IDs' source key and its check on every batch, splitting a
  refused operation, which refusals count as undeliverable, retries, and the dead-letter topic.
  Its end-to-end tests start a dev server with the `temporal` CLI and deliver to a receiver in
  another namespace. They show that a batch delivered again starts nothing new
  once its operation completed, and runs again if it failed.
- `nexusworker/` tests that a read heartbeats before it reaches Kafka, which consumer groups a
  read may use, and that reads waiting for their batches don't hold up writes. Its end-to-end tests
  write and read across namespaces through the endpoint, refuse reads for the wrong consumer group,
  and run the test caller's `ReaderWorkflow` against two Nexus workers serving two topics through
  two endpoints.
- `test/nexuscaller/` tests that `ReaderWorkflow` acknowledges every batch, reads its sources in
  turn, and carries their positions across continue-as-new.

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
