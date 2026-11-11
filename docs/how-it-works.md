# How it works

## Folders

| Folder | What is in it |
|---|---|
| `kafka/` | Kafka code both methods share: connecting, reading in batches, writing, dead letters. No Temporal code. |
| `streamer/` | Method 1: the program that streams each message into Temporal. |
| `nexusworker/` | Method 2: the program that serves `read` and `write` as Nexus operations. |
| `configui/` | A page for working out and testing the settings; see [Setup](setup.md#work-out-your-settings-in-a-browser). |
| `generated/kafkaapi/` | Go types for the contract, generated from `kafka.nexusrpc.yaml`. |
| `test/` | Testing only: a receiver for the streamer, a caller for the Nexus worker, shared end-to-end test setup, and a local Kafka. |

At the top level, `kafka.nexusrpc.yaml` is the contract and `endpoint-description.md` is the Nexus
worker endpoint's description.

## The contract

[`kafka.nexusrpc.yaml`](../kafka.nexusrpc.yaml) defines two Nexus services:

- **`KafkaService`**, served by the Nexus worker: `read` and `write`.
- **`KafkaMessageReceiver`**, which the streamer calls: `receive`, one call per batch, with the
  batch's messages from one partition in offset order. You implement it to receive the topic's
  messages.

Messages are delivered as a `Record`: `topic`, `partition`, `offset`, `timestamp`, `key`, `value`,
`tombstone`, and `headers`. Keys, values, and header values are bytes, base64 in JSON. `tombstone`
is true for a message with no value at all (null in Kafka), which on a compacted topic deletes its
key. Topic, partition, and offset together identify a message.

`make generate` turns the contract into `generated/kafkaapi` with
[nexgen](https://github.com/temporalio/nexgen). The generated types check their input when decoded.

### Changing the contract

Edit `kafka.nexusrpc.yaml`, then run `make generate` and commit `generated/kafkaapi`. It
downloads nexgen into `.build/` the first time. CI fails if the generated code is out of date.

## Reading in batches

Both methods read in batches, with the same rule. Given a batch size and a time limit, a read
returns as soon as it has the batch size. Otherwise it returns what it has when the time limit
passes. With no time limit, it returns what Kafka has right away:

- **The Nexus worker** returns the messages already in the topic, up to the batch size, or an
  empty batch if there are none.
- **The streamer** waits until Kafka has messages, then returns the first ones Kafka sends, up to
  the batch size.

Messages from one partition are always in order.

## Where reading starts

A group that has committed offsets continues after them. For a partition the group has no
committed offset for (a new group, or a partition added to the topic), `KAFKA_START_FROM` decides:
`oldest`, the default, starts at the oldest message still in the topic, and `newest` reads only
messages written from then on. Both methods commit a start chosen this way before reading from it,
so a retried read starts at the same place even with `newest`.

## The streamer

The streamer is a live member of its consumer group (`KAFKA_GROUP`). Kafka splits the topic's
partitions among the group's members, so running more streamers spreads the load, up to one per
partition. When one stops, Kafka gives its partitions to the others.

For each batch, it starts one standalone Nexus operation per partition, through `STREAM_ENDPOINT`,
with that partition's messages in offset order. If they are larger together than an operation's
input can be (`STREAM_MAX_INPUT_BYTES`), it splits them over as few operations as fit, in order.
Partitions are delivered in parallel. It doesn't wait for the operations to finish. Once every
message in the batch has an operation, it commits the batch.

**Nothing comes back.** The streamer only sends messages in. It doesn't receive the operations'
results, and it can't send replies to Kafka. A service that needs to send something back to Kafka
uses the Nexus worker's `write`.

**Delivery is at least once.** If the streamer stops after starting a batch's operations but before
committing, the group delivers the batch again. Each operation's ID is built from its messages:
`kafka-<source>-<partition>-<first offset>-<last offset>`. `<source>` is 32 hex characters, a hash
of the Kafka cluster's ID, the topic's name and ID, and the group. The topic's ID changes if the
topic is deleted and created again, so a new topic's offsets don't reuse the old one's IDs.

The start uses two policies: `USE_EXISTING` (if the operation is still running, attach to it) and
`ALLOW_DUPLICATE_FAILED_ONLY` (if it completed, count it as delivered; if it failed, run it again).

**Duplicates are possible when streaming in batches.** A batch delivered again starts at the same
offset but can end at a different one, giving it a different ID and starting a new operation. This
happens only when the streamer stops between starting operations and committing them. Set
`KAFKA_BATCH_SIZE=1` to rule it out: each operation then holds one message, so its ID never
changes.

**The dead-letter topic.** A message goes to `KAFKA_DEAD_LETTER_TOPIC` when no retry could deliver
it: it is larger on its own than an operation's input can be, or the server refuses it alone as too
large. When the server refuses an operation of several messages as too large, the streamer splits it
in two and tries each half. Set `DELETE_DEAD_LETTERS=true` to delete undeliverable messages instead.
Every other error (Temporal unreachable, missing endpoint, etc.) is retried with backoff until it
works. The stream waits rather than drain into dead letters.

**When a streamer operation fails.** Once Temporal accepts an operation, it is durable. An operation
still ends unsuccessfully if the receiving service fails it, it is cancelled or terminated, or it
runs past `STREAM_OPERATION_TIMEOUT`. To find these:

```bash
temporal nexus operation list --namespace kafka-connector \
  --query 'ExecutionStatus IN ("Failed", "TimedOut", "Canceled", "Terminated")'
```

See [Troubleshooting](troubleshooting.md#when-a-streamer-operation-fails) for how to replay them.

## The Nexus worker

**One topic per Nexus worker.** A Nexus worker serves the topic in `KAFKA_TOPIC`; callers can't
choose it. To serve several topics, run a Nexus worker for each, every one with its own
`NEXUS_TASK_QUEUE` and a Nexus endpoint that targets it.

**The consumer group.** A Nexus worker started with `KAFKA_GROUP` serves only that consumer group;
a read that names another fails with `InvalidRequest`. A Nexus worker without `KAFKA_GROUP` serves
any consumer group, and a read must name one. The Nexus worker never joins the group: it uses the
group only to store offsets, so any worker process can serve any read.

**`read(group, batchSize, timeLimitSeconds, ack)`** returns a batch of records and a `position`.
Pass the position back as `ack` on your next read. A read with an `ack` commits it first, then
reads from there. See [Usage](../AGENTS.md#run-the-nexus-worker) for the read loop pattern.

**One reader per group.** Two readers of one group read the same batches. Make the caller's
workflow ID name its endpoint and consumer group to prevent a second from starting.

**Don't use a group that has live members**, such as the streamer's. Kafka won't let a non-member
commit for such a group, so reads fail with `GroupInUse`.

**Reads and writes run on separate task queues.** The endpoint targets `NEXUS_TASK_QUEUE`
(`nexus-connector-kafka` by default), where Nexus operations and writes run. Reads run on the same
name with `-reads` added, capped at 8 concurrent readers so a read never waits for a consumer.

**`write(messages)`** writes messages in order and waits for Kafka to acknowledge them. Each
message gets a `dedupe-id` header, `<activity ID>-<index>`, the same on every retry attempt.

**Each read heartbeats** every 70 seconds until it returns, starting 70 seconds in. A read that
returns within 70 seconds sends no heartbeat (each heartbeat is a billable Action). A worker that
dies during such a read is noticed at its start-to-close timeout instead.

**Batch size in bytes.** A read's result is at most `KAFKA_MAX_BATCH_BYTES` (2 MiB by default).
A message too large to fit on its own fails with `RecordTooLarge` and blocks the group's reads.

## The test caller

[`test/nexuscaller/workflows.go`](../test/nexuscaller/workflows.go) shows how to read in a loop
from one or more Nexus endpoints. `ReaderWorkflow` takes a list of sources, each an endpoint and a
consumer group. Each pass reads a batch from each source in turn, processes its messages in parallel
(one activity each), and acknowledges the batch on its next read. When Temporal suggests it
(because the history is getting long), it continues as new and passes each source's position to the
new run.

One activity per message shows messages processed in parallel, but it is the expensive choice:
see [Cost](#cost).

## Cost

Temporal Cloud bills by [Action](https://docs.temporal.io/evaluate/cloud/actions):

- **The streamer:** one standalone Nexus operation per partition in each batch, plus whatever the
  receiving service starts. It costs nothing while the topic is idle.
- **The Nexus worker:** each `read` or `write` is one Action for the Nexus operation and one for
  the activity behind it, plus one per heartbeat (none for reads that return within 70 seconds).

With full batches, the connector's share is small. What decides the bill:

- **Process a batch, not a message.** One activity or workflow per message costs one Action per
  message. Passing a batch to one activity costs one Action per batch.
- **Long-poll with `read`.** Give reads a time limit (such as the test caller's 60 seconds) rather
  than reading with no time limit and pausing after an empty batch. A 60-second read costs about
  120 Actions per hour while idle; a 5-second pause loop costs about 2,200.
- **Let the streamer's batches fill.** With `KAFKA_BATCH_TIME_LIMIT` 0, a quiet topic's batches
  hold a message or two. A time limit such as `1s` lets batches fill at the cost of that delay.

## Possible future changes

- **Keep a group's consumer between reads.** A worker process could keep the consumer a read used,
  with what it fetched beyond the batch still buffered, and give it to the next read for that group
  if that read's `ack` matches where it stopped. Worth doing if measurements show reads fetching
  much more than they return.
- **Completion callbacks for the streamer.** Once the Go SDK supports them, the streamer could
  write replies to Kafka when an operation completes — but only replies, not messages that originate
  in the caller.
