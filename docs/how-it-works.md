# How it works

The Kafka connector ships as two programs: the Nexus Connector, called the connector below, and the
Nexus Connector Relay, called the relay.

## Folders

| Folder | What is in it |
|---|---|
| `kafka/` | Kafka code both programs share: connecting, reading in batches, writing, dead letters. No Temporal code. `settings.go` declares the Kafka variables both programs read. |
| `relay/` | Method 1: the relay, which streams each message into Temporal. |
| `connector/` | Method 2: the connector, which serves `write` as a Nexus operation. |
| `setup/` | Every environment variable each program reads (`setup.Relay`, `setup.Connector`), with its help, default and check, and the Temporal client options. The programs load their configuration through it. |
| `settings/` | Declaring and reading environment variables: `Program`, `Variable`, `Env` (the process's or a map). The only code that reads the process's environment; a test fails if any other does. Also generates the tables in `docs/setup.md`. The same in every Go connector repo. |
| `configui/` | The setup page (`make configui`): shows `setup.Relay` and `setup.Connector`, tests values without changing any environment, and lists the `export` lines; see [Setup](setup.md#work-out-your-settings-in-a-browser). Not part of either program. `configui/gendocs` writes `docs/setup.md`'s tables (`make docs`). |
| `generated/kafkaapi/` | Go types for the contract, generated from `kafka.nexusrpc.yaml`. |
| `test/` | Testing only: a receiver for the relay, a caller for the connector, shared end-to-end test setup, and a local Kafka. |

At the top level, `kafka.nexusrpc.yaml` is the contract and `endpoint-description.md` is the
description for the connector's Nexus endpoint.

## The contract

[`kafka.nexusrpc.yaml`](../kafka.nexusrpc.yaml) defines two Nexus services:

- **`KafkaService`**, served by the connector: `write`.
- **`KafkaMessageReceiver`**, which the relay calls: `receive`, one call per batch, with the
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

The relay reads in batches. Given a batch size (`KAFKA_BATCH_SIZE`) and a time limit
(`KAFKA_BATCH_TIME_LIMIT`), a batch is complete as soon as it has the batch size. Otherwise it
holds what has arrived when the time limit passes. With no time limit, the relay waits until Kafka
has messages, then takes the first ones Kafka sends, up to the batch size.

Messages from one partition are always in order.

## Where reading starts

A group that has committed offsets continues after them. For a partition the group has no
committed offset for (a new group, or a partition added to the topic), `KAFKA_START_FROM` decides:
`oldest`, the default, starts at the oldest message still in the topic, and `newest` reads only
messages written from then on. The relay commits a start chosen this way before reading from it,
so a relay that stops before its first commit starts at the same place even with `newest`.

## The relay

The relay is a live member of its consumer group (`KAFKA_GROUP`). Kafka splits the topic's
partitions among the group's members, so running more relays spreads the load, up to one per
partition. When one stops, Kafka gives its partitions to the others.

For each batch, it starts one standalone Nexus operation per partition, through `RELAY_ENDPOINT`,
with that partition's messages in offset order. If they are larger together than an operation's
input can be (`RELAY_MAX_INPUT_BYTES`), it splits them over as few operations as fit, in order.
Partitions are delivered in parallel. It doesn't wait for the operations to finish. Once every
message in the batch has an operation, it commits the batch.

**Nothing comes back.** The relay only sends messages in. It doesn't receive the operations'
results, and it can't send replies to Kafka. A service that needs to send something back to Kafka
uses the connector's `write`.

**Delivery is at least once.** If the relay stops after starting a batch's operations but before
committing, the group delivers the batch again. Each operation's ID is built from its messages:
`kafka-<source>-<partition>-<first offset>-<last offset>`. `<source>` is 32 hex characters, a hash
of the Kafka cluster's ID, the topic's name and ID, and the group. The topic's ID changes if the
topic is deleted and created again, so a new topic's offsets don't reuse the old one's IDs.

The start uses two policies: `USE_EXISTING` (if the operation is still running, attach to it) and
`ALLOW_DUPLICATE_FAILED_ONLY` (if it completed, count it as delivered; if it failed, run it again).

**Duplicates are possible when streaming in batches.** A batch delivered again starts at the same
offset but can end at a different one, giving it a different ID and starting a new operation. This
happens only when the relay stops between starting operations and committing them. Set
`KAFKA_BATCH_SIZE=1` to rule it out: each operation then holds one message, so its ID never
changes.

**The dead-letter topic.** A message goes to `KAFKA_DEAD_LETTER_TOPIC` when no retry could deliver
it: it is larger on its own than an operation's input can be, or the server refuses it alone as too
large. When the server refuses an operation of several messages as too large, the relay splits it
in two and tries each half. Set `DELETE_DEAD_LETTERS=true` to delete undeliverable messages instead.
Every other error (Temporal unreachable, missing endpoint, etc.) is retried with backoff until it
works. The stream waits rather than drain into dead letters.

**When a relay operation fails.** Once Temporal accepts an operation, it is durable. An operation
still ends unsuccessfully if the receiving service fails it, it is cancelled or terminated, or it
runs past `RELAY_OPERATION_TIMEOUT`. To find these:

```bash
temporal nexus operation list --namespace connectors \
  --query 'ExecutionStatus IN ("Failed", "TimedOut", "Canceled", "Terminated")'
```

See [Troubleshooting](troubleshooting.md#when-a-relay-operation-fails) for how to replay them.

## The connector

**One topic per connector.** A connector serves the topic in `KAFKA_TOPIC`; callers can't
choose it. To serve several topics, run a connector for each, every one with its own
`NEXUS_TASK_QUEUE` and a Nexus endpoint that targets it.

**One task queue.** The endpoint targets `NEXUS_TASK_QUEUE` (`nexus-connector-kafka` by default).
One worker on it runs the Nexus operations and the writes.

**`write(messages)`** writes messages in order and waits for Kafka to acknowledge them. Each
message gets a `dedupe-id` header, `<activity ID>-<index>`, the same on every retry attempt.
[`test/nexuscaller/workflows.go`](../test/nexuscaller/workflows.go) (`WriterWorkflow`) shows a
call.

**The connector doesn't read.** To consume the topic, run the relay.

## Cost

Temporal Cloud bills by [Action](https://docs.temporal.io/evaluate/cloud/actions):

- **The relay:** one standalone Nexus operation per partition in each batch, plus whatever the
  receiving service starts. It costs nothing while the topic is idle.
- **The connector:** each `write` is one Action for the Nexus operation and one for the activity
  behind it.

With full batches, the Kafka connector's share is small. What decides the bill:

- **Process a batch, not a message.** One activity or workflow per message costs one Action per
  message. Passing a batch to one activity costs one Action per batch.
- **Let the relay's batches fill.** With `KAFKA_BATCH_TIME_LIMIT` 0, a quiet topic's batches
  hold a message or two. A time limit such as `1s` lets batches fill at the cost of that delay.

## Possible future changes

- **Reading at a workflow's own pace.** The connector once had a `read` operation: a workflow read
  a batch, then acknowledged it on its next read. It was removed because it can't guarantee one
  reader per consumer group (Kafka gives that only to live members, and the connector committed
  from outside the group), or that a batch is ever acknowledged (that depends on the caller calling
  again). It could come back with a single owner per group, such as a Temporal workflow that
  serializes the group's reads, if there is demand. A read with no consumer group that commits
  nothing, like `kafka.ReadLast`, would also be safe, for ad-hoc reads.
- **Completion callbacks for the relay.** Once the Go SDK supports them, the relay could
  write replies to Kafka when an operation completes — but only replies, not messages that originate
  in the caller.
