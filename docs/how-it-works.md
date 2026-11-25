# How it works

The Kafka connector ships as two programs: the Inbound Connector and the Outbound Connector.

## Folders

| Folder | What is in it |
|---|---|
| `kafka/` | Kafka code both programs share: connecting, reading in batches, writing, dead letters. No Temporal code. |
| `inbound/` | Method 1: the Inbound Connector, which streams each message into Temporal. |
| `outbound/` | Method 2: the Outbound Connector, which serves `write` as a Nexus operation. |
| `setup/` | Every environment variable each program reads (`setup.Inbound`, `setup.Outbound`), with its help, default and check; `kafka.go` has the Kafka variables both programs read. The programs load their configuration through it. |
| `configui/` | The setup page (`make configui`): `connector.go` lists the two programs and their Kafka checks; the page itself is in `nexus-connector-shared/configui`. It shows `setup.Inbound` and `setup.Outbound`, tests values without changing any environment, and lists the `export` lines; see [Setup](setup.md#work-out-your-settings-in-a-browser). Not part of either program. `configui/gendocs` writes `docs/setup.md`'s tables (`make docs`). |
| `generated/kafkaapi/` | Go types for the contract, generated from `kafka.nexusrpc.yaml`. |
| `test/` | Testing only: a receiver for the Inbound Connector, a caller for the Outbound Connector, shared end-to-end test setup, and a local Kafka. |
| `nexus-connector-shared/` | Code every Go connector shares, the same in every Go connector repo; see its [README](../nexus-connector-shared/README.md). `settings` (declaring and reading environment variables; the only code that reads the environment), `temporalclient` (the Temporal client options), `codec` ([encryption](encryption.md)), `startup` (how each program starts), `report` (the startup report), `deadline` (a call's time limit, for Outbound Connectors whose calls run as standalone activities; this repo's doesn't use it), `nexussend` (how the Inbound Connector sends each batch: starting the operation, retrying, the rate limit, ordered mode), `configui` (the setup page), and test helpers with the demo codec (`test/encryption`). |

At the top level, `kafka.nexusrpc.yaml` is the contract and `endpoint-description.md` is the
description for the Outbound Connector's Nexus endpoint.

## The contract

[`kafka.nexusrpc.yaml`](../kafka.nexusrpc.yaml) defines two Nexus services:

- **`KafkaService`**, served by the Outbound Connector: `write`.
- **`KafkaMessageReceiver`**, which the Inbound Connector calls: `receive`, one call per batch, with
  the batch's messages from one partition in offset order. You implement it to receive the topic's
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

The Inbound Connector reads in batches. Given a batch size (`KAFKA_BATCH_SIZE`) and a time limit
(`KAFKA_BATCH_TIME_LIMIT`), a batch is complete as soon as it has the batch size. Otherwise it holds
what has arrived when the time limit passes. With no time limit, the Inbound Connector waits until
Kafka has messages, then takes the first ones Kafka sends, up to the batch size.

Messages from one partition are always in order.

## Where reading starts

A group that has committed offsets continues after them. For a partition the group has no committed
offset for (a new group, or a partition added to the topic), `KAFKA_START_FROM` decides: `oldest`,
the default, starts at the oldest message still in the topic, and `newest` reads only messages
written from then on. The Inbound Connector commits a start chosen this way before reading from it,
so an Inbound Connector that stops before its first commit starts at the same place even with
`newest`.

## The Inbound Connector

The Inbound Connector is a live member of its consumer group (`KAFKA_GROUP`). Kafka splits the
topic's partitions among the group's members, so running more Inbound Connectors spreads the load,
up to one per partition. When one stops, Kafka gives its partitions to the others.

For each batch, it starts one standalone Nexus operation per partition, through `INBOUND_ENDPOINT`,
with that partition's messages in offset order. If they are larger together than an operation's
input can be (`INBOUND_MAX_INPUT_BYTES`), it splits them over as few operations as fit, in order.
Partitions are delivered in parallel. It doesn't wait for the operations to finish. Once every
message in the batch has an operation, it commits the batch.

**Nothing comes back.** The Inbound Connector only sends messages in. It doesn't receive the
operations' results, and it can't send replies to Kafka. A service that needs to send something back
to Kafka uses the Outbound Connector's `write`.

**Delivery is at least once.** If the Inbound Connector stops after starting a batch's operations
but before committing, the group delivers the batch again. Each operation's ID is built from its
messages: `kafka-<source>-<partition>-<first offset>-<last offset>`. `<source>` is 32 hex
characters, a hash of the Kafka cluster's ID, the topic's name and ID, and the group. The topic's ID
changes if the topic is deleted and created again, so a new topic's offsets don't reuse the old
one's IDs.

The start uses two policies: `USE_EXISTING` (if the operation is still running, attach to it) and
`REJECT_DUPLICATE` (if it has closed, count it as delivered, whether it succeeded or failed). An
operation is never run twice: a failed one was delivered, and is handled in Temporal.

**Duplicates are possible when streaming in batches.** A batch delivered again starts at the same
offset but can end at a different one, giving it a different ID and starting a new operation. This
happens only when the Inbound Connector stops between starting operations and committing them. Set
`KAFKA_BATCH_SIZE=1` to rule it out: each operation then holds one message, so its ID never
changes.

**The dead-letter topic.** A message goes to `KAFKA_DEAD_LETTER_TOPIC` when no retry could deliver
it: it is larger on its own than an operation's input can be, or the server refuses it alone as too
large. When the server refuses an operation of several messages as too large, the Inbound Connector
splits it in two and tries each half. Set `DELETE_DEAD_LETTERS=true` to delete undeliverable
messages instead. Every other error (Temporal unreachable, missing endpoint, etc.) is retried with
backoff until it works. The stream waits rather than drain into dead letters.

**The rate limit.** With `INBOUND_MAX_MESSAGES_PER_MINUTE` set, the Inbound Connector sends at
most that many messages a minute, spread evenly. Before each operation it waits until the
operation's messages fit in the rate; messages waiting their turn stay in Kafka, not in Temporal,
so a slow receiver or a downstream limit doesn't pile operations up in Temporal. A full batch can go
at once, so one minute can see up to the limit plus one batch. The limit is per process: three
Inbound Connectors in one group, each with `600`, send up to 1,800 a minute between them. A batch is
counted once, however many attempts its start takes; messages sent to the dead-letter topic aren't
counted. `KAFKA_BATCH_SIZE` can't be more than the limit, or a full batch could never be sent, so the
Inbound Connector refuses to start with one. While it waits, Kafka can still rebalance: the wait
ends, the batch is released, and it is delivered later.

**When an Inbound Connector operation fails.** (With `INBOUND_ORDERING=partition`, see
[Ordering](#ordering): the Inbound Connector waits for each operation and handles its failure.) Once Temporal accepts an operation, it is durable. An
operation still ends unsuccessfully if the receiving service fails it, it is cancelled or
terminated, or it runs past `INBOUND_OPERATION_TIMEOUT`. To find these:

```bash
temporal nexus operation list --namespace connectors \
  --query 'ExecutionStatus IN ("Failed", "TimedOut", "Canceled", "Terminated")'
```

See [Troubleshooting](troubleshooting.md#when-an-inbound-connector-operation-fails) for how to replay them.

## Ordering

Kafka orders messages within a partition, never across partitions. `INBOUND_ORDERING` decides
whether the Inbound Connector keeps that order through Temporal.

**`none` (the default): as fast as possible.** For each batch, the Inbound Connector starts one
operation per partition and moves on as soon as Temporal accepts them; it commits then too. It
never waits for an operation to complete, so the next batch's operation for a partition can run
alongside, or before, the previous one. A failed operation doesn't hold anything back: find it
with `temporal nexus operation list` (see below).

**`partition`: in order within each partition.** For each batch, the Inbound Connector starts one
operation per partition and waits for it to complete (the standalone Nexus client's `Get`), then
starts the next operation for that partition if the batch was split, and commits once every
partition's operations have completed. Only then does it read the next batch. So for any one
partition, one operation runs at a time, in offset order. Different partitions still run in
parallel, so parallelism is the number of partitions this Inbound Connector has, and a slow
partition holds back the next batch for the others.

When an operation completes unsuccessfully in `partition` mode, whether it failed (with a
retryable error or not), timed out (`INBOUND_OPERATION_TIMEOUT`), or was cancelled or terminated,
its messages were delivered: the Inbound Connector logs an `ERROR` with the operation ID, doesn't
start it again or send its messages to the dead-letter topic, and the partition moves on. The
failure is handled in Temporal (see [Troubleshooting](troubleshooting.md#when-an-inbound-connector-operation-fails)).
Cancelling or terminating an operation is how to skip a batch that is taking too long.

Only Temporal being unreachable while waiting is retried, with backoff (1 second, doubling to 30);
the retry attaches to the operation, or finds it closed.

The order the receiver sees covers only what its operation waits for. An operation backed by a
workflow completes when the workflow does, so the work is in order. An operation that starts work
elsewhere and returns at once lets the next batch start before that work is done.

**Duplicates, in either mode.** A batch can be delivered again: after a crash before its commit,
or a rebalance while its operation was running. The second delivery reuses the operation ID when
the batch has the same first and last offsets, and then attaches to the running operation (or, if
it has closed, successfully or not, starts nothing). A batch redelivered with different boundaries gets a new ID, so its
messages can be processed twice and, in `partition` mode, can overlap a still-running operation.
Make the receiver idempotent: process each message in a workflow or activity whose ID is derived
from the message, such as `<topic>-<partition>-<offset>`, so a duplicate is rejected rather than
run twice.

## The Outbound Connector

**One topic per Outbound Connector.** An Outbound Connector serves the topic in `KAFKA_TOPIC`;
callers can't choose it. To serve several topics, run an Outbound Connector for each, every one with its own
`NEXUS_TASK_QUEUE` and a Nexus endpoint that targets it.

**One task queue.** The endpoint targets `NEXUS_TASK_QUEUE` (`nexus-connector-kafka` by default).
One worker on it serves the Nexus service. There is no activity or workflow behind it.

**`write(messages)`** writes messages in order and waits for Kafka to acknowledge them. Each
message gets a `dedupe-id` header, `kafka-write-<Nexus request ID>-<index>`. The caller's Temporal
server stores the request ID with the operation, so the header is the same on every attempt, and
consumers can drop the copies a retried write leaves.
[`test/nexuscaller/workflows.go`](../test/nexuscaller/workflows.go) (`WriterWorkflow`) shows a
call.

**Each attempt runs within one Nexus request.** `write` is a synchronous Nexus operation: it writes
and replies within the request. The caller's Temporal server gives the request a deadline (10
seconds by default, dynamic config `component.nexusoperations.request.timeout`). The write stops
waiting for Kafka 1 second before it, so it can reply with the reason.

**Retries are the caller's server's.** A write Kafka refuses for good (`kafka.ErrRejected`, such
as a message too large, or `TOPIC_AUTHORIZATION_FAILED`) fails the operation at once with a
`Rejected` error. A batch's messages go to several partitions, so Kafka can write some and refuse
others: the error's details (`WriteFailures`) list every message not written, by index, with
Kafka's reason, and every message not listed was written. Messages that failed with an error a
moment later can pass (`UNKNOWN_SERVER_ERROR`, the idempotent producer's sequence and epoch
errors) are sent again within the attempt, alone, at once and then halfway to its deadline. A write
that wrote some messages and failed the rest for a reason that may pass ends with `PartlyWritten`
rather than being retried, which would write the others twice; only a write that wrote nothing is
retried by the caller's server. Any other failure is a retryable `UNAVAILABLE` handler error, which the caller's
Temporal server retries with its own backoff (server-wide: 1 second, growing to at most 1 hour
between attempts; `component.nexusoperations.retryPolicy.*`) until the caller's Nexus operation's
`ScheduleToCloseTimeout`. Nothing else bounds the retries, so callers must give the call a timeout.
A caller whose retries never succeed gets a timeout error, not the Kafka error; see
[Troubleshooting](troubleshooting.md#a-write-times-out-with-no-kafka-error).

**The Outbound Connector only writes.** To consume the topic, run the Inbound Connector.

## Cost

Temporal Cloud bills by [Action](https://docs.temporal.io/evaluate/cloud/actions):

- **The Inbound Connector:** one standalone Nexus operation per partition in each batch, plus
  whatever the receiving service starts. It costs nothing while the topic is idle.
- **The Outbound Connector:** each `write` is one Action, for its Nexus operation. There is no activity
  behind it.

With full batches, the Kafka connector's share is small. What decides the bill:

- **Process a batch, not a message.** One activity or workflow per message costs one Action per
  message. Passing a batch to one activity costs one Action per batch.
- **Let the Inbound Connector's batches fill.** With `KAFKA_BATCH_TIME_LIMIT` 0, a quiet topic's
  batches hold a message or two. A time limit such as `1s` lets batches fill at the cost of that
  delay.

## Possible future changes

- **Reading at a workflow's own pace.** The Outbound Connector once had a `read` operation: a
  workflow read a batch, then acknowledged it on its next read. It was removed because it can't
  guarantee a single consumer per consumer group (Kafka gives that only to live members, and the
  Outbound Connector committed from outside the group), or that a batch is ever acknowledged (that
  depends on the caller calling again). It could come back with a single owner per group, such as a
  Temporal workflow that serializes the group's reads, if there is demand. A read with no consumer
  group that commits nothing, like `kafka.ReadLast`, would also be safe, for ad-hoc reads.
- **Completion callbacks for the Inbound Connector.** Once the Go SDK supports them, the Inbound
  Connector could write replies to Kafka when an operation completes — but only replies, not
  messages that originate in the caller.
