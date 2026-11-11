![Nexus Connectors](docs/images/NexusConnectors.webp)

# nexus-connector-kafka

## What are Nexus Connectors?

A Nexus Connector lets a Temporal workflow work with a system outside Temporal, such as a message
broker, a payment provider, a chat platform, or an HTTP API, without writing that integration
itself. Each connector runs as a Temporal worker and offers its operations through a
[Nexus](https://docs.temporal.io/nexus) endpoint. Some operations map directly to one call on the
outside system; others wrap several calls into one durable step, such as creating, finalizing, and
sending an invoice. Because callers reach the connector through Nexus, it is written once and
called from any language Temporal supports, with [typed clients](docs/clients.md) generated from
the connector's contract.

Connectors lower the cost of adopting Temporal. Teams arriving from low-code platforms that ship
hundreds of prebuilt connectors can reach the systems they already depend on without building
each integration by hand, and a service moving into Temporal can keep talking to the services
around it over the Kafka topics or HTTP calls they already use. Running a connector behind a Nexus
endpoint also keeps credentials in one place: the connector holds the keys, and you decide which
namespaces may call which endpoint, such as a read-only endpoint for one team and a read-write one
for another, following the principle of least privilege.

## What it is

**Durable Kafka integration for Temporal workflows.** The Kafka connector bridges a Kafka topic and
Temporal. It ships as two programs that share their Kafka code and can run side by side on the same
topic with separate consumer groups.

- **The Nexus worker** serves the topic as `KafkaService`, with `read` and `write` operations. A
  workflow calls `read` in a loop, acknowledging each batch before the next. Each operation runs as
  a standalone activity.
- **The streamer** joins a consumer group and starts one standalone Nexus operation per partition
  batch, delivering every message to a service you implement (`KafkaMessageReceiver.receive`). It
  scales across partitions, costs nothing while the topic is idle, and doesn't wait for results.

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

Use the **streamer** to move every message from Kafka into Temporal as it arrives. Use the **Nexus
worker** to write to Kafka, or to read when a workflow should pull messages at its own pace.

## The problem it solves

Every team that connects Kafka to Temporal tends to build the same things:

- **A polling and delivery loop.** Kafka splits a topic's partitions across a consumer group's
  members. The streamer handles assignment, rebalancing, commits, and at-least-once delivery into
  Temporal, so each message reaches a Nexus operation without you writing any of that.
- **At-least-once delivery guarantees.** A batch is committed only after Temporal accepts its
  operations. If the streamer restarts before committing, a redelivered batch reuses its operation
  ID, so a batch that already completed isn't started again. With batches of more than one
  message, a message can still be delivered twice; `KAFKA_BATCH_SIZE=1` rules that out.
- **Dead-letter handling.** Messages too large to deliver go to a dead-letter topic instead of
  stalling the stream indefinitely.
- **A read loop with ack semantics.** The Nexus worker's `read` takes the previous batch's
  `position` as an `ack`, commits it, and returns the next batch. Offset management is built in.
- **Identifying retried writes.** The Nexus worker adds a `dedupe-id` header to each written
  message, the same on every retry, so consumers can drop a message a retried write sent twice.
- **Keeping credentials in one place.** The connector holds the SASL credentials. Callers reach
  the topic through a Nexus endpoint without needing Kafka access themselves.

## When to use it

Use it when:

- Workflows need to receive every message from a Kafka topic without writing a polling loop.
- Workflows write to Kafka and want retries, retry-safe `dedupe-id` headers, and credential isolation handled for
  them.
- Multiple namespaces or languages read the same topic through one connector that holds the SASL
  credentials.
- You want to control who can read or write by granting access to a Nexus endpoint rather than
  distributing credentials.

It is not a fit when:

- Your Kafka cluster uses TLS or a SASL mechanism other than SCRAM-SHA-256 (not currently
  supported; set `KAFKA_USERNAME` and `KAFKA_PASSWORD` for SCRAM-SHA-256, or leave both unset for
  no authentication).
- You need exactly-once delivery; the streamer delivers at least once.
- A single read result exceeds 2 MiB; the Nexus worker returns at most `KAFKA_MAX_BATCH_BYTES`
  (2 MiB by default) per read.
- You need to serve more than one topic from one process; run a separate connector per topic.

## FAQ

**Why do I need this? I can write an Activity that reads from or writes to Kafka myself.**
You can, and for a single write that is often enough. For reading in a loop, you also need to
manage consumer group offsets, commit them correctly, heartbeat during long polls, and restart
cleanly after a failure. The connector does all of that. Running it behind a Nexus endpoint adds
what an activity in your own worker can't: other teams, namespaces, and languages call the same
connector, the SASL credentials live only on the connector, and access is granted per endpoint.
The trade-off is a Nexus call in front of each operation.

**Can I generate strongly typed clients for this connector?**
Yes. The connector's operations are defined in `kafka.nexusrpc.yaml`, and a tool called nexgen
generates typed input and output classes in Go, Python, TypeScript, and Java. See
[Typed clients](docs/clients.md).

**How do I configure SASL credentials?**
Set `KAFKA_USERNAME` and `KAFKA_PASSWORD` on the connector process. Only SASL/SCRAM-SHA-256 is
supported; TLS and other SASL mechanisms are not currently supported. Leave both unset for no
authentication. See [Setup](docs/setup.md) for the full environment variable reference.

**Is delivery at-least-once or exactly-once?**
At-least-once. The streamer commits a batch only after Temporal accepts its operations. If it
restarts before committing, the batch is delivered again. Set `KAFKA_BATCH_SIZE=1` to ensure each
message always has the same operation ID and can't be delivered twice with different offsets. The
Nexus worker's `write` adds a `dedupe-id` header that is stable across retries; use it to
deduplicate on the consumer side.

**Where does reading start — from the beginning of the topic or from now?**
From `KAFKA_START_FROM`: `oldest` (the default) starts at the oldest message still in the topic;
`newest` reads only messages written after the connector starts. This applies only when a consumer
group has no committed offsets yet. See
[How it works: where reading starts](docs/how-it-works.md#where-reading-starts).

**What is the difference between the streamer and the Nexus worker?**
The streamer pushes every message into Temporal as it arrives: it is a live member of a consumer
group, delivers continuously, scales across partitions, and costs nothing while the topic is idle.
The Nexus worker lets workflows pull messages at their own pace through `read`, or write messages
through `write`. Both can run on the same topic at the same time with separate consumer groups.
See [How it works](docs/how-it-works.md) for more detail.

## Documentation

- [Setup](docs/setup.md): prerequisites, environment variables, creating endpoints, running the
  programs, the configui settings browser, and the full settings reference.
- [Testing](docs/testing.md): demo walkthrough with a local dev server and Docker Kafka, and unit
  and end-to-end tests.
- [Typed clients](docs/clients.md): the contract, and generating clients for Go, Python,
  TypeScript, and Java.
- [How it works](docs/how-it-works.md): folders, the contract, reading in batches, the streamer
  and Nexus worker in depth, the test caller, cost, and possible future changes.
- [Troubleshooting](docs/troubleshooting.md): startup diagnostics, error types, consumer group
  rules, and known limitations.

AI agents: see [AGENTS.md](AGENTS.md) for how to set up and run this connector, and how the code
works.

## License

MIT License, please see [LICENSE](LICENSE) for details.
