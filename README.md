![Nexus Connectors](docs/images/NexusConnectors.webp)

# Kafka Connector

## What are Nexus Connectors?

A Nexus connector lets a Temporal workflow work with a system outside Temporal, such as a message
broker, a payment provider, a chat platform, or an HTTP API, without writing that integration
itself. Each connector runs as a Temporal worker and offers its operations through a
[Nexus](https://docs.temporal.io/nexus) endpoint. Some operations map directly to one call on the
outside system; others wrap several calls into one durable step, such as creating, finalizing, and
sending an invoice. Because callers reach the connector through Nexus, it is written once and
called from any language Temporal supports, with [typed clients](docs/clients.md) generated from
the connector's contract.

Connectors lower the cost of adopting Temporal. Teams arriving from platforms that ship
hundreds of prebuilt connectors can reach the systems they already depend on without building
each integration by hand, and a service moving into Temporal can keep talking to the services
around it over the Kafka topics or HTTP calls they already use. Running a connector behind a Nexus
endpoint also keeps credentials in one place: the connector holds the keys, and you decide which
namespaces may call which endpoint, such as a write endpoint for one team and not another,
following the principle of least privilege.

## What is the Kafka Connector?

**Durable Kafka integration for Temporal workflows.** The Kafka connector bridges a Kafka topic and
Temporal. It ships as two programs, which share their Kafka code and can run side by side on the
same topic:

- **The Inbound Connector** reads messages from a Kafka topic and sends them to a Nexus endpoint
  you implement: it brings messages **in** to Temporal from Kafka.
- **The Outbound Connector** runs as a Nexus operation that writes messages to a Kafka topic: it
  sends messages **out** of Temporal to Kafka.

```
 Inbound Connector                           namespace: kafka-test
┌──────────────────────────────┐  Nexus     ┌─────────────────────────────────┐
│ consumer group member        │──────────▶ │ KafkaMessageReceiver.receive    │
│  one operation per batch     │  endpoint  │  (your service; one call per    │
└──────────────────────────────┘            │   batch)                        │
                                            └─────────────────────────────────┘

 namespace: kafka-test                       namespace: connectors
┌──────────────────────────────┐  Nexus     ┌─────────────────────────────────┐
│ your workflow                │──────────▶ │ Outbound Connector:             │
│  write(messages)             │  endpoint  │  KafkaService.write →           │
│                              │ ◀───────── │  standalone activity            │
└──────────────────────────────┘            └─────────────────────────────────┘
```

## The problem it solves

Every team that connects Kafka to Temporal tends to build the same things:

- **A polling and delivery loop.** Kafka splits a topic's partitions across a consumer group's
  members. The Inbound Connector handles assignment, rebalancing, commits, and at-least-once
  delivery into Temporal, so each message reaches a Nexus operation without you writing any of
  that.
- **At-least-once delivery guarantees.** A batch is committed only after Temporal accepts its
  operations. If the Inbound Connector restarts before committing, a redelivered batch reuses its
  operation ID, so a batch that already completed isn't started again. With batches of more than
  one message, a message can still be delivered twice; `KAFKA_BATCH_SIZE=1` rules that out.
- **Dead-letter handling.** Messages too large to deliver go to a dead-letter topic instead of
  stalling the stream indefinitely.
- **Rate limiting.** `INBOUND_MAX_MESSAGES_PER_MINUTE` caps how many messages a minute go into
  Temporal, so a burst on the topic can't overwhelm your service; the rest wait in Kafka.
- **Identifying retried writes.** The Outbound Connector adds a `dedupe-id` header to each written
  message, the same on every retry, so consumers can drop a message a retried write sent twice.
- **Keeping credentials in one place.** The Outbound Connector holds the Kafka login. Callers
  reach the topic through a Nexus endpoint without needing Kafka access themselves.

## When to use it

Use it when:

- Workflows need to receive every message from a Kafka topic without writing a polling loop.
- Workflows write to Kafka and want retries, retry-safe `dedupe-id` headers, and credential isolation handled for
  them.
- Multiple namespaces or languages write to the same topic through one Outbound Connector that
  holds the Kafka login.
- You want to control who can write by granting access to a Nexus endpoint rather than
  distributing credentials.

It is not a fit when:

- Your Kafka logs in with AWS IAM, OAuth, or Kerberos (not currently supported). TLS, a username
  and password, and client certificates (mTLS) are supported; see [Connecting to
  Kafka](docs/setup.md#connecting-to-kafka).
- You need exactly-once delivery; the Inbound Connector delivers at least once.
- You need to serve more than one topic from one process; run a separate Outbound Connector or
  Inbound Connector per topic.

## Message order

**By default, batches can be processed out of order.** The Inbound Connector goes as fast as it
can (`INBOUND_ORDERING=none`): it starts each batch's operation without waiting for the one before,
so your receiver can use every worker it has. Messages within one operation are in offset order,
but two batches from the same partition can run at the same time and finish in either order, and a
failed operation doesn't hold back the ones after it.

**To process each partition in order, set `INBOUND_ORDERING=partition`.** The Inbound Connector
then waits for each operation to complete before starting the next one for that partition, and
commits only then. Partitions still run in parallel; Kafka never orders messages across partitions.

- **It is slower.** At most one operation per partition runs at a time, so parallelism is the number
  of partitions: with 6 partitions, 6 operations at once, however many workers you have. Add
  partitions to go faster. A batch's partitions must all finish before the next batch is read.
- **A failed operation is retried if its error is retryable.** A retryable application error, a
  timeout, or Temporal being unreachable starts the operation again, with backoff, until it
  succeeds; its partition waits, and each failure is logged as a warning.
- **Otherwise its messages go to the dead-letter topic.** An operation that failed with a
  non-retryable error (including a Nexus "operation failed"), or was cancelled or terminated,
  can't succeed by running again. Its messages go to `KAFKA_DEAD_LETTER_TOPIC` (or are deleted with
  `DELETE_DEAD_LETTERS=true`), logged as an error, and the partition moves on. This is the one place
  ordered mode lets a later message go before an earlier one.
- **Order covers only what the operation waits for.** Back the receiver's operation with a workflow
  that does the work before it completes. An operation that only starts the work and returns lets
  the next batch start while that work still runs.

**Either way, a message can be delivered more than once.** If the Inbound Connector stops after
starting a batch's operation but before committing, the batch is delivered again. Make the receiver
idempotent: process each message in a workflow or activity whose ID is derived from the message,
such as `<topic>-<partition>-<offset>`, so a second start for a message already handled is
rejected as a duplicate.

See [How it works: ordering](docs/how-it-works.md#ordering).

## FAQ

**Why do I need this? I can write an Activity that reads from or writes to Kafka myself.**
You can, and for a single write that is often enough. To consume a topic, you also need to manage
consumer group membership and offsets, commit them correctly, and restart cleanly after a failure.
The Inbound Connector does all of that. Running the Outbound Connector behind a Nexus endpoint adds
what an activity in your own worker can't: other teams, namespaces, and languages call the same
Outbound Connector, the Kafka login lives only on it, and access is granted per endpoint.
The trade-off is a Nexus call in front of each operation.

**Can I generate strongly typed clients for this Outbound Connector?**
Yes. The Outbound Connector's operations are defined in `kafka.nexusrpc.yaml`, and a tool called
nexgen generates typed input and output classes in Go, Python, TypeScript, and Java. See
[Typed clients](docs/clients.md).

**How do I connect it to my Kafka's login and TLS?**
Set `KAFKA_TLS=true` if your Kafka needs encrypted connections, and either `KAFKA_USERNAME` and
`KAFKA_PASSWORD` (with `KAFKA_SASL_MECHANISM` if it isn't SCRAM-SHA-256) or a client certificate,
on the Outbound Connector or Inbound Connector process. [Connecting to
Kafka](docs/setup.md#connecting-to-kafka) explains which to use, with a Confluent Cloud example. See [Environment variables](docs/setup.md#environment-variables)
for every variable, and [the setup page](docs/setup.md#work-out-your-settings-in-a-browser) to try
your settings before running either program.

**Is delivery at-least-once or exactly-once?**
At-least-once. The Inbound Connector commits a batch only after Temporal accepts its operations. If
it restarts before committing, the batch is delivered again. Set `KAFKA_BATCH_SIZE=1` to ensure each
message always has the same operation ID and can't be delivered twice with different offsets. The
Outbound Connector's `write` adds a `dedupe-id` header that is stable across retries; use it to
deduplicate on the consumer side.

**Can my data reach Temporal only encrypted?**
Yes. Set `TEMPORAL_CODEC_ENDPOINT` to your payload codec server on both programs, or build them
with your codec, and every payload they send is encrypted before it leaves the process. Your own
workers use the same codec. Messages in Kafka are not changed. See [Encryption](docs/encryption.md).

**Where does reading start — from the beginning of the topic or from now?**
From `KAFKA_START_FROM`: `oldest` (the default) starts at the oldest message still in the topic;
`newest` reads only messages written after the Inbound Connector starts. This applies only when the
Inbound Connector's consumer group has no committed offsets yet. See
[How it works: where reading starts](docs/how-it-works.md#where-reading-starts).

**What is the difference between the Inbound Connector and the Outbound Connector?**
The Inbound Connector pushes every message into Temporal as it arrives: it is a live member of a
consumer group, delivers continuously, scales across partitions, and costs nothing while the topic is idle.
The Outbound Connector lets workflows write messages through `write`. Both can run on the same
topic at the same time.
See [How it works](docs/how-it-works.md) for more detail.

## Documentation

- [Setup](docs/setup.md): prerequisites, creating endpoints, running the programs, and every
  environment variable. To work out and test the environment variables in a form, run
  `make configui` and open <http://localhost:8090>: its **Test** checks every variable as the
  program does at startup, connects to Temporal and checks the namespace and the Nexus endpoint,
  and reads the topic's last message through the programs' own Kafka code (see
  [Work out your settings in a browser](docs/setup.md#work-out-your-settings-in-a-browser)).
- [Testing](docs/testing.md): demo walkthrough with a local dev server and Docker Kafka, and unit
  and end-to-end tests.
- [Typed clients](docs/clients.md): the contract, and generating clients for Go, Python,
  TypeScript, and Java.
- [How it works](docs/how-it-works.md): folders, the contract, reading in batches, the Inbound
  Connector and the Outbound Connector in depth, cost, and possible future changes.
- [Encryption](docs/encryption.md): encrypting every payload so data reaches Temporal only
  encrypted, with your codec server or a codec compiled in, and trying it in the demo.
- [Troubleshooting](docs/troubleshooting.md): startup diagnostics, error types, consumer group
  rules, and known limitations.

AI agents: see [AGENTS.md](AGENTS.md) for how to set up and run the Kafka connector, and how the code
works.

## License

MIT License, please see [LICENSE](LICENSE) for details.
