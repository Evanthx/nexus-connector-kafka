![Nexus Connectors](docs/images/NexusConnectors.webp)

# Kafka Connector

## What are Nexus Connectors?

A Nexus Connector lets a Temporal workflow work with a system outside Temporal, such as a message
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
Temporal. It ships as two programs, the Nexus Connector and the Nexus Connector Relay, called the
connector and the relay from here on. They share their Kafka code and can run side by side on the
same topic.

- **The connector** (`connector/`) serves the topic as `KafkaService`, with a `write` operation.
  Each write runs as a standalone activity.
- **The relay** (`relay/`) joins a consumer group and starts one standalone Nexus operation per
  partition batch, delivering every message to a service you implement
  (`KafkaMessageReceiver.receive`). It scales across partitions, costs nothing while the topic is
  idle, and doesn't wait for results.

```
 relay                                       namespace: kafka-test
┌──────────────────────────────┐  Nexus     ┌─────────────────────────────────┐
│ consumer group member        │──────────▶ │ KafkaMessageReceiver.receive    │
│  one operation per batch     │  endpoint  │  (your service; one call per    │
└──────────────────────────────┘            │   batch)                        │
                                            └─────────────────────────────────┘

 namespace: kafka-test                       namespace: connectors
┌──────────────────────────────┐  Nexus     ┌─────────────────────────────────┐
│ your workflow                │──────────▶ │ connector: KafkaService         │
│  write(messages)             │  endpoint  │  write → standalone activity    │
│                              │ ◀───────── │                                 │
└──────────────────────────────┘            └─────────────────────────────────┘
```

Use the **relay** to move every message from Kafka into Temporal as it arrives; it is the only
way to consume a topic. Use the **connector** to write to Kafka.

## The problem it solves

Every team that connects Kafka to Temporal tends to build the same things:

- **A polling and delivery loop.** Kafka splits a topic's partitions across a consumer group's
  members. The relay handles assignment, rebalancing, commits, and at-least-once delivery into
  Temporal, so each message reaches a Nexus operation without you writing any of that.
- **At-least-once delivery guarantees.** A batch is committed only after Temporal accepts its
  operations. If the relay restarts before committing, a redelivered batch reuses its operation
  ID, so a batch that already completed isn't started again. With batches of more than one
  message, a message can still be delivered twice; `KAFKA_BATCH_SIZE=1` rules that out.
- **Dead-letter handling.** Messages too large to deliver go to a dead-letter topic instead of
  stalling the stream indefinitely.
- **Identifying retried writes.** The connector adds a `dedupe-id` header to each written
  message, the same on every retry, so consumers can drop a message a retried write sent twice.
- **Keeping credentials in one place.** The connector holds the SASL credentials. Callers reach
  the topic through a Nexus endpoint without needing Kafka access themselves.

## When to use it

Use it when:

- Workflows need to receive every message from a Kafka topic without writing a polling loop.
- Workflows write to Kafka and want retries, retry-safe `dedupe-id` headers, and credential isolation handled for
  them.
- Multiple namespaces or languages write to the same topic through one connector that holds the
  SASL credentials.
- You want to control who can write by granting access to a Nexus endpoint rather than
  distributing credentials.

It is not a fit when:

- Your Kafka cluster uses TLS or a SASL mechanism other than SCRAM-SHA-256 (not currently
  supported; set `KAFKA_USERNAME` and `KAFKA_PASSWORD` for SCRAM-SHA-256, or leave both unset for
  no authentication).
- You need exactly-once delivery; the relay delivers at least once.
- You need to serve more than one topic from one process; run a separate connector or relay per
  topic.

## FAQ

**Why do I need this? I can write an Activity that reads from or writes to Kafka myself.**
You can, and for a single write that is often enough. To consume a topic, you also need to manage
consumer group membership and offsets, commit them correctly, and restart cleanly after a failure.
The relay does all of that. Running the connector behind a Nexus endpoint adds what an activity in
your own worker can't: other teams, namespaces, and languages call the same
connector, the SASL credentials live only on the connector, and access is granted per endpoint.
The trade-off is a Nexus call in front of each operation.

**Can I generate strongly typed clients for this connector?**
Yes. The connector's operations are defined in `kafka.nexusrpc.yaml`, and a tool called nexgen
generates typed input and output classes in Go, Python, TypeScript, and Java. See
[Typed clients](docs/clients.md).

**How do I configure SASL credentials?**
Set `KAFKA_USERNAME` and `KAFKA_PASSWORD` on the connector or relay process. Only
SASL/SCRAM-SHA-256 is supported; TLS and other SASL mechanisms are not currently supported. Leave
both unset for no authentication. See [Environment variables](docs/setup.md#environment-variables)
for every variable, and [the setup page](docs/setup.md#work-out-your-settings-in-a-browser) to try
your settings before running either program.

**Is delivery at-least-once or exactly-once?**
At-least-once. The relay commits a batch only after Temporal accepts its operations. If it
restarts before committing, the batch is delivered again. Set `KAFKA_BATCH_SIZE=1` to ensure each
message always has the same operation ID and can't be delivered twice with different offsets. The
connector's `write` adds a `dedupe-id` header that is stable across retries; use it to
deduplicate on the consumer side.

**Where does reading start — from the beginning of the topic or from now?**
From `KAFKA_START_FROM`: `oldest` (the default) starts at the oldest message still in the topic;
`newest` reads only messages written after the relay starts. This applies only when the relay's
consumer group has no committed offsets yet. See
[How it works: where reading starts](docs/how-it-works.md#where-reading-starts).

**What is the difference between the relay and the connector?**
The relay pushes every message into Temporal as it arrives: it is a live member of a consumer
group, delivers continuously, scales across partitions, and costs nothing while the topic is idle.
The connector lets workflows write messages through `write`. Both can run on the same topic at the
same time.
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
- [How it works](docs/how-it-works.md): folders, the contract, reading in batches, the relay
  and the connector in depth, cost, and possible future changes.
- [Troubleshooting](docs/troubleshooting.md): startup diagnostics, error types, consumer group
  rules, and known limitations.

AI agents: see [AGENTS.md](AGENTS.md) for how to set up and run the Kafka connector, and how the code
works.

## License

MIT License, please see [LICENSE](LICENSE) for details.
