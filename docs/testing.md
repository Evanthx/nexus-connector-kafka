# Testing

## Unit tests

```bash
make test              # all tests, with the race detector
go test -short ./...   # skip the end-to-end tests
```

The tests need no Docker. They run Kafka in memory with franz-go's `kfake`. The end-to-end tests
are skipped if `temporal` isn't on the `PATH`. `make ci` runs vet, workflowcheck, lint, and the
tests, as CI does.

What the tests cover:

- `setup/`: parsing the Inbound Connector's routes (syntax errors with line and column, unknown
  keys, wrong types), merging defaults, the derived consumer group, every route rule, and
  `KAFKA_TOPICS`.
- `kafka/`: reading as a group member (batch size and time limits, commits, releasing a batch, a
  released first batch surviving a handoff, noticing a waiting rebalance, where it starts), fetch
  errors Kafka's client recovers from, tombstones, waiting for several topics at startup, and
  refusing a topic Kafka marks internal.
- `inbound/`: delivery order, splitting batches to fit an input, commits, redelivery with the
  same operation IDs, the duplicate when a redelivered batch ends elsewhere, releasing a stuck
  batch for a rebalance, operation IDs' source key and its check on every batch, splitting a
  refused operation, which refusals count as undeliverable, retries, the dead-letter topic, the
  per-route rate limit, a route that can't deliver not holding up another, and one failing route
  stopping every route with each member leaving its group. Its end-to-end tests start a dev server
  and deliver to a receiver in another namespace, and two routes to two endpoints.
- `kafka/`: `writer_test.go` has a fake Kafka (`testenv.Refusing`, one partition per broker) refuse
  one partition, and checks the writer lists exactly that partition's records.
- `outbound/write_test.go`: the tries within one attempt against a fake that refuses a partition
  a set number of times: only failed records are sent again, the third try waits until halfway,
  a failure left after three tries is `Rejected`, and the topic holds each message exactly once.
- `outbound/`: its configuration and startup report, and the `dedupe-id` header built from the
  Nexus request ID, `KAFKA_TOPICS`, and refusing a topic not on the list. Its end-to-end tests
  write across namespaces through the endpoint, write two topics through one Outbound Connector,
  refuse a topic not allowed as `Rejected` with nothing written, fail a write Kafka rejects at once as `Rejected`, report each message of a partly rejected
  batch by index (with every other message in the topic), end a partly written batch as
  `PartlyWritten` without writing anything twice, retry a
  failing write until the caller's Nexus operation times out, and write with encryption.

## Demo

The demo is one run-through, top to bottom. Steps 1 and 2 start Temporal and Kafka, which both
methods use. Steps 3 to 5 try Method 1 (stream two topics), and steps 6 to 8 try Method 2 (write
to two topics) alongside it. **Leave every program running until [Tear down](#tear-down)**: by the
end you have six terminals open.

### Prerequisites

- **Go 1.26** or later.
- **Temporal CLI 1.9.1** or later. Check with `temporal --version`.
- **Docker**, for the local Kafka.

Run every command from the repository root.

### 1. Start Temporal (terminal 1)

```bash
temporal server start-dev \
  --dynamic-config-value nexusoperation.enableStandalone=true \
  --namespace connectors \
  --namespace kafka-test
```

The `connectors` namespace is where the Inbound Connector and the Outbound Connector run, and
`kafka-test` is where the test programs run. The Web UI is at <http://localhost:8233>.

### 2. Start Kafka and seed it (terminal 2)

```bash
docker compose -f test/kafka/docker-compose.yml up -d --wait
test/kafka/scripts/seed.sh
```

This starts a single-node Kafka on `localhost:9092` that requires a username and password, and
returns when it is up. It also listens with TLS on `9094` and with mTLS on `9095`; see [Try TLS and
mTLS](#try-tls-and-mtls). The seed script creates two topics with 3 partitions each, `customers` and
`orders`, plus `dead-letters` for the Inbound Connector. It writes 10 messages to `customers` and 3
to `orders`. The settings are in [`test/kafka/.env`](../test/kafka/.env), and the Inbound
Connector's routes in [`test/kafka/routes.json`](../test/kafka/routes.json).

## Method 1: stream messages into Temporal

### 3. Create the endpoints and start the receiver (terminal 2)

The Inbound Connector has two routes: `customers` to `kafka-receiver-endpoint`, and `orders` to
`kafka-orders-receiver-endpoint`. Both endpoints lead to the same example receiver here; each
could have its own.

```bash
temporal operator nexus endpoint create \
  --name kafka-receiver-endpoint \
  --target-namespace kafka-test \
  --target-task-queue kafka-receiver
temporal operator nexus endpoint create \
  --name kafka-orders-receiver-endpoint \
  --target-namespace kafka-test \
  --target-task-queue kafka-receiver
go run ./test/inboundreceiver
```

### 4. Start the Inbound Connector (terminal 3)

```bash
set -a; source test/kafka/.env; set +a
go run ./inbound
```

`.env` sets `INBOUND_CONFIG_FILE=test/kafka/routes.json`. The startup report shows each route with
its settings resolved, then the Inbound Connector delivers the 10 seeded customers and 3 seeded
orders as one batch per partition. Terminal 2 logs each message:

```
INFO  Received message ... WorkflowID kafka-batch-customers-0-0-2 ... topic customers partition 0 offset 0 value seed message 1 of 10
```

### 5. Write a message and watch it arrive (terminal 6)

Open another terminal; step 8 uses it too.

```bash
test/kafka/scripts/produce.sh "hello from the console producer"
test/kafka/scripts/produce.sh "an order from the console producer" orders
```

Terminal 2 logs both within a second or two. **Leave the Inbound Connector running** before you
go on.

In the Web UI, the `connectors` namespace shows one standalone Nexus operation per batch under
Nexus, and `kafka-test` shows the `ReceiveWorkflow` each one started.

## Method 2: write through the Outbound Connector

Keep everything from steps 1 to 5 running. One Outbound Connector allows both topics
(`KAFKA_TOPICS=customers,orders` in `.env`) behind one endpoint, `kafka-outbound-endpoint`; each
write names its topic. The Inbound Connector from step 4 still streams both, so what you write
reaches the receiver.

### 6. Start the Outbound Connector (terminal 4)

```bash
temporal operator nexus endpoint create \
  --name kafka-outbound-endpoint \
  --target-namespace connectors \
  --target-task-queue nexus-connector-kafka \
  --description-file ./endpoint-description.md
set -a; source test/kafka/.env; set +a
go run ./outbound
```

### 7. Start the test caller's worker (terminal 5)

```bash
go run ./test/nexuscaller/worker
```

The test caller's worker polls `kafka-nexuscaller` and runs `WriterWorkflow`. Once started, it
logs that it is waiting and then nothing more: this is expected. Step 8 starts a workflow.

### 8. Write to each topic (terminal 6)

```bash
go run ./test/nexuscaller/starter write "customer Ada" "customer Grace"
go run ./test/nexuscaller/starter write -topic orders "first order"
```

Each command starts a `WriterWorkflow` and waits for it. The receiver in terminal 2 logs all three
messages, delivered by the Inbound Connector. To see a write refused, name a topic the Outbound
Connector doesn't allow:

```bash
go run ./test/nexuscaller/starter write -topic dead-letters "not allowed"
```

It fails at once with a `Rejected` error, `topic "dead-letters" isn't one this Outbound Connector
writes to`, and writes nothing. To see what is in a topic from the command line:

```bash
test/kafka/scripts/consume.sh orders
```

It prints every message in the topic, with its partition, offset and headers, then exits after 5
seconds without new messages. It reads from the beginning under a throwaway consumer group of its
own, so it never moves the Inbound Connector's offsets; run it as often as you like. Among the 3
seeded orders is `first order`, with a `dedupe-id` header the Outbound Connector added. Partitions
print interleaved, so it isn't necessarily the last line. With no topic, it shows `customers`.

## Run it encrypted

To run the same demo with every payload encrypted, see
[Encryption: try it in the demo](encryption.md#try-it-in-the-demo).

## Try TLS and mTLS

The local Kafka also has TLS listeners, with certificates from a test CA that it makes in
`test/kafka/certs/` the first time it starts. To run any step above over TLS, set these in its
terminal after loading `test/kafka/.env`:

```bash
# TLS, with a username and password (SCRAM-SHA-256, SCRAM-SHA-512 or PLAIN)
export KAFKA_BROKERS=localhost:9094
export KAFKA_TLS_SERVER_CA_CERT_PATH=test/kafka/certs/ca.pem
export KAFKA_SASL_MECHANISM=PLAIN

# or mTLS: a client certificate instead of a username and password
export KAFKA_BROKERS=localhost:9095
export KAFKA_TLS_SERVER_CA_CERT_PATH=test/kafka/certs/ca.pem
export KAFKA_TLS_CLIENT_CERT_PATH=test/kafka/certs/client.pem
export KAFKA_TLS_CLIENT_KEY_PATH=test/kafka/certs/client-key.pem
unset KAFKA_USERNAME KAFKA_PASSWORD
```

The startup report's `KAFKA_TLS` line shows what the program uses.

## Tear down

When you are done, stop the programs with Ctrl-C, then:

```bash
docker compose -f test/kafka/docker-compose.yml down -v
temporal operator nexus endpoint delete --name kafka-receiver-endpoint
temporal operator nexus endpoint delete --name kafka-orders-receiver-endpoint
temporal operator nexus endpoint delete --name kafka-outbound-endpoint
```

Stopping the dev server discards everything else, including the endpoints.
