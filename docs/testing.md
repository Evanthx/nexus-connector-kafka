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

- `kafka/`: reading for a group (acks, ack-only reads, retried first reads, batch and byte limits,
  time limits, where a new group starts, groups in use, a topic created again), reading as a group
  member (commits, releasing a batch, a released first batch surviving a handoff, noticing a
  waiting rebalance, where it starts), fetch errors Kafka's client recovers from, and tombstones.
- `relay/`: delivery order, splitting batches to fit an input, commits, redelivery with the
  same operation IDs, the duplicate when a redelivered batch ends elsewhere, releasing a stuck
  batch for a rebalance, operation IDs' source key and its check on every batch, splitting a
  refused operation, which refusals count as undeliverable, retries, and the dead-letter topic.
  Its end-to-end tests start a dev server and deliver to a receiver in another namespace.
- `connector/`: that a read heartbeats before it reaches Kafka, which consumer groups a read may
  use, and that reads waiting for their batches don't hold up writes. Its end-to-end tests write
  and read across namespaces through the endpoint, refuse reads for the wrong consumer group, and
  run the test caller's `ReaderWorkflow` against two connectors serving two topics.
- `test/nexuscaller/`: that `ReaderWorkflow` acknowledges every batch, reads its sources in turn,
  and carries their positions across continue-as-new.

## Demo

The demo is one run-through, top to bottom. Steps 1 and 2 start Temporal and Kafka, which both
methods use. Steps 3 to 5 try Method 1 (stream), and steps 6 to 10 try Method 2 (read/write)
alongside it. **Leave every program running until [Tear down](#tear-down)**: by the end you have
seven terminals open.

### Prerequisites

- **Go 1.26** or later.
- **Temporal CLI 1.9.1** or later. Check with `temporal --version`.
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

`kafka-connector` is where the relay and the connector run, and `kafka-test` is where the
test programs run. The Web UI is at <http://localhost:8233>.

### 2. Start Kafka and seed it (terminal 2)

```bash
docker compose -f test/kafka/docker-compose.yml up -d --wait
test/kafka/scripts/seed.sh
```

This starts a single-node Kafka on `localhost:9092` that requires a SASL/SCRAM login, and returns
when it is up. The seed script creates two topics with 3 partitions each, `customers` and `orders`,
plus `customers-dead-letters` for the relay. It writes 10 messages to `customers` and 3 to
`orders`. The settings are in [`test/kafka/.env`](../test/kafka/.env).

## Method 1: stream messages into Temporal

### 3. Create the endpoint and start the receiver (terminal 2)

```bash
temporal operator nexus endpoint create \
  --name kafka-receiver-endpoint \
  --target-namespace kafka-test \
  --target-task-queue kafka-receiver
go run ./test/relayreceiver
```

### 4. Start the relay (terminal 3)

```bash
set -a; source test/kafka/.env; set +a
export KAFKA_GROUP=relay KAFKA_DEAD_LETTER_TOPIC=customers-dead-letters RELAY_ENDPOINT=kafka-receiver-endpoint
go run ./relay
```

The relay delivers the 10 seeded messages as one batch per partition. Terminal 2 logs each one:

```
INFO  Received message ... WorkflowID kafka-batch-customers-0-0-2 ... topic customers partition 0 offset 0 value seed message 1 of 10
```

### 5. Write a message and watch it arrive (terminal 7)

Open another terminal; steps 9 and 10 use it too.

```bash
test/kafka/scripts/produce.sh "hello from the console producer"
```

Terminal 2 logs it within a second or two. **Leave the relay running** before you go on.

In the Web UI, `kafka-connector` shows one standalone Nexus operation per batch under Nexus, and
`kafka-test` shows the `ReceiveWorkflow` each one started.

## Method 2: read and write through the connector

Keep everything from steps 1 to 5 running. Each connector serves one topic behind a Nexus
endpoint. Here two connectors serve the two topics: `kafka-customers-endpoint` routes to
`customers`, and `kafka-orders-endpoint` routes to `orders`.

### 6. Start a connector for the customers topic (terminal 4)

```bash
temporal operator nexus endpoint create \
  --name kafka-customers-endpoint \
  --target-namespace kafka-connector \
  --target-task-queue nexus-connector-kafka \
  --description-file ./endpoint-description.md
set -a; source test/kafka/.env; set +a
go run ./connector
```

`KAFKA_GROUP` isn't set, so each read must name its own consumer group.

### 7. Start a second connector for the orders topic (terminal 5)

```bash
temporal operator nexus endpoint create \
  --name kafka-orders-endpoint \
  --target-namespace kafka-connector \
  --target-task-queue nexus-connector-kafka-orders \
  --description-file ./endpoint-description.md
set -a; source test/kafka/.env; set +a
export KAFKA_TOPIC=orders KAFKA_GROUP=orders-reader NEXUS_TASK_QUEUE=nexus-connector-kafka-orders
go run ./connector
```

`KAFKA_GROUP` is set, so every read through this endpoint uses the consumer group `orders-reader`.

### 8. Start the test caller's worker (terminal 6)

```bash
go run ./test/nexuscaller/worker
```

The test caller's worker polls `kafka-nexuscaller` and runs `ReaderWorkflow`, `WriterWorkflow`,
and the `ProcessRecord` activity. Once started, it logs that it is waiting and then nothing more:
this is expected. Step 9 starts a workflow, and reading begins.

### 9. Read from both topics (terminal 7)

```bash
go run ./test/nexuscaller/starter read
```

This asks Temporal to start a `ReaderWorkflow`, which keeps running on the test caller's worker.
**Watch terminal 6** for what the reader does; the Web UI also shows the workflow in `kafka-test`.

The reader reads from both endpoints: `kafka-customers-endpoint` for consumer group `reader1`, and
`kafka-orders-endpoint` for that endpoint's own group `orders-reader`. It logs each read and each
message processed, with the topic between `***`:

```
time=16:06:07 level=INFO msg="Reading through kafka-customers-endpoint; the read waits up to 60 seconds for its batch to fill"
time=16:06:07 level=INFO msg="Read 10 message(s) from topic *** customers *** through kafka-customers-endpoint"
time=16:06:07 level=INFO msg="Processed message from topic *** customers ***" partition=0 offset=0 value="seed message 1 of 10"
```

Expect pauses: a read with fewer than 10 messages waits up to 60 seconds for more. The reader runs
until you stop it:

```bash
temporal workflow terminate --namespace kafka-test \
  --workflow-id kafka-reader-kafka-customers-endpoint/reader1+kafka-orders-endpoint
```

### 10. Write to each topic (terminal 7)

```bash
go run ./test/nexuscaller/starter write -endpoint kafka-customers-endpoint "customer Ada" "customer Grace"
go run ./test/nexuscaller/starter write -endpoint kafka-orders-endpoint "first order"
```

Each command starts a `WriterWorkflow` and waits for it. Watch the reader in terminal 6 log the
new messages on its next read. The receiver in terminal 2 also logs the two `customers` messages, delivered by the relay.

Each written message has a `dedupe-id` header; see them with `test/kafka/scripts/consume.sh [topic]`.

## Tear down

When you are done, stop the programs with Ctrl-C, then:

```bash
docker compose -f test/kafka/docker-compose.yml down -v
temporal operator nexus endpoint delete --name kafka-receiver-endpoint
temporal operator nexus endpoint delete --name kafka-customers-endpoint
temporal operator nexus endpoint delete --name kafka-orders-endpoint
```

Stopping the dev server discards everything else, including the endpoints.
