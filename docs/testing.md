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

- `kafka/`: reading as a group member (batch size and time limits, commits, releasing a batch, a
  released first batch surviving a handoff, noticing a waiting rebalance, where it starts), fetch
  errors Kafka's client recovers from, and tombstones.
- `relay/`: delivery order, splitting batches to fit an input, commits, redelivery with the
  same operation IDs, the duplicate when a redelivered batch ends elsewhere, releasing a stuck
  batch for a rebalance, operation IDs' source key and its check on every batch, splitting a
  refused operation, which refusals count as undeliverable, retries, and the dead-letter topic.
  Its end-to-end tests start a dev server and deliver to a receiver in another namespace.
- `connector/`: its configuration and startup report, and how long a write may retry. Its
  end-to-end tests write across namespaces through the endpoint, write two topics through two
  connectors, and refuse a caller's Nexus timeout too short to run a write.

## Demo

The demo is one run-through, top to bottom. Steps 1 and 2 start Temporal and Kafka, which both
methods use. Steps 3 to 5 try Method 1 (stream), and steps 6 to 9 try Method 2 (write)
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
  --namespace connectors \
  --namespace kafka-test
```

The `connectors` namespace is where the relay and the connector run, and `kafka-test` is where the
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

Open another terminal; step 9 uses it too.

```bash
test/kafka/scripts/produce.sh "hello from the console producer"
```

Terminal 2 logs it within a second or two. **Leave the relay running** before you go on.

In the Web UI, the `connectors` namespace shows one standalone Nexus operation per batch under
Nexus, and `kafka-test` shows the `ReceiveWorkflow` each one started.

## Method 2: write through the connector

Keep everything from steps 1 to 5 running. Each connector serves one topic behind a Nexus
endpoint. Here two connectors serve the two topics: `kafka-customers-endpoint` routes to
`customers`, and `kafka-orders-endpoint` routes to `orders`. The relay from step 4 still streams
`customers`, so what you write there reaches the receiver.

### 6. Start a connector for the customers topic (terminal 4)

```bash
temporal operator nexus endpoint create \
  --name kafka-customers-endpoint \
  --target-namespace connectors \
  --target-task-queue nexus-connector-kafka \
  --description-file ./endpoint-description.md
set -a; source test/kafka/.env; set +a
go run ./connector
```

### 7. Start a second connector for the orders topic (terminal 5)

```bash
temporal operator nexus endpoint create \
  --name kafka-orders-endpoint \
  --target-namespace connectors \
  --target-task-queue nexus-connector-kafka-orders \
  --description-file ./endpoint-description.md
set -a; source test/kafka/.env; set +a
export KAFKA_TOPIC=orders NEXUS_TASK_QUEUE=nexus-connector-kafka-orders
go run ./connector
```

Each connector needs its own `NEXUS_TASK_QUEUE`, since both run in `connectors`.

### 8. Start the test caller's worker (terminal 6)

```bash
go run ./test/nexuscaller/worker
```

The test caller's worker polls `kafka-nexuscaller` and runs `WriterWorkflow`. Once started, it
logs that it is waiting and then nothing more: this is expected. Step 9 starts a workflow.

### 9. Write to each topic (terminal 7)

```bash
go run ./test/nexuscaller/starter write -endpoint kafka-customers-endpoint "customer Ada" "customer Grace"
go run ./test/nexuscaller/starter write -endpoint kafka-orders-endpoint "first order"
```

Each command starts a `WriterWorkflow` and waits for it. The receiver in terminal 2 logs the two
`customers` messages, delivered by the relay. No relay streams `orders`, so confirm that write
from the command line:

```bash
test/kafka/scripts/consume.sh orders
```

It prints every message in the topic, with its partition, offset and headers, then exits after 5
seconds without new messages. It reads from the beginning under a throwaway consumer group of its
own, so it never moves the relay's offsets; run it as often as you like. Among the 3 seeded orders
is `first order`, with a `dedupe-id` header the connector added. Partitions print interleaved, so
it isn't necessarily the last line. Pass another topic name, such as `customers`, to see that topic.

## Tear down

When you are done, stop the programs with Ctrl-C, then:

```bash
docker compose -f test/kafka/docker-compose.yml down -v
temporal operator nexus endpoint delete --name kafka-receiver-endpoint
temporal operator nexus endpoint delete --name kafka-customers-endpoint
temporal operator nexus endpoint delete --name kafka-orders-endpoint
```

Stopping the dev server discards everything else, including the endpoints.
