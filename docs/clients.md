# Typed clients

A workflow calls the connector through Nexus by naming a service, an operation, and an input. You
can build that input by hand as JSON, but a typed client is easier: your editor knows the fields,
and a bad request fails in your code, with the field named, before it is sent.

## The contract

The connector's operations are defined in one file, [`kafka.nexusrpc.yaml`](../kafka.nexusrpc.yaml),
its *contract*. It names two services:

- **`KafkaService`**, served by the Nexus worker: `read` and `write`.
- **`KafkaMessageReceiver`**, which the streamer calls: `receive`, one call per partition batch.
  Implement this service to receive the topic's messages.

The contract also defines every input and output type as JSON Schema. It is the source of truth for
what a request may contain; the connector rejects anything it doesn't allow.

## nexgen

[nexgen](https://github.com/temporalio/nexgen) is a code generator from Temporal. Given a contract,
it writes, for one language:

- the input and output types, which check the contract when they are encoded and decoded, and
- a service definition you pass to that language's Nexus client.

It doesn't write the call itself; you still use your Temporal SDK's Nexus client, as in the
[Nexus worker usage examples](../AGENTS.md#run-the-nexus-worker).

## Go

Go types are already generated and committed in this repo, in `generated/kafkaapi`. Import them:

```go
import "github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
```

[`test/nexuscaller/workflows.go`](../test/nexuscaller/workflows.go) shows a complete read and write
loop using these types.

## Other languages

Download nexgen 0.2.9, the version this repo pins (macOS on Apple silicon shown; the release also
has `x86_64-apple-darwin`, `aarch64-unknown-linux-gnu`, and `x86_64-unknown-linux-gnu`):

```bash
curl --silent --show-error --fail --location \
  https://github.com/temporalio/nexgen/releases/download/v0.2.9/nexgen-v0.2.9-aarch64-apple-darwin.tar.gz \
  | tar --extract --gzip nexgen
```

Then, from your project, with a copy of `kafka.nexusrpc.yaml`:

```bash
./nexgen python     kafka.nexusrpc.yaml --output kafkaapi
./nexgen typescript kafka.nexusrpc.yaml --output kafkaapi
./nexgen java       kafka.nexusrpc.yaml --output src/main/java/com/example/kafkaapi --package-name com.example.kafkaapi
```

For Java, the last part of `--package-name` must match the output folder's name.

To see what each language produces without setting up a project, run `make clients` in this repo.
It generates all four into `.build/clients/<language>`, which is not committed.

## Without a generated client

Any Nexus client can send the input as plain JSON. The fields are defined in
[`kafka.nexusrpc.yaml`](../kafka.nexusrpc.yaml) and described in
[AGENTS.md](../AGENTS.md#run-the-nexus-worker). A request that breaks the contract is rejected with
a `BAD_REQUEST` handler error.

## When the contract changes

Regenerate your client from the new `kafka.nexusrpc.yaml`. The service and operation names on the
wire are pinned in the contract, so a regenerated client keeps reaching the same operations.
