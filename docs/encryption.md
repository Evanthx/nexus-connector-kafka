# Encryption

Both programs can encrypt every payload they send to Temporal and decrypt every payload they read,
so your data reaches Temporal, and Temporal Cloud, only encrypted. They use a **payload codec**,
the standard Temporal mechanism: the same codec your own workers use, and the same codec server the
Temporal UI uses to show payloads decrypted.

With no codec set, payloads go to Temporal as they are. The startup report's `payload codec` line
says which.

## How it fits together

```
 Kafka ──▶ Inbound Connector ──encrypt──▶ Temporal ──▶ your receiver ──decrypt──▶ your code
           (codec)                        (ciphertext)  (same codec)

 your workflow ──encrypt──▶ Temporal ──▶ Outbound Connector ──decrypt──▶ Kafka
 (codec)                    (ciphertext)  (same codec)
```

**Everything that reads or writes these payloads must use the same codec and keys**: the Inbound
Connector and your receiver's workers; your callers' workers and the Outbound Connector; and the
codec server behind the Temporal UI, if you want to read payloads there. A worker without the codec
fails to decode what the others encrypted.

The connectors have to see plaintext to do their job: the Inbound Connector reads messages from
Kafka, and the Outbound Connector writes them. Run them where you would run any code that handles
the data.

## Turn it on

### With a codec server (no code)

Point each program at your codec server, the HTTP service that encodes and decodes payloads for
the Temporal UI and CLI:

```bash
export TEMPORAL_CODEC_ENDPOINT=https://codec.example.com
export TEMPORAL_CODEC_AUTH="Bearer <token>"     # if the server wants an Authorization header
```

Or set them in the temporal CLI's profile file, which the programs read like the CLI does:

```toml
[profile.default.codec]
endpoint = "https://codec.example.com"
auth = "Bearer <token>"
```

The program sends each payload to `<endpoint>/encode` and `<endpoint>/decode`, with the
`X-Namespace` header set to its namespace, as the UI and CLI do, so a codec server that picks keys
by namespace works unchanged. The programs hold no keys.

At startup, after the configuration report, each program encrypts a test payload and decrypts it
again:

```
INFO Payload codec works: payloads are encoded with the remote codec at https://codec.example.com (TEMPORAL_CODEC_AUTH set)
```

Until that works it waits, logging why every 5 seconds, as it waits for Kafka:

```
WARN Can't use the payload codec yet; retrying in 5s. Check the codec server is running and TEMPORAL_CODEC_ENDPOINT and TEMPORAL_CODEC_AUTH are right. error="encode a test payload: Unauthorized: ..."
```

An endpoint that isn't an `http` or `https` URL stops the program at once. A codec server that
returns the test payload unchanged is a warning: payloads would reach Temporal unencrypted.

Each call is an HTTP round trip: for the Inbound Connector, one per batch; for the Outbound
Connector, a few per `write`. Run the codec server close to the programs.

### With a codec compiled in

If you would rather not run a codec server, build the programs with your codec. Add a file to the
`codec` package, such as `nexus-connector-shared/codec/mycodec.go`:

```go
package codec

import "example.com/yourcompany/temporalcodec"

func init() { PayloadCodecs = append(PayloadCodecs, temporalcodec.New()) }
```

then build as usual (`go build -o bin/ ./inbound ./outbound`). The startup report says
`encoded with 1 codec(s) compiled in`, and the same startup check runs. You can combine both: the
codec server's encoding runs first, then the compiled-in codecs.

## What is encrypted

| | Encrypted | Not encrypted |
|---|---|---|
| **Inbound Connector** | The batch of messages (the operation's input). The operation's summary (topic, partition, offsets). Errors, as below. | The operation ID (`kafka-<hash>-<partition>-<first>-<last>`: no message content). The endpoint, service and operation names. |
| **Outbound Connector** | Each `write`'s input and result. Errors it returns: their details, messages and stack traces. | Each error's type. |
| **Kafka** | | Messages: the Outbound Connector writes what the caller passed, decrypted, and the Inbound Connector delivers what is in the topic. To keep Kafka encrypted too, encrypt message values yourself before `write`. Dead letters are the original Kafka messages. |

**Errors are encrypted too.** With a codec set, both programs use a failure converter that
encrypts each error's details, message and stack trace (`EncodeCommonAttributes`). Temporal then
stores the message as `Encoded failure`; the error's type (such as `Rejected`) and whether it
is retryable stay readable, so callers can still branch on them. Your workers need the same failure
converter to read the messages: `temporal.NewDefaultFailureConverter` with your data converter and
`EncodeCommonAttributes: true`; the demo's ENCRYPTION blocks set it. Without it, a worker sees `Encoded failure` and no error is
raised. The Temporal UI shows the messages through the codec server; without it, `Encoded failure`.
A worker that encrypts errors reads unencrypted ones as they are, so the two can be mixed.
In ordered mode (a route with `"ordering": "partition"`), your receiver's errors are encrypted only if its own
failure converter encrypts them, as the demo receiver's does.

## Try it in the demo

The demo ([Testing](testing.md#demo)) can run encrypted end to end. It uses a demo codec,
AES-256-GCM with a key derived from a passphrase (`nexus-connector-shared/test/encryption`), and a
demo codec server that serves it (`nexus-connector-shared/test/encryption/codecserver`). They are
for the demo only: a real codec gets its keys from a key management service.

1. Pick a passphrase and use it in every terminal below:

   ```bash
   export DEMO_ENCRYPTION_KEY="correct horse battery staple"
   ```

2. Start the demo codec server (its own terminal):

   ```bash
   go run ./nexus-connector-shared/test/encryption/codecserver     # serves http://localhost:8089
   ```

3. Start Temporal with the UI using it (terminal 1 of the demo, instead of the command there):

   ```bash
   temporal server start-dev \
     --dynamic-config-value nexusoperation.enableStandalone=true \
     --namespace connectors --namespace kafka-test \
     --ui-codec-endpoint http://localhost:8089
   ```

4. Run the demo as written, with `DEMO_ENCRYPTION_KEY` set in the receiver's and the test caller's
   terminals, and with this in the Inbound Connector's and the Outbound Connector's:

   ```bash
   export TEMPORAL_CODEC_ENDPOINT=http://localhost:8089
   ```

   The receiver (`test/inboundreceiver`) and the test caller (`test/nexuscaller`) encrypt with the
   codec directly, in a block of their code marked `BEGIN ENCRYPTION (optional)`; the connectors
   use the codec server. With the same passphrase everywhere, everything works as before.

5. See that Temporal holds only ciphertext. The UI shows payloads decrypted, through the codec
   server; the CLI, without it, shows what Temporal stores:

   ```bash
   temporal workflow show --namespace kafka-test --workflow-id <a ReceiveWorkflow's ID> --output json | grep -m3 encoding
   ```

   Each payload's `encoding` is `binary/encrypted` (base64 in the output).

### Leaving encryption out of your own code

The demo's encryption is self-contained so you can copy the demo without it. In
`test/nexuscaller/client.go` and `test/inboundreceiver/main.go`, delete the block between
`BEGIN ENCRYPTION (optional)` and `END ENCRYPTION (optional)`, and the import marked
`ENCRYPTION (optional)`. Nothing else uses them. To keep encryption, keep both and replace the demo
codec with yours.

## Troubleshooting

- **`Can't use the payload codec yet`** at startup: the codec server isn't reachable, or refused the
  request (`Unauthorized`: check `TEMPORAL_CODEC_AUTH`). The program waits; fix it and it carries on.
- **A worker fails to decode payloads with encoding `binary/encrypted`**: that worker has no codec,
  or a different one. Give every worker the same codec and keys.
- **`payload encrypted with key "...", but this codec has key "..."`** (demo codec): the programs
  have different `DEMO_ENCRYPTION_KEY` values.
- **The UI shows `binary/encrypted` payloads**: start it with the codec server
  (`--ui-codec-endpoint` for the dev server; on Temporal Cloud, set the codec server in the UI's
  settings).
