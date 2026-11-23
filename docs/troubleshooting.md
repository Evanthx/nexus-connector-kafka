# Troubleshooting

## Reading startup logs

Both programs log a configuration report at startup: every setting, defaults marked, the password
shown only as set or not set, and what the program will do. Read it back to confirm the setup.

Then the program waits for Kafka. It logs a warning every 5 seconds until Kafka answers:

```
WARN  Can't reach Kafka or see the topic yet; retrying  reason=...
```

Common reasons:

| Reason | Cause | Fix |
|---|---|---|
| `connection refused` or dial error | Brokers wrong or unreachable. | Check `KAFKA_BROKERS`. |
| `SASL_AUTHENTICATION_FAILED` | Username or password wrong, or the wrong `KAFKA_SASL_MECHANISM`. | Check `KAFKA_USERNAME`, `KAFKA_PASSWORD` and `KAFKA_SASL_MECHANISM`. |
| `UNSUPPORTED_SASL_MECHANISM` | Kafka doesn't accept that login method. | Set `KAFKA_SASL_MECHANISM` to one it accepts. |
| `is TLS missing?`, `plaintext connection speaking to a tls endpoint`, or `EOF` | Kafka needs TLS and the program isn't using it. | Set `KAFKA_TLS=true`. |
| `i/o timeout` with TLS on | The broker's port doesn't use TLS. | Unset `KAFKA_TLS`, or use the broker's TLS port. |
| `certificate is not trusted` or `certificate signed by unknown authority` | The brokers' certificate isn't from a CA the program trusts. | Set `KAFKA_TLS_SERVER_CA_CERT_PATH` to the CA's certificate. |
| `certificate required` or `bad certificate` | Kafka needs a client certificate (mTLS), or refused the one given. | Set or check `KAFKA_TLS_CLIENT_CERT_PATH` and `KAFKA_TLS_CLIENT_KEY_PATH`. |
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic doesn't exist. | Create the topic or check `KAFKA_TOPIC`. |

Once Kafka answers, it logs `Connected to Kafka brokers=... topic=... cluster=...`.

Before waiting for Kafka, both programs verify their configuration and connect to Temporal.
If anything fails, the program reports every problem in one message rather than stopping at the
first:

```
The Outbound Connector can't start:
Invalid Outbound Connector configuration:
  - KAFKA_MAX_DURATION_MINUTES must be a whole number of minutes, at least 1, not "0"
Failed to connect to Temporal:
  - failed reaching server: ... connection refused
```

To find these problems before starting either program, use the
[setup page](setup.md#work-out-your-settings-in-a-browser) (`make configui`): its **Test** makes
the same checks, and connects to Kafka and reads the topic's last message.

## After startup: reading the log

Once running, the log shows what the Kafka client and the consumer group are doing, so a stream
that stops moving says why:

- `Kafka client: ...` lines (`component=kafka-client`) come from the Kafka client library itself,
  at `KAFKA_CLIENT_LOG_LEVEL` (`warn` by default): brokers it can't reach, failed logins, consumer
  group errors. During a Kafka outage these repeat until Kafka is back. Set
  `KAFKA_CLIENT_LOG_LEVEL=debug` to see every request when chasing a problem.
- `The consumer group gave this Inbound Connector partitions` and `... took partitions` (INFO)
  show each rebalance. `This Inbound Connector lost partitions` (WARN) means its group session
  ended, such as when Kafka was unreachable; it rejoins by itself, and the next `gave ...
  partitions` line shows it has.
- `Waiting for messages` (INFO), every `INBOUND_STATUS_INTERVAL` (5 minutes by default) with
  nothing delivered, lists the partitions it has and when it last delivered. Partitions and no
  errors means the topic is quiet. `no partitions` means the group gave it none: other Inbound
  Connectors in the group have them all, or the group hasn't finished joining; look for the group's
  lines above it.

## Errors from `write`

Failures that retrying can't fix are `ApplicationError`s:

| Type | Cause | Fix |
|---|---|---|
| `InvalidRequest` | The caller's Nexus operation timeout is 10 seconds or less, too short to run a write. | Give the call a longer timeout. |
| `Rejected` | Kafka refused a write, such as a message over the broker's size limit. | Fix the message. |

Other failures are retried for 10 minutes, or `KAFKA_MAX_DURATION_MINUTES`. Retries stop 10
seconds before the caller's Nexus operation times out, if that comes first.

## Consumer group rules

**Never share the Inbound Connector's consumer group** with any other live Kafka consumer. Each
consumer group gets every message, so separate groups can read the same topic independently.

**Kafka forgets an idle group's offsets** after `offsets.retention.minutes` (7 days by default)
since the group's last commit. A consumer group whose Inbound Connectors all stop for longer starts
again where `KAFKA_START_FROM` says.

## When an Inbound Connector operation fails

A batch is committed once Temporal accepts its operation. Find operations that ended
unsuccessfully:

```bash
temporal nexus operation list --namespace connectors \
  --query 'ExecutionStatus IN ("Failed", "TimedOut", "Canceled", "Terminated")'
```

Each ID names the partition and offsets, so you know which messages it held.

To replay them:

- **If the receiving service still has the work**, retry it there. For a workflow-backed `receive`,
  the workflow's history holds the messages, so you can reset the workflow.
- **To re-deliver the messages from Kafka**, stop every Inbound Connector in the group, move the
  group's offset for that partition back to the operation's first offset, then restart the Inbound
  Connectors:

  ```bash
  kafka-consumer-groups.sh --bootstrap-server <brokers> --command-config <client config> \
    --group <group> --topic <topic>:<partition> --reset-offsets --to-offset <first offset> --execute
  ```

  With `KAFKA_BATCH_SIZE=1`, each message's ID is the same as before, so messages whose operations
  completed start nothing new, and only the failed ones run again.

## The Inbound Connector won't dead-letter on outages

Only messages too large for an operation's input go to the dead-letter topic. Every other error
(Temporal unreachable, endpoint missing, standalone Nexus operations not enabled) is retried
forever. The stream waits rather than drain working messages into dead letters. Read the
Inbound Connector's log to diagnose a stalled stream.

## The Inbound Connector falls behind the topic

If `INBOUND_MAX_MESSAGES_PER_MINUTE` is set and messages arrive faster than that, the Inbound
Connector sends at the limit and the rest wait in Kafka: the consumer group's lag grows, and the
log shows batches delivered steadily with no errors. That is the limit working. Raise it, or run
more Inbound Connectors in the same group (up to one per partition; the limit is per process). The
startup report's `INBOUND_MAX_MESSAGES_PER_MINUTE` line shows the limit in use.

## A partition stopped moving with `INBOUND_ORDERING=partition`

In ordered mode, a partition waits for its operation to complete. The status line says `Still
delivering a batch` with how long. Then look in the log for `The operation didn't start or didn't
succeed; retrying` warnings: an operation that keeps failing with a retryable error is started
again until it succeeds, and the partition waits behind it. Fix the receiver, or, to skip that batch,
cancel or terminate its operation (its ID is in the warning): its messages then go to the
dead-letter topic and the partition moves on.

```bash
temporal nexus operation terminate --namespace connectors --operation-id <operation ID> --reason "skip batch"
```

## Known limitations

- **One topic** per Outbound Connector or Inbound Connector process, set by `KAFKA_TOPIC`.
- **No AWS IAM, OAuth, or Kerberos login yet.** TLS, a username and password (SCRAM-SHA-256,
  SCRAM-SHA-512, PLAIN), and client certificates (mTLS) are supported. See [Connecting to
  Kafka](setup.md#connecting-to-kafka).
- **The dead-letter topic is only for messages Temporal refuses.** The Inbound Connector doesn't see
  failures inside the operations it starts.
- **`write` can't write a tombstone.** A message's `value` is always bytes, so `write` has no way
  to write a null value.

## Relevant documentation

- [Temporal Nexus](https://docs.temporal.io/nexus)
- [Temporal Cloud actions billing](https://docs.temporal.io/evaluate/cloud/actions)
- [Temporal CLI reference](https://docs.temporal.io/cli)
