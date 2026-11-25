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
  - set KAFKA_TOPIC: the topic to serve
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

A failure that retrying can't fix ends the operation at once, with Kafka's message and an
`ApplicationError` cause the caller finds with `errors.As`:

| Type | Cause | Fix |
|---|---|---|
| `Rejected` | Kafka refused a write, such as a message over the broker's size limit, or `TOPIC_AUTHORIZATION_FAILED`. | Fix the message, or the Kafka login's permissions on the topic. |

Every other failure (Kafka unreachable, slow, or not acknowledging) is retried by the caller's
Temporal server, with its own backoff, until the caller's Nexus operation times out.

### A write times out with no Kafka error

A caller whose write never succeeds gets a timeout error (`operation timed out (type:
ScheduleToClose)`), not the Kafka error. To find the reason:

- While the caller's server is still retrying, the reason is the last attempt's failure on the
  caller workflow's pending Nexus operation. `temporal workflow describe --workflow-id <id>`
  (in the caller's namespace) shows pending Nexus operations, such as `Kafka didn't acknowledge the
  write in time; the Outbound Connector's log has the Kafka client's errors`.
- The Outbound Connector's log has the Kafka client's own errors (`Kafka client: ...` lines), such
  as brokers it can't reach.

Give every call to `write` a Nexus operation timeout (`ScheduleToCloseTimeout`). Nothing else stops
the caller's server retrying a failing write.

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

The Inbound Connector never starts a failed operation again: its messages were delivered, and the
failure is handled in Temporal, in the receiver's namespace. Moving the consumer group's offset back
doesn't run them again either: a redelivered batch with the same ID counts as delivered.

- **An activity in the receiver's workflow keeps failing** (the workflow is still running): fix and
  deploy the receiver's worker. The activity's next retry runs the new code. Nothing else to do.
- **The receiver's workflow failed**: fix and deploy the worker, then reset the workflow. In the
  Temporal UI, open the receiver's namespace, filter the workflows by status Failed, open one, and
  choose **Reset** at the first workflow task; it runs again from its input, the messages. For many
  at once:

  ```bash
  temporal workflow reset --namespace <receiver namespace> \
    --query 'ExecutionStatus = "Failed" AND WorkflowType = "<type>"' \
    --type FirstWorkflowTask --reason "fixed the receiver"
  ```

  The reset runs the work again, so the receiver must be idempotent.
- **The receiver has no workflow** (a synchronous handler): nothing in Temporal holds the work to
  run again, which is why the receiver should be backed by a workflow.

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
delivering a batch` with how long: the receiver's operation is still running, such as a workflow
whose activity keeps retrying. A failed operation doesn't stop the partition: it is logged as an
`ERROR` and the partition moves on. Fix the receiver, or, to skip that batch, cancel or terminate
its operation (its ID is in the status line): the partition then moves on, and its messages are
handled in Temporal like any failed operation. `The operation didn't start, or Temporal couldn't be
reached while waiting for it; retrying` warnings mean the Inbound Connector can't reach Temporal.

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
