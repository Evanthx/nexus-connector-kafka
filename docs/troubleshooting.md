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
| `SASL_AUTHENTICATION_FAILED` | Username or password wrong. | Check `KAFKA_USERNAME` and `KAFKA_PASSWORD`. |
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic doesn't exist. | Create the topic or check `KAFKA_TOPIC`. |

Once Kafka answers, it logs `Connected to Kafka brokers=... topic=... cluster=...`.

Before waiting for Kafka, both programs verify their configuration and connect to Temporal.
If anything fails, the program reports every problem in one message rather than stopping at the
first:

```
The connector can't start:
Invalid connector configuration:
  - KAFKA_MAX_BATCH_BYTES must be a number of bytes greater than 65536, not "100"
  - KAFKA_MAX_DURATION_MINUTES must be a whole number of minutes, at least 1, not "0"
Failed to connect to Temporal:
  - failed reaching server: ... connection refused
```

## Errors from `read` and `write`

Failures that retrying can't fix are `ApplicationError`s:

| Type | Cause | Fix |
|---|---|---|
| `InvalidRequest` | No consumer group named when the connector has none; a different one named when it has `KAFKA_GROUP`; a malformed `ack`; an `ack` from before the topic was deleted and recreated; a caller's Nexus timeout too short to run the operation. | Fix the request. For a recreated topic, read without `ack`. For a timeout, give the call a longer one. |
| `GroupInUse` | The consumer group has live members, such as a relay. | Use another consumer group. |
| `RecordTooLarge` | The next message doesn't fit in a read's result (`KAFKA_MAX_BATCH_BYTES`, 2 MiB). | The message blocks that consumer group. Report to the user. |
| `Rejected` | Kafka refused a write, such as a message over the broker's size limit. | Fix the message. |

Other failures are retried: a read for its time limit plus 5 minutes, a write for 10 minutes, or
`KAFKA_MAX_DURATION_MINUTES` for either. Retries stop 10 seconds before the caller's Nexus
operation times out, if that comes first.

## Consumer group rules

**Never share a consumer group between the relay and a connector reader**, or with any live
Kafka consumer. Reads then fail with `GroupInUse`. Each consumer group gets every message, so
separate groups can read the same topic independently.

**One reader per consumer group.** Two readers of one group read the same batches. Use the
caller's workflow ID to name its endpoint and consumer group, so a second can't start. The test
caller does this.

**Kafka forgets an idle group's offsets** after `offsets.retention.minutes` (7 days by default)
since the group's last commit. A reader that stops for longer starts again where `KAFKA_START_FROM`
says.

**A topic deleted and created again** causes an `ack` returned before then to fail with
`InvalidRequest`. Read without an `ack` to start over.

## When a relay operation fails

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
- **To re-deliver the messages from Kafka**, stop every relay in the group, move the group's
  offset for that partition back to the operation's first offset, then restart the relays:

  ```bash
  kafka-consumer-groups.sh --bootstrap-server <brokers> --command-config <client config> \
    --group <group> --topic <topic>:<partition> --reset-offsets --to-offset <first offset> --execute
  ```

  With `KAFKA_BATCH_SIZE=1`, each message's ID is the same as before, so messages whose operations
  completed start nothing new, and only the failed ones run again.

## The relay won't dead-letter on outages

Only messages too large for an operation's input go to the dead-letter topic. Every other error
(Temporal unreachable, endpoint missing, standalone Nexus operations not enabled) is retried
forever. The stream waits rather than drain working messages into dead letters. Read the
relay's log to diagnose a stalled stream.

## Known limitations

- **One topic** per connector or relay process, set by `KAFKA_TOPIC`.
- **SASL/SCRAM-SHA-256 or no authentication.** TLS and other SASL mechanisms aren't supported yet.
- **A message too large for a read's result blocks its group** in the connector. The message
  stays the group's next message; the group can't read past it.
- **The dead-letter topic is only for messages Temporal refuses.** The relay doesn't see
  failures inside the operations it starts.
- **`write` can't write a tombstone.** A message's `value` is always bytes, so `write` has no way
  to write a null value.

## Relevant documentation

- [Temporal Nexus](https://docs.temporal.io/nexus)
- [Temporal Cloud actions billing](https://docs.temporal.io/evaluate/cloud/actions)
- [Temporal CLI reference](https://docs.temporal.io/cli)
