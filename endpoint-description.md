# nexus-connector-kafka

Reads from and writes to one Kafka topic. The `KafkaService` service has two operations:

- `read` returns the next batch of messages for a consumer group. Pass the `position` it returns
  as `ack` on your next read, once you have processed the batch; a read without an `ack` returns
  the group's oldest unacknowledged batch again. Use one reader per group. Name the consumer
  group in `group`, unless this endpoint has a consumer group of its own: then leave `group`
  empty, or name that one.
- `write` writes messages. Each gets a `dedupe-id` header that stays the same if the write is
  retried.

Failures retrying can't fix are `ApplicationError`s of type `InvalidRequest`, `GroupInUse`,
`RecordTooLarge`, or `Rejected`.
