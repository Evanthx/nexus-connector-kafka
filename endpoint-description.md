# Kafka Connector

Writes to one Kafka topic. The `KafkaService` service has one operation:

- `write` writes messages. Each gets a `dedupe-id` header that stays the same if the write is
  retried.

Failures retrying can't fix are `ApplicationError`s of type `InvalidRequest` or `Rejected`.
