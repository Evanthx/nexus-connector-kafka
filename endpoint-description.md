# Kafka Connector

Writes to one Kafka topic. The `KafkaService` service has one operation:

- `write` writes messages. Each gets a `dedupe-id` header that stays the same if the write is
  retried.

A write Kafka refuses, such as a message over the broker's size limit, fails at once with an
`ApplicationError` of type `Rejected`. Its details list every message not written, by index,
with Kafka's reason; every message not listed was written. Other failures are retried until the
caller's Nexus operation times out, so give the call a timeout.
