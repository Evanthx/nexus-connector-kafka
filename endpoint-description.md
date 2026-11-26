# Kafka Connector

Writes to the Kafka topics this Outbound Connector allows. The `KafkaService` service has one
operation:

- `write` writes messages to the topic it names. Each gets a `dedupe-id` header that stays the same
  if the write is retried.

A write that ends with some messages not written fails at once with an `ApplicationError` of
type `Rejected` (Kafka refused a message for good, such as one over the broker's size limit, or
the topic isn't one this Outbound Connector writes to) or
`PartlyWritten` (the rest failed for a reason that may pass). Its details list every message not
written, by index, with Kafka's reason; every message not listed was written, so resend only
those. Other failures are retried until the
caller's Nexus operation times out, so give the call a timeout.
