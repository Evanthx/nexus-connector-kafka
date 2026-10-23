#!/usr/bin/env bash
# Writes one message to the topic with Kafka's console producer, bypassing the connector.
#
# Usage: test/kafka/scripts/produce.sh <message>
set -euo pipefail

MESSAGE="${1:?usage: produce.sh <message>}"
cd "$(dirname "$0")/.."
set -a; source .env; set +a

scripts/client-config.sh
printf '%s\n' "$MESSAGE" | docker exec -i nexus-connector-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --command-config /tmp/client.properties --topic "$KAFKA_TOPIC"
echo "Wrote 1 message to $KAFKA_TOPIC"
