#!/usr/bin/env bash
# Writes one message to a topic with Kafka's console producer, bypassing the connector.
#
# Usage: test/kafka/scripts/produce.sh <message> [topic]    (default: the topic in .env)
set -euo pipefail

MESSAGE="${1:?usage: produce.sh <message> [topic]}"
cd "$(dirname "$0")/.."
set -a; source .env; set +a
TOPIC="${2:-$KAFKA_TOPIC}"

scripts/client-config.sh
printf '%s\n' "$MESSAGE" | docker exec -i nexus-connector-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --command-config /tmp/client.properties --topic "$TOPIC"
echo "Wrote 1 message to $TOPIC"
