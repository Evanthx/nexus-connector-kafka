#!/usr/bin/env bash
# Prints every message in a topic, with its headers, then exits. Use it to see the dead-letter
# topic, or the dedupe-id headers on messages the nexusworker wrote.
#
# Usage: test/kafka/scripts/consume.sh [topic]    (default: the topic in .env)
set -euo pipefail

cd "$(dirname "$0")/.."
set -a; source .env; set +a
TOPIC="${1:-$KAFKA_TOPIC}"

scripts/client-config.sh
docker exec nexus-connector-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --command-config /tmp/client.properties --topic "$TOPIC" \
  --from-beginning --timeout-ms 5000 --property print.headers=true --property print.partition=true \
  --property print.offset=true 2>/dev/null || true
