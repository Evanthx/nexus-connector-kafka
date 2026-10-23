#!/usr/bin/env bash
# Creates the topic (3 partitions) and its dead-letter topic, and writes messages to the topic.
#
# Usage: test/kafka/scripts/seed.sh [count]    (default 10)
set -euo pipefail

cd "$(dirname "$0")/.."
set -a; source .env; set +a
COUNT="${1:-10}"

# `docker compose up --wait` can return before kafka-init has created the SCRAM user.
docker compose wait kafka-init >/dev/null
scripts/client-config.sh

for topic in "$KAFKA_TOPIC" "$KAFKA_TOPIC-dead-letters"; do
  docker exec nexus-connector-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --command-config /tmp/client.properties \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done

# key:value lines; the keys spread the messages across the partitions.
for i in $(seq 1 "$COUNT"); do
  echo "key-$i:seed message $i of $COUNT"
done | docker exec -i nexus-connector-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --command-config /tmp/client.properties --topic "$KAFKA_TOPIC" \
  --reader-property parse.key=true --reader-property key.separator=:

echo "Wrote $COUNT messages to $KAFKA_TOPIC"
