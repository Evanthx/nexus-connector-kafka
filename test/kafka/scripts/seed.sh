#!/usr/bin/env bash
# Creates the topic (3 partitions), its dead-letter topic, and a second topic, orders, for the
# second Nexus worker in the README walkthrough. Writes count messages to the topic and 3 to orders.
#
# Usage: test/kafka/scripts/seed.sh [count]    (default 10)
set -euo pipefail

cd "$(dirname "$0")/.."
set -a; source .env; set +a
COUNT="${1:-10}"

# `docker compose up --wait` can return before kafka-init has created the SCRAM user.
docker compose wait kafka-init >/dev/null
scripts/client-config.sh

for topic in "$KAFKA_TOPIC" "$KAFKA_TOPIC-dead-letters" orders; do
  docker exec nexus-connector-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --command-config /tmp/client.properties \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done

# produce writes count messages, "<label> 1 of <count>" and so on, to a topic. They are key:value
# lines; the keys spread the messages across the partitions.
produce() {
  local topic="$1" count="$2" label="$3"
  for i in $(seq 1 "$count"); do
    echo "key-$i:$label $i of $count"
  done | docker exec -i nexus-connector-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --command-config /tmp/client.properties --topic "$topic" \
    --reader-property parse.key=true --reader-property key.separator=:
  echo "Wrote $count messages to $topic"
}

produce "$KAFKA_TOPIC" "$COUNT" "seed message"
produce orders 3 "seed order"
