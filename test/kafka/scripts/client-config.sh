#!/usr/bin/env bash
# Writes the SASL client settings from test/kafka/.env to /tmp/client.properties inside the Kafka
# container, for Kafka's command-line tools. The other scripts call it.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a; source .env; set +a

docker exec -i nexus-connector-kafka sh -c 'cat > /tmp/client.properties' <<PROPS
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_USERNAME}" password="${KAFKA_PASSWORD}";
PROPS
