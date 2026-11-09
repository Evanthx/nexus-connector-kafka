#!/usr/bin/env bash
# Starts the local Kafka, seeds it, writes one more message to be the topic's last, then runs the
# settings page with the local Kafka's settings already filled in. Open http://localhost:8090 and
# press Test: it reads that last message.
#
# Usage: test/kafka/scripts/configui.sh    (from the repository root; Ctrl-C stops the page)
set -euo pipefail

cd "$(dirname "$0")/../../.."
docker compose -f test/kafka/docker-compose.yml up -d --wait
test/kafka/scripts/seed.sh
test/kafka/scripts/produce.sh "Last message, written $(date '+%Y-%m-%d %H:%M:%S')"

set -a; source test/kafka/.env; set +a
exec go run ./configui
