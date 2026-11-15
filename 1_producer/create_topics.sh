#!/usr/bin/env bash
# Creates the 3 project topics. Run it after Kafka is up.
# Usage:  ./create_topics.sh            (Kafka container is named "kafka")
#         KAFKA_CONTAINER=my_kafka ./create_topics.sh
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-kafka}"
KT="/opt/kafka/bin/kafka-topics.sh"
BOOT="localhost:9092"     # inside the container

create() {  # name partitions
  docker exec "$CONTAINER" "$KT" --bootstrap-server "$BOOT" \
    --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1
}

create lms_raw 3
create lms_clean 3
create lms_dead_letter 1

echo "--- topics now in Kafka ---"
docker exec "$CONTAINER" "$KT" --bootstrap-server "$BOOT" --list
