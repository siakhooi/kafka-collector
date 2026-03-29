#!/bin/bash
set -e

kafka_name="kafka"

BOOTSTRAP_SERVER="${1:-localhost:9092}"
TOPICS="${2:-topic1,topic2,topic3}"

IFS=',' read -ra TOPIC_ARRAY <<< "$TOPICS"

echo "Feeding test data to topics: ${TOPICS}"
echo "Bootstrap server: ${BOOTSTRAP_SERVER}"
echo ""

counter=1
while true; do
    for topic in "${TOPIC_ARRAY[@]}"; do
        timestamp=$(date -Iseconds)
        message="{\"id\": $counter, \"message\": \"Test message $counter\", \"timestamp\": \"$timestamp\"}"

        echo "$message" | docker exec -i "$kafka_name" \
            /opt/kafka/bin/kafka-console-producer.sh \
            --bootstrap-server "$BOOTSTRAP_SERVER" \
            --topic "$topic" \
            2>/dev/null

        echo "Sent to $topic: $message"
        ((counter++))
    done

    sleep 2
done
