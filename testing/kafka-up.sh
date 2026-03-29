#!/bin/bash
set -e

echo "Starting Kafka..."
docker compose up -d

echo "Waiting for Kafka to be ready..."
sleep 5

echo "Kafka is running on localhost:9092"
