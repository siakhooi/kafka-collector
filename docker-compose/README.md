# kafka-collector Docker Compose Example

This directory contains an example Docker Compose configuration for running `kafka-collector` with Apache Kafka.

## Quick Start

```bash
docker compose up
```

This starts:
- **Kafka** - Apache Kafka broker (KRaft mode, no Zookeeper required)
- **kafka-collector** - Collects messages from configured topics

## Configuration

The `kafka-collector` service is configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_TOPICS` | Comma-separated list of topics to consume | (required) |
| `KAFKA_BOOTSTRAP_SERVER` | Kafka bootstrap server address | `localhost:9092` |
| `KAFKA_GROUP` | Consumer group ID | random UUID |
| `COLLECTOR_MODE` | Run mode: `cli` or `service` | `cli` |
| `COLLECTOR_SERVICE_PORT` | HTTP port for service mode | `8080` |
| `COLLECTOR_CAPTURE_DIR` | Capture directory for service mode | `/tmp/kafka-collector` |

## Service Mode

This example runs kafka-collector in service mode, which provides an HTTP service that captures messages to files.

Captured messages are stored in the `./captures` directory (mounted as `/data/captures` in the container).

## Testing

1. Start the services:
   ```bash
   docker compose up -d
   ```

2. Create a test topic and send messages:
   ```bash
   docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server localhost:9092 \
     --create --topic topic1

   docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server localhost:9092 \
     --topic topic1
   ```

3. View collected messages:
   ```bash
   docker compose logs -f kafka-collector
   ```

## Cleanup

```bash
docker compose down -v
```
