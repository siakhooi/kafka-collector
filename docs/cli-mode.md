# CLI Mode

CLI mode is the default operating mode for kafka-collector. It continuously consumes messages from Kafka topics and outputs them as JSON lines to stdout or a file.

## Usage

```bash
kafka-collector -m cli -t topic1,topic2 -b localhost:9092
```

Or simply (CLI is the default mode):

```bash
kafka-collector -t topic1,topic2
```

## Options

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `-t, --topics` | `KAFKA_TOPICS` | (required) | Comma-separated list of Kafka topics |
| `-b, --bootstrap-server` | `KAFKA_BOOTSTRAP_SERVER` | `localhost:9092` | Kafka bootstrap server address |
| `-g, --group` | `KAFKA_GROUP` | Random UUID | Consumer group ID |
| `-o, --output` | - | `-` (stdout) | Output file path, use `-` for stdout |
| `-m, --mode` | `COLLECTOR_MODE` | `cli` | Run mode |

**Note:** The `-c/--capture-dir` and `-p/--port` options are ignored in CLI mode.

## Output Format

Messages are output as JSON lines (one JSON object per line). Each message contains:

```json
{
  "topic": "my-topic",
  "timestamp": 1711900800000,
  "header": {"key": "value"},
  "value": "message content",
  "key": "message-key"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `topic` | string | The Kafka topic name |
| `timestamp` | integer | Message timestamp in milliseconds |
| `header` | object | Message headers as key-value pairs |
| `value` | string \| null | Message value (UTF-8 decoded) |
| `key` | string \| null | Message key (UTF-8 decoded) |

## Examples

### Output to stdout

```bash
kafka-collector -t orders,payments -b kafka:9092
```

### Output to a file

```bash
kafka-collector -t orders -o /var/log/kafka-messages.jsonl
```

### Using environment variables

```bash
export KAFKA_TOPICS=orders,payments
export KAFKA_BOOTSTRAP_SERVER=kafka:9092
export KAFKA_GROUP=my-consumer-group

kafka-collector
```

### Pipe to other tools

```bash
# Filter messages with jq
kafka-collector -t orders | jq 'select(.value | contains("urgent"))'

# Count messages
kafka-collector -t orders | wc -l
```

## Behavior

- **Offset Reset:** Starts consuming from the latest offset (`auto_offset_reset=latest`)
- **Auto Commit:** Commits offsets automatically (`enable_auto_commit=True`)
- **Continuous:** Runs indefinitely until interrupted (Ctrl+C)
- **Flush:** Output is flushed after each message for real-time streaming
