# Service Mode

Service mode runs kafka-collector as an HTTP service that captures Kafka messages to files and provides REST API endpoints for managing and downloading captures.

## Usage

```bash
kafka-collector -m service -t topic1,topic2 -b localhost:9092 -p 8080
```

## Options

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `-t, --topics` | `KAFKA_TOPICS` | (required) | Comma-separated list of Kafka topics |
| `-b, --bootstrap-server` | `KAFKA_BOOTSTRAP_SERVER` | `localhost:9092` | Kafka bootstrap server address |
| `-g, --group` | `KAFKA_GROUP` | Random UUID | Consumer group ID |
| `-c, --capture-dir` | `COLLECTOR_CAPTURE_DIR` | `/tmp/kafka-collector` | Directory to store capture files |
| `-p, --port` | `COLLECTOR_SERVICE_PORT` | `8080` | HTTP service port |
| `-m, --mode` | `COLLECTOR_MODE` | `cli` | Run mode (set to `service`) |
| `--log-level` | `LOG_LEVEL` | `info` | Logging level: debug, info, warning, error, critical |
| `--debug` | - | - | Shortcut for `--log-level debug` |

**Note:** The `-o/--output` option is ignored in service mode.

## How It Works

1. Service starts and begins consuming messages from configured Kafka topics
2. Messages are written to a capture file in the capture directory
3. Use `/reset` endpoint to finalize current capture and start a new one
4. Use `/download` endpoint to retrieve completed captures
5. Use `/files` endpoint to list all completed captures

## Capture Files

Capture files are stored in JSONL format (JSON Lines) with the naming pattern:

```
kafka-collector_YYYYMMDD_HHMMSS_ffffff.jsonl
```

Each line in the file is a JSON object with the same format as CLI mode:

```json
{
  "topic": "my-topic",
  "timestamp": 1711900800000,
  "header": {"key": "value"},
  "value": "message content",
  "key": "message-key"
}
```

## REST API Endpoints

### POST /reset

Finalizes the current capture file and starts a new one.

**Request Body (optional):**

```json
{
  "name": "my-capture-name"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | No | Custom name for the capture. If omitted, a random 8-character ID is generated. |

**Name Constraints:**
- Maximum 256 characters
- Allowed characters: `a-z`, `A-Z`, `0-9`, `_`, `.`, `-`
- Must be unique among completed captures

**Response (200 OK):**

```json
{
  "status": "ok",
  "new_file": "/tmp/kafka-collector/kafka-collector_20240331_120000_123456.jsonl"
}
```

**Error Responses:**
- `400 Bad Request` - Invalid or duplicate name

### GET /files

Lists all completed capture files.

**Response (200 OK):**

```json
[
  {
    "name": "capture-1",
    "path": "/tmp/kafka-collector/kafka-collector_20240331_100000_123456.jsonl",
    "completed_at": "2024-03-31T12:00:00.123456+00:00"
  },
  {
    "name": "capture-2",
    "path": "/tmp/kafka-collector/kafka-collector_20240331_120000_654321.jsonl",
    "completed_at": "2024-03-31T14:00:00.654321+00:00"
  }
]
```

### GET /download

Downloads a completed capture file.

**Query Parameters:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `name` | No | Last completed | Name of the capture to download |
| `type` | No | `jsonl` | Download format: `jsonl` or `zip` |

**Examples:**

```bash
# Download the last completed capture as JSONL
curl http://localhost:8080/download -o capture.jsonl

# Download a specific capture by name
curl "http://localhost:8080/download?name=my-capture" -o my-capture.jsonl

# Download as ZIP
curl "http://localhost:8080/download?type=zip" -o capture.zip

# Download specific capture as ZIP
curl "http://localhost:8080/download?name=my-capture&type=zip" -o my-capture.zip
```

**Response:**
- `200 OK` - File download with appropriate MIME type (`application/jsonl` or `application/zip`)

**Error Responses:**
- `400 Bad Request` - Invalid parameters or no completed captures
- `404 Not Found` - Capture name not found or file missing

## Examples

### Basic service startup

```bash
kafka-collector -m service -t orders,payments -c /data/captures -p 8080
```

### Using environment variables

```bash
export KAFKA_TOPICS=orders,payments
export KAFKA_BOOTSTRAP_SERVER=kafka:9092
export COLLECTOR_MODE=service
export COLLECTOR_CAPTURE_DIR=/data/captures
export COLLECTOR_SERVICE_PORT=8080
export LOG_LEVEL=warning

kafka-collector
```

### Typical workflow

```bash
# 1. Start the service
kafka-collector -m service -t orders -p 8080 &

# 2. Let it collect messages for a while...

# 3. Finalize current capture with a name
curl -X POST http://localhost:8080/reset \
  -H "Content-Type: application/json" \
  -d '{"name": "morning-batch"}'

# 4. List completed captures
curl http://localhost:8080/files

# 5. Download the capture
curl "http://localhost:8080/download?name=morning-batch" -o morning-batch.jsonl
```

## Behavior

- **Offset Reset:** Starts consuming from the latest offset
- **Auto Commit:** Commits offsets automatically
- **Continuous:** Runs indefinitely until stopped
- **Thread-safe:** Message writing and file management are thread-safe
- **Flush:** Messages are flushed to disk immediately after writing
- **Host Binding:** Service binds to `0.0.0.0` (all interfaces)
- **Logging:** Log level can be set via `--debug`, `--log-level` or `LOG_LEVEL` environment variable
