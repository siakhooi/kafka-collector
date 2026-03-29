import json
import os
import sys
from datetime import datetime

from kafka import KafkaConsumer

from kafka_collector.args import ArgumentValidationError, Options, parse_args


def print_to_stderr_and_exit(e: Exception, exit_code: int) -> None:
    print(f"Error: {e}", file=sys.stderr)
    exit(exit_code)


def run() -> None:
    try:
        options: Options = parse_args()
    except ArgumentValidationError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    topics = options.topics
    bootstrap_server = options.bootstrap_server
    group_id = options.group_id
    output_file = options.output_file
    capture_dir = options.capture_dir
    mode = options.mode

    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_server,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )

        while not consumer.assignment():
            consumer.poll(timeout_ms=100)

        if mode == "service":
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"kafka-collector_{timestamp}.jsonl"
            filepath = os.path.join(capture_dir, filename)
            out = open(filepath, "a")
            should_close = True
        elif output_file == "-":
            out = sys.stdout
            should_close = False
        else:
            out = open(output_file, "a")
            should_close = True

        try:
            for message in consumer:
                output = {
                    "topic": message.topic,
                    "timestamp": message.timestamp,
                    "header": dict(message.headers) if message.headers else {},
                    "value": (
                        message.value.decode("utf-8")
                        if message.value else None
                    ),
                    "key": (
                        message.key.decode("utf-8")
                        if message.key else None
                    ),
                }
                print(json.dumps(output), file=out, flush=True)
        finally:
            if should_close:
                out.close()

    except Exception as e:
        print_to_stderr_and_exit(e, 1)
