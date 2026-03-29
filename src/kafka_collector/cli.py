import json
import sys
import threading

from kafka import KafkaConsumer

from kafka_collector.args import (
    ArgumentValidationError, Mode, Options, parse_args
)
from kafka_collector.service import FileManager, create_app


def print_to_stderr_and_exit(e: Exception, exit_code: int) -> None:
    print(f"Error: {e}", file=sys.stderr)
    exit(exit_code)


def run_cli_mode(
    consumer: KafkaConsumer,
    output_file: str
) -> None:
    if output_file == "-":
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


def run_service_mode(
    consumer: KafkaConsumer,
    capture_dir: str,
    port: int
) -> None:
    file_manager = FileManager(capture_dir)
    file_manager.open_new_file()

    def consume_messages() -> None:
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
            file_manager.write(json.dumps(output) + "\n")

    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

    app = create_app(file_manager)
    app.run(host="0.0.0.0", port=port)


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
    port = options.port

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

        if mode == Mode.SERVICE:
            run_service_mode(consumer, capture_dir, port)
        else:
            run_cli_mode(consumer, output_file)

    except Exception as e:
        print_to_stderr_and_exit(e, 1)
