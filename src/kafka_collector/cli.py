import json
import signal
import sys
import threading
from contextlib import contextmanager
from typing import Any, Callable, Generator, TextIO

from kafka import KafkaConsumer

from kafka_collector.args import Options, parse_args
from kafka_collector.logging_config import get_logger, setup_logging
from kafka_collector.constants import (
    FLASK_HOST,
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_ENABLE_AUTO_COMMIT,
    KAFKA_POLL_TIMEOUT_MS,
    Mode,
)
from kafka_collector.exceptions import ArgumentValidationError
from kafka_collector.file_manager import FileManager
from kafka_collector.service import create_app

logger = get_logger(__name__)


def print_to_stderr_and_exit(e: Exception, exit_code: int) -> None:
    print(f"Error: {e}", file=sys.stderr)
    exit(exit_code)


def _format_message(message: Any) -> dict:
    return {
        "topic": message.topic,
        "timestamp": message.timestamp,
        "header": dict(message.headers) if message.headers else {},
        "value": message.value.decode("utf-8") if message.value else None,
        "key": message.key.decode("utf-8") if message.key else None,
    }


def _create_consumer(options: Options) -> KafkaConsumer:
    logger.info(
        "Connecting to Kafka at %s with group_id=%s",
        options.bootstrap_server,
        options.group_id,
    )
    logger.debug("Subscribing to topics: %s", options.topics)
    consumer = KafkaConsumer(
        *options.topics,
        bootstrap_servers=options.bootstrap_server,
        group_id=options.group_id,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        enable_auto_commit=KAFKA_ENABLE_AUTO_COMMIT,
    )
    while not consumer.assignment():
        consumer.poll(timeout_ms=KAFKA_POLL_TIMEOUT_MS)
    logger.info("Consumer connected and assigned partitions")
    return consumer


@contextmanager
def _open_output(output_file: str) -> Generator[TextIO, None, None]:
    if output_file == "-":
        yield sys.stdout
    else:
        f = open(output_file, "a")
        try:
            yield f
        finally:
            f.close()


@contextmanager
def _graceful_shutdown() -> Generator[threading.Event, None, None]:
    shutdown_event = threading.Event()

    def signal_handler(signum: int, frame: Any) -> None:
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    yield shutdown_event


def _consume_messages(
    consumer: KafkaConsumer,
    shutdown_event: threading.Event,
    process_message: Callable[[Any], None],
) -> None:
    """Generic message consumption loop with shutdown handling."""
    msg_count = 0
    for message in consumer:
        if shutdown_event.is_set():
            logger.info("Shutdown signal received")
            break
        process_message(message)
        msg_count += 1
        if msg_count % 1000 == 0:
            logger.debug("Processed %d messages", msg_count)


def run_cli_mode(consumer: KafkaConsumer, output_file: str) -> None:
    logger.info("Starting CLI mode, output=%s", output_file)
    with _graceful_shutdown() as shutdown_event:
        try:
            with _open_output(output_file) as out:
                def process_message(message: Any) -> None:
                    formatted = json.dumps(_format_message(message))
                    print(formatted, file=out, flush=True)

                _consume_messages(consumer, shutdown_event, process_message)
        finally:
            logger.info("Closing consumer")
            consumer.close()


def run_service_mode(
    consumer: KafkaConsumer,
    capture_dir: str,
    port: int
) -> None:
    logger.info(
        "Starting service mode, capture_dir=%s, port=%d", capture_dir, port
    )
    file_manager = FileManager(capture_dir)
    file_manager.open_new_file()

    with _graceful_shutdown() as shutdown_event:
        def process_message(message: Any) -> None:
            file_manager.write(json.dumps(_format_message(message)) + "\n")

        def consume_messages() -> None:
            _consume_messages(consumer, shutdown_event, process_message)

        consumer_thread = threading.Thread(
            target=consume_messages, daemon=True
        )
        consumer_thread.start()
        logger.debug("Consumer thread started")

        try:
            app = create_app(file_manager)
            logger.info("Starting Flask server on %s:%d", FLASK_HOST, port)
            app.run(host=FLASK_HOST, port=port)
        finally:
            logger.info("Shutting down service")
            shutdown_event.set()
            consumer.close()
            file_manager.close()


def run() -> None:
    try:
        options: Options = parse_args()
    except ArgumentValidationError as e:
        print_to_stderr_and_exit(e, 1)

    setup_logging(log_level=options.log_level)
    logger.debug("Options: %s", options)

    try:
        consumer = _create_consumer(options)

        if options.mode == Mode.SERVICE:
            run_service_mode(consumer, options.capture_dir, options.port)
        else:
            run_cli_mode(consumer, options.output_file)

    except Exception as e:
        logger.exception("Fatal error")
        print_to_stderr_and_exit(e, 1)
