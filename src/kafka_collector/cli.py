import argparse
import json
import sys
import uuid
from importlib.metadata import version

from kafka import KafkaConsumer


def print_to_stderr_and_exit(e: Exception, exit_code: int) -> None:
    print(f"Error: {e}", file=sys.stderr)
    exit(exit_code)


def run() -> None:
    __version__: str = version("kafka-collector")

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="collect kafka messages from multiple topics"
    )

    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s {__version__}"
    )

    parser.add_argument(
        "-t",
        "--topics",
        required=True,
        help="comma separated list of kafka topics to be listened to",
    )

    parser.add_argument(
        "-b",
        "--bootstrap-server",
        default="localhost:9092",
        help="kafka bootstrap server (default: localhost:9092)",
    )

    parser.add_argument(
        "-g",
        "--group",
        default=None,
        help="consumer group id (default: random uuid)",
    )

    args = parser.parse_args()

    topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    if not topics:
        print("Error: --topics must contain at least one topic", file=sys.stderr)
        sys.exit(1)

    bootstrap_server = args.bootstrap_server
    group_id = args.group if args.group else str(uuid.uuid4())

    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_server,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )

        for message in consumer:
            output = {
                "topic": message.topic,
                "timestamp": message.timestamp,
                "header": dict(message.headers) if message.headers else {},
                "value": message.value.decode("utf-8") if message.value else None,
                "key": message.key.decode("utf-8") if message.key else None,
            }
            print(json.dumps(output), flush=True)

    except Exception as e:
        print_to_stderr_and_exit(e, 1)
