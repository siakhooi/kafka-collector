import argparse
import os
import sys
import uuid
from dataclasses import dataclass
from importlib.metadata import version
from typing import List


class ArgumentValidationError(Exception):
    pass


@dataclass
class Options:
    topics: List[str]
    bootstrap_server: str
    group_id: str
    output_file: str
    capture_dir: str
    mode: str


def parse_args() -> Options:
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

    parser.add_argument(
        "-o",
        "--output",
        default="-",
        help="output file path, use '-' for stdout (default: stdout)",
    )

    parser.add_argument(
        "-c",
        "--capture-dir",
        default="/tmp/kafka-collector",
        help="capture directory for service mode "
        "(default: /tmp/kafka-collector)",
    )

    parser.add_argument(
        "-m",
        "--mode",
        default="cli",
        choices=["cli", "service"],
        help="run mode: cli or service (default: cli)",
    )

    args = parser.parse_args()

    topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    if not topics:
        raise ArgumentValidationError(
            "--topics must contain at least one topic"
        )

    bootstrap_server = args.bootstrap_server
    group_id = args.group if args.group else str(uuid.uuid4())
    output_file = args.output
    capture_dir = args.capture_dir
    mode = args.mode

    if mode == "service":
        if args.output != "-":
            print(
                "Warning: -o/--output will be ignored in service mode",
                file=sys.stderr
            )
        try:
            os.makedirs(capture_dir, exist_ok=True)
        except OSError as e:
            raise ArgumentValidationError(
                f"Failed to create capture directory '{capture_dir}': {e}"
            )
    else:
        if args.capture_dir != "/tmp/kafka-collector":
            print(
                "Warning: -c/--capture-dir will be ignored in cli mode",
                file=sys.stderr
            )

    return Options(
        topics=topics,
        bootstrap_server=bootstrap_server,
        group_id=group_id,
        output_file=output_file,
        capture_dir=capture_dir,
        mode=mode,
    )
