import argparse
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

    args = parser.parse_args()

    topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    if not topics:
        raise ArgumentValidationError(
            "--topics must contain at least one topic"
        )

    bootstrap_server = args.bootstrap_server
    group_id = args.group if args.group else str(uuid.uuid4())
    output_file = args.output

    return Options(
        topics=topics,
        bootstrap_server=bootstrap_server,
        group_id=group_id,
        output_file=output_file,
    )
