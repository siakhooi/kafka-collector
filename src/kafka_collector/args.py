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
    port: int


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
        default=None,
        help="comma separated list of kafka topics to be listened to",
    )

    parser.add_argument(
        "-b",
        "--bootstrap-server",
        default=None,
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
        default=None,
        help="output file path, use '-' for stdout (default: stdout)",
    )

    parser.add_argument(
        "-c",
        "--capture-dir",
        default=None,
        help="capture directory for service mode "
        "(default: /tmp/kafka-collector)",
    )

    parser.add_argument(
        "-m",
        "--mode",
        default=None,
        choices=["cli", "service"],
        help="run mode: cli or service (default: cli)",
    )

    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=None,
        help="service port for service mode (default: 8080)",
    )

    args = parser.parse_args()

    env_topics = os.environ.get("KAFKA_TOPICS")
    env_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVER")
    env_group = os.environ.get("KAFKA_GROUP")
    env_capture_dir = os.environ.get("COLLECTOR_CAPTURE_DIR")
    env_mode = os.environ.get("COLLECTOR_MODE")
    env_port = os.environ.get("COLLECTOR_SERVICE_PORT")

    topics_str = args.topics if args.topics is not None else env_topics
    if not topics_str:
        raise ArgumentValidationError(
            "--topics or KAFKA_TOPICS must be provided"
        )
    topics = [t.strip() for t in topics_str.split(",") if t.strip()]
    if not topics:
        raise ArgumentValidationError(
            "--topics must contain at least one topic"
        )

    bootstrap_server = (
        args.bootstrap_server if args.bootstrap_server is not None
        else (env_bootstrap if env_bootstrap else "localhost:9092")
    )

    group_id = (
        args.group if args.group is not None
        else (env_group if env_group else str(uuid.uuid4()))
    )

    mode_value = args.mode if args.mode is not None else env_mode
    if mode_value is not None and mode_value not in ["cli", "service"]:
        raise ArgumentValidationError(
            f"Invalid mode '{mode_value}'. Must be 'cli' or 'service'"
        )
    mode = mode_value if mode_value else "cli"

    output_file = args.output if args.output is not None else "-"

    capture_dir = (
        args.capture_dir if args.capture_dir is not None
        else (env_capture_dir if env_capture_dir else "/tmp/kafka-collector")
    )

    port_value = args.port
    if port_value is None and env_port:
        try:
            port_value = int(env_port)
        except ValueError:
            raise ArgumentValidationError(
                f"Invalid COLLECTOR_SERVICE_PORT '{env_port}'. Must be integer"
            )
    port = port_value if port_value is not None else 8080

    if mode == "service":
        if args.output is not None:
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
        if args.capture_dir is not None:
            print(
                "Warning: -c/--capture-dir will be ignored in cli mode",
                file=sys.stderr
            )
        if args.port is not None:
            print(
                "Warning: -p/--port will be ignored in cli mode",
                file=sys.stderr
            )

    return Options(
        topics=topics,
        bootstrap_server=bootstrap_server,
        group_id=group_id,
        output_file=output_file,
        capture_dir=capture_dir,
        mode=mode,
        port=port,
    )
