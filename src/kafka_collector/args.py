import argparse
import os
import sys
import uuid
from dataclasses import dataclass
from enum import Enum
from importlib.metadata import version
from typing import List, Optional, TypeVar

T = TypeVar('T')


def _resolve_value(
    arg_value: Optional[T],
    env_value: Optional[str],
    default: T
) -> T:
    if arg_value is not None:
        return arg_value
    if env_value:
        return env_value  # type: ignore
    return default


class Mode(Enum):
    CLI = "cli"
    SERVICE = "service"


DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
DEFAULT_CAPTURE_DIR = "/tmp/kafka-collector"
DEFAULT_PORT = 8080
DEFAULT_MODE = Mode.CLI

ENV_TOPICS = "KAFKA_TOPICS"
ENV_BOOTSTRAP_SERVER = "KAFKA_BOOTSTRAP_SERVER"
ENV_GROUP = "KAFKA_GROUP"
ENV_CAPTURE_DIR = "COLLECTOR_CAPTURE_DIR"
ENV_MODE = "COLLECTOR_MODE"
ENV_SERVICE_PORT = "COLLECTOR_SERVICE_PORT"


class ArgumentValidationError(Exception):
    pass


@dataclass
class Options:
    topics: List[str]
    bootstrap_server: str
    group_id: str
    output_file: str
    capture_dir: str
    mode: Mode
    port: int


def _create_parser() -> argparse.ArgumentParser:
    __version__: str = version("kafka-collector")

    parser = argparse.ArgumentParser(
        description="collect kafka messages from multiple topics"
    )

    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s {__version__}"
    )
    parser.add_argument(
        "-t", "--topics", default=None,
        help="comma separated list of kafka topics to be listened to",
    )
    parser.add_argument(
        "-b", "--bootstrap-server", default=None,
        help="kafka bootstrap server (default: localhost:9092)",
    )
    parser.add_argument(
        "-g", "--group", default=None,
        help="consumer group id (default: random uuid)",
    )
    parser.add_argument(
        "-o", "--output", default=None,
        help="output file path, use '-' for stdout (default: stdout)",
    )
    parser.add_argument(
        "-c", "--capture-dir", default=None,
        help="capture directory for service mode "
        "(default: /tmp/kafka-collector)",
    )
    parser.add_argument(
        "-m", "--mode", default=None,
        choices=[m.value for m in Mode],
        help="run mode: cli or service (default: cli)",
    )
    parser.add_argument(
        "-p", "--port", type=int, default=None,
        help="service port for service mode (default: 8080)",
    )

    return parser


def _resolve_options(args: argparse.Namespace) -> Options:
    env_topics = os.environ.get(ENV_TOPICS)
    env_bootstrap = os.environ.get(ENV_BOOTSTRAP_SERVER)
    env_group = os.environ.get(ENV_GROUP)
    env_capture_dir = os.environ.get(ENV_CAPTURE_DIR)
    env_mode = os.environ.get(ENV_MODE)
    env_port = os.environ.get(ENV_SERVICE_PORT)

    topics_str = args.topics if args.topics is not None else env_topics
    if not topics_str:
        raise ArgumentValidationError(
            f"--topics or {ENV_TOPICS} must be provided"
        )
    topics = [t.strip() for t in topics_str.split(",") if t.strip()]
    if not topics:
        raise ArgumentValidationError(
            "--topics must contain at least one topic"
        )

    bootstrap_server = _resolve_value(
        args.bootstrap_server, env_bootstrap, DEFAULT_BOOTSTRAP_SERVER
    )
    group_id = _resolve_value(args.group, env_group, str(uuid.uuid4()))

    mode_str = args.mode if args.mode is not None else env_mode
    if mode_str is not None:
        try:
            mode = Mode(mode_str)
        except ValueError:
            valid_modes = ", ".join([m.value for m in Mode])
            raise ArgumentValidationError(
                f"Invalid mode '{mode_str}'. Must be one of: {valid_modes}"
            )
    else:
        mode = DEFAULT_MODE

    output_file = args.output if args.output is not None else "-"
    capture_dir = _resolve_value(
        args.capture_dir, env_capture_dir, DEFAULT_CAPTURE_DIR
    )

    port_value = args.port
    if port_value is None and env_port:
        try:
            port_value = int(env_port)
        except ValueError:
            raise ArgumentValidationError(
                f"Invalid {ENV_SERVICE_PORT} '{env_port}'. Must be integer"
            )
    port = port_value if port_value is not None else DEFAULT_PORT

    return Options(
        topics=topics,
        bootstrap_server=bootstrap_server,
        group_id=group_id,
        output_file=output_file,
        capture_dir=capture_dir,
        mode=mode,
        port=port,
    )


def _validate_mode_options(
    args: argparse.Namespace,
    options: Options
) -> None:
    if options.mode == Mode.SERVICE:
        if args.output is not None:
            print(
                "Warning: -o/--output will be ignored in service mode",
                file=sys.stderr
            )
        try:
            os.makedirs(options.capture_dir, exist_ok=True)
        except OSError as e:
            raise ArgumentValidationError(
                f"Failed to create capture directory "
                f"'{options.capture_dir}': {e}"
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


def parse_args() -> Options:
    parser = _create_parser()
    args = parser.parse_args()
    options = _resolve_options(args)
    _validate_mode_options(args, options)
    return options
