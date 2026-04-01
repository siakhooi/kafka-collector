import argparse
import os
import sys
import uuid
from dataclasses import dataclass, field
from importlib.metadata import version
from typing import List, Optional, TypeVar

from kafka_collector.constants import (
    DEFAULT_BOOTSTRAP_SERVER,
    DEFAULT_CAPTURE_DIR,
    DEFAULT_MODE,
    DEFAULT_PORT,
    DEBUG_LOG_LEVEL,
    DEFAULT_LOG_LEVEL,
    ENV_LOG_LEVEL,
    ENV_BOOTSTRAP_SERVER,
    ENV_CAPTURE_DIR,
    ENV_GROUP,
    ENV_MODE,
    ENV_SERVICE_PORT,
    ENV_TOPICS,
    Mode,
)
from kafka_collector.exceptions import ArgumentValidationError

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


def _warn_ignored(option: str, mode: str) -> None:
    print(f"Warning: {option} will be ignored in {mode} mode", file=sys.stderr)


def _resolve_topics(
    arg_topics: Optional[str],
    env_topics: Optional[str]
) -> List[str]:
    topics_str = arg_topics if arg_topics is not None else env_topics
    if not topics_str:
        raise ArgumentValidationError(
            f"--topics or {ENV_TOPICS} must be provided"
        )
    topics = [t.strip() for t in topics_str.split(",") if t.strip()]
    if not topics:
        raise ArgumentValidationError(
            "--topics must contain at least one topic"
        )
    return topics


def _resolve_mode(
    arg_mode: Optional[str],
    env_mode: Optional[str]
) -> Mode:
    mode_str = arg_mode if arg_mode is not None else env_mode
    if mode_str is not None:
        try:
            return Mode(mode_str)
        except ValueError:
            valid_modes = ", ".join([m.value for m in Mode])
            raise ArgumentValidationError(
                f"Invalid mode '{mode_str}'. Must be one of: {valid_modes}"
            )
    return DEFAULT_MODE


def _resolve_port(
    arg_port: Optional[int],
    env_port: Optional[str]
) -> int:
    if arg_port is not None:
        return arg_port
    if env_port:
        try:
            return int(env_port)
        except ValueError:
            raise ArgumentValidationError(
                f"Invalid {ENV_SERVICE_PORT} '{env_port}'. Must be integer"
            )
    return DEFAULT_PORT


def _resolve_log_level(
    debug: bool,
    arg_log_level: Optional[str],
    env_log_level: Optional[str],
    default: str
) -> str:
    if debug:
        return DEBUG_LOG_LEVEL
    if arg_log_level is not None:
        return arg_log_level.upper()
    if env_log_level:
        return env_log_level.upper()
    return default.upper()


@dataclass
class Options:
    topics: List[str] = field(default_factory=list)
    bootstrap_server: str = DEFAULT_BOOTSTRAP_SERVER
    group_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    output_file: str = "-"
    capture_dir: str = DEFAULT_CAPTURE_DIR
    mode: Mode = DEFAULT_MODE
    port: int = DEFAULT_PORT
    log_level: Optional[str] = None


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
    parser.add_argument(
        "--log-level", default=None,
        choices=["debug", "info", "warning", "error", "critical"],
        help="logging level (default: info)",
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="enable debug logging (shortcut for --log-level debug)",
    )

    return parser


def _resolve_options(args: argparse.Namespace) -> Options:
    topics = _resolve_topics(
        args.topics, os.environ.get(ENV_TOPICS)
    )
    bootstrap_server = _resolve_value(
        args.bootstrap_server,
        os.environ.get(ENV_BOOTSTRAP_SERVER),
        DEFAULT_BOOTSTRAP_SERVER
    )
    group_id = _resolve_value(
        args.group, os.environ.get(ENV_GROUP), str(uuid.uuid4())
    )
    mode = _resolve_mode(
        args.mode, os.environ.get(ENV_MODE)
    )
    output_file = args.output if args.output is not None else "-"
    capture_dir = _resolve_value(
        args.capture_dir,
        os.environ.get(ENV_CAPTURE_DIR),
        DEFAULT_CAPTURE_DIR
    )
    port = _resolve_port(
        args.port, os.environ.get(ENV_SERVICE_PORT)
    )
    log_level = _resolve_log_level(
        args.debug,
        args.log_level,
        os.environ.get(ENV_LOG_LEVEL),
        DEFAULT_LOG_LEVEL
    )

    return Options(
        topics=topics,
        bootstrap_server=bootstrap_server,
        group_id=group_id,
        output_file=output_file,
        capture_dir=capture_dir,
        mode=mode,
        port=port,
        log_level=log_level
    )


def _validate_mode_options(
    args: argparse.Namespace,
    options: Options
) -> None:
    if options.mode == Mode.SERVICE:
        if args.output is not None:
            _warn_ignored("-o/--output", "service")
        try:
            os.makedirs(options.capture_dir, exist_ok=True)
        except OSError as e:
            raise ArgumentValidationError(
                f"Failed to create capture directory "
                f"'{options.capture_dir}': {e}"
            )
    else:
        if args.capture_dir is not None:
            _warn_ignored("-c/--capture-dir", "cli")
        if args.port is not None:
            _warn_ignored("-p/--port", "cli")


def parse_args() -> Options:
    parser = _create_parser()
    args = parser.parse_args()
    options = _resolve_options(args)
    _validate_mode_options(args, options)
    return options
