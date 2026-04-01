import argparse
import os
import sys
import uuid
from dataclasses import dataclass, field
from importlib.metadata import version
from typing import Any, Callable, List, Optional, TypeVar

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

__version__: str = version("kafka-collector")


def _resolve(
    arg_value: Any,
    env_value: Optional[str],
    default: T,
    converter: Optional[Callable[[str], T]] = None,
    validator: Optional[Callable[[T], T]] = None,
    required: bool = False,
    error_msg: Optional[str] = None
) -> T:
    if arg_value is not None:
        if converter and isinstance(arg_value, str):
            result = converter(arg_value)
        else:
            result = arg_value
    elif env_value:
        result = converter(env_value) if converter else env_value
    elif required:
        raise ArgumentValidationError(
            error_msg or "Required value not provided"
        )
    else:
        result = default
    return validator(result) if validator else result


def _warn_ignored(option: str, mode: str) -> None:
    print(f"Warning: {option} will be ignored in {mode} mode", file=sys.stderr)


def _parse_topics(value: str) -> List[str]:
    topics = [t.strip() for t in value.split(",") if t.strip()]
    if not topics:
        raise ArgumentValidationError(
            "--topics must contain at least one topic"
        )
    return topics


def _parse_mode(value: str) -> Mode:
    try:
        return Mode(value)
    except ValueError:
        valid_modes = ", ".join([m.value for m in Mode])
        raise ArgumentValidationError(
            f"Invalid mode '{value}'. Must be one of: {valid_modes}"
        )


def _parse_port(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        raise ArgumentValidationError(
            f"Invalid {ENV_SERVICE_PORT} '{value}'. Must be integer"
        )


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


def _get_env_vars() -> dict:
    return {
        'topics': os.environ.get(ENV_TOPICS),
        'bootstrap_server': os.environ.get(ENV_BOOTSTRAP_SERVER),
        'group': os.environ.get(ENV_GROUP),
        'mode': os.environ.get(ENV_MODE),
        'capture_dir': os.environ.get(ENV_CAPTURE_DIR),
        'port': os.environ.get(ENV_SERVICE_PORT),
        'log_level': os.environ.get(ENV_LOG_LEVEL),
    }


def _resolve_options(args: argparse.Namespace) -> Options:
    env = _get_env_vars()

    topics = _resolve(
        args.topics, env['topics'], [],
        converter=_parse_topics,
        required=True,
        error_msg=f"--topics or {ENV_TOPICS} must be provided"
    )
    bootstrap_server = _resolve(
        args.bootstrap_server,
        env['bootstrap_server'],
        DEFAULT_BOOTSTRAP_SERVER
    )
    group_id = _resolve(
        args.group, env['group'], str(uuid.uuid4())
    )
    mode = _resolve(
        args.mode, env['mode'], DEFAULT_MODE,
        converter=_parse_mode
    )
    output_file = _resolve(args.output, None, "-")
    capture_dir = _resolve(
        args.capture_dir, env['capture_dir'], DEFAULT_CAPTURE_DIR
    )
    port = _resolve(
        args.port, env['port'], DEFAULT_PORT,
        converter=_parse_port
    )
    log_level = _resolve(
        DEBUG_LOG_LEVEL if args.debug else args.log_level,
        env['log_level'],
        DEFAULT_LOG_LEVEL,
        validator=lambda x: x.upper()
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
