import logging
import sys
from typing import Optional

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})

LOGGERS_TO_SUPPRESS = {
    "kafka": logging.WARNING,
    "werkzeug": logging.WARNING,
}


def _validate_log_level(log_level: Optional[str]) -> str:
    """Validate log level and return valid level or default."""
    if log_level not in VALID_LOG_LEVELS:
        valid = ", ".join(sorted(VALID_LOG_LEVELS))
        print(
            f"Warning: Invalid log level '{log_level}'. "
            f"Valid levels: {valid}. Using INFO.",
            file=sys.stderr,
        )
        return "INFO"
    return log_level


def setup_logging(
    log_level: Optional[str] = None,
) -> None:

    log_level = log_level or "INFO"
    log_level = _validate_log_level(log_level)

    level = getattr(logging, log_level)

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        handlers=[logging.StreamHandler(sys.stderr)],
    )

    for logger_name, suppress_level in LOGGERS_TO_SUPPRESS.items():
        logging.getLogger(logger_name).setLevel(suppress_level)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
