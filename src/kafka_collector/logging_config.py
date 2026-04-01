import logging
import sys
from typing import Optional

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


def setup_logging(
    log_level: Optional[str] = None,
) -> None:

    if log_level not in VALID_LOG_LEVELS:
        valid = ", ".join(sorted(VALID_LOG_LEVELS))
        print(
            f"Warning: Invalid log level '{log_level}'. "
            f"Valid levels: {valid}. Using INFO.",
            file=sys.stderr,
        )
        log_level = "INFO"

    level = getattr(logging, log_level)

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        handlers=[logging.StreamHandler(sys.stderr)],
    )

    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
