"""Parse and validate HTTP query arguments for the collector service."""

import re

from kafka_collector.constants import DEFAULT_DOWNLOAD_KIND, DOWNLOAD_KINDS

MAX_CAPTURE_NAME_LEN = 256
_NAME_ALLOWED = re.compile(r"^[a-zA-Z0-9_.-]+$")

_DOWNLOAD_TYPE_ERROR = (
    "type must be " + " or ".join(sorted(DOWNLOAD_KINDS))
)


def parse_name_args(raw_values: list[str]) -> tuple[str | None, str | None]:
    """Validate ``name`` query values. Returns ``(error_message, name)``.
    ``name`` is ``None`` when the parameter is absent; otherwise it is stripped
    and validated. ``error_message`` is set when the client should receive 400.
    """
    if len(raw_values) > 1:
        return ("duplicate name parameter", None)
    if not raw_values:
        return (None, None)
    stripped = raw_values[0].strip()
    if not stripped:
        return ("name must not be empty", None)
    if len(stripped) > MAX_CAPTURE_NAME_LEN:
        return (
            f"name exceeds maximum length ({MAX_CAPTURE_NAME_LEN})",
            None,
        )
    if not _NAME_ALLOWED.fullmatch(stripped):
        return ("name contains invalid characters", None)
    return (None, stripped)


def parse_type_args(raw_values: list[str]) -> tuple[str | None, str]:
    """Validate ``type`` query values. Returns ``(error_message, kind)`` where
    ``kind`` is a member of ``DOWNLOAD_KINDS`` when there is no error.
    """
    if len(raw_values) > 1:
        return ("duplicate type parameter", DEFAULT_DOWNLOAD_KIND)
    if not raw_values:
        return (None, DEFAULT_DOWNLOAD_KIND)
    t = raw_values[0].strip().lower()
    if t not in DOWNLOAD_KINDS:
        return (_DOWNLOAD_TYPE_ERROR, DEFAULT_DOWNLOAD_KIND)
    return (None, t)
