"""Flask JSON errors and capture-file download helpers for service mode."""

import io
import logging
import os
import zipfile

from flask import jsonify, send_file
from flask.typing import ResponseReturnValue

from kafka_collector.constants import (
    API_ERROR_CAPTURE_FILE_MISSING,
    API_JSON_ERROR_KEY,
    DOWNLOAD_KIND_ZIP,
    MIME_TYPE_JSONL,
    MIME_TYPE_ZIP,
)
from kafka_collector.file_manager import FileManager

logger = logging.getLogger(__name__)


def _error_payload(message: str) -> dict[str, str]:
    return {API_JSON_ERROR_KEY: message}


def json_error(message: str, status: int) -> ResponseReturnValue:
    return jsonify(_error_payload(message)), status


def resolve_download_target(
    file_manager: FileManager,
    name: str | None,
) -> tuple[str, str]:
    if name is not None:
        return name, file_manager.get_file_by_name(name)
    return file_manager.get_last_completed_file()


def response_if_capture_missing(
    filepath: str,
) -> ResponseReturnValue | None:
    if os.path.exists(filepath):
        return None
    logger.warning(
        "Capture path recorded but file missing on disk: %s",
        filepath,
    )
    return json_error(API_ERROR_CAPTURE_FILE_MISSING, 404)


def send_capture_file(
    filepath: str,
    download_name: str,
    file_type: str,
) -> ResponseReturnValue:
    if file_type == DOWNLOAD_KIND_ZIP:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(filepath, os.path.basename(filepath))
        zip_buffer.seek(0)
        return send_file(
            zip_buffer,
            mimetype=MIME_TYPE_ZIP,
            as_attachment=True,
            download_name=f"{download_name}.zip",
        )
    return send_file(
        filepath,
        mimetype=MIME_TYPE_JSONL,
        as_attachment=True,
        download_name=os.path.basename(filepath),
    )
