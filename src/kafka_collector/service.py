import io
import logging
import os
import re
import zipfile

from flask import Flask, request, jsonify, send_file
from flask.typing import ResponseReturnValue

from kafka_collector.constants import (
    API_ERROR_CAPTURE_FILE_MISSING,
    API_JSON_ERROR_KEY,
    DEFAULT_DOWNLOAD_KIND,
    DOWNLOAD_KINDS,
    DOWNLOAD_KIND_ZIP,
    MIME_TYPE_JSONL,
    MIME_TYPE_ZIP,
)
from kafka_collector.exceptions import (
    CaptureNameNotFoundError,
    DuplicateCaptureNameError,
    EmptyCaptureNameError,
    NoCompletedCapturesError,
)
from kafka_collector.file_manager import FileManager

logger = logging.getLogger(__name__)

_DOWNLOAD_TYPE_ERROR = (
    "type must be " + " or ".join(sorted(DOWNLOAD_KINDS))
)

MAX_CAPTURE_NAME_LEN = 256
_NAME_ALLOWED = re.compile(r"^[a-zA-Z0-9_.-]+$")


def _error_payload(message: str) -> dict[str, str]:
    return {API_JSON_ERROR_KEY: message}


def _json_error(message: str, status: int) -> ResponseReturnValue:
    return jsonify(_error_payload(message)), status


def _parse_name_args(raw_values: list[str]) -> tuple[str | None, str | None]:
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


def _parse_type_args(raw_values: list[str]) -> tuple[str | None, str]:
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


def create_app(file_manager: FileManager) -> Flask:
    app = Flask(__name__)

    @app.route("/reset", methods=["POST"])
    def reset() -> ResponseReturnValue:
        err, name = _parse_name_args(request.args.getlist("name"))
        if err:
            return _json_error(err, 400)
        try:
            new_filepath = file_manager.reset(name)
            return jsonify({
                "status": "ok",
                "new_file": new_filepath
            })
        except (DuplicateCaptureNameError, EmptyCaptureNameError) as e:
            return _json_error(str(e), 400)

    @app.route("/files", methods=["GET"])
    def get_files() -> ResponseReturnValue:
        files = file_manager.get_files()
        return jsonify(files)

    @app.route("/download", methods=["GET"])
    def download() -> ResponseReturnValue:
        err, name = _parse_name_args(request.args.getlist("name"))
        if err:
            return _json_error(err, 400)
        err_type, file_type = _parse_type_args(request.args.getlist("type"))
        if err_type:
            return _json_error(err_type, 400)

        try:
            if name:
                filepath = file_manager.get_file_by_name(name)
                download_name = name
            else:
                download_name, filepath = \
                    file_manager.get_last_completed_file()

            if not os.path.exists(filepath):
                logger.warning(
                    "Capture path recorded but file missing on disk: %s",
                    filepath,
                )
                return _json_error(API_ERROR_CAPTURE_FILE_MISSING, 404)

            if file_type == DOWNLOAD_KIND_ZIP:
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(
                    zip_buffer, "w", zipfile.ZIP_DEFLATED
                ) as zf:
                    zf.write(filepath, os.path.basename(filepath))
                zip_buffer.seek(0)
                return send_file(
                    zip_buffer,
                    mimetype=MIME_TYPE_ZIP,
                    as_attachment=True,
                    download_name=f"{download_name}.zip"
                )
            else:
                return send_file(
                    filepath,
                    mimetype=MIME_TYPE_JSONL,
                    as_attachment=True,
                    download_name=os.path.basename(filepath)
                )
        except CaptureNameNotFoundError as e:
            return _json_error(str(e), 404)
        except NoCompletedCapturesError as e:
            return _json_error(str(e), 400)

    return app
