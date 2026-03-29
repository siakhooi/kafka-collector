import io
import os
import re
import zipfile
from typing import Any

from flask import Flask, request, jsonify, send_file

from kafka_collector.exceptions import (
    CaptureNameNotFoundError,
    NoCompletedCapturesError,
)
from kafka_collector.file_manager import FileManager

MAX_CAPTURE_NAME_LEN = 256
_NAME_ALLOWED = re.compile(r"^[a-zA-Z0-9_.-]+$")


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
    ``kind`` is ``jsonl`` or ``zip`` when there is no error.
    """
    if len(raw_values) > 1:
        return ("duplicate type parameter", "jsonl")
    if not raw_values:
        return (None, "jsonl")
    t = raw_values[0].strip().lower()
    if t not in ("jsonl", "zip"):
        return ("type must be jsonl or zip", "jsonl")
    return (None, t)


def create_app(file_manager: FileManager) -> Flask:
    app = Flask(__name__)

    @app.route("/reset", methods=["POST"])
    def reset() -> Any:
        err, name = _parse_name_args(request.args.getlist("name"))
        if err:
            return jsonify({"error": err}), 400
        try:
            new_filepath = file_manager.reset(name)
            return jsonify({
                "status": "ok",
                "new_file": new_filepath
            })
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    @app.route("/files", methods=["GET"])
    def get_files() -> Any:
        files = file_manager.get_files()
        return jsonify(files)

    @app.route("/download", methods=["GET"])
    def download() -> Any:
        err, name = _parse_name_args(request.args.getlist("name"))
        if err:
            return jsonify({"error": err}), 400
        err_type, file_type = _parse_type_args(request.args.getlist("type"))
        if err_type:
            return jsonify({"error": err_type}), 400

        try:
            if name:
                filepath = file_manager.get_file_by_name(name)
                download_name = name
            else:
                download_name, filepath = \
                    file_manager.get_last_completed_file()

            if not os.path.exists(filepath):
                return jsonify({"error": f"file not found: {filepath}"}), 404

            if file_type == "zip":
                zip_buffer = io.BytesIO()
                zf = zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED)
                zf.write(filepath, os.path.basename(filepath))
                zf.close()
                zip_buffer.seek(0)
                return send_file(
                    zip_buffer,
                    mimetype="application/zip",
                    as_attachment=True,
                    download_name=f"{download_name}.zip"
                )
            else:
                return send_file(
                    filepath,
                    mimetype="application/jsonl",
                    as_attachment=True,
                    download_name=os.path.basename(filepath)
                )
        except CaptureNameNotFoundError as e:
            return jsonify({"error": str(e)}), 404
        except NoCompletedCapturesError as e:
            return jsonify({"error": str(e)}), 400

    return app
