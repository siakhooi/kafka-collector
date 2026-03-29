import io
import logging
import os
import zipfile

from flask import Flask, request, jsonify, send_file
from flask.typing import ResponseReturnValue

from kafka_collector.constants import (
    API_ERROR_CAPTURE_FILE_MISSING,
    API_JSON_ERROR_KEY,
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
from kafka_collector.http_args import parse_name_args, parse_type_args

logger = logging.getLogger(__name__)


def _error_payload(message: str) -> dict[str, str]:
    return {API_JSON_ERROR_KEY: message}


def _json_error(message: str, status: int) -> ResponseReturnValue:
    return jsonify(_error_payload(message)), status


def _resolve_download_target(
    file_manager: FileManager,
    name: str | None,
) -> tuple[str, str]:
    if name is not None:
        return name, file_manager.get_file_by_name(name)
    return file_manager.get_last_completed_file()


def _response_if_capture_missing(
    filepath: str,
) -> ResponseReturnValue | None:
    if os.path.exists(filepath):
        return None
    logger.warning(
        "Capture path recorded but file missing on disk: %s",
        filepath,
    )
    return _json_error(API_ERROR_CAPTURE_FILE_MISSING, 404)


def _send_capture_file(
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


def create_app(file_manager: FileManager) -> Flask:
    app = Flask(__name__)

    @app.route("/reset", methods=["POST"])
    def reset() -> ResponseReturnValue:
        err, name = parse_name_args(request.args.getlist("name"))
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
        err, name = parse_name_args(request.args.getlist("name"))
        if err:
            return _json_error(err, 400)
        err_type, file_type = parse_type_args(request.args.getlist("type"))
        if err_type:
            return _json_error(err_type, 400)

        try:
            download_name, filepath = _resolve_download_target(
                file_manager,
                name,
            )
            missing = _response_if_capture_missing(filepath)
            if missing is not None:
                return missing
            return _send_capture_file(
                filepath,
                download_name,
                file_type,
            )
        except CaptureNameNotFoundError as e:
            return _json_error(str(e), 404)
        except NoCompletedCapturesError as e:
            return _json_error(str(e), 400)

    return app
