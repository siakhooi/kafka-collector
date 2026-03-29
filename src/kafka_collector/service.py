from flask import Flask, jsonify, request
from flask.typing import ResponseReturnValue

from kafka_collector.exceptions import (
    CaptureNameNotFoundError,
    DuplicateCaptureNameError,
    EmptyCaptureNameError,
    NoCompletedCapturesError,
)
from kafka_collector.file_manager import FileManager
from kafka_collector.http_args import (
    parse_name_args,
    parse_name_value,
    parse_type_args,
)
from kafka_collector.service_helpers import (
    json_error,
    resolve_download_target,
    response_if_capture_missing,
    send_capture_file,
)


def create_app(file_manager: FileManager) -> Flask:
    # Programmatic operator/automation API: no cookie-based session auth;
    # the usual browser CSRF scenario does not apply. Restrict exposure in
    # prod (bind address, firewall, reverse-proxy auth). Sonar: python:S4502.
    app = Flask(__name__)  # NOSONAR

    @app.route("/reset", methods=["POST"])
    def reset() -> ResponseReturnValue:
        body = request.get_json(silent=True) or {}
        err, name = parse_name_value(body.get("name"))
        if err:
            return json_error(err, 400)
        try:
            new_filepath = file_manager.reset(name)
            return jsonify({
                "status": "ok",
                "new_file": new_filepath
            })
        except (DuplicateCaptureNameError, EmptyCaptureNameError) as e:
            return json_error(str(e), 400)

    @app.route("/files", methods=["GET"])
    def get_files() -> ResponseReturnValue:
        files = file_manager.get_files()
        return jsonify(files)

    def _handle_download(
        name: str | None,
        file_type: str,
    ) -> ResponseReturnValue:
        download_name, filepath = resolve_download_target(file_manager, name)
        missing = response_if_capture_missing(filepath)
        if missing is not None:
            return missing
        return send_capture_file(filepath, download_name, file_type)

    @app.route("/download", methods=["GET"])
    def download() -> ResponseReturnValue:
        err, name = parse_name_args(request.args.getlist("name"))
        if err:
            return json_error(err, 400)
        err_type, file_type = parse_type_args(request.args.getlist("type"))
        if err_type:
            return json_error(err_type, 400)

        try:
            return _handle_download(name, file_type)
        except CaptureNameNotFoundError as e:
            return json_error(str(e), 404)
        except NoCompletedCapturesError as e:
            return json_error(str(e), 400)

    return app
