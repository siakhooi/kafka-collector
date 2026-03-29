import io
import os
import zipfile
from typing import Any

from flask import Flask, request, jsonify, send_file

from kafka_collector.file_manager import FileManager


def create_app(file_manager: FileManager) -> Flask:
    app = Flask(__name__)

    @app.route("/reset", methods=["POST"])
    def reset() -> Any:
        name = request.args.get("name")
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
        name = request.args.get("name")
        file_type = request.args.get("type", "jsonl")

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
        except ValueError as e:
            return jsonify({"error": str(e)}), 404

    return app
