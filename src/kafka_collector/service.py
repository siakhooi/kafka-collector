import os
import threading
import uuid
from datetime import datetime
from typing import IO, List, Dict, Any

from flask import Flask, request, jsonify


class FileManager:
    def __init__(self, capture_dir: str):
        self.capture_dir = capture_dir
        self.current_file: IO | None = None
        self.current_filepath: str | None = None
        self.completed_files: List[Dict[str, str]] = []
        self.lock = threading.Lock()

    def _generate_filepath(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"kafka-collector_{timestamp}.jsonl"
        return os.path.join(self.capture_dir, filename)

    def _generate_short_id(self) -> str:
        return uuid.uuid4().hex[:8]

    def open_new_file(self) -> str:
        with self.lock:
            filepath = self._generate_filepath()
            self.current_file = open(filepath, "a")
            self.current_filepath = filepath
            return filepath

    def write(self, data: str) -> None:
        with self.lock:
            if self.current_file:
                self.current_file.write(data)
                self.current_file.flush()

    def reset(self, name: str | None = None) -> str:
        with self.lock:
            file_name = name if name else self._generate_short_id()

            if any(f["name"] == file_name for f in self.completed_files):
                raise ValueError(f"name '{file_name}' already exists")

            if self.current_file and self.current_filepath:
                self.current_file.close()
                self.completed_files.append({
                    "name": file_name,
                    "path": self.current_filepath
                })

            filepath = self._generate_filepath()
            self.current_file = open(filepath, "a")
            self.current_filepath = filepath
            return filepath

    def get_files(self) -> List[Dict[str, str]]:
        with self.lock:
            return list(self.completed_files)

    def close(self) -> None:
        with self.lock:
            if self.current_file:
                self.current_file.close()
                self.current_file = None


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

    return app
