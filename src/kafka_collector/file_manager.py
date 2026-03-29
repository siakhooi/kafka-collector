import os
import threading
import uuid
from datetime import datetime
from typing import IO, List, Dict, Tuple


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

    def _start_new_capture_unlocked(self) -> str:
        # Caller must hold self.lock.
        filepath = self._generate_filepath()
        self.current_file = open(filepath, "a")
        self.current_filepath = filepath
        return filepath

    def open_new_file(self) -> str:
        with self.lock:
            return self._start_new_capture_unlocked()

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

            return self._start_new_capture_unlocked()

    def get_files(self) -> List[Dict[str, str]]:
        with self.lock:
            return list(self.completed_files)

    def get_file_by_name(self, name: str) -> str:
        with self.lock:
            for f in self.completed_files:
                if f["name"] == name:
                    return f["path"]
            raise ValueError(f"name '{name}' not found")

    def get_last_completed_file(self) -> Tuple[str, str]:
        with self.lock:
            if not self.completed_files:
                raise ValueError("no completed files")
            last = self.completed_files[-1]
            return last["name"], last["path"]

    def close(self) -> None:
        with self.lock:
            if self.current_file:
                self.current_file.close()
            self.current_file = None
            self.current_filepath = None
