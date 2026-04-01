import os
import threading
import uuid
from datetime import datetime, timezone
from typing import TextIO

from kafka_collector.logging_config import get_logger
from kafka_collector.constants import (
    CAPTURE_FILENAME_EXTENSION,
    CAPTURE_FILENAME_PREFIX,
    CAPTURE_FILENAME_TIMESTAMP_FORMAT,
)
from kafka_collector.exceptions import (
    CaptureNameNotFoundError,
    DuplicateCaptureNameError,
    EmptyCaptureNameError,
    NoCompletedCapturesError,
)
from kafka_collector.models import CompletedCapture

logger = get_logger(__name__)


class FileManager:
    def __init__(self, capture_dir: str):
        self.capture_dir = capture_dir
        self.current_file: TextIO | None = None
        self.current_filepath: str | None = None
        self.completed_files: list[CompletedCapture] = []
        self.lock = threading.Lock()

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)

    def _generate_filepath(self) -> str:
        timestamp = self._utc_now().strftime(
            CAPTURE_FILENAME_TIMESTAMP_FORMAT
        )
        filename = (
            f"{CAPTURE_FILENAME_PREFIX}{timestamp}{CAPTURE_FILENAME_EXTENSION}"
        )
        return os.path.join(self.capture_dir, filename)

    def _completed_entry(self, file_name: str, path: str) -> CompletedCapture:
        return {
            "name": file_name,
            "path": path,
            "completed_at": self._utc_now().isoformat(),
        }

    def _generate_short_id(self) -> str:
        return uuid.uuid4().hex[:8]

    def _start_new_capture_unlocked(self) -> str:
        # Caller must hold self.lock.
        filepath = self._generate_filepath()
        self.current_file = open(filepath, "a")
        self.current_filepath = filepath
        logger.debug("Started new capture file: %s", filepath)
        return filepath

    def _finalize_current_capture_unlocked(self, file_name: str) -> None:
        # Caller must hold self.lock.
        if self.current_file and self.current_filepath:
            self.current_file.close()
            self.completed_files.append(
                self._completed_entry(file_name, self.current_filepath)
            )
            logger.info(
                "Finalized capture '%s' at %s",
                file_name, self.current_filepath
            )

    def open_new_file(self) -> str:
        with self.lock:
            return self._start_new_capture_unlocked()

    def write(self, data: str) -> None:
        with self.lock:
            if self.current_file:
                self.current_file.write(data)
                self.current_file.flush()

    def reset(self, name: str | None = None) -> str:
        logger.debug("Reset requested with name=%s", name)
        with self.lock:
            if name is None:
                file_name = self._generate_short_id()
            else:
                stripped = name.strip()
                if not stripped:
                    raise EmptyCaptureNameError("name must not be empty")
                file_name = stripped

            if any(f["name"] == file_name for f in self.completed_files):
                raise DuplicateCaptureNameError(
                    f"name '{file_name}' already exists"
                )

            self._finalize_current_capture_unlocked(file_name)

            return self._start_new_capture_unlocked()

    def get_files(self) -> list[CompletedCapture]:
        with self.lock:
            return list(self.completed_files)

    def get_file_by_name(self, name: str) -> str:
        with self.lock:
            for f in self.completed_files:
                if f["name"] == name:
                    return f["path"]
            raise CaptureNameNotFoundError(f"name '{name}' not found")

    def get_last_completed_file(self) -> tuple[str, str]:
        with self.lock:
            if not self.completed_files:
                raise NoCompletedCapturesError("no completed files")
            last = self.completed_files[-1]
            return last["name"], last["path"]

    def close(self) -> None:
        with self.lock:
            if self.current_file:
                self.current_file.close()
                logger.debug("Closed current capture file")
            self.current_file = None
            self.current_filepath = None
