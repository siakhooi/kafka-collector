import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from kafka_collector.constants import (
    CAPTURE_FILENAME_EXTENSION,
    CAPTURE_FILENAME_PREFIX,
)
from kafka_collector.exceptions import (
    CaptureNameNotFoundError,
    DuplicateCaptureNameError,
    EmptyCaptureNameError,
    NoCompletedCapturesError,
)
from kafka_collector.file_manager import FileManager


@pytest.fixture
def capture_dir(tmp_path):
    d = tmp_path / "capture"
    d.mkdir()
    return d


@pytest.fixture
def fm_open(capture_dir):
    fm = FileManager(str(capture_dir))
    fm.open_new_file()
    return fm


def test_open_new_file_path_under_capture_dir_and_pattern(capture_dir):
    fm = FileManager(str(capture_dir))
    path = fm.open_new_file()
    assert path.startswith(str(capture_dir))
    name = Path(path).name
    assert name.startswith(CAPTURE_FILENAME_PREFIX)
    assert name.endswith(CAPTURE_FILENAME_EXTENSION)
    assert (capture_dir / name).is_file()


def test_write_persists_and_flush(fm_open):
    fm_open.write("line1\n")
    p = fm_open.current_filepath
    assert p is not None
    assert Path(p).read_text(encoding="utf-8") == "line1\n"


def test_write_without_open_file_is_noop(capture_dir):
    fm = FileManager(str(capture_dir))
    fm.write("x")
    assert fm.current_file is None


def test_reset_none_uses_eight_hex_id(fm_open):
    fm_open.reset(None)
    files = fm_open.get_files()
    assert len(files) == 1
    assert re.fullmatch(r"[0-9a-f]{8}", files[0]["name"])


def test_reset_strips_name(fm_open):
    fm_open.reset("  seg  ")
    assert fm_open.get_files()[0]["name"] == "seg"


def test_reset_empty_string_raises(fm_open):
    with pytest.raises(EmptyCaptureNameError, match="name must not be empty"):
        fm_open.reset("")


def test_reset_whitespace_only_raises(fm_open):
    with pytest.raises(EmptyCaptureNameError, match="name must not be empty"):
        fm_open.reset("   \t")


def test_reset_duplicate_name_raises(fm_open):
    fm_open.reset("one")
    with pytest.raises(
        DuplicateCaptureNameError,
        match="name 'one' already exists",
    ):
        fm_open.reset("one")


def test_reset_records_completed_path_and_completed_at(fm_open, monkeypatch):
    fixed = datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(FileManager, "_utc_now", staticmethod(lambda: fixed))

    first_path = fm_open.current_filepath
    fm_open.reset("seg")

    files = fm_open.get_files()
    assert len(files) == 1
    assert files[0]["name"] == "seg"
    assert files[0]["path"] == first_path
    assert files[0]["completed_at"] == fixed.isoformat()


def test_reset_without_open_still_starts_capture(capture_dir):
    fm = FileManager(str(capture_dir))
    path = fm.reset(None)
    assert path.startswith(str(capture_dir))
    assert Path(path).is_file()
    assert fm.get_files() == []


def test_get_files_returns_new_list_each_call(fm_open):
    fm_open.reset("a")
    a = fm_open.get_files()
    b = fm_open.get_files()
    assert a == b
    assert a is not b
    a.append(
        {
            "name": "ghost",
            "path": "/nope",
            "completed_at": "x",
        }
    )
    assert len(fm_open.get_files()) == 1


def test_get_file_by_name_returns_path(fm_open):
    fm_open.reset("x")
    path = fm_open.get_file_by_name("x")
    assert path.endswith(CAPTURE_FILENAME_EXTENSION)
    assert Path(path).is_file()


def test_get_file_by_name_missing_raises(fm_open):
    fm_open.reset("only")
    with pytest.raises(
        CaptureNameNotFoundError,
        match="name 'nope' not found",
    ):
        fm_open.get_file_by_name("nope")


def test_get_last_completed_empty_raises(capture_dir):
    fm = FileManager(str(capture_dir))
    fm.open_new_file()
    with pytest.raises(NoCompletedCapturesError, match="no completed files"):
        fm.get_last_completed_file()


def test_get_last_completed_is_most_recent(fm_open):
    fm_open.reset("first")
    fm_open.reset("second")
    name, path = fm_open.get_last_completed_file()
    assert name == "second"
    assert path.endswith(CAPTURE_FILENAME_EXTENSION)


def test_close_clears_handle_and_filepath(fm_open):
    fm_open.write("z")
    fm_open.close()
    assert fm_open.current_file is None
    assert fm_open.current_filepath is None
    fm_open.write("more")
    assert fm_open.current_file is None


def test_completed_at_is_parseable_utc_iso(fm_open):
    fm_open.reset("t")
    raw = fm_open.get_files()[0]["completed_at"]
    parsed = datetime.fromisoformat(raw)
    assert parsed.tzinfo is not None
