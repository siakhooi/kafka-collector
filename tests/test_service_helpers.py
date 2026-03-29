import io
import logging
import zipfile
from pathlib import Path

import pytest
from flask import Flask

from kafka_collector.constants import (
    API_ERROR_CAPTURE_FILE_MISSING,
    API_JSON_ERROR_KEY,
    DOWNLOAD_KIND_JSONL,
    DOWNLOAD_KIND_ZIP,
    MIME_TYPE_JSONL,
    MIME_TYPE_ZIP,
)
from kafka_collector.exceptions import (
    CaptureNameNotFoundError,
    NoCompletedCapturesError,
)
from kafka_collector.file_manager import FileManager
from kafka_collector.service_helpers import (
    json_error,
    resolve_download_target,
    response_if_capture_missing,
    send_capture_file,
)


@pytest.fixture
def flask_app():
    return Flask(__name__)


@pytest.fixture
def app_ctx(flask_app):
    with flask_app.app_context():
        yield flask_app


@pytest.fixture
def app_req_ctx(flask_app):
    with flask_app.app_context():
        with flask_app.test_request_context():
            yield flask_app


def test_json_error(app_ctx):
    resp, status = json_error("something failed", 400)
    assert status == 400
    assert resp.get_json()[API_JSON_ERROR_KEY] == "something failed"


@pytest.fixture
def file_manager_with_completed(tmp_path):
    cap = tmp_path / "capture"
    cap.mkdir()
    fm = FileManager(str(cap))
    fm.open_new_file()
    fm.reset("segment_a")
    return fm


def test_resolve_with_name(file_manager_with_completed):
    fm = file_manager_with_completed
    dn, path = resolve_download_target(fm, "segment_a")
    assert dn == "segment_a"
    assert path.endswith(".jsonl")
    assert Path(path).is_file()


def test_resolve_last_completed_when_name_none(file_manager_with_completed):
    fm = file_manager_with_completed
    fm.reset("segment_b")
    dn, path = resolve_download_target(fm, None)
    assert dn == "segment_b"
    assert Path(path).is_file()


def test_resolve_download_target_unknown_name_raises(tmp_path):
    cap = tmp_path / "c"
    cap.mkdir()
    fm = FileManager(str(cap))
    fm.open_new_file()
    fm.reset("only")
    with pytest.raises(
        CaptureNameNotFoundError,
        match="name 'nope' not found",
    ):
        resolve_download_target(fm, "nope")


def test_resolve_download_target_none_raises_when_no_completed(tmp_path):
    cap = tmp_path / "c"
    cap.mkdir()
    fm = FileManager(str(cap))
    fm.open_new_file()
    with pytest.raises(NoCompletedCapturesError, match="no completed files"):
        resolve_download_target(fm, None)


def test_response_if_capture_missing_returns_none_when_exists(tmp_path):
    p = tmp_path / "exists.jsonl"
    p.write_text("data", encoding="utf-8")
    assert response_if_capture_missing(str(p)) is None


def test_response_missing_file_404_and_logs(app_ctx, caplog, tmp_path):
    caplog.set_level(logging.WARNING, logger="kafka_collector.service_helpers")
    gone = str(tmp_path / "missing.jsonl")
    out = response_if_capture_missing(gone)
    assert out is not None
    resp, status = out
    assert status == 404
    body = resp.get_json()[API_JSON_ERROR_KEY]
    assert body == API_ERROR_CAPTURE_FILE_MISSING
    assert gone in caplog.text


def test_send_capture_file_jsonl(app_req_ctx, tmp_path):
    p = tmp_path / "capture.jsonl"
    p.write_text('{"x":1}\n', encoding="utf-8")
    resp = send_capture_file(str(p), "logical", DOWNLOAD_KIND_JSONL)
    assert resp.status_code == 200
    assert resp.mimetype == MIME_TYPE_JSONL
    raw = b"".join(resp.iter_encoded())
    assert b'{"x":1}' in raw


def test_send_capture_file_zip(app_req_ctx, tmp_path):
    p = tmp_path / "inside.jsonl"
    p.write_text("line\n", encoding="utf-8")
    resp = send_capture_file(str(p), "bundle", DOWNLOAD_KIND_ZIP)
    assert resp.status_code == 200
    assert resp.mimetype == MIME_TYPE_ZIP
    raw = b"".join(resp.iter_encoded())
    buf = io.BytesIO(raw)
    with zipfile.ZipFile(buf, "r") as zf:
        names = zf.namelist()
        assert len(names) == 1
        assert zf.read(names[0]).decode() == "line\n"
