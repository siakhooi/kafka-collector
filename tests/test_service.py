import logging
from pathlib import Path

import pytest

from kafka_collector.constants import (
    API_ERROR_CAPTURE_FILE_MISSING,
    API_JSON_ERROR_KEY,
)
from kafka_collector.file_manager import FileManager
from kafka_collector.service import create_app


@pytest.fixture
def app(tmp_path):
    capture = tmp_path / "cap"
    capture.mkdir()
    fm = FileManager(str(capture))
    fm.open_new_file()
    return create_app(fm)


@pytest.fixture
def client(app):
    return app.test_client()


def test_reset_without_name_ok(client):
    r = client.post("/reset", json={})
    assert r.status_code == 200
    assert r.get_json()["status"] == "ok"


def test_reset_with_valid_name_ok(client):
    r = client.post("/reset", json={"name": "segment1"})
    assert r.status_code == 200


def test_reset_rejects_empty_name(client):
    r = client.post("/reset", json={"name": ""})
    assert r.status_code == 400
    assert r.get_json()[API_JSON_ERROR_KEY] == "name must not be empty"


def test_reset_rejects_whitespace_name(client):
    r = client.post("/reset", json={"name": "  "})
    assert r.status_code == 400
    assert r.get_json()[API_JSON_ERROR_KEY] == "name must not be empty"


def test_reset_rejects_invalid_name_characters(client):
    r = client.post("/reset", json={"name": "bad space"})
    assert r.status_code == 400
    err = r.get_json()[API_JSON_ERROR_KEY]
    assert err == "name contains invalid characters"


def test_reset_duplicate_logical_name_400(client):
    client.post("/reset", json={"name": "same"})
    r = client.post("/reset", json={"name": "same"})
    assert r.status_code == 400
    assert "already exists" in r.get_json()[API_JSON_ERROR_KEY]


def test_get_files_returns_empty_list_initially(client):
    r = client.get("/files")
    assert r.status_code == 200
    assert r.get_json() == []


def test_get_files_returns_completed_captures(client):
    client.post("/reset", json={"name": "seg1"})
    client.post("/reset", json={"name": "seg2"})
    r = client.get("/files")
    assert r.status_code == 200
    files = r.get_json()
    assert len(files) == 2
    names = [f["name"] for f in files]
    assert "seg1" in names
    assert "seg2" in names


def test_download_rejects_duplicate_name_parameter(client):
    r = client.get("/download?name=a&name=b")
    assert r.status_code == 400
    assert r.get_json()[API_JSON_ERROR_KEY] == "duplicate name parameter"


def test_download_no_completed_files_400(client):
    r = client.get("/download")
    assert r.status_code == 400
    assert r.get_json()[API_JSON_ERROR_KEY] == "no completed files"


def test_download_last_completed_ok(client):
    client.post("/reset", json={"name": "seg"})
    r = client.get("/download")
    assert r.status_code == 200


def test_download_unknown_name_404(client):
    client.post("/reset", json={"name": "known"})
    r = client.get("/download?name=missing")
    assert r.status_code == 404
    assert "not found" in r.get_json()[API_JSON_ERROR_KEY]


def test_download_missing_file_on_disk_404(client, caplog):
    client.post("/reset", json={"name": "seg"})
    path = client.get("/files").get_json()[0]["path"]
    Path(path).unlink()
    caplog.set_level(logging.WARNING, logger="kafka_collector.service_helpers")
    r = client.get("/download?name=seg")
    assert r.status_code == 404
    assert r.get_json()[API_JSON_ERROR_KEY] == API_ERROR_CAPTURE_FILE_MISSING
    assert path in caplog.text


def test_download_invalid_type_400(client):
    client.post("/reset", json={"name": "x"})
    r = client.get("/download?name=x&type=exe")
    assert r.status_code == 400
    assert r.get_json()[API_JSON_ERROR_KEY] == "type must be jsonl or zip"


def test_download_duplicate_type_parameter_400(client):
    client.post("/reset", json={"name": "x"})
    r = client.get("/download?name=x&type=zip&type=jsonl")
    assert r.status_code == 400
    assert r.get_json()[API_JSON_ERROR_KEY] == "duplicate type parameter"


def test_download_type_zip_ok(client):
    client.post("/reset", json={"name": "z"})
    r = client.get("/download?name=z&type=zip")
    assert r.status_code == 200
    assert r.headers.get("Content-Type", "").startswith("application/zip")
