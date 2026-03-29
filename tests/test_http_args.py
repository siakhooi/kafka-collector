import pytest

from kafka_collector.constants import (
    DEFAULT_DOWNLOAD_KIND,
    DOWNLOAD_KIND_JSONL,
    DOWNLOAD_KIND_ZIP,
)
from kafka_collector.http_args import (
    MAX_CAPTURE_NAME_LEN,
    parse_name_args,
    parse_name_value,
    parse_type_args,
)


def test_parse_name_value_none():
    assert parse_name_value(None) == (None, None)


def test_parse_name_value_valid():
    assert parse_name_value("segment1") == (None, "segment1")


def test_parse_name_value_strips():
    assert parse_name_value("  x  ") == (None, "x")


def test_parse_name_value_empty_string():
    err, name = parse_name_value("")
    assert err == "name must not be empty"
    assert name is None


def test_parse_name_value_whitespace_only():
    err, name = parse_name_value("  \t  ")
    assert err == "name must not be empty"
    assert name is None


def test_parse_name_value_too_long():
    s = "a" * (MAX_CAPTURE_NAME_LEN + 1)
    err, name = parse_name_value(s)
    assert err == f"name exceeds maximum length ({MAX_CAPTURE_NAME_LEN})"
    assert name is None


def test_parse_name_value_invalid_characters():
    err, name = parse_name_value("bad space")
    assert err == "name contains invalid characters"
    assert name is None


def test_parse_name_args_absent():
    assert parse_name_args([]) == (None, None)


def test_parse_name_args_valid():
    assert parse_name_args(["segment1"]) == (None, "segment1")


def test_parse_name_args_strips():
    assert parse_name_args(["  x  "]) == (None, "x")


def test_parse_name_args_duplicate_keys():
    err, name = parse_name_args(["a", "b"])
    assert err == "duplicate name parameter"
    assert name is None


def test_parse_name_args_empty_string():
    err, name = parse_name_args([""])
    assert err == "name must not be empty"
    assert name is None


def test_parse_name_args_whitespace_only():
    err, name = parse_name_args(["  \t  "])
    assert err == "name must not be empty"
    assert name is None


def test_parse_name_args_too_long():
    s = "a" * (MAX_CAPTURE_NAME_LEN + 1)
    err, name = parse_name_args([s])
    assert err == f"name exceeds maximum length ({MAX_CAPTURE_NAME_LEN})"
    assert name is None


def test_parse_name_args_max_length_ok():
    s = "a" * MAX_CAPTURE_NAME_LEN
    assert parse_name_args([s]) == (None, s)


def test_parse_name_args_invalid_characters():
    err, name = parse_name_args(["bad space"])
    assert err == "name contains invalid characters"
    assert name is None


def test_parse_name_args_allowed_punctuation():
    assert parse_name_args(["a_b.c-1"]) == (None, "a_b.c-1")


def test_parse_type_args_absent_defaults_jsonl():
    assert parse_type_args([]) == (None, DEFAULT_DOWNLOAD_KIND)
    assert DEFAULT_DOWNLOAD_KIND == DOWNLOAD_KIND_JSONL


@pytest.mark.parametrize(
    "raw,expected_kind",
    [
        (["jsonl"], DOWNLOAD_KIND_JSONL),
        (["JSONL"], DOWNLOAD_KIND_JSONL),
        (["zip"], DOWNLOAD_KIND_ZIP),
        (["  ZIP  "], DOWNLOAD_KIND_ZIP),
    ],
)
def test_parse_type_args_valid(raw, expected_kind):
    assert parse_type_args(raw) == (None, expected_kind)


def test_parse_type_args_duplicate_keys():
    err, kind = parse_type_args(["zip", "jsonl"])
    assert err == "duplicate type parameter"
    assert kind == DEFAULT_DOWNLOAD_KIND


def test_parse_type_args_invalid_kind():
    err, kind = parse_type_args(["exe"])
    assert err == "type must be jsonl or zip"
    assert kind == DEFAULT_DOWNLOAD_KIND


def test_parse_type_args_empty_string_invalid():
    err, kind = parse_type_args([""])
    assert err == "type must be jsonl or zip"
    assert kind == DEFAULT_DOWNLOAD_KIND
