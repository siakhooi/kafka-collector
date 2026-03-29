import argparse
import os

import pytest

from kafka_collector.args import (
    Options,
    _resolve_mode,
    _resolve_options,
    _resolve_port,
    _resolve_topics,
    _resolve_value,
    _validate_mode_options,
    _warn_ignored,
    parse_args,
)
from kafka_collector.constants import (
    DEFAULT_BOOTSTRAP_SERVER,
    DEFAULT_CAPTURE_DIR,
    DEFAULT_MODE,
    DEFAULT_PORT,
    ENV_BOOTSTRAP_SERVER,
    ENV_CAPTURE_DIR,
    ENV_GROUP,
    ENV_MODE,
    ENV_SERVICE_PORT,
    ENV_TOPICS,
    Mode,
)
from kafka_collector.exceptions import ArgumentValidationError


class TestResolveValue:
    def test_arg_value_takes_precedence(self):
        result = _resolve_value("arg", "env", "default")
        assert result == "arg"

    def test_env_value_used_when_arg_is_none(self):
        result = _resolve_value(None, "env", "default")
        assert result == "env"

    def test_default_used_when_both_none(self):
        result = _resolve_value(None, None, "default")
        assert result == "default"

    def test_default_used_when_env_is_empty(self):
        result = _resolve_value(None, "", "default")
        assert result == "default"


class TestWarnIgnored:
    def test_prints_warning_to_stderr(self, capsys):
        _warn_ignored("-o/--output", "service")
        captured = capsys.readouterr()
        assert captured.err == (
            "Warning: -o/--output will be ignored in service mode\n"
        )


class TestResolveTopics:
    def test_arg_topics_takes_precedence(self):
        result = _resolve_topics("topic1,topic2", "env_topic")
        assert result == ["topic1", "topic2"]

    def test_env_topics_used_when_arg_is_none(self):
        result = _resolve_topics(None, "topic1,topic2")
        assert result == ["topic1", "topic2"]

    def test_strips_whitespace(self):
        result = _resolve_topics(" topic1 , topic2 ", None)
        assert result == ["topic1", "topic2"]

    def test_filters_empty_topics(self):
        result = _resolve_topics("topic1,,topic2,", None)
        assert result == ["topic1", "topic2"]

    def test_raises_when_no_topics_provided(self):
        with pytest.raises(ArgumentValidationError) as exc_info:
            _resolve_topics(None, None)
        assert f"--topics or {ENV_TOPICS} must be provided" in str(
            exc_info.value
        )

    def test_raises_when_topics_string_is_empty(self):
        with pytest.raises(ArgumentValidationError) as exc_info:
            _resolve_topics("", None)
        assert f"--topics or {ENV_TOPICS} must be provided" in str(
            exc_info.value
        )

    def test_raises_when_all_topics_are_whitespace(self):
        with pytest.raises(ArgumentValidationError) as exc_info:
            _resolve_topics(" , , ", None)
        assert "--topics must contain at least one topic" in str(
            exc_info.value
        )


class TestResolveMode:
    def test_arg_mode_takes_precedence(self):
        result = _resolve_mode("service", "cli")
        assert result == Mode.SERVICE

    def test_env_mode_used_when_arg_is_none(self):
        result = _resolve_mode(None, "service")
        assert result == Mode.SERVICE

    def test_returns_default_when_both_none(self):
        result = _resolve_mode(None, None)
        assert result == DEFAULT_MODE

    def test_cli_mode(self):
        result = _resolve_mode("cli", None)
        assert result == Mode.CLI

    def test_raises_on_invalid_mode(self):
        with pytest.raises(ArgumentValidationError) as exc_info:
            _resolve_mode("invalid", None)
        assert "Invalid mode 'invalid'" in str(exc_info.value)
        assert "cli, service" in str(exc_info.value)

    def test_raises_on_invalid_env_mode(self):
        with pytest.raises(ArgumentValidationError) as exc_info:
            _resolve_mode(None, "invalid")
        assert "Invalid mode 'invalid'" in str(exc_info.value)


class TestResolvePort:
    def test_arg_port_takes_precedence(self):
        result = _resolve_port(9090, "8080")
        assert result == 9090

    def test_env_port_used_when_arg_is_none(self):
        result = _resolve_port(None, "9090")
        assert result == 9090

    def test_returns_default_when_both_none(self):
        result = _resolve_port(None, None)
        assert result == DEFAULT_PORT

    def test_returns_default_when_env_is_empty(self):
        result = _resolve_port(None, "")
        assert result == DEFAULT_PORT

    def test_raises_on_invalid_env_port(self):
        with pytest.raises(ArgumentValidationError) as exc_info:
            _resolve_port(None, "not_a_number")
        assert f"Invalid {ENV_SERVICE_PORT}" in str(exc_info.value)
        assert "Must be integer" in str(exc_info.value)


class TestResolveOptions:
    def test_resolves_all_options_from_args(self, monkeypatch):
        monkeypatch.delenv(ENV_TOPICS, raising=False)
        monkeypatch.delenv(ENV_BOOTSTRAP_SERVER, raising=False)
        monkeypatch.delenv(ENV_GROUP, raising=False)
        monkeypatch.delenv(ENV_MODE, raising=False)
        monkeypatch.delenv(ENV_CAPTURE_DIR, raising=False)
        monkeypatch.delenv(ENV_SERVICE_PORT, raising=False)

        args = argparse.Namespace(
            topics="topic1,topic2",
            bootstrap_server="kafka:9092",
            group="my-group",
            mode="service",
            output="/tmp/out.json",
            capture_dir="/tmp/captures",
            port=9090,
        )

        result = _resolve_options(args)

        assert result.topics == ["topic1", "topic2"]
        assert result.bootstrap_server == "kafka:9092"
        assert result.group_id == "my-group"
        assert result.mode == Mode.SERVICE
        assert result.output_file == "/tmp/out.json"
        assert result.capture_dir == "/tmp/captures"
        assert result.port == 9090

    def test_resolves_options_from_env(self, monkeypatch):
        monkeypatch.setenv(ENV_TOPICS, "env_topic")
        monkeypatch.setenv(ENV_BOOTSTRAP_SERVER, "env-kafka:9092")
        monkeypatch.setenv(ENV_GROUP, "env-group")
        monkeypatch.setenv(ENV_MODE, "service")
        monkeypatch.setenv(ENV_CAPTURE_DIR, "/env/captures")
        monkeypatch.setenv(ENV_SERVICE_PORT, "7070")

        args = argparse.Namespace(
            topics=None,
            bootstrap_server=None,
            group=None,
            mode=None,
            output=None,
            capture_dir=None,
            port=None,
        )

        result = _resolve_options(args)

        assert result.topics == ["env_topic"]
        assert result.bootstrap_server == "env-kafka:9092"
        assert result.group_id == "env-group"
        assert result.mode == Mode.SERVICE
        assert result.output_file == "-"
        assert result.capture_dir == "/env/captures"
        assert result.port == 7070

    def test_uses_defaults_when_no_args_or_env(self, monkeypatch):
        monkeypatch.setenv(ENV_TOPICS, "topic1")
        monkeypatch.delenv(ENV_BOOTSTRAP_SERVER, raising=False)
        monkeypatch.delenv(ENV_GROUP, raising=False)
        monkeypatch.delenv(ENV_MODE, raising=False)
        monkeypatch.delenv(ENV_CAPTURE_DIR, raising=False)
        monkeypatch.delenv(ENV_SERVICE_PORT, raising=False)

        args = argparse.Namespace(
            topics=None,
            bootstrap_server=None,
            group=None,
            mode=None,
            output=None,
            capture_dir=None,
            port=None,
        )

        result = _resolve_options(args)

        assert result.bootstrap_server == DEFAULT_BOOTSTRAP_SERVER
        assert result.mode == DEFAULT_MODE
        assert result.capture_dir == DEFAULT_CAPTURE_DIR
        assert result.port == DEFAULT_PORT
        assert result.output_file == "-"


class TestValidateModeOptions:
    def test_service_mode_warns_on_output_option(self, capsys):
        args = argparse.Namespace(
            output="/tmp/out.json",
            capture_dir=None,
            port=None,
        )
        options = Options(
            topics=["topic1"],
            mode=Mode.SERVICE,
            capture_dir="/tmp/test-captures",
        )

        _validate_mode_options(args, options)

        captured = capsys.readouterr()
        assert "-o/--output will be ignored in service mode" in captured.err

    def test_service_mode_creates_capture_dir(self, tmp_path):
        capture_dir = tmp_path / "new_captures"
        args = argparse.Namespace(output=None, capture_dir=None, port=None)
        options = Options(
            topics=["topic1"],
            mode=Mode.SERVICE,
            capture_dir=str(capture_dir),
        )

        _validate_mode_options(args, options)

        assert capture_dir.exists()

    def test_service_mode_raises_on_dir_creation_failure(self, monkeypatch):
        args = argparse.Namespace(output=None, capture_dir=None, port=None)
        options = Options(
            topics=["topic1"],
            mode=Mode.SERVICE,
            capture_dir="/nonexistent/path/that/cannot/be/created",
        )

        def mock_makedirs(path, exist_ok=False):
            raise OSError("Permission denied")

        monkeypatch.setattr(os, "makedirs", mock_makedirs)

        with pytest.raises(ArgumentValidationError) as exc_info:
            _validate_mode_options(args, options)
        assert "Failed to create capture directory" in str(exc_info.value)

    def test_cli_mode_warns_on_capture_dir_option(self, capsys):
        args = argparse.Namespace(
            output=None,
            capture_dir="/tmp/captures",
            port=None,
        )
        options = Options(topics=["topic1"], mode=Mode.CLI)

        _validate_mode_options(args, options)

        captured = capsys.readouterr()
        assert "-c/--capture-dir will be ignored in cli mode" in captured.err

    def test_cli_mode_warns_on_port_option(self, capsys):
        args = argparse.Namespace(
            output=None,
            capture_dir=None,
            port=9090,
        )
        options = Options(topics=["topic1"], mode=Mode.CLI)

        _validate_mode_options(args, options)

        captured = capsys.readouterr()
        assert "-p/--port will be ignored in cli mode" in captured.err

    def test_cli_mode_no_warnings_when_no_service_options(self, capsys):
        args = argparse.Namespace(output=None, capture_dir=None, port=None)
        options = Options(topics=["topic1"], mode=Mode.CLI)

        _validate_mode_options(args, options)

        captured = capsys.readouterr()
        assert captured.err == ""


class TestParseArgs:
    def test_parses_all_args(self, monkeypatch, tmp_path):
        capture_dir = tmp_path / "captures"
        monkeypatch.setattr(
            "sys.argv",
            [
                "kafka-collector",
                "-t", "topic1,topic2",
                "-b", "kafka:9092",
                "-g", "my-group",
                "-m", "service",
                "-c", str(capture_dir),
                "-p", "9090",
            ],
        )
        monkeypatch.delenv(ENV_TOPICS, raising=False)

        result = parse_args()

        assert result.topics == ["topic1", "topic2"]
        assert result.bootstrap_server == "kafka:9092"
        assert result.group_id == "my-group"
        assert result.mode == Mode.SERVICE
        assert result.capture_dir == str(capture_dir)
        assert result.port == 9090

    def test_uses_env_vars_when_no_args(self, monkeypatch, tmp_path):
        capture_dir = tmp_path / "env_captures"
        monkeypatch.setattr("sys.argv", ["kafka-collector"])
        monkeypatch.setenv(ENV_TOPICS, "env_topic")
        monkeypatch.setenv(ENV_BOOTSTRAP_SERVER, "env-kafka:9092")
        monkeypatch.setenv(ENV_MODE, "service")
        monkeypatch.setenv(ENV_CAPTURE_DIR, str(capture_dir))

        result = parse_args()

        assert result.topics == ["env_topic"]
        assert result.bootstrap_server == "env-kafka:9092"
        assert result.mode == Mode.SERVICE

    def test_raises_when_no_topics(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["kafka-collector"])
        monkeypatch.delenv(ENV_TOPICS, raising=False)

        with pytest.raises(ArgumentValidationError) as exc_info:
            parse_args()
        assert f"--topics or {ENV_TOPICS} must be provided" in str(
            exc_info.value
        )
