import json
import sys
from unittest.mock import MagicMock, patch

import pytest

from kafka_collector.args import Options
from kafka_collector.cli import (
    _create_consumer,
    _format_message,
    _graceful_shutdown,
    _open_output,
    print_to_stderr_and_exit,
    run,
    run_cli_mode,
    run_service_mode,
)


@pytest.mark.skipif(sys.version_info < (3, 13), reason="Python 3.13+ only")
@pytest.mark.parametrize("option_help", ["-h", "--help"])
def test_run_help(monkeypatch, capsys, option_help):
    monkeypatch.setenv("COLUMNS", "80")
    monkeypatch.setattr(
        "sys.argv",
        ["kafka-collector", option_help],
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 0

    with open("tests/expected-output/cli-help.txt", "r") as f:
        expected_output = f.read()

    captured = capsys.readouterr()
    assert captured.out == expected_output


@pytest.mark.skipif(sys.version_info >= (3, 13), reason="Python <3.13 only")
@pytest.mark.parametrize("option_help", ["-h", "--help"])
def test_run_help312(monkeypatch, capsys, option_help):
    monkeypatch.setenv("COLUMNS", "80")
    monkeypatch.setattr(
        "sys.argv",
        ["kafka-collector", option_help],
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 0

    with open("tests/expected-output/cli-help312.txt", "r") as f:
        expected_output = f.read()

    captured = capsys.readouterr()
    assert captured.out == expected_output


@pytest.mark.parametrize("option_version", ["-v", "--version"])
def test_run_show_version(monkeypatch, capsys, option_version):
    monkeypatch.setattr(
        "sys.argv",
        ["kafka-collector", option_version],
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 0

    with open("tests/expected-output/cli-version.txt", "r") as f:
        expected_output = f.read()

    captured = capsys.readouterr()
    assert captured.out == expected_output


@pytest.mark.parametrize("options", [["-q"], ["-q", "-j"]])
def test_run_wrong_options(monkeypatch, capsys, options):
    monkeypatch.setattr(
        "sys.argv",
        ["kafka-collector", "-t", "xxx"] + options,
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 2

    captured = capsys.readouterr()
    assert "usage: kafka-collector [-h] [-v]" in captured.err
    assert "kafka-collector: error: unrecognized arguments:" in captured.err


class TestPrintToStderrAndExit:
    def test_prints_error_and_exits(self, capsys):
        with pytest.raises(SystemExit):
            print_to_stderr_and_exit(Exception("test error"), 1)

        captured = capsys.readouterr()
        assert "Error: test error" in captured.err

    def test_exits_with_correct_code(self):
        with pytest.raises(SystemExit) as exc_info:
            print_to_stderr_and_exit(Exception("test"), 42)
        assert exc_info.value.code == 42


class TestFormatMessage:
    def test_formats_message_with_all_fields(self):
        message = MagicMock()
        message.topic = "test-topic"
        message.timestamp = 1234567890
        message.headers = [("key1", b"value1")]
        message.value = b"test value"
        message.key = b"test key"

        result = _format_message(message)

        assert result["topic"] == "test-topic"
        assert result["timestamp"] == 1234567890
        assert result["header"] == {"key1": b"value1"}
        assert result["value"] == "test value"
        assert result["key"] == "test key"

    def test_formats_message_with_none_value(self):
        message = MagicMock()
        message.topic = "test-topic"
        message.timestamp = 1234567890
        message.headers = None
        message.value = None
        message.key = None

        result = _format_message(message)

        assert result["header"] == {}
        assert result["value"] is None
        assert result["key"] is None

    def test_formats_message_with_empty_headers(self):
        message = MagicMock()
        message.topic = "test-topic"
        message.timestamp = 1234567890
        message.headers = []
        message.value = b"value"
        message.key = None

        result = _format_message(message)

        assert result["header"] == {}


class TestOpenOutput:
    def test_yields_stdout_for_dash(self):
        with _open_output("-") as out:
            assert out is sys.stdout

    def test_yields_file_for_path(self, tmp_path):
        file_path = tmp_path / "output.txt"

        with _open_output(str(file_path)) as out:
            out.write("test content")

        assert file_path.read_text() == "test content"

    def test_closes_file_after_context(self, tmp_path):
        file_path = tmp_path / "output.txt"

        with _open_output(str(file_path)) as out:
            file_handle = out

        assert file_handle.closed

    def test_appends_to_existing_file(self, tmp_path):
        file_path = tmp_path / "output.txt"
        file_path.write_text("existing\n")

        with _open_output(str(file_path)) as out:
            out.write("new content")

        assert file_path.read_text() == "existing\nnew content"


class TestGracefulShutdown:
    def test_yields_shutdown_event(self):
        with _graceful_shutdown() as shutdown_event:
            assert not shutdown_event.is_set()

    def test_signal_handler_sets_event(self):
        import signal
        with _graceful_shutdown() as shutdown_event:
            assert not shutdown_event.is_set()
            signal.raise_signal(signal.SIGINT)
            assert shutdown_event.is_set()


class TestCreateConsumer:
    @patch("kafka_collector.cli.KafkaConsumer")
    def test_creates_consumer_with_options(self, mock_consumer_class):
        mock_consumer = MagicMock()
        mock_consumer.assignment.return_value = ["partition"]
        mock_consumer_class.return_value = mock_consumer

        options = Options(
            topics=["topic1", "topic2"],
            bootstrap_server="localhost:9092",
            group_id="test-group",
        )

        result = _create_consumer(options)

        mock_consumer_class.assert_called_once()
        call_args = mock_consumer_class.call_args
        assert "topic1" in call_args[0]
        assert "topic2" in call_args[0]
        assert call_args[1]["bootstrap_servers"] == "localhost:9092"
        assert call_args[1]["group_id"] == "test-group"
        assert result is mock_consumer

    @patch("kafka_collector.cli.KafkaConsumer")
    def test_polls_until_assignment(self, mock_consumer_class):
        mock_consumer = MagicMock()
        mock_consumer.assignment.side_effect = [[], [], ["partition"]]
        mock_consumer_class.return_value = mock_consumer

        options = Options(
            topics=["topic1"],
            bootstrap_server="localhost:9092",
            group_id="test-group",
        )

        _create_consumer(options)

        assert mock_consumer.poll.call_count == 2


class TestRunCliMode:
    def test_writes_messages_to_stdout(self, capsys):
        mock_consumer = MagicMock()
        message1 = MagicMock()
        message1.topic = "topic1"
        message1.timestamp = 123
        message1.headers = None
        message1.value = b"value1"
        message1.key = None

        mock_consumer.__iter__ = MagicMock(return_value=iter([message1]))

        run_cli_mode(mock_consumer, "-")

        captured = capsys.readouterr()
        output = json.loads(captured.out.strip())
        assert output["topic"] == "topic1"
        assert output["value"] == "value1"

    def test_writes_messages_to_file(self, tmp_path):
        mock_consumer = MagicMock()
        message1 = MagicMock()
        message1.topic = "topic1"
        message1.timestamp = 123
        message1.headers = None
        message1.value = b"value1"
        message1.key = None

        mock_consumer.__iter__ = MagicMock(return_value=iter([message1]))

        output_file = tmp_path / "output.jsonl"
        run_cli_mode(mock_consumer, str(output_file))

        content = output_file.read_text().strip()
        output = json.loads(content)
        assert output["topic"] == "topic1"

    def test_stops_on_shutdown_event(self, capsys):
        import signal

        mock_consumer = MagicMock()
        messages_returned = []

        def message_generator():
            msg1 = MagicMock()
            msg1.topic = "topic1"
            msg1.timestamp = 123
            msg1.headers = None
            msg1.value = b"value1"
            msg1.key = None
            messages_returned.append(msg1)
            yield msg1
            signal.raise_signal(signal.SIGINT)
            msg2 = MagicMock()
            msg2.topic = "topic2"
            msg2.timestamp = 456
            msg2.headers = None
            msg2.value = b"value2"
            msg2.key = None
            messages_returned.append(msg2)
            yield msg2

        mock_consumer.__iter__ = MagicMock(return_value=message_generator())

        run_cli_mode(mock_consumer, "-")

        captured = capsys.readouterr()
        lines = [line for line in captured.out.strip().split('\n') if line]
        assert len(lines) == 1
        assert json.loads(lines[0])["topic"] == "topic1"


class TestRunServiceMode:
    @patch("kafka_collector.cli.create_app")
    @patch("kafka_collector.cli.FileManager")
    def test_starts_flask_app(self, mock_fm_class, mock_create_app, tmp_path):
        mock_consumer = MagicMock()
        mock_consumer.__iter__ = MagicMock(return_value=iter([]))

        mock_file_manager = MagicMock()
        mock_file_manager.__enter__ = MagicMock(return_value=mock_file_manager)
        mock_file_manager.__exit__ = MagicMock(return_value=None)
        mock_fm_class.return_value = mock_file_manager

        mock_app = MagicMock()
        mock_create_app.return_value = mock_app

        capture_dir = tmp_path / "captures"

        run_service_mode(mock_consumer, str(capture_dir), 8080)

        mock_fm_class.assert_called_once_with(str(capture_dir))
        mock_file_manager.__enter__.assert_called_once()
        mock_create_app.assert_called_once_with(mock_file_manager)
        mock_app.run.assert_called_once()

    @patch("kafka_collector.cli.create_app")
    @patch("kafka_collector.cli.FileManager")
    def test_consumes_messages_in_background(
        self, mock_fm_class, mock_create_app, tmp_path
    ):
        import time

        mock_consumer = MagicMock()
        message1 = MagicMock()
        message1.topic = "topic1"
        message1.timestamp = 123
        message1.headers = None
        message1.value = b"value1"
        message1.key = None

        mock_consumer.__iter__ = MagicMock(return_value=iter([message1]))

        mock_file_manager = MagicMock()
        mock_file_manager.__enter__ = MagicMock(return_value=mock_file_manager)
        mock_file_manager.__exit__ = MagicMock(return_value=None)
        mock_fm_class.return_value = mock_file_manager

        mock_app = MagicMock()

        def run_and_wait(*args, **kwargs):
            time.sleep(0.1)

        mock_app.run = run_and_wait
        mock_create_app.return_value = mock_app

        capture_dir = tmp_path / "captures"

        run_service_mode(mock_consumer, str(capture_dir), 8080)

        assert mock_file_manager.write.call_count >= 1
        written = mock_file_manager.write.call_args[0][0]
        assert "topic1" in written


class TestRunIntegration:
    @patch("kafka_collector.cli._create_consumer")
    @patch("kafka_collector.cli.run_cli_mode")
    def test_runs_cli_mode(
        self, mock_run_cli, mock_create_consumer, monkeypatch
    ):
        monkeypatch.setattr(
            "sys.argv",
            ["kafka-collector", "-t", "topic1"],
        )

        mock_consumer = MagicMock()
        mock_create_consumer.return_value = mock_consumer

        run()

        mock_run_cli.assert_called_once()

    @patch("kafka_collector.cli._create_consumer")
    @patch("kafka_collector.cli.run_service_mode")
    def test_runs_service_mode(
        self, mock_run_service, mock_create_consumer, monkeypatch, tmp_path
    ):
        capture_dir = tmp_path / "captures"
        monkeypatch.setattr(
            "sys.argv",
            [
                "kafka-collector",
                "-t", "topic1",
                "-m", "service",
                "-c", str(capture_dir),
            ],
        )

        mock_consumer = MagicMock()
        mock_create_consumer.return_value = mock_consumer

        run()

        mock_run_service.assert_called_once()

    def test_handles_argument_validation_error(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["kafka-collector"])

        with pytest.raises(SystemExit) as exc_info:
            run()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Error:" in captured.err

    @patch("kafka_collector.cli._create_consumer")
    def test_handles_consumer_exception(
        self, mock_create_consumer, monkeypatch, capsys
    ):
        monkeypatch.setattr(
            "sys.argv",
            ["kafka-collector", "-t", "topic1"],
        )

        mock_create_consumer.side_effect = Exception("Connection failed")

        with pytest.raises(SystemExit) as exc_info:
            run()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Error: Connection failed" in captured.err
