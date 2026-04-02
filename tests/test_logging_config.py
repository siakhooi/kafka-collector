import logging
from unittest.mock import patch

import pytest

from kafka_collector.logging_config import (
    LOG_FORMAT,
    LOGGERS_TO_SUPPRESS,
    VALID_LOG_LEVELS,
    _validate_log_level,
    get_logger,
    setup_logging,
)


class TestValidateLogLevel:
    def test_valid_log_levels(self):
        """Test that valid log levels are returned unchanged."""
        for level in VALID_LOG_LEVELS:
            assert _validate_log_level(level) == level

    def test_invalid_log_level_prints_warning_and_returns_info(self, capsys):
        """Test that invalid log levels print warning and return INFO."""
        result = _validate_log_level("INVALID")
        assert result == "INFO"

        captured = capsys.readouterr()
        assert "Warning: Invalid log level 'INVALID'" in captured.err
        assert ("Valid levels: CRITICAL, DEBUG, ERROR, INFO, WARNING"
                in captured.err)
        assert "Using INFO." in captured.err

    def test_none_log_level_returns_info(self):
        """Test that None log level returns INFO."""
        assert _validate_log_level(None) == "INFO"

    @pytest.mark.parametrize(
        "invalid_level", ["", "invalid", "TRACE", "FATAL"]
    )
    def test_various_invalid_levels(self, invalid_level, capsys):
        """Test various invalid log levels."""
        result = _validate_log_level(invalid_level)
        assert result == "INFO"

        captured = capsys.readouterr()
        assert f"Invalid log level '{invalid_level}'" in captured.err


class TestSetupLogging:
    def test_setup_with_valid_log_level(self):
        """Test setup_logging with a valid log level."""
        with patch("logging.basicConfig") as mock_basic_config:
            setup_logging("DEBUG")

            mock_basic_config.assert_called_once()
            call_args = mock_basic_config.call_args
            assert call_args[1]["level"] == logging.DEBUG
            assert call_args[1]["format"] == LOG_FORMAT
            assert isinstance(
                call_args[1]["handlers"][0], logging.StreamHandler
            )

    def test_setup_with_none_uses_info(self):
        """Test setup_logging with None uses INFO level."""
        with patch("logging.basicConfig") as mock_basic_config:
            setup_logging(None)

            mock_basic_config.assert_called_once()
            call_args = mock_basic_config.call_args
            assert call_args[1]["level"] == logging.INFO

    def test_setup_with_invalid_log_level_prints_warning(self, capsys):
        """Test setup_logging with invalid log level prints warning."""
        with patch("logging.basicConfig"):
            setup_logging("INVALID")

        captured = capsys.readouterr()
        assert "Warning: Invalid log level 'INVALID'" in captured.err

    def test_setup_suppresses_loggers(self):
        """Test that setup_logging suppresses the configured loggers."""
        with patch("logging.basicConfig"), \
             patch("logging.getLogger") as mock_get_logger:

            mock_kafka_logger = mock_get_logger.return_value
            mock_werkzeug_logger = mock_get_logger.return_value

            setup_logging("INFO")

            # Should call getLogger for each suppressed logger
            assert mock_get_logger.call_count == len(LOGGERS_TO_SUPPRESS)

            # Should set levels on the loggers
            mock_kafka_logger.setLevel.assert_called_with(logging.WARNING)
            mock_werkzeug_logger.setLevel.assert_called_with(logging.WARNING)

    @pytest.mark.parametrize(
        "level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    )
    def test_setup_with_all_valid_levels(self, level):
        """Test setup_logging with all valid log levels."""
        expected_level = getattr(logging, level)

        with patch("logging.basicConfig") as mock_basic_config:
            setup_logging(level)

            call_args = mock_basic_config.call_args
            assert call_args[1]["level"] == expected_level


class TestGetLogger:
    def test_get_logger_returns_logger_with_name(self):
        """Test that get_logger returns a logger with the specified name."""
        with patch("logging.getLogger") as mock_get_logger:
            mock_logger = mock_get_logger.return_value

            result = get_logger("test_logger")

            mock_get_logger.assert_called_once_with("test_logger")
            assert result == mock_logger

    def test_get_logger_with_different_names(self):
        """Test get_logger with different logger names."""
        with patch("logging.getLogger") as mock_get_logger:
            get_logger("logger1")
            get_logger("logger2")

            mock_get_logger.assert_any_call("logger1")
            mock_get_logger.assert_any_call("logger2")
