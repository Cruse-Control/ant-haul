"""Unit tests for seed_storage.config logging utilities.

Tests JSON formatting, secret masking, required log fields, and task_log
duration tracking. No real infrastructure or credentials required.
"""

from __future__ import annotations

import json
import logging

import pytest

from seed_storage.config import (
    _JsonFormatter,
    _mask_secrets,
    _SecretMaskingFilter,
    configure_logging,
    task_log,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _capture_record(msg: str, level: int = logging.INFO, **kwargs) -> str:
    """Emit a log record through JsonFormatter and return the JSON string."""
    formatter = _JsonFormatter()
    record = logging.LogRecord(
        name="test.logger",
        level=level,
        pathname="",
        lineno=0,
        msg=msg,
        args=(),
        exc_info=None,
    )
    for k, v in kwargs.items():
        setattr(record, k, v)
    return formatter.format(record)


# ---------------------------------------------------------------------------
# JSON format
# ---------------------------------------------------------------------------


class TestJsonFormat:
    def test_output_is_valid_json(self):
        output = _capture_record("hello world")
        parsed = json.loads(output)  # raises if not valid JSON
        assert isinstance(parsed, dict)

    def test_required_fields_present(self):
        parsed = json.loads(_capture_record("test message"))
        assert "timestamp" in parsed
        assert "level" in parsed
        assert "name" in parsed
        assert "message" in parsed

    def test_message_field_contains_log_text(self):
        parsed = json.loads(_capture_record("specific log text"))
        assert parsed["message"] == "specific log text"

    def test_level_field_matches_log_level(self):
        parsed = json.loads(_capture_record("msg", level=logging.WARNING))
        assert parsed["level"] == "WARNING"

    def test_name_field_contains_logger_name(self):
        parsed = json.loads(_capture_record("msg"))
        assert parsed["name"] == "test.logger"

    def test_duration_ms_included_when_present(self):
        parsed = json.loads(_capture_record("msg", duration_ms=123))
        assert parsed["duration_ms"] == 123

    def test_duration_ms_absent_when_not_set(self):
        parsed = json.loads(_capture_record("msg"))
        assert "duration_ms" not in parsed


# ---------------------------------------------------------------------------
# Secret masking
# ---------------------------------------------------------------------------


class TestSecretMasking:
    def test_openai_key_masked_in_message(self):
        output = _capture_record("api_key=sk-1234567890abcdef1234567890")
        parsed = json.loads(output)
        assert "sk-1234567890" not in parsed["message"]
        assert "***MASKED***" in parsed["message"]

    def test_anthropic_key_masked(self):
        output = _capture_record("key=sk-ant-api03-abcdefghijklmnopqrstuvwxyz123456")
        parsed = json.loads(output)
        assert "sk-ant-api03" not in parsed["message"]
        assert "***MASKED***" in parsed["message"]

    def test_groq_key_masked(self):
        output = _capture_record("key=gsk_abcdefghijklmnopqrstuvwxyz12345678")
        parsed = json.loads(output)
        assert "gsk_abcdefghijklmnopqrstuvwxyz" not in parsed["message"]
        assert "***MASKED***" in parsed["message"]

    def test_normal_text_not_masked(self):
        output = _capture_record("processing message from #general")
        parsed = json.loads(output)
        assert parsed["message"] == "processing message from #general"

    def test_masking_at_debug_level(self):
        """Secrets must be masked even at DEBUG level."""
        output = _capture_record("debug: sk-1234567890abcdefghijklmnop", level=logging.DEBUG)
        parsed = json.loads(output)
        assert "sk-1234567890" not in parsed["message"]
        assert "***MASKED***" in parsed["message"]


# ---------------------------------------------------------------------------
# _mask_secrets helper
# ---------------------------------------------------------------------------


class TestMaskSecretsHelper:
    def test_openai_key_masked(self):
        result = _mask_secrets("key=sk-proj-1234567890abcdefghijklmnop")
        assert "sk-proj-1234567890" not in result
        assert "***MASKED***" in result

    def test_plain_text_unchanged(self):
        text = "no secrets here"
        assert _mask_secrets(text) == text

    def test_empty_string_unchanged(self):
        assert _mask_secrets("") == ""


# ---------------------------------------------------------------------------
# SecretMaskingFilter
# ---------------------------------------------------------------------------


class TestSecretMaskingFilter:
    def test_filter_masks_record_msg(self):
        filt = _SecretMaskingFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="sk-1234567890abcdefghijklmnop",
            args=(),
            exc_info=None,
        )
        filt.filter(record)
        assert "sk-1234567890" not in record.msg
        assert "***MASKED***" in record.msg

    def test_filter_always_returns_true(self):
        filt = _SecretMaskingFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="hello",
            args=(),
            exc_info=None,
        )
        assert filt.filter(record) is True

    def test_filter_masks_string_args(self):
        filt = _SecretMaskingFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="key=%s",
            args=("sk-1234567890abcdefghijklmnop",),
            exc_info=None,
        )
        filt.filter(record)
        assert "sk-1234567890" not in record.args[0]


# ---------------------------------------------------------------------------
# task_log context manager
# ---------------------------------------------------------------------------


class TestTaskLog:
    def test_duration_ms_logged(self):
        """task_log emits a record with duration_ms set."""
        captured: list[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured.append(record)

        logger = logging.getLogger("test.task_log")
        logger.addHandler(_Capture())
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        with task_log(logger, "my_task"):
            pass

        assert len(captured) == 1
        assert hasattr(captured[0], "duration_ms")
        assert isinstance(captured[0].duration_ms, int)
        assert captured[0].duration_ms >= 0

    def test_task_name_in_log_message(self):
        captured: list[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured.append(record)

        logger = logging.getLogger("test.task_log2")
        logger.addHandler(_Capture())
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        with task_log(logger, "enrich_message"):
            pass

        assert "enrich_message" in captured[0].getMessage()

    def test_exception_propagates(self):
        logger = logging.getLogger("test.task_log3")
        logger.propagate = False

        with pytest.raises(ValueError, match="boom"):
            with task_log(logger, "failing_task"):
                raise ValueError("boom")

    def test_duration_logged_even_on_exception(self):
        captured: list[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured.append(record)

        logger = logging.getLogger("test.task_log4")
        logger.addHandler(_Capture())
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        with pytest.raises(RuntimeError):
            with task_log(logger, "failing"):
                raise RuntimeError("oops")

        assert len(captured) == 1
        assert hasattr(captured[0], "duration_ms")


# ---------------------------------------------------------------------------
# configure_logging
# ---------------------------------------------------------------------------


class TestConfigureLogging:
    def test_configure_logging_sets_json_handler(self):
        """After configure_logging(), root logger has a handler with JsonFormatter."""
        configure_logging(level="DEBUG")
        root = logging.getLogger()
        assert len(root.handlers) >= 1
        assert any(isinstance(h.formatter, _JsonFormatter) for h in root.handlers)

    def test_configure_logging_is_idempotent(self):
        """Calling configure_logging() twice doesn't stack handlers."""
        configure_logging(level="INFO")
        count_after_first = len(logging.getLogger().handlers)
        configure_logging(level="INFO")
        assert len(logging.getLogger().handlers) == count_after_first
