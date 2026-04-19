"""Security tests: credential isolation in logs.

~4 tests: no keys in startup logs, no keys in task logs,
          masking format correct, bot token absent.

Verifies that the JSON logging + secret masking pipeline prevents credentials
from appearing in log output at any level.
"""

from __future__ import annotations

import io
import logging

import pytest

from seed_storage.config import _JsonFormatter, _mask_secrets, _SecretMaskingFilter

pytestmark = pytest.mark.security

# Sample credential values that must never appear in logs
_FAKE_OPENAI_KEY = "sk-abcdefghijklmnopqrstuvwxyz1234567890ABCD"
_FAKE_ANTHROPIC_KEY = "sk-ant-abcdefghijklmnopqrstuvwxyz1234567890"
_FAKE_GROQ_KEY = "gsk_abcdefghijklmnopqrstuvwxyz1234567890"
_FAKE_BOT_TOKEN = "Bot FAKE_BASE64_ID.FAKE_TS.fake_hmac_not_a_real_token"


def _capture_log_output(logger_name: str) -> tuple[logging.Logger, io.StringIO]:
    """Set up a logger with JSON formatter + mask filter. Return (logger, buf)."""
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(_JsonFormatter())
    handler.addFilter(_SecretMaskingFilter())
    logger = logging.getLogger(f"test.{logger_name}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.handlers = [handler]
    return logger, buf


def test_no_keys_in_startup_logs():
    """API keys logged at startup are masked before hitting any handler."""
    logger, buf = _capture_log_output("startup")

    logger.info(
        "Starting service with API key %s and token %s",
        _FAKE_OPENAI_KEY,
        _FAKE_BOT_TOKEN,
    )

    output = buf.getvalue()
    assert _FAKE_OPENAI_KEY not in output
    assert _FAKE_BOT_TOKEN not in output
    assert "***MASKED***" in output


def test_no_keys_in_task_logs():
    """Credentials passed as log arguments are masked in task log output.

    Note: credentials embedded in exception *messages* are sanitized by the
    _SecretMaskingFilter applied to the log record args. This test verifies
    the argument-masking path (the primary production path).
    """
    logger, buf = _capture_log_output("task")

    # Simulate a task that logs a credential as a format argument
    logger.error(
        "Task failed due to auth error — key=%s retries=%d",
        _FAKE_ANTHROPIC_KEY,
        3,
    )
    logger.warning("Retrying with groq key: %s", _FAKE_GROQ_KEY)

    output = buf.getvalue()
    assert _FAKE_ANTHROPIC_KEY not in output
    assert _FAKE_GROQ_KEY not in output
    assert "***MASKED***" in output


def test_masking_format():
    """_mask_secrets() replaces known credential patterns with ***MASKED***."""
    cases = [
        (_FAKE_OPENAI_KEY, "sk- OpenAI key"),
        (_FAKE_ANTHROPIC_KEY, "sk-ant- Anthropic key"),
        (_FAKE_GROQ_KEY, "gsk_ Groq key"),
        (_FAKE_BOT_TOKEN, "Bot <token> Discord"),
    ]
    for credential, description in cases:
        masked = _mask_secrets(credential)
        assert masked == "***MASKED***", f"{description} was not fully masked: got {masked!r}"


def test_bot_token_absent_from_logs():
    """Discord bot token never appears in log output even as part of a larger message."""
    logger, buf = _capture_log_output("bot")

    logger.warning(
        "Bot reconnecting, using token: %s (retry 3/5)",
        _FAKE_BOT_TOKEN,
    )
    logger.debug("Full config: %s", {"DISCORD_BOT_TOKEN": _FAKE_BOT_TOKEN})

    output = buf.getvalue()
    # The raw token string must not appear anywhere in the logged output
    assert _FAKE_BOT_TOKEN not in output
    # The masked placeholder must be present
    assert "***MASKED***" in output
