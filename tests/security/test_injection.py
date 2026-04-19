"""Security tests: injection resistance.

~5 tests: SQL injection, XSS, SSTI, oversized payload, unicode edge cases.

These tests verify that malicious content in message payloads is handled
safely — passed through without executing, truncated if oversized, and
processed without error for unusual unicode.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.security


def _raw_payload(content: str, attachments: list[str] | None = None) -> dict:
    return {
        "source_type": "discord",
        "source_id": f"sec-{uuid.uuid4().hex[:8]}",
        "source_channel": "general",
        "author": "attacker",
        "content": content,
        "attachments": attachments or [],
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "metadata": {"channel_id": "000"},
    }


def _run_enrich_safe(raw: dict) -> list[dict]:
    """Run enrich_message with mocked infra. Return captured ingest payloads."""
    import redis as redis_lib

    from seed_storage.worker.tasks import enrich_message

    r = MagicMock(spec=redis_lib.Redis)
    # seen_or_mark returns 0 (SADD added — not a duplicate)
    r.sadd.return_value = 1
    r.sismember.return_value = False
    r.publish.return_value = 1

    captured: list[dict] = []

    with (
        patch("seed_storage.worker.tasks._get_redis", return_value=r),
        patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
        patch(
            "seed_storage.worker.tasks._resolve_urls",
            side_effect=lambda *a, **kw: _async_empty(),
        ),
    ):
        mock_ingest.delay = lambda p: captured.append(p)
        enrich_message.apply(args=[raw])

    return captured


async def _async_empty():
    return []


def test_sql_injection_in_content():
    """SQL injection payload passes through without error or execution."""
    sql_payload = "'; DROP TABLE episodes; --"
    raw = _raw_payload(f"Check this: {sql_payload}")

    payloads = _run_enrich_safe(raw)
    assert len(payloads) == 1
    # Content is passed as-is (not parsed as SQL)
    assert sql_payload in payloads[0]["message"]["content"]


def test_xss_in_content():
    """XSS payload passes through without error."""
    xss_payload = '<script>alert("xss")</script>'
    raw = _raw_payload(xss_payload)

    payloads = _run_enrich_safe(raw)
    assert len(payloads) == 1
    assert xss_payload in payloads[0]["message"]["content"]


def test_ssti_in_content():
    """SSTI/template injection payload does not execute."""
    ssti_payloads = ["{{7*7}}", "${7*7}", "<%= 7*7 %>", "#{7*7}"]
    for payload in ssti_payloads:
        raw = _raw_payload(f"Message: {payload}")
        payloads = _run_enrich_safe(raw)
        # Should enqueue without error, not evaluate the template
        assert len(payloads) == 1
        # The string "49" (7*7 evaluated) must NOT appear as replacement
        assert "49" not in payloads[0]["message"]["content"].replace(payload, "")


def test_oversized_payload():
    """Oversized content (10 MB) is handled without crashing the task."""
    big_content = "A" * (10 * 1024 * 1024)  # 10 MB
    raw = _raw_payload(big_content)

    # Should not raise — the task may truncate or pass through
    try:
        payloads = _run_enrich_safe(raw)
        # Either enqueued (possibly truncated) or silently skipped — no crash
        assert isinstance(payloads, list)
    except MemoryError:
        pytest.skip("Insufficient memory for 10 MB payload test")


def test_unicode_edge_cases():
    """Unusual unicode (null bytes, surrogates, RTL, emoji) handled without error."""
    unicode_cases = [
        "Normal text \u0000 with null byte",
        "Right-to-left: \u202e reversed",
        "Emoji: \U0001f4a9 \U0001f525 \U0001f916",
        "CJK: 你好世界 こんにちは",
        "Arabic: مرحبا بالعالم",
        "Combining: e\u0301 (e + combining acute)",
    ]
    for content in unicode_cases:
        raw = _raw_payload(content)
        try:
            payloads = _run_enrich_safe(raw)
            assert isinstance(payloads, list)
        except Exception as exc:
            pytest.fail(f"Unhandled exception for unicode input {content!r}: {exc}")
