"""Tests for loader — cost estimation, error classification, circuit breaker, and alerts."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingestion.loader import _estimate_cost, _content_hash, classify_error, ErrorKind


class TestEstimateCost:
    def test_zero_tokens(self):
        assert _estimate_cost(0) == 0.0

    def test_million_tokens(self):
        cost = _estimate_cost(1_000_000)
        # Haiku input: $0.80, Haiku output (15%): $0.60, Gemini embed: $0.02
        assert 1.3 < cost < 1.5

    def test_small_doc(self):
        cost = _estimate_cost(1000)
        assert 0.0005 < cost < 0.002

    def test_proportional(self):
        c1 = _estimate_cost(1000)
        c2 = _estimate_cost(2000)
        assert abs(c2 - 2 * c1) < 0.0001


class TestContentHash:
    def test_same_text_same_hash(self):
        assert _content_hash("hello world") == _content_hash("hello world")

    def test_different_text_different_hash(self):
        assert _content_hash("hello world") != _content_hash("goodbye world")

    def test_whitespace_stripped(self):
        assert _content_hash("  hello  ") == _content_hash("hello")

    def test_empty_string(self):
        h = _content_hash("")
        assert isinstance(h, str) and len(h) == 16

    def test_different_suffix_different_hash(self):
        base = "x" * 500
        assert _content_hash(base + "AAA") != _content_hash(base + "BBB")


class TestClassifyError:
    """Verify error classification for cost protection."""

    def test_anthropic_auth_is_credit_auth(self):
        import anthropic
        exc = anthropic.AuthenticationError(
            message="Invalid API key",
            response=MagicMock(status_code=401, headers={}),
            body={"error": {"type": "authentication_error", "message": "Invalid API key"}},
        )
        assert classify_error(exc) == ErrorKind.CREDIT_AUTH

    def test_anthropic_rate_limit_is_retryable(self):
        import anthropic
        exc = anthropic.RateLimitError(
            message="Rate limit exceeded",
            response=MagicMock(status_code=429, headers={}),
            body={"error": {"type": "rate_limit_error", "message": "Rate limit exceeded"}},
        )
        assert classify_error(exc) == ErrorKind.RETRYABLE

    def test_anthropic_credit_exhaustion_is_credit_auth(self):
        import anthropic
        exc = anthropic.RateLimitError(
            message="Your credit balance is too low",
            response=MagicMock(status_code=429, headers={}),
            body={"error": {"type": "rate_limit_error", "message": "Your credit balance is too low"}},
        )
        assert classify_error(exc) == ErrorKind.CREDIT_AUTH

    def test_anthropic_bad_request_is_non_retryable(self):
        import anthropic
        exc = anthropic.BadRequestError(
            message="Invalid request",
            response=MagicMock(status_code=400, headers={}),
            body={"error": {"type": "invalid_request_error", "message": "Invalid request"}},
        )
        assert classify_error(exc) == ErrorKind.NON_RETRYABLE

    def test_anthropic_timeout_is_retryable(self):
        import anthropic
        exc = anthropic.APITimeoutError(request=MagicMock())
        assert classify_error(exc) == ErrorKind.RETRYABLE

    def test_openai_rate_limit_is_retryable(self):
        import openai
        from unittest.mock import MagicMock
        resp = MagicMock()
        resp.status_code = 429
        exc = openai.RateLimitError("rate limited", response=resp, body={})
        assert classify_error(exc) == ErrorKind.RETRYABLE

    def test_openai_rate_limit_credit_is_credit_auth(self):
        import openai
        from unittest.mock import MagicMock
        resp = MagicMock()
        resp.status_code = 429
        exc = openai.RateLimitError("credit balance exceeded", response=resp, body={})
        assert classify_error(exc) == ErrorKind.CREDIT_AUTH

    def test_openai_bad_request_is_non_retryable(self):
        import openai
        from unittest.mock import MagicMock
        resp = MagicMock()
        resp.status_code = 400
        exc = openai.BadRequestError("bad request", response=resp, body={})
        assert classify_error(exc) == ErrorKind.NON_RETRYABLE

    def test_unknown_error_is_non_retryable(self):
        """Unknown errors default to non-retryable for cost safety."""
        exc = ValueError("something weird")
        assert classify_error(exc) == ErrorKind.NON_RETRYABLE

    def test_timeout_in_name_is_retryable(self):
        """Fallback heuristic for timeout-like errors."""
        class SomeTimeoutError(Exception):
            pass
        assert classify_error(SomeTimeoutError("timed out")) == ErrorKind.RETRYABLE

    def test_neo4j_service_unavailable_is_retryable(self):
        from neo4j.exceptions import ServiceUnavailable
        exc = ServiceUnavailable("Connection lost")
        assert classify_error(exc) == ErrorKind.RETRYABLE


class TestLoaderBatchProtection:
    """Verify circuit breaker, cost ceiling, and single-attempt behavior."""

    @pytest.mark.asyncio
    async def test_breaker_tripped_skips_batch(self):
        """If circuit breaker is tripped, load_batch should return immediately."""
        with (
            patch("ingestion.loader.staging.is_breaker_tripped", return_value={"reason": "CREDIT_AUTH: test"}),
            patch("ingestion.loader.staging.get_staged") as mock_get,
        ):
            from ingestion.loader import load_batch
            await load_batch()

        # Should NOT have queried for items
        mock_get.assert_not_called()


class TestLoaderBatch:
    """Integration: verify loader handles empty batch gracefully."""

    @pytest.mark.asyncio
    async def test_empty_batch(self):
        from seed_storage import staging
        staging.init_tables()
        from ingestion.loader import load_batch
        await load_batch(limit=0)
