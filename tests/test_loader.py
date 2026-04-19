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

    def test_graphiti_rate_limit_is_non_retryable(self):
        """Graphiti already retried 4x — don't retry again."""
        from graphiti_core.llm_client.errors import RateLimitError
        exc = RateLimitError("Rate limit exceeded. Please try again later.")
        assert classify_error(exc) == ErrorKind.NON_RETRYABLE

    def test_graphiti_rate_limit_credit_is_credit_auth(self):
        from graphiti_core.llm_client.errors import RateLimitError
        exc = RateLimitError("Rate limit exceeded. Error: Your credit balance is too low")
        assert classify_error(exc) == ErrorKind.CREDIT_AUTH

    def test_graphiti_refusal_is_non_retryable(self):
        from graphiti_core.llm_client.errors import RefusalError
        exc = RefusalError("Content refused")
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
    async def test_credit_error_trips_breaker_and_alerts(self):
        """Credit error should trip persistent breaker, alert Discord, fail item."""
        import anthropic
        from seed_storage import staging
        staging.init_tables()

        fake_item = {
            "id": "aaaaaaaa-0000-0000-0000-000000000010",
            "source_type": "web",
            "source_uri": "https://example.com/credit-fail",
            "raw_content": "some content",
            "channel": "test",
            "token_estimate": 100,
        }

        async def credit_fail(**kwargs):
            raise anthropic.AuthenticationError(
                message="Invalid API key",
                response=MagicMock(status_code=401, headers={}),
                body={"error": {"type": "authentication_error", "message": "Invalid"}},
            )

        with (
            patch("ingestion.loader.staging.get_staged", return_value=[fake_item]),
            patch("ingestion.loader.staging.update_status") as mock_status,
            patch("ingestion.loader.staging.is_breaker_tripped", return_value=None),
            patch("ingestion.loader.staging.reset_orphaned_loading", return_value=0),
            patch("ingestion.loader.staging.trip_breaker") as mock_trip,
            patch("ingestion.loader.add_episode", side_effect=credit_fail),
            patch("ingestion.loader.discord_touch.react", new_callable=AsyncMock),
            patch("ingestion.loader.discord_touch.alert", new_callable=AsyncMock) as mock_alert,
            patch("ingestion.loader.close", new_callable=AsyncMock),
        ):
            from ingestion.loader import load_batch
            await load_batch(limit=1, concurrency=1)

        # Should have tripped the persistent breaker
        mock_trip.assert_called_once()
        assert "CREDIT_AUTH" in mock_trip.call_args[0][0]

        # Should have posted urgent alert
        mock_alert.assert_called()
        alert_call = mock_alert.call_args
        assert alert_call.kwargs.get("urgent") is True

    @pytest.mark.asyncio
    async def test_non_retryable_error_fails_immediately(self):
        """Non-retryable error should mark failed with no retry, single attempt."""
        from seed_storage import staging
        staging.init_tables()

        fake_item = {
            "id": "aaaaaaaa-0000-0000-0000-000000000011",
            "source_type": "web",
            "source_uri": "https://example.com/bad-request",
            "raw_content": "content",
            "channel": "test",
            "token_estimate": 100,
        }

        call_count = 0
        async def bad_request(**kwargs):
            nonlocal call_count
            call_count += 1
            raise ValueError("bad data")

        with (
            patch("ingestion.loader.staging.get_staged", return_value=[fake_item]),
            patch("ingestion.loader.staging.update_status") as mock_status,
            patch("ingestion.loader.staging.is_breaker_tripped", return_value=None),
            patch("ingestion.loader.staging.reset_orphaned_loading", return_value=0),
            patch("ingestion.loader.staging.trip_breaker"),
            patch("ingestion.loader.add_episode", side_effect=bad_request),
            patch("ingestion.loader.discord_touch.react", new_callable=AsyncMock),
            patch("ingestion.loader.discord_touch.alert", new_callable=AsyncMock),
            patch("ingestion.loader.close", new_callable=AsyncMock),
        ):
            from ingestion.loader import load_batch
            await load_batch(limit=1, concurrency=1)

        # Only one attempt (no retries)
        assert call_count == 1
        # Marked as failed
        failed_calls = [c for c in mock_status.call_args_list if len(c[0]) >= 2 and c[0][1] == "failed"]
        assert len(failed_calls) >= 1

    @pytest.mark.asyncio
    async def test_retryable_error_returns_to_enriched(self):
        """Retryable error should set item back to 'enriched' for next batch."""
        from seed_storage import staging
        staging.init_tables()

        fake_item = {
            "id": "aaaaaaaa-0000-0000-0000-000000000012",
            "source_type": "web",
            "source_uri": "https://example.com/retry-later",
            "raw_content": "content",
            "channel": "test",
            "token_estimate": 100,
        }

        class FakeTimeoutError(Exception):
            pass

        async def timeout_fail(**kwargs):
            raise FakeTimeoutError("connection timed out")

        with (
            patch("ingestion.loader.staging.get_staged", return_value=[fake_item]),
            patch("ingestion.loader.staging.update_status") as mock_status,
            patch("ingestion.loader.staging.is_breaker_tripped", return_value=None),
            patch("ingestion.loader.staging.reset_orphaned_loading", return_value=0),
            patch("ingestion.loader.staging.trip_breaker"),
            patch("ingestion.loader.add_episode", side_effect=timeout_fail),
            patch("ingestion.loader.discord_touch.react", new_callable=AsyncMock),
            patch("ingestion.loader.discord_touch.alert", new_callable=AsyncMock),
            patch("ingestion.loader.close", new_callable=AsyncMock),
        ):
            from ingestion.loader import load_batch
            await load_batch(limit=1, concurrency=1)

        # Item should be set back to enriched (not failed)
        enriched_calls = [c for c in mock_status.call_args_list if len(c[0]) >= 2 and c[0][1] == "enriched"]
        assert len(enriched_calls) >= 1

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
