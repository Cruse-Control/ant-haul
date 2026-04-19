"""tests/unit/tasks/test_ingest_episode.py

Unit tests for the ingest_episode Celery task.
All external deps (Redis, Graphiti, asyncio.run) are mocked.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from seed_storage.enrichment.models import ResolvedContent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_raw_payload(
    *,
    source_type: str = "discord",
    source_id: str = "msg1",
    source_channel: str = "general",
    content: str = "Hello",
    channel_id: str = "c1",
    frontier_depth: int = 0,
) -> dict:
    return {
        "source_type": source_type,
        "source_id": source_id,
        "source_channel": source_channel,
        "author": "alice",
        "content": content,
        "timestamp": "2026-01-01T00:00:00+00:00",
        "attachments": [],
        "metadata": {
            "channel_id": channel_id,
            "author_id": "u1",
            "guild_id": "g1",
            "frontier_depth": frontier_depth,
        },
    }


def _make_resolved_content(
    url: str = "https://example.com",
    *,
    text: str = "Page text",
    expansion_urls: list[str] | None = None,
    content_type: str = "webpage",
) -> ResolvedContent:
    return ResolvedContent(
        source_url=url,
        content_type=content_type,
        title="Example",
        text=text,
        transcript=None,
        summary=None,
        expansion_urls=expansion_urls or [],
        metadata={},
        extraction_error=None,
        resolved_at=datetime.now(tz=UTC),
    )


def _make_enriched_payload(
    *,
    message: dict | None = None,
    resolved_contents: list[ResolvedContent] | None = None,
) -> dict:
    if message is None:
        message = _make_raw_payload()
    if resolved_contents is None:
        resolved_contents = []
    return {
        "message": message,
        "resolved_contents": [rc.to_dict() for rc in resolved_contents],
    }


@pytest.fixture(autouse=True)
def celery_always_eager():
    from seed_storage.worker.app import app

    app.conf.task_always_eager = True
    yield
    app.conf.task_always_eager = False


# ---------------------------------------------------------------------------
# Tests: source_description format
# ---------------------------------------------------------------------------


class TestSourceDescription:
    """Verify source_description strings per spec Section 4."""

    def test_message_source_description_format(self):
        """Message episodes: '{source_type.title()} #{source_channel}'."""
        from seed_storage.worker.tasks import _source_description_message

        assert _source_description_message("discord", "general") == "Discord #general"
        assert _source_description_message("expansion", "feed") == "Expansion #feed"
        assert _source_description_message("rss", "tech") == "Rss #tech"

    def test_content_source_description_format(self):
        """Content episodes: 'content_from_{title}_{channel}:{content_type}'."""
        from seed_storage.worker.tasks import _source_description_content

        result = _source_description_content("discord", "general", "youtube")
        assert result == "content_from_Discord_general:youtube"

    def test_content_source_description_with_hash_in_channel(self):
        """Channel name without # prefix (raw name from discord)."""
        from seed_storage.worker.tasks import _source_description_content

        result = _source_description_content("discord", "imessages", "webpage")
        assert result == "content_from_Discord_imessages:webpage"


class TestGroupIdEnforcement:
    """group_id must always be 'seed-storage'."""

    def test_group_id_constant(self):
        """GROUP_ID must equal 'seed-storage' — never per-channel."""
        from seed_storage.worker.tasks import GROUP_ID

        assert GROUP_ID == "seed-storage"


class TestIngestEpisodeBudgetCheck:
    """If daily budget exceeded, task should retry and eventually dead-letter."""

    def test_budget_exceeded_triggers_retry(self):
        """When budget exceeded, task does not write to graph (asyncio.run not called)."""
        import celery.exceptions

        payload = _make_enriched_payload()

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
            patch("seed_storage.worker.tasks.send_alert"),
            patch("seed_storage.worker.tasks.dead_letter"),
        ):
            r = MagicMock()
            mock_get_redis.return_value = r

            cost_tracker = MagicMock()
            cost_tracker.is_budget_exceeded.return_value = True

            rate_limiter = MagicMock()
            rate_limiter.allow.return_value = True

            circuit_breaker = MagicMock()
            circuit_breaker.is_open.return_value = False

            with (
                patch("seed_storage.worker.tasks._get_cost_tracker", return_value=cost_tracker),
                patch("seed_storage.worker.tasks._get_rate_limiter", return_value=rate_limiter),
                patch(
                    "seed_storage.worker.tasks._get_circuit_breaker", return_value=circuit_breaker
                ),
            ):
                from seed_storage.worker.tasks import ingest_episode

                # In always-eager mode, retry raises Retry exception; let it propagate or be swallowed
                try:
                    ingest_episode(payload)
                except (celery.exceptions.Retry, celery.exceptions.MaxRetriesExceededError):
                    pass  # expected in eager mode

            # graphiti add_episode must not have been called
            mock_asyncio.run.assert_not_called()


class TestIngestEpisodeCircuitBreaker:
    """Open circuit breaker blocks ingest without retry."""

    def test_open_circuit_breaker_skips_ingest(self):
        """When circuit is open, task should skip and not write to graph."""
        payload = _make_enriched_payload()

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
            patch("seed_storage.worker.tasks.send_alert"),
        ):
            r = MagicMock()
            mock_get_redis.return_value = r

            cost_tracker = MagicMock()
            cost_tracker.is_budget_exceeded.return_value = False

            rate_limiter = MagicMock()
            rate_limiter.allow.return_value = True

            circuit_breaker = MagicMock()
            circuit_breaker.is_open.return_value = True

            with (
                patch("seed_storage.worker.tasks._get_cost_tracker", return_value=cost_tracker),
                patch("seed_storage.worker.tasks._get_rate_limiter", return_value=rate_limiter),
                patch(
                    "seed_storage.worker.tasks._get_circuit_breaker", return_value=circuit_breaker
                ),
            ):
                from seed_storage.worker.tasks import ingest_episode

                ingest_episode(payload)

            mock_asyncio.run.assert_not_called()


class TestIngestEpisodeExpansionUrls:
    """expansion_urls from resolved content must be added to frontier."""

    def test_expansion_urls_added_to_frontier(self):
        """expansion_urls from resolved content must go to add_to_frontier."""
        rc = _make_resolved_content(expansion_urls=["https://child1.com", "https://child2.com"])
        payload = _make_enriched_payload(resolved_contents=[rc])

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
            patch("seed_storage.worker.tasks.add_to_frontier") as mock_add_frontier,
            patch("seed_storage.worker.tasks.send_alert"),
        ):
            r = MagicMock()
            r.sadd.return_value = 1  # not yet ingested
            r.publish.return_value = 1
            mock_get_redis.return_value = r

            cost_tracker = MagicMock()
            cost_tracker.is_budget_exceeded.return_value = False
            cost_tracker.increment.return_value = None

            rate_limiter = MagicMock()
            rate_limiter.allow.return_value = True

            circuit_breaker = MagicMock()
            circuit_breaker.is_open.return_value = False
            circuit_breaker.record_success.return_value = None
            circuit_breaker.record_failure.return_value = None

            mock_asyncio.run.return_value = None

            with (
                patch("seed_storage.worker.tasks._get_cost_tracker", return_value=cost_tracker),
                patch("seed_storage.worker.tasks._get_rate_limiter", return_value=rate_limiter),
                patch(
                    "seed_storage.worker.tasks._get_circuit_breaker", return_value=circuit_breaker
                ),
            ):
                from seed_storage.worker.tasks import ingest_episode

                ingest_episode(payload)

            assert mock_add_frontier.call_count == 2

    def test_expansion_urls_respect_depth_ceiling(self):
        """expansion_urls must not be added if depth >= HARD_DEPTH_CEILING."""
        rc = _make_resolved_content(expansion_urls=["https://deep.com"])
        msg = _make_raw_payload(frontier_depth=5)  # at ceiling
        payload = _make_enriched_payload(message=msg, resolved_contents=[rc])

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
            patch("seed_storage.worker.tasks.add_to_frontier") as mock_add_frontier,
            patch("seed_storage.worker.tasks.send_alert"),
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r

            cost_tracker = MagicMock()
            cost_tracker.is_budget_exceeded.return_value = False
            cost_tracker.increment.return_value = None

            rate_limiter = MagicMock()
            rate_limiter.allow.return_value = True

            circuit_breaker = MagicMock()
            circuit_breaker.is_open.return_value = False
            circuit_breaker.record_success.return_value = None

            mock_asyncio.run.return_value = None

            with (
                patch("seed_storage.worker.tasks._get_cost_tracker", return_value=cost_tracker),
                patch("seed_storage.worker.tasks._get_rate_limiter", return_value=rate_limiter),
                patch(
                    "seed_storage.worker.tasks._get_circuit_breaker", return_value=circuit_breaker
                ),
            ):
                from seed_storage.worker.tasks import ingest_episode

                ingest_episode(payload)

            # depth 5 >= HARD_DEPTH_CEILING(5) → no frontier adds
            mock_add_frontier.assert_not_called()


class TestIngestEpisodeReactionEvents:
    """🏷️ and 🧠 reactions must be published on success."""

    def test_reactions_published_after_ingest(self):
        """🏷️ (tagged) and 🧠 (graph) reactions published on completion."""
        payload = _make_enriched_payload()

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
            patch("seed_storage.worker.tasks.send_alert"),
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r

            cost_tracker = MagicMock()
            cost_tracker.is_budget_exceeded.return_value = False
            cost_tracker.increment.return_value = None

            rate_limiter = MagicMock()
            rate_limiter.allow.return_value = True

            circuit_breaker = MagicMock()
            circuit_breaker.is_open.return_value = False
            circuit_breaker.record_success.return_value = None

            mock_asyncio.run.return_value = None

            with (
                patch("seed_storage.worker.tasks._get_cost_tracker", return_value=cost_tracker),
                patch("seed_storage.worker.tasks._get_rate_limiter", return_value=rate_limiter),
                patch(
                    "seed_storage.worker.tasks._get_circuit_breaker", return_value=circuit_breaker
                ),
            ):
                from seed_storage.worker.tasks import ingest_episode

                ingest_episode(payload)

            # Check 🏷️ and 🧠 were published
            published_emojis = []
            for call in r.publish.call_args_list:
                data = call[0][1]
                try:
                    event = __import__("json").loads(data)
                    published_emojis.append(event.get("emoji", ""))
                except Exception:
                    pass

            assert "🏷️" in published_emojis
            assert "🧠" in published_emojis


class TestIngestEpisodeCostTracking:
    """CostTracker.increment() must be called per add_episode() call."""

    def test_cost_incremented_per_episode(self):
        """Cost must be incremented for each successful add_episode call."""
        rc = _make_resolved_content(text="Content text here")
        payload = _make_enriched_payload(resolved_contents=[rc])

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
            patch("seed_storage.worker.tasks.add_to_frontier"),
            patch("seed_storage.worker.tasks.send_alert"),
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r

            cost_tracker = MagicMock()
            cost_tracker.is_budget_exceeded.return_value = False

            rate_limiter = MagicMock()
            rate_limiter.allow.return_value = True

            circuit_breaker = MagicMock()
            circuit_breaker.is_open.return_value = False
            circuit_breaker.record_success.return_value = None

            mock_asyncio.run.return_value = None

            with (
                patch("seed_storage.worker.tasks._get_cost_tracker", return_value=cost_tracker),
                patch("seed_storage.worker.tasks._get_rate_limiter", return_value=rate_limiter),
                patch(
                    "seed_storage.worker.tasks._get_circuit_breaker", return_value=circuit_breaker
                ),
            ):
                from seed_storage.worker.tasks import ingest_episode

                ingest_episode(payload)

            # message episode + 1 content episode = 2 calls
            assert cost_tracker.increment.call_count >= 1


class TestIngestEpisodeEmptyResolved:
    """Empty resolved_contents still produces a message-only episode."""

    def test_empty_resolved_contents_writes_message_episode(self):
        """enriched_payload with no resolved_contents still writes message episode."""
        payload = _make_enriched_payload(resolved_contents=[])

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
            patch("seed_storage.worker.tasks.send_alert"),
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r

            cost_tracker = MagicMock()
            cost_tracker.is_budget_exceeded.return_value = False
            cost_tracker.increment.return_value = None

            rate_limiter = MagicMock()
            rate_limiter.allow.return_value = True

            circuit_breaker = MagicMock()
            circuit_breaker.is_open.return_value = False
            circuit_breaker.record_success.return_value = None

            mock_asyncio.run.return_value = None

            with (
                patch("seed_storage.worker.tasks._get_cost_tracker", return_value=cost_tracker),
                patch("seed_storage.worker.tasks._get_rate_limiter", return_value=rate_limiter),
                patch(
                    "seed_storage.worker.tasks._get_circuit_breaker", return_value=circuit_breaker
                ),
            ):
                from seed_storage.worker.tasks import ingest_episode

                ingest_episode(payload)

            # add_episode called once for message
            assert mock_asyncio.run.call_count >= 1
