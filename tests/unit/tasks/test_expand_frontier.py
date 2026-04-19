"""tests/unit/tasks/test_expand_frontier.py

Unit tests for the expand_from_frontier Celery task.
All external deps (Redis, ContentDispatcher, asyncio.run) are mocked.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from seed_storage.enrichment.models import ResolvedContent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_resolved_content(
    url: str = "https://example.com",
    *,
    text: str = "Page text",
    expansion_urls: list[str] | None = None,
) -> ResolvedContent:
    return ResolvedContent(
        source_url=url,
        content_type="webpage",
        title="Example",
        text=text,
        transcript=None,
        summary=None,
        expansion_urls=expansion_urls or [],
        metadata={},
        extraction_error=None,
        resolved_at=datetime.now(tz=UTC),
    )


def _make_frontier_meta(
    url: str = "https://example.com",
    depth: int = 0,
    source_channel: str = "general",
) -> dict:
    return {
        "url": url,
        "url_hash": "abc123",
        "discovered_from_url": "https://parent.com",
        "discovered_from_source_id": "msg1",
        "source_channel": source_channel,
        "depth": depth,
        "resolver_hint": "webpage",
        "discovered_at": "2026-01-01T00:00:00+00:00",
    }


@pytest.fixture(autouse=True)
def celery_always_eager():
    from seed_storage.worker.app import app

    app.conf.task_always_eager = True
    yield
    app.conf.task_always_eager = False


# ---------------------------------------------------------------------------
# Tests: Depth ceiling
# ---------------------------------------------------------------------------


class TestDepthCeiling:
    """Depth >= HARD_DEPTH_CEILING must stop expansion."""

    def test_at_depth_ceiling_removes_and_returns(self):
        """URL at HARD_DEPTH_CEILING depth must be removed and not resolved."""
        url_hash_str = "deadbeef"
        meta = _make_frontier_meta(depth=5)  # at ceiling

        with (
            patch("seed_storage.worker.tasks.get_frontier_meta", return_value=meta),
            patch("seed_storage.worker.tasks.remove_from_frontier") as mock_remove,
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            r.sadd.return_value = 0  # pretend not ingested (won't matter)
            mock_get_redis.return_value = r

            from seed_storage.worker.tasks import expand_from_frontier

            expand_from_frontier(url_hash_str)

            mock_remove.assert_called_once_with(r, url_hash_str)
            mock_asyncio.run.assert_not_called()

    def test_below_ceiling_allows_expansion(self):
        """URL at depth 2 (below ceiling of 5) should be resolved."""
        url_hash_str = "abc123"
        meta = _make_frontier_meta(depth=2)
        resolved = _make_resolved_content()

        with (
            patch("seed_storage.worker.tasks.get_frontier_meta", return_value=meta),
            patch("seed_storage.worker.tasks.remove_from_frontier"),
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
            patch("seed_storage.worker.tasks.add_to_frontier"),
        ):
            r = MagicMock()
            r.sadd.return_value = 1  # not yet ingested
            mock_get_redis.return_value = r
            mock_asyncio.run.return_value = resolved

            from seed_storage.worker.tasks import expand_from_frontier

            expand_from_frontier(url_hash_str)

            mock_ingest.delay.assert_called_once()


class TestAlreadyIngestedDedup:
    """URLs already in seed:ingested_content must not be re-processed."""

    def test_already_ingested_removes_from_frontier(self):
        """If URL hash is in ingested_content set, remove from frontier and return."""
        url_hash_str = "abc123"
        meta = _make_frontier_meta()

        with (
            patch("seed_storage.worker.tasks.get_frontier_meta", return_value=meta),
            patch("seed_storage.worker.tasks.remove_from_frontier") as mock_remove,
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            # sadd returns 0 → already in ingested_content set
            r.sadd.return_value = 0
            mock_get_redis.return_value = r

            from seed_storage.worker.tasks import expand_from_frontier

            expand_from_frontier(url_hash_str)

            mock_remove.assert_called_once_with(r, url_hash_str)
            mock_asyncio.run.assert_not_called()


class TestBuildContentPayloadShape:
    """build_content_payload must match Contract 3."""

    def test_payload_shape_matches_contract3(self):
        """Contract 3: expansion message has source_type='expansion', author='system'."""
        from seed_storage.worker.tasks import build_content_payload

        resolved = _make_resolved_content("https://example.com/page")
        meta = {
            "url_hash": "hash1",
            "source_channel": "general",
            "discovered_from_url": "https://parent.com",
            "discovered_from_source_id": "msg1",
            "depth": 1,
            "discovered_at": "2026-01-01T00:00:00+00:00",
        }
        payload = build_content_payload(resolved, meta)

        assert payload["message"]["source_type"] == "expansion"
        assert payload["message"]["author"] == "system"
        assert payload["message"]["source_id"] == f"frontier_{meta['url_hash']}"
        assert payload["message"]["source_channel"] == meta["source_channel"]
        assert len(payload["resolved_contents"]) == 1
        assert payload["resolved_contents"][0]["source_url"] == "https://example.com/page"

    def test_payload_message_contains_metadata(self):
        """Contract 3: message.metadata has frontier_depth, discovered_from_url, source_id."""
        from seed_storage.worker.tasks import build_content_payload

        resolved = _make_resolved_content()
        meta = {
            "url_hash": "hash2",
            "source_channel": "tech",
            "discovered_from_url": "https://origin.com",
            "discovered_from_source_id": "origin_msg",
            "depth": 2,
            "discovered_at": "2026-01-02T00:00:00+00:00",
        }
        payload = build_content_payload(resolved, meta)
        msg_meta = payload["message"]["metadata"]

        assert msg_meta["frontier_depth"] == 2
        assert msg_meta["discovered_from_url"] == "https://origin.com"
        assert msg_meta["discovered_from_source_id"] == "origin_msg"


class TestChildUrlsAddedToFrontier:
    """Child expansion_urls from resolved content must be added to frontier."""

    def test_child_urls_added_with_incremented_depth(self):
        """Child URLs must be added to frontier at depth+1."""
        url_hash_str = "parent_hash"
        meta = _make_frontier_meta(depth=1)
        resolved = _make_resolved_content(
            expansion_urls=["https://child1.com", "https://child2.com"]
        )

        with (
            patch("seed_storage.worker.tasks.get_frontier_meta", return_value=meta),
            patch("seed_storage.worker.tasks.remove_from_frontier"),
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
            patch("seed_storage.worker.tasks.ingest_episode"),
            patch("seed_storage.worker.tasks.add_to_frontier") as mock_add_frontier,
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            mock_get_redis.return_value = r
            mock_asyncio.run.return_value = resolved

            from seed_storage.worker.tasks import expand_from_frontier

            expand_from_frontier(url_hash_str)

            # 2 child URLs should be added
            assert mock_add_frontier.call_count == 2

    def test_breadth_limit_respected(self):
        """Child URLs must be capped at MAX_EXPANSION_BREADTH (20)."""
        url_hash_str = "parent_hash"
        meta = _make_frontier_meta(depth=0)
        # Create 25 child URLs (more than MAX_EXPANSION_BREADTH=20)
        child_urls = [f"https://child{i}.com" for i in range(25)]
        resolved = _make_resolved_content(expansion_urls=child_urls)

        with (
            patch("seed_storage.worker.tasks.get_frontier_meta", return_value=meta),
            patch("seed_storage.worker.tasks.remove_from_frontier"),
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
            patch("seed_storage.worker.tasks.ingest_episode"),
            patch("seed_storage.worker.tasks.add_to_frontier") as mock_add_frontier,
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            mock_get_redis.return_value = r
            mock_asyncio.run.return_value = resolved

            from seed_storage.worker.tasks import expand_from_frontier

            expand_from_frontier(url_hash_str)

            # Must be capped at MAX_EXPANSION_BREADTH (20)
            assert mock_add_frontier.call_count <= 20


class TestProcessedUrlRemoved:
    """After expansion, URL must be removed from frontier."""

    def test_url_removed_after_successful_expansion(self):
        """Frontier URL must be removed after successful processing."""
        url_hash_str = "processed_hash"
        meta = _make_frontier_meta()
        resolved = _make_resolved_content()

        with (
            patch("seed_storage.worker.tasks.get_frontier_meta", return_value=meta),
            patch("seed_storage.worker.tasks.remove_from_frontier") as mock_remove,
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
            patch("seed_storage.worker.tasks.ingest_episode"),
            patch("seed_storage.worker.tasks.add_to_frontier"),
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            mock_get_redis.return_value = r
            mock_asyncio.run.return_value = resolved

            from seed_storage.worker.tasks import expand_from_frontier

            expand_from_frontier(url_hash_str)

            mock_remove.assert_called_once_with(r, url_hash_str)


class TestMissingMetadata:
    """Missing metadata must be handled gracefully."""

    def test_no_metadata_returns_early(self):
        """Missing frontier metadata causes early return."""
        url_hash_str = "missing_meta_hash"

        with (
            patch("seed_storage.worker.tasks.get_frontier_meta", return_value=None),
            patch("seed_storage.worker.tasks.remove_from_frontier") as mock_remove,
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            mock_get_redis.return_value = r

            from seed_storage.worker.tasks import expand_from_frontier

            expand_from_frontier(url_hash_str)

            mock_asyncio.run.assert_not_called()
            mock_remove.assert_not_called()
