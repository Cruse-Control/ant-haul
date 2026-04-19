"""tests/unit/tasks/test_enrich_message.py

Unit tests for the enrich_message Celery task.
All external dependencies (Redis, ContentDispatcher, asyncio.run) are mocked.
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
    source_id: str = "msg123",
    source_channel: str = "general",
    author: str = "alice",
    content: str = "Hello world https://example.com",
    attachments: list[str] | None = None,
    channel_id: str = "chan456",
) -> dict:
    return {
        "source_type": source_type,
        "source_id": source_id,
        "source_channel": source_channel,
        "author": author,
        "content": content,
        "timestamp": "2026-01-01T00:00:00+00:00",
        "attachments": attachments or [],
        "metadata": {"channel_id": channel_id, "author_id": "user789", "guild_id": "g1"},
    }


def _make_resolved_content(url: str = "https://example.com") -> ResolvedContent:
    return ResolvedContent(
        source_url=url,
        content_type="webpage",
        title="Example",
        text="Some page content",
        transcript=None,
        summary=None,
        expansion_urls=[],
        metadata={},
        extraction_error=None,
        resolved_at=datetime.now(tz=UTC),
    )


# ---------------------------------------------------------------------------
# Import the task under test (with Celery in always-eager mode)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def celery_always_eager(monkeypatch):
    """Make Celery tasks execute synchronously in tests."""
    from seed_storage.worker.app import app

    app.conf.task_always_eager = True
    yield
    app.conf.task_always_eager = False


@pytest.fixture()
def mock_redis():
    """Mock Redis client that returns sensible defaults."""
    r = MagicMock()
    # SADD returns 1 (new) by default; test overrides as needed
    r.sadd.return_value = 1
    r.publish.return_value = 1
    return r


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEnrichMessageSkipRules:
    """Contract 1 error rules: empty message and bot author."""

    def test_skip_empty_content_and_no_attachments(self):
        """Messages with empty content and no attachments must be skipped."""
        payload = _make_raw_payload(content="", attachments=[])

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
        ):
            mock_get_redis.return_value = MagicMock()

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            mock_ingest.delay.assert_not_called()

    def test_skip_whitespace_only_content_no_attachments(self):
        """Whitespace-only content with no attachments should be skipped."""
        payload = _make_raw_payload(content="   ", attachments=[])

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
        ):
            mock_get_redis.return_value = MagicMock()

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            mock_ingest.delay.assert_not_called()

    def test_process_content_with_attachment_no_text(self):
        """Message with no text but has attachments should be processed."""
        payload = _make_raw_payload(
            content="",
            attachments=["https://cdn.example.com/image.png"],
        )

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r

            resolved = _make_resolved_content("https://cdn.example.com/image.png")
            mock_asyncio.run.return_value = [resolved]

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            mock_ingest.delay.assert_called_once()


class TestEnrichMessageDedup:
    """Dedup via seed:seen_messages and seed:seen_urls."""

    def test_duplicate_message_skipped(self):
        """Second occurrence of same source_id must be skipped."""
        payload = _make_raw_payload()

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
        ):
            r = MagicMock()
            # SADD returns 0 → already seen
            r.sadd.return_value = 0
            mock_get_redis.return_value = r

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            mock_ingest.delay.assert_not_called()

    def test_new_message_is_processed(self):
        """First occurrence of a message must be processed."""
        payload = _make_raw_payload(content="Plain text no URLs")

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r
            mock_asyncio.run.return_value = []

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            mock_ingest.delay.assert_called_once()

    def test_url_dedup_skips_seen_url(self):
        """URLs already in seed:seen_urls should not be re-resolved."""
        payload = _make_raw_payload(content="check https://already-seen.com")

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            # Message dedup → new (sadd=1 for messages set)
            # URL dedup → already seen (sadd=0 for urls set)
            r.sadd.side_effect = [1, 0]  # message new, url seen
            r.publish.return_value = 1
            mock_get_redis.return_value = r
            # asyncio.run should not be called (no new URLs)
            mock_asyncio.run.return_value = []

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            # ingest_episode still called with empty resolved_contents
            mock_ingest.delay.assert_called_once()
            call_args = mock_ingest.delay.call_args[0][0]
            assert call_args["resolved_contents"] == []


class TestEnrichMessageURLExtraction:
    """URL extraction from content and attachments."""

    def test_extracts_urls_from_content(self):
        """URLs embedded in message content must be extracted."""
        payload = _make_raw_payload(
            content="Check https://example.com and https://github.com/user/repo",
            attachments=[],
        )

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r

            rc1 = _make_resolved_content("https://example.com")
            rc2 = _make_resolved_content("https://github.com/user/repo")
            mock_asyncio.run.return_value = [rc1, rc2]

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            call_args = mock_ingest.delay.call_args[0][0]
            assert len(call_args["resolved_contents"]) == 2

    def test_attachments_treated_as_urls(self):
        """Attachment URLs must be resolved just like content URLs."""
        payload = _make_raw_payload(
            content="",
            attachments=["https://cdn.example.com/file.pdf"],
        )

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r

            rc = _make_resolved_content("https://cdn.example.com/file.pdf")
            mock_asyncio.run.return_value = [rc]

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            call_args = mock_ingest.delay.call_args[0][0]
            assert len(call_args["resolved_contents"]) == 1

    def test_no_urls_plain_text(self):
        """Messages with no URLs produce enriched_payload with empty resolved_contents."""
        payload = _make_raw_payload(content="Just plain text no links here", attachments=[])

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r
            mock_asyncio.run.return_value = []

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            call_args = mock_ingest.delay.call_args[0][0]
            assert call_args["resolved_contents"] == []

    def test_multiple_urls_resolved_independently(self):
        """Multiple URLs must all be resolved, not just the first."""
        urls = [
            "https://a.com",
            "https://b.com",
            "https://c.com",
        ]
        payload = _make_raw_payload(content=" ".join(urls), attachments=[])

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r

            resolved = [_make_resolved_content(u) for u in urls]
            mock_asyncio.run.return_value = resolved

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            call_args = mock_ingest.delay.call_args[0][0]
            assert len(call_args["resolved_contents"]) == 3


class TestEnrichMessagePayloadShape:
    """Resulting enriched_payload must match Contract 2."""

    def test_enriched_payload_contains_original_message(self):
        """enriched_payload['message'] must be the original raw_payload."""
        payload = _make_raw_payload()

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r

            rc = _make_resolved_content("https://example.com")
            mock_asyncio.run.return_value = [rc]

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            call_args = mock_ingest.delay.call_args[0][0]
            assert call_args["message"] == payload

    def test_enriched_payload_resolved_contents_are_dicts(self):
        """resolved_contents must be a list of dicts (rc.to_dict() output)."""
        payload = _make_raw_payload()

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r

            rc = _make_resolved_content("https://example.com")
            mock_asyncio.run.return_value = [rc]

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            call_args = mock_ingest.delay.call_args[0][0]
            for item in call_args["resolved_contents"]:
                assert isinstance(item, dict)
                assert "source_url" in item
                assert "content_type" in item


class TestEnrichMessageReactionEvents:
    """Reaction events must be published to seed:reactions."""

    def test_reactions_published_to_redis(self):
        """📥 and ⚙️ reactions must be published on success."""
        payload = _make_raw_payload(
            content="hello",
            attachments=[],
            source_id="msg999",
            channel_id="ch1",
        )

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode"),
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r
            mock_asyncio.run.return_value = []

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            # Should have published at least 2 reactions (📥 + ⚙️)
            assert r.publish.call_count >= 2
            published_channels = [c[0][0] for c in r.publish.call_args_list]
            assert all(ch == "seed:reactions" for ch in published_channels)


class TestEnrichMessagePartialFailure:
    """Partial failures during URL resolution must be tolerated."""

    def test_partial_resolution_failure_still_ingests(self):
        """If one URL fails to resolve, others should still be ingested."""
        payload = _make_raw_payload(
            content="https://good.com https://bad.com",
            attachments=[],
        )

        with (
            patch("seed_storage.worker.tasks._get_redis") as mock_get_redis,
            patch("seed_storage.worker.tasks.ingest_episode") as mock_ingest,
            patch("seed_storage.worker.tasks.asyncio") as mock_asyncio,
        ):
            r = MagicMock()
            r.sadd.return_value = 1
            r.publish.return_value = 1
            mock_get_redis.return_value = r

            good_rc = _make_resolved_content("https://good.com")
            bad_rc = ResolvedContent.error_result("https://bad.com", "timeout")
            mock_asyncio.run.return_value = [good_rc, bad_rc]

            from seed_storage.worker.tasks import enrich_message

            enrich_message(payload)

            # Both results should be in enriched_payload
            call_args = mock_ingest.delay.call_args[0][0]
            assert len(call_args["resolved_contents"]) == 2
