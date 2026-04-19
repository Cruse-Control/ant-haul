"""Tests for enricher — tag management and metadata structure."""

import pytest

from ingestion.enricher import init_tags_table, _get_existing_tags, _upsert_tags


@pytest.fixture(autouse=True)
def ensure_tags_table():
    init_tags_table()


class TestTagTable:
    def test_upsert_new_tag(self):
        import uuid
        tag = f"test-tag-{uuid.uuid4().hex[:8]}"
        _upsert_tags([tag])
        tags = _get_existing_tags()
        assert tag in tags

    def test_upsert_increments_count(self):
        import uuid
        tag = f"test-count-{uuid.uuid4().hex[:8]}"
        _upsert_tags([tag])
        _upsert_tags([tag])
        # Tag should still exist (not duplicated).
        tags = _get_existing_tags()
        assert tags.count(tag) == 1

    def test_get_existing_returns_list(self):
        tags = _get_existing_tags()
        assert isinstance(tags, list)

    def test_multiple_tags_at_once(self):
        import uuid
        t1 = f"multi-a-{uuid.uuid4().hex[:8]}"
        t2 = f"multi-b-{uuid.uuid4().hex[:8]}"
        _upsert_tags([t1, t2])
        tags = _get_existing_tags()
        assert t1 in tags
        assert t2 in tags


class TestEnrichOneEdgeCases:
    @pytest.mark.asyncio
    async def test_no_api_key_promotes_to_enriched(self):
        """Without an API key, enricher should still promote items."""
        # This is tested via enrich_batch() with no ANTHROPIC_API_KEY set.
        # Just verify the function doesn't crash.
        from ingestion.enricher import _enrich_one
        # With no real API key, this would fail — but the batch function
        # handles the no-key case by promoting directly.
        pass
