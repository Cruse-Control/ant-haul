"""Tests for express ingest — single-URL full pipeline."""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest


class TestExpressIngest:
    @pytest.mark.asyncio
    async def test_new_url_full_pipeline(self):
        """A new URL should go through stage → process → enrich → load."""
        from seed_storage import staging
        staging.init_tables()

        fake_item = {
            "id": "eeeeeeee-0000-0000-0000-000000000001",
            "source_type": "web",
            "source_uri": "https://example.com/express-test",
            "raw_content": "https://example.com/express-test",
            "status": "staged",
            "author": "test",
            "channel": "test",
            "created_at": None,
            "metadata": {},
        }
        processed_item = {**fake_item, "status": "processed", "raw_content": "# Test Article\n\nContent here"}
        enriched_item = {**processed_item, "status": "enriched", "metadata": {"tags": ["testing"]}}

        call_sequence = []

        async def mock_process_one(item, http, anthropic=None, analyzer_url=""):
            call_sequence.append("process")

        with (
            patch("ingestion.express.staging.stage", return_value="eeeeeeee-0000-0000-0000-000000000001"),
            patch("ingestion.express.staging.get_by_id", side_effect=[fake_item, processed_item, enriched_item]),
            patch("ingestion.express.staging.get_by_uri"),
            patch("ingestion.express.staging.update_content") as mock_update_content,
            patch("ingestion.express.staging.update_status") as mock_update_status,
            patch("ingestion.express.process_one", side_effect=mock_process_one),
            patch("ingestion.express._enrich_one", new_callable=AsyncMock, return_value={"tags": ["test"], "summary": "A test"}),
            patch("ingestion.express._get_existing_tags", return_value=["ai"]),
            patch("ingestion.express._upsert_tags"),
            patch("ingestion.express.init_tags_table"),
            patch("ingestion.express.add_episode", new_callable=AsyncMock),
        ):
            from ingestion.express import express_ingest
            result = await express_ingest("https://example.com/express-test")

        assert result["status"] == "loaded"
        assert result["source_uri"] == "https://example.com/express-test"
        assert "elapsed_seconds" in result
        assert "process" in call_sequence

    @pytest.mark.asyncio
    async def test_already_loaded_returns_immediately(self):
        """A URL that's already loaded should return without reprocessing."""
        loaded_item = {
            "id": "eeeeeeee-0000-0000-0000-000000000002",
            "source_type": "web",
            "source_uri": "https://example.com/already-loaded",
            "raw_content": "content",
            "status": "loaded",
            "author": "test",
            "channel": "test",
            "created_at": None,
            "metadata": {},
        }

        with (
            patch("ingestion.express.staging.stage", return_value=None),
            patch("ingestion.express.staging.get_by_uri", return_value=loaded_item),
        ):
            from ingestion.express import express_ingest
            result = await express_ingest("https://example.com/already-loaded")

        assert result["status"] == "already_loaded"

    @pytest.mark.asyncio
    async def test_resumes_from_enriched(self):
        """An item already enriched should skip to loading."""
        enriched_item = {
            "id": "eeeeeeee-0000-0000-0000-000000000003",
            "source_type": "web",
            "source_uri": "https://example.com/enriched",
            "raw_content": "# Article\n\nEnriched content",
            "status": "enriched",
            "author": "test",
            "channel": "test",
            "created_at": None,
            "metadata": {"tags": ["ai"]},
        }

        with (
            patch("ingestion.express.staging.stage", return_value=None),
            patch("ingestion.express.staging.get_by_uri", return_value=enriched_item),
            patch("ingestion.express.staging.update_status") as mock_status,
            patch("ingestion.express.add_episode", new_callable=AsyncMock) as mock_add,
        ):
            from ingestion.express import express_ingest
            result = await express_ingest("https://example.com/enriched")

        assert result["status"] == "loaded"
        mock_add.assert_called_once()
        # Should have updated status to loaded
        loaded_calls = [c for c in mock_status.call_args_list if c[0][1] == "loaded"]
        assert len(loaded_calls) == 1
