"""Tests for the seed-storage MCP server tools."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestSearchGraph:
    @pytest.mark.asyncio
    async def test_returns_formatted_edges(self):
        fake_edge = MagicMock()
        fake_edge.uuid = "abc-123"
        fake_edge.name = "relates_to"
        fake_edge.fact = "Python is a programming language"
        fake_edge.source_node_uuid = "node-1"
        fake_edge.target_node_uuid = "node-2"
        fake_edge.episodes = ["ep-1"]
        fake_edge.created_at = None
        fake_edge.valid_at = None

        with patch("seed_storage.mcp_server.search", new_callable=AsyncMock, return_value=[fake_edge]):
            from seed_storage.mcp_server import search_graph
            results = await search_graph("python", limit=5)

        assert len(results) == 1
        assert results[0]["fact"] == "Python is a programming language"
        assert results[0]["uuid"] == "abc-123"

    @pytest.mark.asyncio
    async def test_empty_results(self):
        with patch("seed_storage.mcp_server.search", new_callable=AsyncMock, return_value=[]):
            from seed_storage.mcp_server import search_graph
            results = await search_graph("nonexistent")

        assert results == []


class TestRecent:
    @pytest.mark.asyncio
    async def test_returns_loaded_items(self):
        fake_items = [
            {
                "source_type": "github",
                "source_uri": "https://github.com/test/repo",
                "author": "testuser",
                "channel": "gh-inspirational-materials",
                "created_at": None,
                "word_count": 100,
                "metadata": {"tags": ["ai"]},
            }
        ]

        with patch("seed_storage.mcp_server.staging") as mock_staging:
            mock_staging.get_staged.return_value = fake_items
            from seed_storage.mcp_server import recent
            results = await recent(limit=5)

        assert len(results) == 1
        assert results[0]["source_type"] == "github"
        assert results[0]["source_uri"] == "https://github.com/test/repo"

    @pytest.mark.asyncio
    async def test_clamps_limit(self):
        with patch("seed_storage.mcp_server.staging") as mock_staging:
            mock_staging.get_staged.return_value = []
            from seed_storage.mcp_server import recent
            await recent(limit=100)
            mock_staging.get_staged.assert_called_with(status="loaded", limit=50)


class TestStatus:
    @pytest.mark.asyncio
    async def test_returns_counts(self):
        with patch("seed_storage.mcp_server.staging") as mock_staging:
            mock_staging.count_by_status.return_value = {"loaded": 358, "enriched": 1727, "failed": 5}
            from seed_storage.mcp_server import status
            result = await status()

        assert result["loaded"] == 358
        assert result["enriched"] == 1727


class TestGetContext:
    @pytest.mark.asyncio
    async def test_entity_not_found(self):
        mock_result = MagicMock()
        mock_result.records = []

        mock_driver = AsyncMock()
        mock_driver.execute_query = AsyncMock(return_value=mock_result)

        mock_graphiti = MagicMock()
        mock_graphiti.driver = mock_driver

        with patch("seed_storage.mcp_server.get_graphiti", new_callable=AsyncMock, return_value=mock_graphiti):
            from seed_storage.mcp_server import get_context
            result = await get_context("nonexistent_entity")

        assert result["found"] is False
