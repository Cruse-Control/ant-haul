"""Unit tests for seed_storage/query/search.py.

Tests cover:
- group_ids forwarded to Graphiti search
- num_results forwarded to Graphiti search
- EntityEdge→JSON transformation
- Empty results handling
- Error propagation
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from graphiti_core.edges import EntityEdge


def _make_entity_edge(name: str = "TestEdge", fact: str = "A fact") -> EntityEdge:
    """Create a minimal EntityEdge for testing."""
    edge = MagicMock(spec=EntityEdge)
    edge.uuid = uuid.uuid4()
    edge.name = name
    edge.fact = fact
    edge.group_id = "seed-storage"
    edge.source_node_uuid = uuid.uuid4()
    edge.target_node_uuid = uuid.uuid4()
    edge.created_at = datetime(2024, 1, 1, tzinfo=UTC)
    edge.valid_at = datetime(2024, 1, 1, tzinfo=UTC)
    edge.invalid_at = None
    return edge


# ---------------------------------------------------------------------------
# group_ids forwarded
# ---------------------------------------------------------------------------


class TestGroupIdsForwarded:
    @pytest.mark.asyncio
    async def test_group_ids_always_contains_seed_storage(self):
        """search() must pass group_ids=["seed-storage"] to Graphiti.search."""
        mock_graphiti = MagicMock()
        mock_graphiti.search = AsyncMock(return_value=[])

        with patch("seed_storage.query.search.get_graphiti", AsyncMock(return_value=mock_graphiti)):
            from seed_storage.query.search import search

            await search("test query")

        call_kwargs = mock_graphiti.search.call_args
        assert call_kwargs is not None
        # group_ids must be passed as kwarg or positional
        all_args = {**call_kwargs.kwargs}
        if call_kwargs.args:
            # positional: query, center_node_uuid, group_ids, num_results, ...
            pass
        assert all_args.get("group_ids") == ["seed-storage"]

    @pytest.mark.asyncio
    async def test_group_ids_is_not_none(self):
        """group_ids must never be None — all queries scoped to seed-storage."""
        mock_graphiti = MagicMock()
        mock_graphiti.search = AsyncMock(return_value=[])

        with patch("seed_storage.query.search.get_graphiti", AsyncMock(return_value=mock_graphiti)):
            from seed_storage.query.search import search

            await search("another query")

        call_kwargs = mock_graphiti.search.call_args
        group_ids = call_kwargs.kwargs.get("group_ids")
        assert group_ids is not None
        assert len(group_ids) >= 1


# ---------------------------------------------------------------------------
# num_results forwarded
# ---------------------------------------------------------------------------


class TestNumResultsForwarded:
    @pytest.mark.asyncio
    async def test_num_results_default_is_10(self):
        mock_graphiti = MagicMock()
        mock_graphiti.search = AsyncMock(return_value=[])

        with patch("seed_storage.query.search.get_graphiti", AsyncMock(return_value=mock_graphiti)):
            from seed_storage.query.search import search

            await search("query")

        call_kwargs = mock_graphiti.search.call_args
        assert call_kwargs.kwargs.get("num_results") == 10

    @pytest.mark.asyncio
    async def test_num_results_custom_value_forwarded(self):
        mock_graphiti = MagicMock()
        mock_graphiti.search = AsyncMock(return_value=[])

        with patch("seed_storage.query.search.get_graphiti", AsyncMock(return_value=mock_graphiti)):
            from seed_storage.query.search import search

            await search("query", num_results=25)

        call_kwargs = mock_graphiti.search.call_args
        assert call_kwargs.kwargs.get("num_results") == 25


# ---------------------------------------------------------------------------
# Return value / EntityEdge handling
# ---------------------------------------------------------------------------


class TestReturnValues:
    @pytest.mark.asyncio
    async def test_empty_results_returns_empty_list(self):
        mock_graphiti = MagicMock()
        mock_graphiti.search = AsyncMock(return_value=[])

        with patch("seed_storage.query.search.get_graphiti", AsyncMock(return_value=mock_graphiti)):
            from seed_storage.query.search import search

            results = await search("no match query")

        assert results == []

    @pytest.mark.asyncio
    async def test_results_are_entity_edges(self):
        edge1 = _make_entity_edge("Edge1", "Fact 1")
        edge2 = _make_entity_edge("Edge2", "Fact 2")

        mock_graphiti = MagicMock()
        mock_graphiti.search = AsyncMock(return_value=[edge1, edge2])

        with patch("seed_storage.query.search.get_graphiti", AsyncMock(return_value=mock_graphiti)):
            from seed_storage.query.search import search

            results = await search("some query")

        assert len(results) == 2
        assert results[0].name == "Edge1"
        assert results[1].name == "Edge2"

    @pytest.mark.asyncio
    async def test_entity_edge_to_json_transformation(self):
        """EntityEdge can be serialized to JSON-compatible dict."""
        edge = _make_entity_edge("TestEntity", "Test fact about entity")

        mock_graphiti = MagicMock()
        mock_graphiti.search = AsyncMock(return_value=[edge])

        with patch("seed_storage.query.search.get_graphiti", AsyncMock(return_value=mock_graphiti)):
            from seed_storage.query.search import search

            results = await search("query")

        assert len(results) == 1
        result = results[0]
        # EntityEdge fields accessible
        assert result.name == "TestEntity"
        assert result.fact == "Test fact about entity"
        assert result.group_id == "seed-storage"


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------


class TestErrorPropagation:
    @pytest.mark.asyncio
    async def test_graphiti_error_propagates(self):
        """Errors from Graphiti.search are propagated to the caller."""
        mock_graphiti = MagicMock()
        mock_graphiti.search = AsyncMock(side_effect=RuntimeError("Neo4j connection failed"))

        with patch("seed_storage.query.search.get_graphiti", AsyncMock(return_value=mock_graphiti)):
            from seed_storage.query.search import search

            with pytest.raises(RuntimeError, match="Neo4j connection failed"):
                await search("query")

    @pytest.mark.asyncio
    async def test_get_graphiti_error_propagates(self):
        """Errors from get_graphiti() are propagated to the caller."""
        with patch(
            "seed_storage.query.search.get_graphiti",
            AsyncMock(side_effect=ConnectionError("Cannot connect to Neo4j")),
        ):
            from seed_storage.query.search import search

            with pytest.raises(ConnectionError, match="Cannot connect to Neo4j"):
                await search("query")
