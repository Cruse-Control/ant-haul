"""Graphiti search wrapper.

Provides a high-level async search function that wraps Graphiti's search API.
All searches are scoped to group_id="seed-storage".
"""

from __future__ import annotations

from graphiti_core.edges import EntityEdge

from seed_storage.graphiti_client import GROUP_ID, get_graphiti


async def search(query: str, num_results: int = 10) -> list[EntityEdge]:
    """Search the knowledge graph for entities matching the query.

    Args:
        query: Natural language search query.
        num_results: Maximum number of results to return. Defaults to 10.

    Returns:
        List of EntityEdge objects matching the query, scoped to group_id="seed-storage".

    Raises:
        Exception: Propagates any Graphiti search errors to the caller.
    """
    graphiti = await get_graphiti()
    results = await graphiti.search(
        query=query,
        group_ids=[GROUP_ID],
        num_results=num_results,
    )
    return results
