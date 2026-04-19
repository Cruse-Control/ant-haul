"""MCP server exposing the seed-storage knowledge graph to Claude Code sessions.

Run as: uv run python -m seed_storage.mcp_server
"""

from __future__ import annotations

import asyncio
import logging

from mcp.server.fastmcp import FastMCP

from seed_storage import config, staging
from seed_storage.graphiti_client import get_graphiti, search, close

log = logging.getLogger("seed-storage-mcp")

mcp = FastMCP(
    "seed-storage",
    instructions="Query the CruseControl knowledge graph (Neo4j + Graphiti)",
)


def _edge_to_dict(edge) -> dict:
    """Convert an EntityEdge to a serializable dict."""
    return {
        "uuid": edge.uuid,
        "name": edge.name,
        "fact": edge.fact,
        "source_node": edge.source_node_uuid,
        "target_node": edge.target_node_uuid,
        "episodes": edge.episodes,
        "created_at": str(edge.created_at) if edge.created_at else None,
        "valid_at": str(edge.valid_at) if edge.valid_at else None,
    }


@mcp.tool()
async def search_graph(query: str, limit: int = 10) -> list[dict]:
    """Search the knowledge graph using hybrid vector + fulltext search.

    Args:
        query: Natural language search query
        limit: Maximum number of results (default 10)

    Returns facts/relationships matching the query, ranked by relevance.
    """
    results = await search(query, limit=limit)
    return [_edge_to_dict(r) for r in results]


@mcp.tool()
async def get_context(entity: str) -> dict:
    """Get full context for an entity — all connected facts, sources, and relationships.

    Args:
        entity: Name of the entity to look up (person, concept, project, etc.)

    Returns the entity's relationships grouped by direction (incoming/outgoing).
    """
    g = await get_graphiti()
    driver = g.driver

    # Find the entity node by name
    result = await driver.execute_query(
        "MATCH (n:Entity) WHERE toLower(n.name) CONTAINS toLower($name) RETURN n LIMIT 5",
        params={"name": entity},
    )

    if not result.records:
        return {"entity": entity, "found": False, "message": "No entity found matching that name"}

    node = result.records[0]["n"]
    node_id = node.get("uuid") or node.element_id

    # Get all connected edges
    out_result = await driver.execute_query(
        """MATCH (n:Entity {uuid: $uuid})-[r:RELATES_TO]->(m:Entity)
           RETURN r.name AS rel, r.fact AS fact, m.name AS target LIMIT 25""",
        params={"uuid": node_id},
    )
    in_result = await driver.execute_query(
        """MATCH (m:Entity)-[r:RELATES_TO]->(n:Entity {uuid: $uuid})
           RETURN r.name AS rel, r.fact AS fact, m.name AS source LIMIT 25""",
        params={"uuid": node_id},
    )

    return {
        "entity": node.get("name", entity),
        "found": True,
        "summary": node.get("summary", ""),
        "outgoing": [{"relationship": r["rel"], "fact": r["fact"], "target": r["target"]} for r in out_result.records],
        "incoming": [{"relationship": r["rel"], "fact": r["fact"], "source": r["source"]} for r in in_result.records],
    }


@mcp.tool()
async def explore(concept: str, depth: int = 2) -> dict:
    """Explore a concept — search + expand to related themes and domains.

    Args:
        concept: The concept, theme, or domain to explore
        depth: How many hops to traverse (1-3, default 2)

    Returns search results plus connected concepts for broader discovery.
    """
    depth = max(1, min(3, depth))

    # Start with a search
    results = await search(concept, limit=5)
    facts = [_edge_to_dict(r) for r in results]

    # Expand via graph traversal
    g = await get_graphiti()
    driver = g.driver

    related_result = await driver.execute_query(
        f"""MATCH (n:Entity)
            WHERE toLower(n.name) CONTAINS toLower($concept)
            MATCH path = (n)-[r:RELATES_TO*1..{depth}]-(m:Entity)
            RETURN DISTINCT m.name AS name, m.summary AS summary
            LIMIT 20""",
        params={"concept": concept},
    )

    return {
        "concept": concept,
        "facts": facts,
        "related_entities": [{"name": r["name"], "summary": r.get("summary", "")} for r in related_result.records],
    }


@mcp.tool()
async def recent(limit: int = 10) -> list[dict]:
    """Get the most recently loaded items from the knowledge graph.

    Args:
        limit: Number of items to return (default 10, max 50)

    Returns recently loaded items with their metadata.
    """
    limit = max(1, min(50, limit))
    items = staging.get_staged(status="loaded", limit=limit)
    return [
        {
            "source_type": item["source_type"],
            "source_uri": item["source_uri"],
            "author": item.get("author", ""),
            "channel": item.get("channel", ""),
            "created_at": str(item["created_at"]) if item.get("created_at") else None,
            "word_count": item.get("word_count", 0),
            "metadata": item.get("metadata", {}),
        }
        for item in items
    ]


@mcp.tool()
async def status() -> dict:
    """Get pipeline status — item counts by status.

    Returns counts of items in each pipeline stage (staged, processed, enriched, loaded, failed, etc.)
    """
    return staging.count_by_status()


@mcp.tool()
async def express_ingest_url(url: str) -> dict:
    """Immediately ingest a URL into the knowledge graph (5-15 seconds).

    Runs the full pipeline: stage, process (extract content), enrich (tags/summary),
    and load into Neo4j via Graphiti. Use when you need content available as context
    RIGHT NOW instead of waiting for the scheduled pipeline.

    Args:
        url: The URL to ingest (GitHub repo, YouTube video, web article, X post, etc.)

    Returns status, elapsed time, and the source URI.
    """
    from ingestion.express import express_ingest as _express
    return await _express(url=url, author="mcp-express", channel="mcp-express")


@mcp.tool()
async def rush_item(source_uri: str) -> dict:
    """Rush a previously staged item through the pipeline immediately.

    Use when something was posted to Discord but hasn't been loaded into the
    knowledge graph yet. Picks up from whatever stage the item is currently at.

    Args:
        source_uri: The URL or discord:// URI of the staged item to rush.
    """
    from ingestion.express import express_ingest as _express
    return await _express(url=source_uri, author="mcp-rush", channel="mcp-rush")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mcp.run()
