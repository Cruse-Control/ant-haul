"""Seed Storage API — FastAPI server for the knowledge graph."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from . import config, graph, embeddings


@asynccontextmanager
async def lifespan(app: FastAPI):
    await graph.init_schema()
    yield
    await graph.close()


app = FastAPI(title="Seed Storage", lifespan=lifespan)


# ── Models ─────────────────────────────────────────────────────────


class IngestRequest(BaseModel):
    content: str
    source_type: str = "manual"
    source_uri: str = ""
    author: str = "unknown"
    channel: str = ""
    media_type: str = "text"
    metadata: dict[str, Any] | None = None


class QueryRequest(BaseModel):
    query: str
    mode: str = "hybrid"  # "vector" | "fulltext" | "hybrid"
    tiers: list[str] = ["fact"]
    limit: int = 10
    expand: bool = True


class IngestResponse(BaseModel):
    source_id: str
    message: str = "Ingested successfully"


class QueryResult(BaseModel):
    results: list[dict[str, Any]]
    count: int


# ── Endpoints ──────────────────────────────────────────────────────


@app.get("/health")
async def health():
    try:
        driver = await graph.get_driver()
        await driver.verify_connectivity()
        return {"status": "ok", "neo4j": "connected"}
    except Exception as e:
        raise HTTPException(503, detail=f"Neo4j unavailable: {e}")


@app.post("/api/ingest", response_model=IngestResponse)
async def ingest(req: IngestRequest):
    """Ingest raw content as a Source node with embedding."""
    embedding = await embeddings.embed_text(req.content)
    source_id = await graph.create_source(
        source_type=req.source_type,
        source_uri=req.source_uri,
        raw_content=req.content,
        media_type=req.media_type,
        embedding=embedding,
        author=req.author,
        channel=req.channel,
        metadata=req.metadata,
    )
    return IngestResponse(source_id=source_id)


@app.post("/api/query", response_model=QueryResult)
async def query(req: QueryRequest):
    """Hybrid search across the knowledge graph."""
    query_embedding = await embeddings.embed_text(req.query)

    results = []

    if req.mode in ("vector", "hybrid"):
        if req.expand and "fact" in req.tiers:
            vector_results = await graph.vector_search_with_expansion(
                query_embedding, limit=req.limit
            )
        else:
            for tier in req.tiers:
                label = tier.capitalize()
                tier_results = await graph.vector_search(
                    query_embedding, label=label, limit=req.limit
                )
                results.extend(tier_results)
            vector_results = results
        results = vector_results

    if req.mode in ("fulltext", "hybrid"):
        for tier in req.tiers:
            label = tier.capitalize()
            ft_results = await graph.fulltext_search(
                req.query, label=label, limit=req.limit
            )
            results.extend(ft_results)

    # Deduplicate by node id
    seen = set()
    unique = []
    for r in results:
        node = r.get("fact") or r.get("node")
        if node and node.get("id") not in seen:
            seen.add(node["id"])
            unique.append(r)

    # Sort by score descending
    unique.sort(key=lambda r: r.get("score", 0), reverse=True)

    return QueryResult(results=unique[: req.limit], count=len(unique))


@app.get("/api/stats")
async def stats():
    """Graph statistics."""
    return await graph.get_stats()


# ── Entrypoint ─────────────────────────────────────────────────────


def main():
    uvicorn.run(
        "seed_storage.api:app",
        host=config.API_HOST,
        port=config.API_PORT,
        log_level="info",
    )


if __name__ == "__main__":
    main()
