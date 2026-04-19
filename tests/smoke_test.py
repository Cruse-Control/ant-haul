#!/usr/bin/env python3
"""
Smoke tests for seed-storage pipeline.

Tests each medium and component end-to-end against live Neo4j + Gemini.
Run: python -m tests.smoke_test

Requires env vars: GEMINI_API_KEY, NEO4J_URI (defaults to bolt://127.0.0.1:30687)
"""

import asyncio
import os
import sys
import time

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from seed_storage import config, graph, embeddings
from seed_storage.extractor import extract_url

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
results: list[tuple[str, bool, str]] = []


def report(name: str, passed: bool, detail: str = ""):
    results.append((name, passed, detail))
    icon = PASS if passed else FAIL
    print(f"  {icon} {name}" + (f" — {detail}" if detail else ""))


async def test_neo4j_connectivity():
    """Test 1: Can we connect to Neo4j?"""
    try:
        driver = await graph.get_driver()
        await driver.verify_connectivity()
        report("Neo4j connectivity", True)
    except Exception as e:
        report("Neo4j connectivity", False, str(e))


async def test_schema_init():
    """Test 2: Can we initialize the schema (idempotent)?"""
    try:
        await graph.init_schema()
        report("Schema initialization", True)
    except Exception as e:
        report("Schema initialization", False, str(e))


async def test_embed_text():
    """Test 3: Can we embed plain text via Gemini?"""
    try:
        vec = await embeddings.embed_text("CruseControl is building a knowledge graph.")
        assert len(vec) == config.EMBEDDING_DIMS, f"Expected {config.EMBEDDING_DIMS} dims, got {len(vec)}"
        assert all(isinstance(v, float) for v in vec[:5])
        report("Embed text (Gemini)", True, f"{len(vec)} dims")
    except Exception as e:
        report("Embed text (Gemini)", False, str(e))


async def test_embed_batch():
    """Test 4: Can we batch-embed multiple texts?"""
    try:
        texts = ["Hello world", "Knowledge graphs are powerful", "Neo4j + Gemini"]
        vecs = await embeddings.embed_texts(texts)
        assert len(vecs) == 3
        assert all(len(v) == config.EMBEDDING_DIMS for v in vecs)
        report("Embed batch (Gemini)", True, f"{len(vecs)} texts")
    except Exception as e:
        report("Embed batch (Gemini)", False, str(e))


async def test_create_source_node():
    """Test 5: Can we create a Source node with embedding?"""
    try:
        vec = await embeddings.embed_text("Test source content for smoke testing.")
        source_id = await graph.create_source(
            source_type="test",
            source_uri="test://smoke-test/source-1",
            raw_content="Test source content for smoke testing.",
            embedding=vec,
            author="smoke-test",
            channel="test",
        )
        assert source_id
        report("Create Source node", True, f"id={source_id[:8]}...")
        return source_id
    except Exception as e:
        report("Create Source node", False, str(e))
        return None


async def test_create_fact_node(source_id: str):
    """Test 6: Can we create a Fact linked to a Source?"""
    try:
        vec = await embeddings.embed_text("CruseControl uses Neo4j for its knowledge graph.")
        fact_id = await graph.create_fact(
            statement="CruseControl uses Neo4j for its knowledge graph.",
            embedding=vec,
            confidence=0.95,
            source_id=source_id,
        )
        assert fact_id
        report("Create Fact node", True, f"id={fact_id[:8]}...")
        return fact_id
    except Exception as e:
        report("Create Fact node", False, str(e))
        return None


async def test_create_entity(fact_id: str):
    """Test 7: Can we create an Entity and link it to a Fact?"""
    try:
        vec = await embeddings.embed_text("Neo4j graph database")
        entity_id = await graph.create_entity(
            name="Neo4j",
            entity_type="tool",
            description="Graph database with native vector search",
            embedding=vec,
        )
        if fact_id:
            await graph.link_fact_entity(fact_id, entity_id)
        report("Create Entity + link", True, f"id={entity_id[:8]}...")
    except Exception as e:
        report("Create Entity + link", False, str(e))


async def test_vector_search():
    """Test 8: Can we do vector similarity search?"""
    try:
        vec = await embeddings.embed_text("knowledge graph database")
        results = await graph.vector_search(vec, label="Fact", limit=5)
        report("Vector search (Fact)", True, f"{len(results)} result(s)")
    except Exception as e:
        report("Vector search (Fact)", False, str(e))


async def test_vector_search_expansion():
    """Test 9: Vector search with graph expansion (provenance + entities)?"""
    try:
        vec = await embeddings.embed_text("knowledge graph database")
        results = await graph.vector_search_with_expansion(vec, limit=5)
        has_source = any(r.get("source") for r in results)
        has_entities = any(r.get("entities") for r in results)
        report(
            "Vector search + expansion",
            True,
            f"{len(results)} result(s), source={has_source}, entities={has_entities}",
        )
    except Exception as e:
        report("Vector search + expansion", False, str(e))


async def test_fulltext_search():
    """Test 10: Does fulltext search work?"""
    try:
        results = await graph.fulltext_search("Neo4j knowledge graph", label="Fact", limit=5)
        report("Fulltext search (Fact)", True, f"{len(results)} result(s)")
    except Exception as e:
        report("Fulltext search (Fact)", False, str(e))


async def test_extract_url():
    """Test 11: Can we extract content from a URL?"""
    try:
        result = await extract_url("https://neo4j.com/blog/graphrag-manifesto/")
        assert result["content"]
        assert result["word_count"] > 50
        report("URL extraction", True, f"'{result['title'][:40]}...' ({result['word_count']} words)")
    except Exception as e:
        report("URL extraction", False, str(e))


async def test_url_to_graph():
    """Test 12: Full pipeline — extract URL, embed, store as Source."""
    try:
        extracted = await extract_url("https://neo4j.com/blog/graphrag-manifesto/")
        vec = await embeddings.embed_text(extracted["content"][:8000])
        source_id = await graph.create_source(
            source_type="web_page",
            source_uri=extracted["url"],
            raw_content=extracted["content"][:5000],
            embedding=vec,
            author="smoke-test",
            channel="test",
        )
        report("URL → embed → Source node", True, f"id={source_id[:8]}...")
    except Exception as e:
        report("URL → embed → Source node", False, str(e))


async def test_graph_stats():
    """Test 13: Can we get graph statistics?"""
    try:
        stats = await graph.get_stats()
        total_nodes = sum(stats.get("nodes", {}).values())
        total_rels = sum(stats.get("relationships", {}).values())
        report("Graph stats", True, f"{total_nodes} nodes, {total_rels} relationships")
    except Exception as e:
        report("Graph stats", False, str(e))


async def test_cleanup():
    """Cleanup: remove smoke test nodes."""
    try:
        driver = await graph.get_driver()
        async with driver.session() as session:
            result = await session.run(
                "MATCH (n) WHERE n.author = 'smoke-test' OR n.channel = 'test' DETACH DELETE n RETURN count(n) AS deleted"
            )
            record = await result.single()
            report("Cleanup test nodes", True, f"deleted {record['deleted']} node(s)")
    except Exception as e:
        report("Cleanup test nodes", False, str(e))


async def main():
    print("\n🌱 Seed Storage Smoke Tests\n")
    start = time.time()

    await test_neo4j_connectivity()
    await test_schema_init()
    await test_embed_text()
    await test_embed_batch()

    source_id = await test_create_source_node()
    fact_id = await test_create_fact_node(source_id) if source_id else None
    await test_create_entity(fact_id)

    await test_vector_search()
    await test_vector_search_expansion()
    await test_fulltext_search()

    await test_extract_url()
    await test_url_to_graph()
    await test_graph_stats()
    await test_cleanup()

    await graph.close()

    elapsed = time.time() - start
    passed = sum(1 for _, p, _ in results if p)
    failed = sum(1 for _, p, _ in results if not p)
    print(f"\n  {passed} passed, {failed} failed ({elapsed:.1f}s)\n")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
