#!/usr/bin/env python3
"""
Smoke test for Graphiti temporal knowledge graph.

Tests: init, add episode, search, cleanup.
Run: python -m tests.smoke_graphiti

Requires: GEMINI_API_KEY, ANTHROPIC_API_KEY, NEO4J_URI
"""

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from seed_storage import graphiti_client

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
results: list[tuple[str, bool, str]] = []


def report(name: str, passed: bool, detail: str = ""):
    results.append((name, passed, detail))
    icon = PASS if passed else FAIL
    print(f"  {icon} {name}" + (f" — {detail}" if detail else ""))


async def test_init():
    """Test 1: Can we initialize Graphiti with Anthropic + Gemini?"""
    try:
        g = await graphiti_client.get_graphiti()
        assert g is not None
        report("Graphiti init (Haiku + Gemini)", True)
    except Exception as e:
        report("Graphiti init (Haiku + Gemini)", False, str(e))


async def test_add_text_episode():
    """Test 2: Add a plain text episode."""
    try:
        await graphiti_client.add_episode(
            name="smoke-test-text",
            content="CruseControl is a startup founded by Flynn and Wyler. They are building AI agent infrastructure using Neo4j and ant-keeper.",
            source="text",
            source_description="Smoke test — plain text",
        )
        report("Add text episode", True)
    except Exception as e:
        report("Add text episode", False, str(e))


async def test_add_discord_episode():
    """Test 3: Add a Discord-style message episode."""
    try:
        await graphiti_client.add_episode(
            name="smoke-test-discord",
            content="Flynn: Hey Wyler, I set up the Granola sync task. It posts meeting notes to #granola-flynn daily at 8pm.\nWyler: Nice, mine is running too. Did you see the knowledge graph plan?",
            source="text",
            source_description="Smoke test — Discord conversation",
        )
        report("Add Discord episode", True)
    except Exception as e:
        report("Add Discord episode", False, str(e))


async def test_add_url_episode():
    """Test 4: Add extracted URL content as an episode."""
    try:
        from seed_storage.extractor import extract_url

        extracted = await extract_url("https://neo4j.com/blog/graphrag-manifesto/")
        # Truncate for cost — Haiku handles this fine
        content = extracted["content"][:2000]
        await graphiti_client.add_episode(
            name="smoke-test-url",
            content=content,
            source="text",
            source_description=f"Web page: {extracted['title']}",
        )
        report("Add URL episode", True, f"{extracted['word_count']} words extracted")
    except Exception as e:
        report("Add URL episode", False, str(e))


async def test_search():
    """Test 5: Search the Graphiti graph."""
    try:
        results = await graphiti_client.search("What is CruseControl building?")
        report("Search Graphiti", True, f"{len(results)} result(s)")
        for r in results[:3]:
            fact = getattr(r, "fact", None) or str(r)[:100]
            print(f"        → {fact}")
    except Exception as e:
        report("Search Graphiti", False, str(e))


async def test_search_entities():
    """Test 6: Search for a specific entity."""
    try:
        results = await graphiti_client.search("Flynn and Wyler")
        report("Search entities", True, f"{len(results)} result(s)")
    except Exception as e:
        report("Search entities", False, str(e))


async def test_cleanup():
    """Cleanup: remove smoke test episodes from Neo4j."""
    try:
        from seed_storage.graph import get_driver

        driver = await get_driver()
        async with driver.session() as session:
            # Clean up Graphiti's episode nodes from our smoke tests
            result = await session.run(
                """
                MATCH (e:EpisodicNode)
                WHERE e.name STARTS WITH 'smoke-test-'
                DETACH DELETE e
                RETURN count(e) AS deleted
                """
            )
            record = await result.single()
            report("Cleanup episodes", True, f"deleted {record['deleted']} episode(s)")
    except Exception as e:
        report("Cleanup episodes", False, str(e))


async def main():
    print("\n🌳 Graphiti Smoke Tests\n")
    start = time.time()

    await test_init()
    await test_add_text_episode()
    await test_add_discord_episode()
    await test_add_url_episode()
    await test_search()
    await test_search_entities()
    await test_cleanup()

    await graphiti_client.close()

    elapsed = time.time() - start
    passed = sum(1 for _, p, _ in results if p)
    failed = sum(1 for _, p, _ in results if not p)
    print(f"\n  {passed} passed, {failed} failed ({elapsed:.1f}s)\n")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
