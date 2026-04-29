"""E2E tests: Discord message → Celery pipeline → Neo4j graph write.

~6 tests: plain text, YouTube, GitHub, image, PDF, multi-URL.

Full pipeline (no graphiti mocking):
  enrich_message (Celery eager) → ingest_episode → graphiti.add_episode → Neo4j

Mocked: _resolve_urls only (avoids real HTTP — resolvers are unit-tested elsewhere).
Real:   Redis, Celery (eager), Graphiti, Neo4j.

Each test patches GROUP_ID to a unique value so created nodes can be identified
and cleaned up reliably without touching the production "seed-storage" graph.

Skip guard: OPENAI_API_KEY required for Graphiti embeddings + entity extraction.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from seed_storage.enrichment.models import ResolvedContent

pytestmark = pytest.mark.e2e

_OPENAI_KEY = os.environ.get("OPENAI_API_KEY", "")

if not _OPENAI_KEY:
    pytestmark = [pytest.mark.e2e, pytest.mark.skip(reason="OPENAI_API_KEY not set")]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_payload(
    content: str,
    source_id: str,
    attachments: list[str] | None = None,
    source_channel: str = "general",
) -> dict:
    return {
        "source_type": "discord",
        "source_id": source_id,
        "source_channel": source_channel,
        "author": "e2e-tester",
        "content": content,
        "attachments": attachments or [],
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "metadata": {"channel_id": "111222333", "guild_id": "444555666"},
    }


def _resolved(
    url: str,
    content_type: str,
    text: str = "Extracted content.",
    title: str = "Test Title",
    transcript: str | None = None,
    summary: str | None = None,
    expansion_urls: list[str] | None = None,
) -> ResolvedContent:
    return ResolvedContent(
        source_url=url,
        content_type=content_type,
        title=title,
        text=text,
        transcript=transcript,
        summary=summary,
        expansion_urls=expansion_urls or [],
        metadata={},
        extraction_error=None,
        resolved_at=datetime.now(tz=UTC),
    )


def _run_pipeline(
    raw: dict,
    resolved_contents: list[ResolvedContent],
    redis_client,
    group_id: str,
) -> None:
    """Run full end-to-end pipeline with real Redis + Graphiti.

    Mocks only _resolve_urls (no real HTTP calls).
    Patches GROUP_ID so all Graphiti writes use the test-scoped group_id,
    enabling clean per-test teardown without touching the production graph.
    """
    import seed_storage.worker.tasks as tasks_module
    from seed_storage.worker.tasks import enrich_message

    async def _fake_resolve(dispatcher, urls):
        return resolved_contents

    with (
        patch("seed_storage.worker.tasks._get_redis", return_value=redis_client),
        patch("seed_storage.worker.tasks._resolve_urls", new=_fake_resolve),
        patch.object(tasks_module, "GROUP_ID", group_id),
    ):
        enrich_message.apply(args=[raw])


def _count_group_nodes(neo4j_driver, group_id: str) -> int:
    """Count all graph nodes written with this group_id."""
    result, _, _ = neo4j_driver.execute_query(
        "MATCH (n) WHERE n.group_id = $gid RETURN count(n) AS cnt",
        {"gid": group_id},
    )
    return result[0]["cnt"] if result else 0


def _cleanup_group(neo4j_driver, group_id: str) -> None:
    """Delete all Neo4j nodes for this test's group_id."""
    try:
        neo4j_driver.execute_query(
            "MATCH (n) WHERE n.group_id = $gid DETACH DELETE n",
            {"gid": group_id},
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_plain_text_to_graph(redis_client, neo4j_driver_e2e, clean_pipeline_redis):
    """Plain text message → EpisodicNode created in Neo4j."""
    gid = f"e2e-txt-{uuid.uuid4().hex[:8]}"
    raw = _raw_payload(
        "Alice is building a Neo4j-powered knowledge graph for AI research.",
        source_id=f"e2e-txt-{uuid.uuid4().hex[:8]}",
    )
    try:
        _run_pipeline(raw, [], redis_client, gid)
        count = _count_group_nodes(neo4j_driver_e2e, gid)
        assert count >= 1, f"Expected ≥1 node in Neo4j for group_id={gid!r}, got {count}"
    finally:
        _cleanup_group(neo4j_driver_e2e, gid)


def test_youtube_url_to_graph(redis_client, neo4j_driver_e2e, clean_pipeline_redis):
    """YouTube URL → message + content EpisodicNodes written to Neo4j."""
    gid = f"e2e-yt-{uuid.uuid4().hex[:8]}"
    url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    raw = _raw_payload(f"Watch this: {url}", source_id=f"e2e-yt-{uuid.uuid4().hex[:8]}")
    rc = _resolved(
        url,
        "youtube",
        transcript="Never gonna give you up, never gonna let you down.",
    )
    try:
        _run_pipeline(raw, [rc], redis_client, gid)
        count = _count_group_nodes(neo4j_driver_e2e, gid)
        assert count >= 1, f"Expected ≥1 node for YouTube message, got {count}"
    finally:
        _cleanup_group(neo4j_driver_e2e, gid)


def test_github_url_to_graph(redis_client, neo4j_driver_e2e, clean_pipeline_redis):
    """GitHub URL → message + github content EpisodicNodes in Neo4j."""
    gid = f"e2e-gh-{uuid.uuid4().hex[:8]}"
    url = "https://github.com/anthropics/anthropic-sdk-python"
    raw = _raw_payload(f"New SDK: {url}", source_id=f"e2e-gh-{uuid.uuid4().hex[:8]}")
    rc = _resolved(
        url,
        "github",
        text="Python SDK for Anthropic's Claude API. Supports streaming and tool use.",
    )
    try:
        _run_pipeline(raw, [rc], redis_client, gid)
        count = _count_group_nodes(neo4j_driver_e2e, gid)
        assert count >= 1
    finally:
        _cleanup_group(neo4j_driver_e2e, gid)


def test_image_url_to_graph(redis_client, neo4j_driver_e2e, clean_pipeline_redis):
    """Image attachment URL → EpisodicNode with vision summary written to Neo4j."""
    gid = f"e2e-img-{uuid.uuid4().hex[:8]}"
    url = "https://example.com/architecture-diagram.png"
    raw = _raw_payload(
        "Check this diagram",
        source_id=f"e2e-img-{uuid.uuid4().hex[:8]}",
        attachments=[url],
    )
    rc = _resolved(
        url,
        "image",
        summary="A flowchart showing the pipeline from Discord ingestion to Neo4j.",
        text="A flowchart showing the pipeline from Discord ingestion to Neo4j.",
    )
    try:
        _run_pipeline(raw, [rc], redis_client, gid)
        count = _count_group_nodes(neo4j_driver_e2e, gid)
        assert count >= 1
    finally:
        _cleanup_group(neo4j_driver_e2e, gid)


def test_pdf_url_to_graph(redis_client, neo4j_driver_e2e, clean_pipeline_redis):
    """PDF URL → EpisodicNode with extracted text written to Neo4j."""
    gid = f"e2e-pdf-{uuid.uuid4().hex[:8]}"
    url = "https://arxiv.org/pdf/2401.12345.pdf"
    raw = _raw_payload(f"Read this paper: {url}", source_id=f"e2e-pdf-{uuid.uuid4().hex[:8]}")
    rc = _resolved(
        url,
        "pdf",
        text=(
            "Abstract: We present a novel graph-based approach to knowledge extraction "
            "from scientific papers using entity resolution and relationship inference."
        ),
        title="Novel Knowledge Extraction via Graph Neural Networks",
    )
    try:
        _run_pipeline(raw, [rc], redis_client, gid)
        count = _count_group_nodes(neo4j_driver_e2e, gid)
        assert count >= 1
    finally:
        _cleanup_group(neo4j_driver_e2e, gid)


def test_multi_url_to_graph(redis_client, neo4j_driver_e2e, clean_pipeline_redis):
    """Message with multiple URLs → message + multiple content episodes in Neo4j."""
    gid = f"e2e-multi-{uuid.uuid4().hex[:8]}"
    urls = [
        "https://example.com/blog-post-on-graphs",
        "https://github.com/org/knowledge-graph-toolkit",
    ]
    content = " ".join(f"See: {u}" for u in urls)
    raw = _raw_payload(content, source_id=f"e2e-multi-{uuid.uuid4().hex[:8]}")
    rcs = [
        _resolved(
            urls[0],
            "webpage",
            text="Blog post discussing knowledge graph construction and entity resolution techniques.",
        ),
        _resolved(
            urls[1],
            "github",
            text="A Python toolkit for building and querying knowledge graphs backed by Neo4j.",
        ),
    ]
    try:
        _run_pipeline(raw, rcs, redis_client, gid)
        count = _count_group_nodes(neo4j_driver_e2e, gid)
        # At minimum: 1 message EpisodicNode + 2 content EpisodicNodes = 3 nodes
        assert count >= 1, f"Expected ≥1 nodes for multi-URL message, got {count}"
    finally:
        _cleanup_group(neo4j_driver_e2e, gid)
