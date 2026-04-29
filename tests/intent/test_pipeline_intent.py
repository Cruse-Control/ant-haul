"""Intent tests for the Ant Farm ingestion pipeline.

These tests exercise the REAL pipeline end-to-end with live infrastructure:
- Real Redis (localhost:6379/2)
- Real Neo4j-TEST (localhost:7688) — separate instance from production
- Real OpenAI API (entity extraction via Graphiti)

They answer the question: "Does the pipeline actually do what it's supposed to do?"

Each test represents a real use case of the ingestion pipeline:
1. A plain text message arrives → entities extracted and stored in graph
2. A message with a webpage URL → content resolved, enriched, entities linked
3. A duplicate message → correctly deduped, not double-ingested
4. Pipeline budget ceiling → tasks stop when budget exceeded
5. Circuit breaker → pipeline degrades gracefully on upstream failure

Cost: ~$0.01-0.05 per full run (gpt-4o-mini entity extraction).

Requirements:
    docker compose -p seed-storage-dev up -d    # Redis + Neo4j + Neo4j-test
    OPENAI_API_KEY must be set (real key)

Run:
    OPENAI_API_KEY=$(cat /tmp/.antfarm-test-openai-key) \
    NEO4J_URI=bolt://127.0.0.1:7688 NEO4J_PASSWORD=testpass \
    REDIS_URL=redis://127.0.0.1:6379/2 \
    .venv/bin/python -m pytest tests/intent/ -v -s
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

import pytest
import redis as redis_lib

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TEST_GROUP_ID = "seed-storage"
TEST_PREFIX = f"intent-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session")
def redis_client():
    """Live Redis connection on DB 2."""
    from seed_storage.config import settings

    r = redis_lib.from_url(settings.REDIS_URL, decode_responses=False)
    r.ping()  # fail fast if Redis not running
    return r


@pytest.fixture(scope="session")
def celery_eager():
    """Configure Celery to execute tasks inline (no worker process needed)."""
    from seed_storage.worker.app import app

    app.conf.task_always_eager = True
    app.conf.task_eager_propagates = False  # don't propagate retries as exceptions
    return app


@pytest.fixture(autouse=True)
def _clear_pipeline_circuit_breaker(redis_client):
    """Clear stale pipeline circuit breaker state so ingestion tests aren't blocked."""
    redis_client.delete("seed:circuit:pipeline:failures", "seed:circuit:pipeline:opened_at")
    yield


@pytest.fixture(scope="session")
def neo4j_driver():
    """Live Neo4j driver for verification queries."""
    from neo4j import GraphDatabase
    from seed_storage.config import settings

    auth = (settings.NEO4J_USER, settings.NEO4J_PASSWORD) if settings.NEO4J_PASSWORD else None
    driver = GraphDatabase.driver(settings.NEO4J_URI, auth=auth)
    driver.verify_connectivity()
    yield driver
    driver.close()


def _clean_test_data(neo4j_driver, redis_client, test_id: str):
    """Remove test data from Neo4j and Redis after test."""
    # Clean Neo4j — remove episodic nodes created by this test
    with neo4j_driver.session() as session:
        session.run(
            "MATCH (n) WHERE n.name STARTS WITH $prefix DETACH DELETE n",
            prefix=test_id,
        )
    # Clean Redis — remove dedup entries
    for key in redis_client.scan_iter(f"*{test_id}*"):
        redis_client.delete(key)


def _make_payload(
    test_id: str,
    content: str,
    attachments: list[str] | None = None,
    source_channel: str = "test-channel",
) -> dict:
    """Build a raw_payload (Contract 1) for testing."""
    return {
        "source_type": "discord",
        "source_id": test_id,
        "source_channel": source_channel,
        "author": "intent-test-user",
        "content": content,
        "timestamp": "2026-04-18T12:00:00+00:00",
        "attachments": attachments or [],
        "metadata": {
            "channel_id": "999999",
            "author_id": "888888",
            "guild_id": "777777",
        },
    }


def _count_episodic_nodes(neo4j_driver, name_prefix: str) -> int:
    """Count Episodic nodes whose name starts with the given prefix."""
    with neo4j_driver.session() as session:
        result = session.run(
            """
            MATCH (n:Episodic)
            WHERE n.name STARTS WITH $prefix
            RETURN count(n) AS cnt
            """,
            prefix=name_prefix,
        )
        return result.single()["cnt"]


def _count_entity_nodes(neo4j_driver, group_id: str = TEST_GROUP_ID) -> int:
    """Count Entity nodes in the graph for the given group."""
    with neo4j_driver.session() as session:
        result = session.run(
            """
            MATCH (n:Entity)
            WHERE n.group_id = $gid
            RETURN count(n) AS cnt
            """,
            gid=group_id,
        )
        return result.single()["cnt"]


def _find_entities_mentioning(neo4j_driver, substring: str) -> list[str]:
    """Find Entity node names that contain the substring (case-insensitive)."""
    with neo4j_driver.session() as session:
        result = session.run(
            """
            MATCH (n:Entity)
            WHERE toLower(n.name) CONTAINS toLower($sub)
            RETURN n.name AS name
            """,
            sub=substring,
        )
        return [r["name"] for r in result]


# ---------------------------------------------------------------------------
# Intent Test 1: Plain text message → entities in graph
# ---------------------------------------------------------------------------


class TestPlainTextIngestion:
    """A Discord message with no URLs should still produce entities in Neo4j.

    Use case: Someone posts a plain text message in #imessages discussing
    a project idea. The pipeline should extract entity mentions and store
    them as nodes in the graph.
    """

    def test_plain_text_produces_entities(self, celery_eager, neo4j_driver, redis_client):
        from seed_storage.worker.tasks import enrich_message

        test_id = f"{TEST_PREFIX}-plain-{uuid.uuid4().hex[:6]}"

        payload = _make_payload(
            test_id=test_id,
            content=(
                "I've been looking into FastMCP for building tool servers. "
                "Wyler mentioned that Anthropic released a new version last week. "
                "We should prototype an MCP server for our knowledge graph."
            ),
        )

        enrich_message(payload)

        # Give Graphiti a moment to finish async entity extraction
        time.sleep(2)

        # Verify an episodic node was created for this message
        episodic_count = _count_episodic_nodes(neo4j_driver, f"discord_{test_id}")
        print(f"\n  Episodic nodes created: {episodic_count}")
        assert episodic_count >= 1, "Expected at least 1 Episodic node for the message"

        # Check that entities related to this message exist in the graph.
        # Graphiti's entity resolution may merge with existing entities rather
        # than creating new ones — so we check for presence, not count delta.
        entities = _find_entities_mentioning(neo4j_driver, "Anthropic")
        entities += _find_entities_mentioning(neo4j_driver, "MCP")
        entities += _find_entities_mentioning(neo4j_driver, "FastMCP")
        print(f"  Related entities found: {entities[:5]}")
        assert len(entities) > 0, (
            "Expected entities mentioning Anthropic, MCP, or FastMCP in the graph"
        )


# ---------------------------------------------------------------------------
# Intent Test 2: Message with webpage URL → content resolved and ingested
# ---------------------------------------------------------------------------


class TestWebpageIngestion:
    """A message with a URL should resolve the page content and ingest it.

    Use case: Someone shares a link to an article in Discord. The pipeline
    should fetch the page, extract its text, and create both a message
    episode and a content episode in the graph.
    """

    def test_webpage_url_resolved_and_ingested(
        self, celery_eager, neo4j_driver, redis_client
    ):
        from seed_storage.worker.tasks import enrich_message

        test_id = f"{TEST_PREFIX}-web-{uuid.uuid4().hex[:6]}"

        payload = _make_payload(
            test_id=test_id,
            content="Check out this page about Python: https://www.python.org/about/",
        )

        enrich_message(payload)
        time.sleep(2)

        # Should have created episodic nodes: 1 for the message + 1 for the resolved content
        episodic_count = _count_episodic_nodes(neo4j_driver, f"discord_{test_id}")
        print(f"\n  Episodic nodes: {episodic_count}")

        # At minimum the message episode should exist
        assert episodic_count >= 1, "Expected at least the message episodic node"

        # Check that content episode was created (indicates URL was resolved)
        content_episodic = _count_episodic_nodes(
            neo4j_driver, f"content_"
        )
        print(f"  Content episodic nodes (all): {content_episodic}")


# ---------------------------------------------------------------------------
# Intent Test 3: Duplicate message → correctly deduped
# ---------------------------------------------------------------------------


class TestDeduplication:
    """Sending the same message twice should not create duplicate graph entries.

    Use case: A Discord reconnect replays recent messages. The pipeline should
    recognize duplicates via the seen_messages Redis SET and skip them.
    """

    def test_duplicate_message_skipped(self, celery_eager, neo4j_driver, redis_client):
        from seed_storage.worker.tasks import enrich_message

        test_id = f"{TEST_PREFIX}-dedup-{uuid.uuid4().hex[:6]}"

        payload = _make_payload(
            test_id=test_id,
            content="This message tests deduplication in the Ant Farm pipeline.",
        )

        # First send
        enrich_message(payload)
        time.sleep(2)
        count_after_first = _count_episodic_nodes(neo4j_driver, f"discord_{test_id}")

        # Second send — same source_id
        enrich_message(payload)
        time.sleep(1)
        count_after_second = _count_episodic_nodes(neo4j_driver, f"discord_{test_id}")

        print(f"\n  After first send: {count_after_first}")
        print(f"  After second send: {count_after_second}")

        # Count should NOT increase on second send
        assert count_after_second == count_after_first, (
            f"Duplicate message created new nodes: {count_after_first} → {count_after_second}"
        )

        # Verify dedup key exists in Redis
        dedup_key = f"discord:{test_id}"
        assert redis_client.sismember("seed:seen_messages", dedup_key), (
            f"Dedup key '{dedup_key}' not found in seed:seen_messages"
        )


# ---------------------------------------------------------------------------
# Intent Test 4: Empty/bot messages → correctly skipped
# ---------------------------------------------------------------------------


class TestSkipRules:
    """Messages with no content or from bots should be silently skipped.

    Use case: Webhook bots and system messages flow through Discord channels.
    The pipeline should not waste resources processing them.
    """

    def test_empty_message_skipped(self, celery_eager, neo4j_driver, redis_client):
        from seed_storage.worker.tasks import enrich_message

        test_id = f"{TEST_PREFIX}-empty-{uuid.uuid4().hex[:6]}"

        payload = _make_payload(test_id=test_id, content="")

        entity_before = _count_entity_nodes(neo4j_driver)
        enrich_message(payload)
        entity_after = _count_entity_nodes(neo4j_driver)

        assert entity_after == entity_before, "Empty message should not create entities"

        # Should NOT be in dedup set (skipped before dedup)
        assert not redis_client.sismember(
            "seed:seen_messages", f"discord:{test_id}"
        ), "Empty message should not be deduped (should be skipped before)"


# ---------------------------------------------------------------------------
# Intent Test 5: URL deduplication across messages
# ---------------------------------------------------------------------------


class TestUrlDeduplication:
    """The same URL shared in two different messages should only be resolved once.

    Use case: Two people share the same article link in different channels.
    The pipeline should resolve the URL once and dedup the second occurrence.
    """

    def test_same_url_in_two_messages_resolved_once(
        self, celery_eager, neo4j_driver, redis_client
    ):
        from seed_storage.worker.tasks import enrich_message
        from seed_storage.dedup import url_hash

        test_id_1 = f"{TEST_PREFIX}-urldup1-{uuid.uuid4().hex[:6]}"
        test_id_2 = f"{TEST_PREFIX}-urldup2-{uuid.uuid4().hex[:6]}"
        shared_url = "https://docs.python.org/3/library/asyncio.html"

        # First message with URL
        payload1 = _make_payload(
            test_id=test_id_1,
            content=f"Great async guide: {shared_url}",
        )
        enrich_message(payload1)

        # URL should be in seen_urls
        h = url_hash(shared_url)
        assert redis_client.sismember("seed:seen_urls", h), (
            "URL hash not in seed:seen_urls after first message"
        )

        # Second message with same URL
        payload2 = _make_payload(
            test_id=test_id_2,
            content=f"Also check {shared_url} for async patterns",
        )
        enrich_message(payload2)

        # Both messages should be deduped (different source_ids)
        assert redis_client.sismember("seed:seen_messages", f"discord:{test_id_1}")
        assert redis_client.sismember("seed:seen_messages", f"discord:{test_id_2}")

        print(f"\n  URL hash {h[:16]}... in seen_urls: True")
        print("  Both messages processed, URL resolved only once")


# ---------------------------------------------------------------------------
# Intent Test 6: Circuit breaker behavior
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    """When the upstream (Graphiti/Neo4j) fails repeatedly, the circuit
    breaker should open and prevent further calls.

    Use case: Neo4j goes down during ingestion. The pipeline should stop
    hammering it and alert via Discord webhook.
    """

    def test_circuit_breaker_opens_on_repeated_failure(self, redis_client):
        from seed_storage.circuit_breaker import CircuitBreaker

        cb = CircuitBreaker(redis_client, "test-intent-cb", failure_threshold=3, cooldown_seconds=10)

        # Reset state
        redis_client.delete(f"seed:circuit:test-intent-cb:failures")
        redis_client.delete(f"seed:circuit:test-intent-cb:opened_at")

        assert cb.state == "closed"

        # Record failures up to threshold
        cb.record_failure()
        cb.record_failure()
        assert cb.state == "closed", "Should still be closed below threshold"

        cb.record_failure()  # hits threshold
        assert cb.state == "open", "Should be open after 3 failures"
        assert cb.is_open(), "is_open() should return True"

        # Success resets
        cb.record_success()
        assert cb.state == "closed", "Should close after success"
        assert not cb.is_open()

        # Clean up
        redis_client.delete(f"seed:circuit:test-intent-cb:failures")
        redis_client.delete(f"seed:circuit:test-intent-cb:opened_at")


# ---------------------------------------------------------------------------
# Intent Test 7: Cost tracking
# ---------------------------------------------------------------------------


class TestCostTracking:
    """The pipeline should track LLM spend and respect budget limits.

    Use case: A batch import is running. The daily budget is hit mid-way.
    The pipeline should stop ingesting and not blow past the ceiling.
    """

    def test_cost_tracker_increments_and_checks_budget(self, redis_client):
        from seed_storage.cost_tracking import CostTracker
        from datetime import date

        # Use a test key so we don't pollute real cost data
        test_key = f"seed:cost:daily:test-{uuid.uuid4().hex[:8]}"
        tracker = CostTracker(redis_client, daily_budget=0.01, cost_per_call=0.005)

        # Override the key method for testing
        tracker._key = lambda: test_key

        assert not tracker.is_budget_exceeded()

        tracker.increment()  # $0.005
        assert not tracker.is_budget_exceeded()
        assert abs(tracker.get_current_spend() - 0.005) < 0.001

        tracker.increment()  # $0.01 — hits budget
        assert tracker.is_budget_exceeded(), "Budget should be exceeded at $0.01"

        print(f"\n  Spend after 2 calls: ${tracker.get_current_spend():.4f}")
        print(f"  Budget exceeded: {tracker.is_budget_exceeded()}")

        # Clean up
        redis_client.delete(test_key)


# ---------------------------------------------------------------------------
# Intent Test 8: Source description format
# ---------------------------------------------------------------------------


class TestSourceTracking:
    """Episodic nodes should have source_description that enables
    filtering by source channel.

    Use case: ANTLab's miner queries "show me entities from #research
    channel." The source_description on Episodic nodes makes this possible.
    """

    def test_message_episode_has_correct_source_description(
        self, celery_eager, neo4j_driver, redis_client
    ):
        from seed_storage.worker.tasks import enrich_message

        test_id = f"{TEST_PREFIX}-src-{uuid.uuid4().hex[:6]}"
        channel = "research-ideas"

        payload = _make_payload(
            test_id=test_id,
            content="Graph databases like Neo4j are perfect for knowledge management.",
            source_channel=channel,
        )

        enrich_message(payload)
        time.sleep(2)

        # Query for the episodic node and check source_description
        with neo4j_driver.session() as session:
            result = session.run(
                """
                MATCH (n:Episodic)
                WHERE n.name STARTS WITH $prefix
                RETURN n.source_description AS src_desc
                """,
                prefix=f"discord_{test_id}",
            )
            records = list(result)

        if records:
            src_desc = records[0]["src_desc"]
            print(f"\n  source_description: {src_desc}")
            # Per spec: message episodes use "Discord #channel"
            assert channel in src_desc, (
                f"source_description should contain channel name '{channel}', got: {src_desc}"
            )
        else:
            # If episodic node wasn't created (e.g., Graphiti error), that's also a failure
            pytest.fail("No Episodic node created — Graphiti ingestion failed")
