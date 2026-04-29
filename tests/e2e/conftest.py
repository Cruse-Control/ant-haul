"""E2E test fixtures — full stack (Redis + Neo4j + Celery eager).

All E2E tests use a test-specific group_id and clean up their Neo4j data
on teardown via yield fixtures.

Infrastructure requirements (docker-compose):
    docker compose -p seed-storage-dev up -d

Skipped automatically if Redis unavailable.
"""

from __future__ import annotations

import os
import uuid

import pytest
import redis as redis_lib

_REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:***@pytest.fixture(scope="session")
def redis_client():
    """Session-scoped real Redis. Skips session if unavailable."""
    try:
        r = redis_lib.from_url(_REDIS_TEST_URL, socket_connect_timeout=2)
        r.ping()
        return r
    except Exception as exc:
        pytest.skip(f"Redis not available: {exc}. Run: docker compose -p seed-storage-dev up -d")


@pytest.fixture(scope="session", autouse=True)
def celery_eager_session():
    """Enable Celery eager mode for the entire E2E session."""
    from seed_storage.worker.app import app as celery_app

    prev_eager = celery_app.conf.task_always_eager
    prev_broker = celery_app.conf.broker_url
    celery_app.conf.update(
        task_always_eager=True,
        task_eager_propagates=False,
        broker_url=_REDIS_TEST_URL,
        result_backend=_REDIS_TEST_URL,
    )
    yield
    celery_app.conf.update(
        task_always_eager=prev_eager,
        broker_url=prev_broker,
    )


@pytest.fixture
def e2e_source_prefix():
    """Unique source_description prefix for this test. Used for cleanup."""
    return f"e2e-test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def e2e_group_id():
    """Unique group_id for E2E test graph isolation. Cleans up Neo4j on teardown."""
    gid = f"e2e-{uuid.uuid4().hex[:8]}"
    yield gid
    _cleanup_neo4j_group(gid)


def _cleanup_neo4j_group(group_id: str) -> None:
    """Delete all Neo4j nodes with the given group_id."""
    if not _OPENAI_KEY:
        return
    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(
            _NEO4J_TEST_URI,
            auth=(_NEO4J_TEST_USER, _NEO4J_TEST_PASS),
            connection_timeout=3,
        )
        driver.execute_query(
            "MATCH (n) WHERE n.group_id = $gid DETACH DELETE n",
            {"gid": group_id},
        )
        driver.close()
    except Exception:
        pass


@pytest.fixture(scope="session")
def neo4j_driver_e2e():
    """Session-scoped synchronous Neo4j driver for E2E node verification.

    Skips if neo4j package not installed or Neo4j unavailable.
    """
    if not _OPENAI_KEY:
        pytest.skip("OPENAI_API_KEY not set")
    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(
            _NEO4J_TEST_URI,
            auth=(_NEO4J_TEST_USER, _NEO4J_TEST_PASS),
            connection_timeout=3,
        )
        driver.verify_connectivity()
        yield driver
        driver.close()
    except ImportError:
        pytest.skip("neo4j package not installed")
    except Exception as exc:
        pytest.skip(f"Neo4j not available at {_NEO4J_TEST_URI}: {exc}")


@pytest.fixture
def clean_pipeline_redis(redis_client):
    """Delete circuit breaker, rate limiter, and cost keys before and after each test.

    Use this fixture in tests that exercise ingest_episode against real pipeline
    to prevent accumulated failures from tripping the circuit breaker between tests.
    """
    _flush_pipeline_state(redis_client)
    yield
    _flush_pipeline_state(redis_client)


def _flush_pipeline_state(r: redis_lib.Redis) -> None:
    """Remove circuit breaker, rate limiter, and daily cost keys from test Redis."""
    patterns = [
        "seed:circuit:pipeline:*",
        "seed:ratelimit:*",
        "seed:cost:daily:*",
    ]
    for pattern in patterns:
        keys = list(r.scan_iter(pattern))
        if keys:
            r.delete(*keys)

