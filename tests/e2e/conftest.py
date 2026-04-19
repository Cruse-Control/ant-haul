"""E2E test fixtures — full stack (Redis + Neo4j + Celery eager).

All E2E tests use a test-specific group_id and clean up their Neo4j data
on teardown via yield fixtures.

Infrastructure requirements (docker-compose):
    docker compose -p seed-storage-dev up -d

Skipped automatically if Redis unavailable.
Graphiti tests further skip if OPENAI_API_KEY absent.
"""

from __future__ import annotations

import os
import uuid

import pytest
import redis as redis_lib

_REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:6379/9")
_NEO4J_TEST_URI = os.environ.get("NEO4J_TEST_URI", "bolt://localhost:7687")
_NEO4J_TEST_USER = os.environ.get("NEO4J_TEST_USER", "neo4j")
_NEO4J_TEST_PASS = os.environ.get("NEO4J_TEST_PASS", "localdev")
_OPENAI_KEY = os.environ.get("OPENAI_API_KEY", "")


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="module")
def graphiti_env():
    """Set env vars to point Graphiti at test Neo4j. Skips if OPENAI_API_KEY absent.

    Module-scoped: resets the Graphiti singleton before and after the module
    so each module gets a fresh instance using test database credentials.
    """
    if not _OPENAI_KEY:
        pytest.skip("OPENAI_API_KEY not set — E2E graphiti tests require it")

    from seed_storage.graphiti_client import reset_graphiti

    prev: dict[str, str | None] = {}
    overrides = {
        "NEO4J_URI": _NEO4J_TEST_URI,
        "NEO4J_USER": _NEO4J_TEST_USER,
        "NEO4J_PASSWORD": _NEO4J_TEST_PASS,
        "OPENAI_API_KEY": _OPENAI_KEY,
        "LLM_PROVIDER": "openai",
        "LLM_MODEL": "gpt-4o-mini",
    }
    for k, v in overrides.items():
        prev[k] = os.environ.get(k)
        os.environ[k] = v

    reset_graphiti()
    yield
    reset_graphiti()

    for k, v in prev.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


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


@pytest.fixture(autouse=True)
def _reset_graphiti_between_e2e_tests():
    """Reset Graphiti singleton before and after each E2E test.

    Each Celery task invocation runs asyncio.run() which creates a new event
    loop. The Graphiti singleton holds a Neo4j async driver bound to the loop
    it was created in. Reusing the singleton across asyncio.run() calls causes
    a 'Task pending in closed loop' error that silently prevents graph writes.

    Resetting before each test ensures the singleton is re-initialized in the
    current event loop, within the single asyncio.run() call inside the task.
    """
    try:
        from seed_storage.graphiti_client import reset_graphiti
        reset_graphiti()
    except Exception:
        pass
    yield
    try:
        from seed_storage.graphiti_client import reset_graphiti
        reset_graphiti()
    except Exception:
        pass


@pytest.fixture
def clean_pipeline_redis(redis_client):
    """Delete circuit breaker, rate limiter, and cost keys before and after each test.

    Use this fixture in tests that exercise ingest_episode against real Graphiti
    to prevent accumulated failures from tripping the circuit breaker between tests.
    """
    _flush_pipeline_state(redis_client)
    yield
    _flush_pipeline_state(redis_client)


def _flush_pipeline_state(r: redis_lib.Redis) -> None:
    """Remove circuit breaker, rate limiter, and daily cost keys from test Redis."""
    patterns = [
        "seed:circuit:graphiti:*",
        "seed:ratelimit:*",
        "seed:cost:daily:*",
    ]
    for pattern in patterns:
        keys = list(r.scan_iter(pattern))
        if keys:
            r.delete(*keys)
