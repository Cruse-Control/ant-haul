"""Integration test fixtures — real Redis + Neo4j (docker-compose) required.

Start infrastructure:
    docker compose -p seed-storage-dev up -d

Override connection URLs via env vars:
    REDIS_TEST_URL   default: redis://localhost:6379/9
    NEO4J_TEST_URI   default: bolt://localhost:7687
    NEO4J_TEST_USER  default: neo4j
    NEO4J_TEST_PASS  default: localdev
"""

from __future__ import annotations

import os
import uuid

import pytest
import redis as redis_lib

REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:6379/9")
NEO4J_TEST_URI = os.environ.get("NEO4J_TEST_URI", "bolt://localhost:7687")
NEO4J_TEST_USER = os.environ.get("NEO4J_TEST_USER", "neo4j")
NEO4J_TEST_PASS = os.environ.get("NEO4J_TEST_PASS", "localdev")


@pytest.fixture(scope="session")
def redis_client():
    """Session-scoped real Redis connection. Skips entire session if unavailable."""
    try:
        r = redis_lib.from_url(REDIS_TEST_URL, socket_connect_timeout=2)
        r.ping()
        return r
    except Exception as exc:
        pytest.skip(f"Redis not available at {REDIS_TEST_URL}: {exc}. Run docker-compose.")


@pytest.fixture
def test_prefix(redis_client):
    """Unique key prefix per test. Deletes all prefixed keys on teardown."""
    prefix = f"test:{uuid.uuid4().hex[:8]}:"
    yield prefix
    keys = redis_client.keys(f"{prefix}*")
    if keys:
        redis_client.delete(*keys)


@pytest.fixture(scope="session")
def neo4j_driver():
    """Session-scoped Neo4j driver. Skips if Neo4j is unavailable."""
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(
            NEO4J_TEST_URI,
            auth=(NEO4J_TEST_USER, NEO4J_TEST_PASS),
            connection_timeout=3,
        )
        driver.verify_connectivity()
        yield driver
        driver.close()
    except ImportError:
        pytest.skip("neo4j package not installed")
    except Exception as exc:
        pytest.skip(f"Neo4j not available at {NEO4J_TEST_URI}: {exc}. Run docker-compose.")
