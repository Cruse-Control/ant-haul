"""Unit tests for seed_storage.health (~8 tests).

All external dependencies (Redis, Neo4j, Celery) are mocked.
No real infrastructure required.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from aiohttp.test_utils import make_mocked_request

from seed_storage.health import (
    CHECK_TIMEOUT,
    check_bot,
    check_redis,
    health_handler,
    make_app,
)

# ── Shared test data ───────────────────────────────────────────────────────

_EMPTY_DETAILS = {
    "raw_messages_queue_depth": 0,
    "graph_ingest_queue_depth": 0,
    "frontier_size": 0,
    "dead_letter_count": 0,
    "daily_cost_usd": 0.0,
    "daily_budget_usd": 5.0,
    "messages_seen_total": 0,
    "urls_seen_total": 0,
    "open_circuit_breakers": [],
}

_RICH_DETAILS = {
    "raw_messages_queue_depth": 3,
    "graph_ingest_queue_depth": 7,
    "frontier_size": 42,
    "dead_letter_count": 1,
    "daily_cost_usd": 1.25,
    "daily_budget_usd": 5.0,
    "messages_seen_total": 1000,
    "urls_seen_total": 500,
    "open_circuit_breakers": ["neo4j"],
}

_MOCK_REDIS = MagicMock()


def _make_request():
    return make_mocked_request("GET", "/health", app=make_app())


# ── Test 1: All checks pass → 200 + "healthy" ─────────────────────────────


async def test_all_checks_healthy_returns_200():
    with (
        patch("seed_storage.health.redis_lib.from_url", return_value=_MOCK_REDIS),
        patch("seed_storage.health.check_redis", return_value="ok"),
        patch("seed_storage.health.check_neo4j", return_value="ok"),
        patch("seed_storage.health.check_celery", return_value="ok"),
        patch("seed_storage.health.check_bot", return_value="connected"),
        patch("seed_storage.health.get_details", return_value=_EMPTY_DETAILS),
    ):
        resp = await health_handler(_make_request())

    assert resp.status == 200
    data = json.loads(resp.body)
    assert data["status"] == "healthy"
    assert data["checks"]["redis"] == "ok"
    assert data["checks"]["neo4j"] == "ok"
    assert data["checks"]["celery"] == "ok"
    assert data["checks"]["bot"] == "connected"
    assert "details" in data


# ── Test 2: Redis down → 503 ──────────────────────────────────────────────


async def test_redis_down_returns_503():
    with (
        patch("seed_storage.health.redis_lib.from_url", return_value=_MOCK_REDIS),
        patch("seed_storage.health.check_redis", return_value="error"),
        patch("seed_storage.health.check_neo4j", return_value="ok"),
        patch("seed_storage.health.check_celery", return_value="ok"),
        patch("seed_storage.health.check_bot", return_value="connected"),
        patch("seed_storage.health.get_details", return_value=_EMPTY_DETAILS),
    ):
        resp = await health_handler(_make_request())

    assert resp.status == 503
    data = json.loads(resp.body)
    assert data["status"] == "unhealthy"
    assert data["checks"]["redis"] == "error"


# ── Test 3: Neo4j down → 503 ──────────────────────────────────────────────


async def test_neo4j_down_returns_503():
    with (
        patch("seed_storage.health.redis_lib.from_url", return_value=_MOCK_REDIS),
        patch("seed_storage.health.check_redis", return_value="ok"),
        patch("seed_storage.health.check_neo4j", return_value="error"),
        patch("seed_storage.health.check_celery", return_value="ok"),
        patch("seed_storage.health.check_bot", return_value="connected"),
        patch("seed_storage.health.get_details", return_value=_EMPTY_DETAILS),
    ):
        resp = await health_handler(_make_request())

    assert resp.status == 503
    data = json.loads(resp.body)
    assert data["status"] == "unhealthy"
    assert data["checks"]["neo4j"] == "error"


# ── Test 4: Celery down → 503 ─────────────────────────────────────────────


async def test_celery_down_returns_503():
    with (
        patch("seed_storage.health.redis_lib.from_url", return_value=_MOCK_REDIS),
        patch("seed_storage.health.check_redis", return_value="ok"),
        patch("seed_storage.health.check_neo4j", return_value="ok"),
        patch("seed_storage.health.check_celery", return_value="error"),
        patch("seed_storage.health.check_bot", return_value="connected"),
        patch("seed_storage.health.get_details", return_value=_EMPTY_DETAILS),
    ):
        resp = await health_handler(_make_request())

    assert resp.status == 503
    data = json.loads(resp.body)
    assert data["status"] == "unhealthy"
    assert data["checks"]["celery"] == "error"


# ── Test 5: Partial failure (only neo4j down) → 503 ──────────────────────


async def test_partial_failure_returns_503():
    """Redis + Celery ok but Neo4j down → overall unhealthy."""
    with (
        patch("seed_storage.health.redis_lib.from_url", return_value=_MOCK_REDIS),
        patch("seed_storage.health.check_redis", return_value="ok"),
        patch("seed_storage.health.check_neo4j", return_value="error"),
        patch("seed_storage.health.check_celery", return_value="ok"),
        patch("seed_storage.health.check_bot", return_value="disconnected"),
        patch("seed_storage.health.get_details", return_value=_EMPTY_DETAILS),
    ):
        resp = await health_handler(_make_request())

    assert resp.status == 503
    data = json.loads(resp.body)
    assert data["status"] == "unhealthy"
    assert data["checks"]["redis"] == "ok"   # redis was fine
    assert data["checks"]["neo4j"] == "error"


# ── Test 6: Bot disconnected alone does not cause 503 ─────────────────────


async def test_bot_disconnected_does_not_cause_503():
    """'disconnected' is a valid bot state — healthy overall when redis/neo4j/celery pass."""
    with (
        patch("seed_storage.health.redis_lib.from_url", return_value=_MOCK_REDIS),
        patch("seed_storage.health.check_redis", return_value="ok"),
        patch("seed_storage.health.check_neo4j", return_value="ok"),
        patch("seed_storage.health.check_celery", return_value="ok"),
        patch("seed_storage.health.check_bot", return_value="disconnected"),
        patch("seed_storage.health.get_details", return_value=_EMPTY_DETAILS),
    ):
        resp = await health_handler(_make_request())

    assert resp.status == 200
    data = json.loads(resp.body)
    assert data["status"] == "healthy"
    assert data["checks"]["bot"] == "disconnected"


# ── Test 7: Response body includes all required detail fields ─────────────


async def test_response_body_includes_all_detail_fields():
    with (
        patch("seed_storage.health.redis_lib.from_url", return_value=_MOCK_REDIS),
        patch("seed_storage.health.check_redis", return_value="ok"),
        patch("seed_storage.health.check_neo4j", return_value="ok"),
        patch("seed_storage.health.check_celery", return_value="ok"),
        patch("seed_storage.health.check_bot", return_value="connected"),
        patch("seed_storage.health.get_details", return_value=_RICH_DETAILS),
    ):
        resp = await health_handler(_make_request())

    assert resp.status == 200
    details = json.loads(resp.body)["details"]
    assert details["raw_messages_queue_depth"] == 3
    assert details["graph_ingest_queue_depth"] == 7
    assert details["frontier_size"] == 42
    assert details["dead_letter_count"] == 1
    assert abs(details["daily_cost_usd"] - 1.25) < 0.001
    assert abs(details["daily_budget_usd"] - 5.0) < 0.001
    assert details["messages_seen_total"] == 1000
    assert details["urls_seen_total"] == 500
    assert details["open_circuit_breakers"] == ["neo4j"]


# ── Test 8: Exception in check function → treated as "error" → 503 ────────


async def test_check_exception_treated_as_error_and_503():
    """An exception raised inside a check function is caught, counted as 'error', → 503."""
    with (
        patch("seed_storage.health.redis_lib.from_url", return_value=_MOCK_REDIS),
        patch("seed_storage.health.check_redis", side_effect=RuntimeError("simulated timeout")),
        patch("seed_storage.health.check_neo4j", return_value="ok"),
        patch("seed_storage.health.check_celery", return_value="ok"),
        patch("seed_storage.health.check_bot", return_value="connected"),
        patch("seed_storage.health.get_details", return_value=_EMPTY_DETAILS),
    ):
        resp = await health_handler(_make_request())

    assert resp.status == 503
    data = json.loads(resp.body)
    assert data["status"] == "unhealthy"
    assert data["checks"]["redis"] == "error"


# ── Unit tests for individual check helpers ────────────────────────────────


def test_check_redis_ok():
    mock_r = MagicMock()
    mock_r.ping.return_value = True
    assert check_redis(mock_r) == "ok"


def test_check_redis_error():
    mock_r = MagicMock()
    mock_r.ping.side_effect = ConnectionError("refused")
    assert check_redis(mock_r) == "error"


def test_check_bot_connected():
    mock_r = MagicMock()
    mock_r.get.return_value = b"1"
    assert check_bot(mock_r) == "connected"


def test_check_bot_disconnected_when_key_absent():
    mock_r = MagicMock()
    mock_r.get.return_value = None
    assert check_bot(mock_r) == "disconnected"


def test_check_timeout_constant_is_5_seconds():
    assert CHECK_TIMEOUT == 5.0
