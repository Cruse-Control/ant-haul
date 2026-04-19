"""Integration tests for CircuitBreaker — requires real Redis.

Marker: pytest.mark.integration
~5 tests: cross-worker state, concurrent failures, cooldown timing,
          reconnect recovery, KEYS listing.
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest

from seed_storage.circuit_breaker import CircuitBreaker

pytestmark = pytest.mark.integration


def _cb(redis_client, test_prefix, service: str = "test-svc", **kwargs) -> CircuitBreaker:
    """Helper: CircuitBreaker with test-namespaced keys."""
    # Patch the key format to use test prefix to avoid polluting global state.
    cb = CircuitBreaker(redis_client, f"{test_prefix}{service}", **kwargs)
    return cb


def test_cross_worker_state_shared(redis_client, test_prefix):
    """Two CircuitBreaker instances on the same service share failure counter."""
    import redis as redis_lib

    from tests.integration.conftest import REDIS_TEST_URL

    svc = f"svc-{uuid.uuid4().hex[:6]}"
    cb_a = CircuitBreaker(redis_client, svc, failure_threshold=3)
    cb_b = CircuitBreaker(redis_lib.from_url(REDIS_TEST_URL), svc, failure_threshold=3)

    # Cleanup on teardown
    try:
        assert cb_a.state == "closed"
        cb_a.record_failure()
        cb_a.record_failure()
        # Worker B sees the same accumulated count
        assert cb_b.state == "closed"  # not yet at threshold
        cb_b.record_failure()
        # Now at threshold — both see "open"
        assert cb_a.state == "open"
        assert cb_b.state == "open"
    finally:
        redis_client.delete(
            f"seed:circuit:{svc}:failures",
            f"seed:circuit:{svc}:opened_at",
        )


def test_concurrent_failures_accumulate(redis_client, test_prefix):
    """Concurrent record_failure() calls from multiple threads accumulate correctly."""
    svc = f"svc-{uuid.uuid4().hex[:6]}"
    cb = CircuitBreaker(redis_client, svc, failure_threshold=10, cooldown_seconds=5)

    try:
        errors: list[Exception] = []

        def fail():
            try:
                cb.record_failure()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=fail) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        # 10 failures at threshold=10 → open
        assert cb.state == "open"
    finally:
        redis_client.delete(
            f"seed:circuit:{svc}:failures",
            f"seed:circuit:{svc}:opened_at",
        )


def test_cooldown_timing(redis_client, test_prefix):
    """Circuit transitions open → half-open after cooldown TTL expires."""
    svc = f"svc-{uuid.uuid4().hex[:6]}"
    cb = CircuitBreaker(redis_client, svc, failure_threshold=2, cooldown_seconds=1)

    try:
        cb.record_failure()
        cb.record_failure()
        assert cb.state == "open"

        # Wait for opened_at TTL to expire
        time.sleep(1.2)
        assert cb.state == "half-open"
    finally:
        redis_client.delete(
            f"seed:circuit:{svc}:failures",
            f"seed:circuit:{svc}:opened_at",
        )


def test_reconnect_recovery(redis_client, test_prefix):
    """record_success() resets all state regardless of prior failure count."""
    svc = f"svc-{uuid.uuid4().hex[:6]}"
    cb = CircuitBreaker(redis_client, svc, failure_threshold=3, cooldown_seconds=60)

    try:
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        assert cb.state == "open"

        cb.record_success()
        assert cb.state == "closed"
        assert not cb.is_open()
    finally:
        redis_client.delete(
            f"seed:circuit:{svc}:failures",
            f"seed:circuit:{svc}:opened_at",
        )


def test_keys_listing(redis_client, test_prefix):
    """After circuit trips, both Redis keys exist with the expected naming pattern."""
    svc = f"svc-{uuid.uuid4().hex[:6]}"
    cb = CircuitBreaker(redis_client, svc, failure_threshold=2, cooldown_seconds=30)

    try:
        cb.record_failure()
        cb.record_failure()

        failures_key = f"seed:circuit:{svc}:failures"
        opened_key = f"seed:circuit:{svc}:opened_at"

        assert redis_client.exists(failures_key)
        assert redis_client.exists(opened_key)

        # Verify key format matches scan_iter pattern used by health.py
        pattern_keys = list(redis_client.scan_iter(f"seed:circuit:{svc}:*"))
        key_strs = {k.decode() if isinstance(k, bytes) else k for k in pattern_keys}
        assert failures_key in key_strs
        assert opened_key in key_strs
    finally:
        redis_client.delete(
            f"seed:circuit:{svc}:failures",
            f"seed:circuit:{svc}:opened_at",
        )
