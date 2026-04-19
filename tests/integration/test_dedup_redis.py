"""Integration tests for DedupStore — requires real Redis.

Marker: pytest.mark.integration
~6 tests: real SADD/SISMEMBER, concurrent access, atomicity, large set,
          persistence across clients, isolation between stores.
"""

from __future__ import annotations

import threading
import uuid

import pytest

from seed_storage.dedup import DedupStore

pytestmark = pytest.mark.integration


def test_sadd_sismember_basic(redis_client, test_prefix):
    """Real SADD / SISMEMBER: seen_or_mark returns False first time, True on repeat."""
    store = DedupStore(redis_client, f"{test_prefix}seen")
    key = f"msg:{uuid.uuid4().hex}"

    assert not store.is_seen(key)
    first = store.seen_or_mark(key)
    assert first is False  # newly added
    assert store.is_seen(key)
    second = store.seen_or_mark(key)
    assert second is True  # already present


def test_concurrent_access(redis_client, test_prefix):
    """Concurrent SADD: only one thread wins the first-add race."""
    store = DedupStore(redis_client, f"{test_prefix}concurrent")
    key = f"race:{uuid.uuid4().hex}"
    results: list[bool] = []
    lock = threading.Lock()

    def try_mark():
        result = store.seen_or_mark(key)
        with lock:
            results.append(result)

    threads = [threading.Thread(target=try_mark) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Exactly one thread should have gotten False (first add), rest True
    assert results.count(False) == 1
    assert results.count(True) == 9


def test_seen_or_mark_atomicity(redis_client, test_prefix):
    """seen_or_mark is idempotent — repeated calls return True after first."""
    store = DedupStore(redis_client, f"{test_prefix}atomic")
    key = f"idem:{uuid.uuid4().hex}"

    assert store.seen_or_mark(key) is False
    for _ in range(5):
        assert store.seen_or_mark(key) is True


def test_large_set(redis_client, test_prefix):
    """Adding 1000 unique entries all report not-seen on first add."""
    store = DedupStore(redis_client, f"{test_prefix}large")
    keys = [f"item:{i}" for i in range(1000)]

    first_adds = [store.seen_or_mark(k) for k in keys]
    assert all(r is False for r in first_adds)

    # All are seen now
    second_adds = [store.seen_or_mark(k) for k in keys]
    assert all(r is True for r in second_adds)


def test_persistence_across_clients(redis_client, test_prefix):
    """Two DedupStore instances sharing the same Redis key share state."""
    import redis as redis_lib

    from tests.integration.conftest import REDIS_TEST_URL

    set_key = f"{test_prefix}shared"
    store_a = DedupStore(redis_client, set_key)
    store_b = DedupStore(redis_lib.from_url(REDIS_TEST_URL), set_key)

    key = f"persistent:{uuid.uuid4().hex}"
    assert store_a.seen_or_mark(key) is False  # A writes it
    assert store_b.is_seen(key)  # B sees it
    assert store_b.seen_or_mark(key) is True  # B confirms duplicate


def test_isolation_between_stores(redis_client, test_prefix):
    """Two DedupStore instances with different keys are fully isolated."""
    store_msgs = DedupStore(redis_client, f"{test_prefix}messages")
    store_urls = DedupStore(redis_client, f"{test_prefix}urls")

    shared_key = "same-key-value"
    assert store_msgs.seen_or_mark(shared_key) is False
    # The URL store has never seen it
    assert store_urls.seen_or_mark(shared_key) is False
    # Both now see it in their respective sets
    assert store_msgs.is_seen(shared_key)
    assert store_urls.is_seen(shared_key)
