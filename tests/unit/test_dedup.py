"""Unit tests for seed_storage.dedup.DedupStore."""

from unittest.mock import MagicMock

import pytest

from seed_storage.dedup import DedupStore, url_hash

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_redis():
    return MagicMock()


@pytest.fixture()
def store(mock_redis):
    return DedupStore(mock_redis, "seed:seen_messages")


# ---------------------------------------------------------------------------
# is_seen
# ---------------------------------------------------------------------------


def test_is_seen_returns_false_when_not_in_set(store, mock_redis):
    mock_redis.sismember.return_value = False
    assert store.is_seen("discord:123") is False
    mock_redis.sismember.assert_called_once_with("seed:seen_messages", "discord:123")


def test_is_seen_returns_true_when_in_set(store, mock_redis):
    mock_redis.sismember.return_value = True
    assert store.is_seen("discord:123") is True


def test_is_seen_uses_correct_key(mock_redis):
    s = DedupStore(mock_redis, "seed:seen_urls")
    mock_redis.sismember.return_value = False
    s.is_seen("abc")
    mock_redis.sismember.assert_called_once_with("seed:seen_urls", "abc")


# ---------------------------------------------------------------------------
# mark_seen
# ---------------------------------------------------------------------------


def test_mark_seen_calls_sadd(store, mock_redis):
    store.mark_seen("discord:456")
    mock_redis.sadd.assert_called_once_with("seed:seen_messages", "discord:456")


def test_mark_seen_noop_when_already_present(store, mock_redis):
    mock_redis.sadd.return_value = 0  # already in set
    store.mark_seen("discord:456")
    mock_redis.sadd.assert_called_once()


# ---------------------------------------------------------------------------
# seen_or_mark
# ---------------------------------------------------------------------------


def test_seen_or_mark_returns_false_when_new(store, mock_redis):
    mock_redis.sadd.return_value = 1  # newly added
    assert store.seen_or_mark("discord:789") is False


def test_seen_or_mark_returns_true_when_already_seen(store, mock_redis):
    mock_redis.sadd.return_value = 0  # already present
    assert store.seen_or_mark("discord:789") is True


def test_seen_or_mark_calls_sadd_once(store, mock_redis):
    mock_redis.sadd.return_value = 1
    store.seen_or_mark("msg_key")
    mock_redis.sadd.assert_called_once_with("seed:seen_messages", "msg_key")


def test_seen_or_mark_same_message_twice(mock_redis):
    """Simulate calling seen_or_mark on the same key twice."""
    mock_redis.sadd.side_effect = [1, 0]  # first: new; second: already seen
    store = DedupStore(mock_redis, "seed:seen_messages")
    assert store.seen_or_mark("discord:111") is False
    assert store.seen_or_mark("discord:111") is True


# ---------------------------------------------------------------------------
# separate set keys are independent
# ---------------------------------------------------------------------------


def test_separate_set_keys_independent(mock_redis):
    messages = DedupStore(mock_redis, "seed:seen_messages")
    urls = DedupStore(mock_redis, "seed:seen_urls")
    mock_redis.sismember.return_value = False
    messages.is_seen("key")
    urls.is_seen("key")
    calls = mock_redis.sismember.call_args_list
    assert calls[0][0][0] == "seed:seen_messages"
    assert calls[1][0][0] == "seed:seen_urls"


# ---------------------------------------------------------------------------
# URL dedup uses url_hash
# ---------------------------------------------------------------------------


def test_url_dedup_with_hash(mock_redis):
    url_store = DedupStore(mock_redis, "seed:seen_urls")
    h = url_hash("https://example.com/page?utm_source=twitter")
    mock_redis.sadd.return_value = 1
    url_store.seen_or_mark(h)
    mock_redis.sadd.assert_called_once_with("seed:seen_urls", h)


def test_canonical_url_matching(mock_redis):
    """Same URL with and without tracking params should produce the same hash."""
    h1 = url_hash("https://example.com/page")
    h2 = url_hash("https://example.com/page?utm_source=twitter&fbclid=abc")
    assert h1 == h2
