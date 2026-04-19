"""Unit tests for seed_storage.rate_limiting.RateLimiter (~8 tests)."""

from unittest.mock import MagicMock, patch

import pytest

from seed_storage.rate_limiting import RateLimiter

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_redis():
    r = MagicMock()
    pipe = MagicMock()
    r.pipeline.return_value = pipe
    return r


def _make_limiter(mock_redis, key="seed:ratelimit:graphiti", max_per_minute=10):
    return RateLimiter(mock_redis, key, max_per_minute)


def _set_pipeline_count(mock_redis, count: int):
    """Configure pipeline.execute() to return [None, count]."""
    mock_redis.pipeline.return_value.execute.return_value = [None, count]


# ---------------------------------------------------------------------------
# allow() — under limit
# ---------------------------------------------------------------------------


def test_allow_returns_true_when_under_limit(mock_redis):
    _set_pipeline_count(mock_redis, 5)  # 5 < 10
    limiter = _make_limiter(mock_redis)
    assert limiter.allow() is True


def test_allow_adds_member_when_under_limit(mock_redis):
    _set_pipeline_count(mock_redis, 0)
    limiter = _make_limiter(mock_redis)
    limiter.allow()
    mock_redis.zadd.assert_called_once()
    args = mock_redis.zadd.call_args
    assert args[0][0] == "seed:ratelimit:graphiti"


def test_first_request_always_allowed(mock_redis):
    _set_pipeline_count(mock_redis, 0)
    limiter = _make_limiter(mock_redis, max_per_minute=1)
    assert limiter.allow() is True


# ---------------------------------------------------------------------------
# allow() — at or over limit
# ---------------------------------------------------------------------------


def test_allow_returns_false_at_limit(mock_redis):
    _set_pipeline_count(mock_redis, 10)  # == max_per_minute
    limiter = _make_limiter(mock_redis, max_per_minute=10)
    assert limiter.allow() is False


def test_allow_does_not_add_member_when_at_limit(mock_redis):
    _set_pipeline_count(mock_redis, 10)
    limiter = _make_limiter(mock_redis, max_per_minute=10)
    limiter.allow()
    mock_redis.zadd.assert_not_called()


def test_zero_max_always_returns_false(mock_redis):
    _set_pipeline_count(mock_redis, 0)
    limiter = _make_limiter(mock_redis, max_per_minute=0)
    assert limiter.allow() is False


# ---------------------------------------------------------------------------
# Sliding window cleanup
# ---------------------------------------------------------------------------


def test_zremrangebyscore_called_with_window(mock_redis):
    _set_pipeline_count(mock_redis, 0)
    limiter = _make_limiter(mock_redis)
    with patch("seed_storage.rate_limiting.time") as mock_time:
        mock_time.time.return_value = 1000.0
        limiter.allow()
    pipe = mock_redis.pipeline.return_value
    pipe.zremrangebyscore.assert_called_once_with("seed:ratelimit:graphiti", "-inf", 1000.0 - 60.0)


def test_zcard_called_after_cleanup(mock_redis):
    _set_pipeline_count(mock_redis, 3)
    limiter = _make_limiter(mock_redis)
    limiter.allow()
    pipe = mock_redis.pipeline.return_value
    pipe.zcard.assert_called_once_with("seed:ratelimit:graphiti")
