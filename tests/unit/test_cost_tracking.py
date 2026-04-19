"""Unit tests for seed_storage.cost_tracking.CostTracker (~10 tests)."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from seed_storage.cost_tracking import _TTL_SECONDS, CostTracker

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_redis():
    r = MagicMock()
    pipe = MagicMock()
    r.pipeline.return_value = pipe
    pipe.execute.return_value = [0.001, True]
    return r


@pytest.fixture()
def tracker(mock_redis):
    return CostTracker(mock_redis, daily_budget=5.0, cost_per_call=0.001)


# ---------------------------------------------------------------------------
# Key format
# ---------------------------------------------------------------------------


def test_key_uses_today(tracker, mock_redis):
    mock_redis.get.return_value = None
    tracker.get_current_spend()
    expected_key = f"seed:cost:daily:{date.today().isoformat()}"
    mock_redis.get.assert_called_once_with(expected_key)


# ---------------------------------------------------------------------------
# get_current_spend
# ---------------------------------------------------------------------------


def test_get_current_spend_returns_zero_when_no_key(tracker, mock_redis):
    mock_redis.get.return_value = None
    assert tracker.get_current_spend() == 0.0


def test_get_current_spend_returns_float(tracker, mock_redis):
    mock_redis.get.return_value = b"1.234"
    assert tracker.get_current_spend() == pytest.approx(1.234)


# ---------------------------------------------------------------------------
# increment
# ---------------------------------------------------------------------------


def test_increment_calls_incrbyfloat(mock_redis):
    ct = CostTracker(mock_redis, daily_budget=5.0, cost_per_call=0.002)
    ct.increment()
    pipe = mock_redis.pipeline.return_value
    pipe.incrbyfloat.assert_called_once_with(f"seed:cost:daily:{date.today().isoformat()}", 0.002)


def test_increment_sets_48h_ttl(mock_redis):
    ct = CostTracker(mock_redis, daily_budget=5.0, cost_per_call=0.001)
    ct.increment()
    pipe = mock_redis.pipeline.return_value
    pipe.expire.assert_called_once_with(f"seed:cost:daily:{date.today().isoformat()}", _TTL_SECONDS)


def test_increment_calls_execute(mock_redis):
    ct = CostTracker(mock_redis, daily_budget=5.0, cost_per_call=0.001)
    ct.increment()
    mock_redis.pipeline.return_value.execute.assert_called_once()


# ---------------------------------------------------------------------------
# is_budget_exceeded
# ---------------------------------------------------------------------------


def test_budget_not_exceeded_when_under(tracker, mock_redis):
    mock_redis.get.return_value = b"3.99"
    assert tracker.is_budget_exceeded() is False


def test_budget_exceeded_at_exact_limit(tracker, mock_redis):
    mock_redis.get.return_value = b"5.0"
    assert tracker.is_budget_exceeded() is True


def test_budget_exceeded_when_over(tracker, mock_redis):
    mock_redis.get.return_value = b"6.00"
    assert tracker.is_budget_exceeded() is True


# ---------------------------------------------------------------------------
# is_warning_threshold
# ---------------------------------------------------------------------------


def test_warning_threshold_not_reached(tracker, mock_redis):
    mock_redis.get.return_value = b"3.99"
    assert tracker.is_warning_threshold() is False  # 3.99 < 5.0 * 0.8 = 4.0


def test_warning_threshold_reached_at_80_percent(tracker, mock_redis):
    mock_redis.get.return_value = b"4.0"
    assert tracker.is_warning_threshold() is True  # 4.0 >= 5.0 * 0.8


def test_custom_warning_threshold(mock_redis):
    ct = CostTracker(mock_redis, daily_budget=10.0, cost_per_call=0.01, warning_threshold=0.5)
    mock_redis.get.return_value = b"5.0"  # exactly 50%
    assert ct.is_warning_threshold() is True
    mock_redis.get.return_value = b"4.99"
    assert ct.is_warning_threshold() is False
