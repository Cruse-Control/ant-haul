"""Unit tests for seed_storage.circuit_breaker.CircuitBreaker (~12 tests)."""

from unittest.mock import MagicMock, patch

import pytest

from seed_storage.circuit_breaker import CircuitBreaker

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_redis():
    return MagicMock()


def _make_cb(mock_redis, threshold=3, cooldown=60):
    return CircuitBreaker(
        mock_redis, "graphiti", failure_threshold=threshold, cooldown_seconds=cooldown
    )


def _set_state(mock_redis, failures: int | None, opened: bool):
    """Configure mock redis.get returns for (failures_key, opened_key).

    Provides enough values for two consecutive state checks (e.g. cb.state
    followed by cb.is_open(), each of which calls cb.state internally).
    """
    if failures is None:
        failures_val = None
    else:
        failures_val = str(failures).encode()
    opened_val = b"1234567890.0" if opened else None
    mock_redis.get.side_effect = [
        failures_val,
        opened_val,
        failures_val,
        opened_val,
    ]


# ---------------------------------------------------------------------------
# Initial / closed state
# ---------------------------------------------------------------------------


def test_initial_state_is_closed(mock_redis):
    mock_redis.get.return_value = None
    cb = _make_cb(mock_redis)
    assert cb.state == "closed"


def test_is_open_false_when_closed(mock_redis):
    mock_redis.get.return_value = None
    cb = _make_cb(mock_redis)
    assert cb.is_open() is False


def test_failures_below_threshold_stay_closed(mock_redis):
    _set_state(mock_redis, failures=2, opened=False)
    cb = _make_cb(mock_redis, threshold=3)
    assert cb.state == "closed"


# ---------------------------------------------------------------------------
# record_failure
# ---------------------------------------------------------------------------


def test_record_failure_increments_counter(mock_redis):
    mock_redis.incr.return_value = 1
    cb = _make_cb(mock_redis)
    cb.record_failure()
    mock_redis.incr.assert_called_once_with("seed:circuit:graphiti:failures")


def test_record_failure_opens_circuit_at_threshold(mock_redis):
    mock_redis.incr.return_value = 3  # == threshold
    cb = _make_cb(mock_redis, threshold=3)
    cb.record_failure()
    mock_redis.set.assert_called_once()
    _, kwargs = mock_redis.set.call_args
    assert kwargs.get("ex") == 60


def test_record_failure_does_not_open_below_threshold(mock_redis):
    mock_redis.incr.return_value = 2  # below threshold=3
    cb = _make_cb(mock_redis, threshold=3)
    cb.record_failure()
    mock_redis.set.assert_not_called()


def test_record_failure_sends_alert_when_circuit_opens():
    mock_redis = MagicMock()
    mock_redis.incr.return_value = 5
    cb = CircuitBreaker(mock_redis, "neo4j", failure_threshold=5)
    with patch("seed_storage.circuit_breaker.send_alert") as mock_alert:
        cb.record_failure()
        mock_alert.assert_called_once()
        assert "OPEN" in mock_alert.call_args[0][0]
        assert "neo4j" in mock_alert.call_args[0][0]


# ---------------------------------------------------------------------------
# open / half-open states
# ---------------------------------------------------------------------------


def test_state_is_open_when_within_cooldown(mock_redis):
    _set_state(mock_redis, failures=3, opened=True)
    cb = _make_cb(mock_redis, threshold=3)
    assert cb.state == "open"
    assert cb.is_open() is True


def test_state_is_half_open_after_cooldown(mock_redis):
    """opened_at key has expired but failures key still exists."""
    _set_state(mock_redis, failures=3, opened=False)
    cb = _make_cb(mock_redis, threshold=3)
    assert cb.state == "half-open"
    assert cb.is_open() is False


# ---------------------------------------------------------------------------
# record_success
# ---------------------------------------------------------------------------


def test_record_success_deletes_both_keys(mock_redis):
    _set_state(mock_redis, failures=None, opened=False)
    cb = _make_cb(mock_redis)
    cb.record_success()
    mock_redis.delete.assert_called_once_with(
        "seed:circuit:graphiti:failures",
        "seed:circuit:graphiti:opened_at",
    )


def test_record_success_sends_alert_when_was_open():
    mock_redis = MagicMock()
    # First two .get calls for self.state (failures >= threshold, opened key present)
    mock_redis.get.side_effect = [b"3", b"timestamp", None, None]
    cb = CircuitBreaker(mock_redis, "graphiti", failure_threshold=3)
    with patch("seed_storage.circuit_breaker.send_alert") as mock_alert:
        cb.record_success()
        mock_alert.assert_called_once()
        assert "CLOSED" in mock_alert.call_args[0][0]


def test_record_success_no_alert_when_already_closed():
    mock_redis = MagicMock()
    mock_redis.get.return_value = None  # failures=None → closed
    cb = CircuitBreaker(mock_redis, "graphiti", failure_threshold=3)
    with patch("seed_storage.circuit_breaker.send_alert") as mock_alert:
        cb.record_success()
        mock_alert.assert_not_called()
