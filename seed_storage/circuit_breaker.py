"""seed_storage/circuit_breaker.py — Redis-backed per-service circuit breaker."""

from __future__ import annotations

import time
from typing import Literal

import redis

# STUB: provided by alerts-agent
try:
    from seed_storage.notifications import send_alert
except ImportError:  # pragma: no cover

    def send_alert(message: str, debounce_key: str | None = None) -> None:  # type: ignore[misc]
        pass


class CircuitBreaker:
    """Per-service circuit breaker backed by Redis.

    State is shared across all workers via two Redis keys:
      - ``seed:circuit:{service_name}:failures``  — integer failure counter
      - ``seed:circuit:{service_name}:opened_at`` — set when circuit trips; TTL = cooldown

    States:
      - **closed**    — failures < threshold (normal operation)
      - **open**      — failures >= threshold AND opened_at key still alive
      - **half-open** — failures >= threshold but opened_at key has expired (cooldown passed)
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        service_name: str,
        failure_threshold: int = 5,
        cooldown_seconds: int = 300,
    ) -> None:
        self._redis = redis_client
        self._service = service_name
        self._threshold = failure_threshold
        self._cooldown = cooldown_seconds
        self._failures_key = f"seed:circuit:{service_name}:failures"
        self._opened_key = f"seed:circuit:{service_name}:opened_at"

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def record_success(self) -> None:
        """Reset the circuit. Fires an alert if circuit was open/half-open."""
        prev_state = self.state
        self._redis.delete(self._failures_key, self._opened_key)
        if prev_state in ("open", "half-open"):
            send_alert(
                f"Circuit breaker CLOSED: {self._service}",
                debounce_key=f"circuit_close:{self._service}",
            )

    def record_failure(self) -> None:
        """Increment failure counter. Opens circuit when threshold is reached."""
        count = self._redis.incr(self._failures_key)
        if int(count) == self._threshold:
            self._redis.set(self._opened_key, time.time(), ex=self._cooldown)
            send_alert(
                f"Circuit breaker OPEN: {self._service}",
                debounce_key=f"circuit_open:{self._service}",
            )

    # ------------------------------------------------------------------
    # State inspection
    # ------------------------------------------------------------------

    def is_open(self) -> bool:
        """Return True only when state is 'open' (within cooldown window)."""
        return self.state == "open"

    @property
    def state(self) -> Literal["closed", "open", "half-open"]:
        failures_val = self._redis.get(self._failures_key)
        if failures_val is None or int(failures_val) < self._threshold:
            return "closed"
        opened_val = self._redis.get(self._opened_key)
        if opened_val is not None:
            return "open"
        return "half-open"
