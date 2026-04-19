"""seed_storage/cost_tracking.py — Redis-backed daily LLM cost counter."""

from __future__ import annotations

from datetime import date

import redis

_TTL_SECONDS = 48 * 3600  # 48-hour TTL so keys survive a midnight rollover


class CostTracker:
    """Tracks daily LLM API spend in Redis.

    Key pattern: ``seed:cost:daily:YYYY-MM-DD``
    TTL: 48 hours (survives midnight rollover).
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        daily_budget: float,
        cost_per_call: float,
        warning_threshold: float = 0.8,
    ) -> None:
        self._redis = redis_client
        self._daily_budget = daily_budget
        self._cost_per_call = cost_per_call
        self._warning_threshold = warning_threshold

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _key(self) -> str:
        return f"seed:cost:daily:{date.today().isoformat()}"

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def increment(self) -> None:
        """Increment daily counter by cost_per_call. Sets 48h TTL on first call."""
        key = self._key()
        pipe = self._redis.pipeline()
        pipe.incrbyfloat(key, self._cost_per_call)
        pipe.expire(key, _TTL_SECONDS)
        pipe.execute()

    def get_current_spend(self) -> float:
        """Return today's accumulated spend in USD (0.0 if no calls yet)."""
        val = self._redis.get(self._key())
        if val is None:
            return 0.0
        return float(val)

    def is_budget_exceeded(self) -> bool:
        """Return True if today's spend has reached or exceeded the daily budget."""
        return self.get_current_spend() >= self._daily_budget

    def is_warning_threshold(self) -> bool:
        """Return True if today's spend has reached or exceeded warning_threshold * budget."""
        return self.get_current_spend() >= self._daily_budget * self._warning_threshold
