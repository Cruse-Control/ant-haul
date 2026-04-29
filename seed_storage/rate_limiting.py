"""seed_storage/rate_limiting.py — Redis-backed sliding window rate limiter."""

from __future__ import annotations

import time
import uuid

import redis


class RateLimiter:
    """Sliding window rate limiter backed by a Redis sorted set.

    Each allowed request adds a member ``{timestamp}:{uuid4}`` with score = now.
    Members older than 60 s are pruned on every check.

    Canonical key: ``seed:ratelimit:pipeline``
    """

    _WINDOW_SECONDS: float = 60.0

    def __init__(self, redis_client: redis.Redis, key: str, max_per_minute: int) -> None:
        self._redis = redis_client
        self._key = key
        self._max = max_per_minute

    def allow(self) -> bool:
        """Sliding window check. Returns True if under limit and records the request."""
        now = time.time()
        window_start = now - self._WINDOW_SECONDS

        pipe = self._redis.pipeline()
        pipe.zremrangebyscore(self._key, "-inf", window_start)
        pipe.zcard(self._key)
        results = pipe.execute()
        count = int(results[1])

        if count < self._max:
            member = f"{now}:{uuid.uuid4()}"
            self._redis.zadd(self._key, {member: now})
            return True
        return False
