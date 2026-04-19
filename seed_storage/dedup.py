"""seed_storage/dedup.py — Redis-backed dedup store and URL canonicalization."""

from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import redis

# Tracking / noise params to strip from URLs before deduplication.
_STRIP_PARAMS: frozenset[str] = frozenset(
    {
        "fbclid",
        "ref",
        "si",
        "t",
        "s",
    }
)


def canonicalize_url(url: str) -> str:
    """Normalize URL for dedup.

    Strips utm_*, fbclid, ref, si, t, s params. Lowercases scheme+host.
    Preserves path case. Sorts remaining query params. Removes trailing
    slash and fragment. Returns original on malformed input.
    """
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return url
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        # Strip trailing slash unless path is just "/"
        path = parsed.path
        if path != "/":
            path = path.rstrip("/")
        # Filter and sort query params
        params = sorted(
            (k, v)
            for k, v in parse_qsl(parsed.query, keep_blank_values=True)
            if k not in _STRIP_PARAMS and not k.startswith("utm_")
        )
        query = urlencode(params)
        # Drop fragment
        return urlunparse((scheme, netloc, path, parsed.params, query, ""))
    except Exception:
        return url


def url_hash(url: str) -> str:
    """SHA256 hex digest of canonicalize_url(url)."""
    return hashlib.sha256(canonicalize_url(url).encode()).hexdigest()


class DedupStore:
    """Redis SET-backed deduplication store.

    Usage pattern (three separate instances):
        seen_messages  = DedupStore(redis, "seed:seen_messages")
        seen_urls      = DedupStore(redis, "seed:seen_urls")
        ingested_urls  = DedupStore(redis, "seed:ingested_content")
    """

    def __init__(self, redis_client: redis.Redis, set_key: str) -> None:
        self._redis = redis_client
        self._key = set_key

    def is_seen(self, key: str) -> bool:
        """Return True if key is a member of the set."""
        return bool(self._redis.sismember(self._key, key))

    def mark_seen(self, key: str) -> None:
        """Add key to the set (no-op if already present)."""
        self._redis.sadd(self._key, key)

    def seen_or_mark(self, key: str) -> bool:
        """Atomic SADD. Returns True if already seen (not added), False if newly added."""
        # SADD returns number of elements actually added: 0 if already present.
        added = self._redis.sadd(self._key, key)
        return int(added) == 0
