"""seed_storage/expansion/frontier.py — Redis-backed URL frontier operations.

The frontier is a Redis sorted set (seed:frontier) where the score is the
priority (higher = more urgent). Metadata for each URL is stored in a
companion hash at seed:frontier:meta:{url_hash}.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

FRONTIER_KEY = "seed:frontier"
META_KEY_PREFIX = "seed:frontier:meta:"


def add_to_frontier(redis_client, url_hash: str, priority: float, meta: dict) -> None:
    """ZADD NX to seed:frontier + HSET seed:frontier:meta:{url_hash}.

    ZADD NX means the URL is only added if it is not already in the frontier.
    The metadata hash is always written (allows updating meta for existing URLs).

    Args:
        redis_client: Redis client instance.
        url_hash:     SHA256 hex hash of the canonical URL (from dedup.url_hash).
        priority:     Score in the sorted set — higher = picked first.
        meta:         Frontier metadata dict (see frontier_meta shape in spec).
    """
    added = redis_client.zadd(FRONTIER_KEY, {url_hash: priority}, nx=True)
    meta_key = f"{META_KEY_PREFIX}{url_hash}"
    redis_client.hset(meta_key, mapping={k: str(v) for k, v in meta.items()})
    if added:
        logger.debug("Frontier: added %s (priority=%.3f)", url_hash[:12], priority)
    else:
        logger.debug("Frontier: %s already present, metadata refreshed", url_hash[:12])


def pick_top(
    redis_client,
    batch_size: int,
    min_threshold: float,
    depth_policies: dict,
) -> list[dict]:
    """Top N URLs from frontier where score >= threshold and depth within policy.

    Queries the frontier sorted set in descending score order and filters out
    any entries whose depth exceeds the policy for their resolver_hint.

    Args:
        redis_client:   Redis client instance.
        batch_size:     Maximum number of results to return.
        min_threshold:  Minimum score (inclusive) for an entry to be considered.
        depth_policies: Dict mapping resolver_hint → max allowed depth.
                        Falls back to depth_policies["default"] when a hint is
                        not found; defaults to 5 if "default" is also absent.

    Returns:
        List of dicts, each containing url_hash, score, and all metadata fields.
        Sorted by priority descending.
    """
    candidates = redis_client.zrevrangebyscore(FRONTIER_KEY, "+inf", min_threshold, withscores=True)

    result: list[dict] = []
    for raw_hash, score in candidates:
        if len(result) >= batch_size:
            break
        url_hash = raw_hash.decode() if isinstance(raw_hash, bytes) else raw_hash
        meta = get_frontier_meta(redis_client, url_hash)
        if meta is None:
            continue
        depth = int(meta.get("depth", 0))
        resolver_hint = meta.get("resolver_hint", "unknown")
        max_depth = depth_policies.get(resolver_hint, depth_policies.get("default", 5))
        if depth > max_depth:
            continue
        result.append({"url_hash": url_hash, "score": score, **meta})

    return result


def remove_from_frontier(redis_client, url_hash: str) -> None:
    """ZREM + DEL metadata hash.

    Idempotent — safe to call when the URL is not in the frontier.
    """
    redis_client.zrem(FRONTIER_KEY, url_hash)
    redis_client.delete(f"{META_KEY_PREFIX}{url_hash}")
    logger.debug("Frontier: removed %s", url_hash[:12])


def get_frontier_meta(redis_client, url_hash: str) -> dict | None:
    """HGETALL seed:frontier:meta:{url_hash}.

    Returns None when the metadata hash does not exist (URL not in frontier
    or metadata was deleted independently).

    The ``depth`` field is returned as int; all other fields are strings.
    """
    raw = redis_client.hgetall(f"{META_KEY_PREFIX}{url_hash}")
    if not raw:
        return None

    meta: dict = {}
    for k, v in raw.items():
        key = k.decode() if isinstance(k, bytes) else k
        val = v.decode() if isinstance(v, bytes) else v
        if key == "depth":
            try:
                meta[key] = int(val)
            except (ValueError, TypeError):
                meta[key] = 0
        else:
            meta[key] = val

    return meta
