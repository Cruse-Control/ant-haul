"""seed_storage/expansion/scanner.py — Frontier scanner for automatic URL expansion.

scan_frontier() is the core logic for the Celery beat task of the same name.
worker/tasks.py (worker-agent) registers it as a @app.task and schedules it
via the beat configuration.
"""

from __future__ import annotations

import logging

import redis as redis_lib

from seed_storage.config import settings
from seed_storage.expansion.frontier import pick_top
from seed_storage.expansion.policies import DEPTH_POLICIES

logger = logging.getLogger(__name__)


def scan_frontier(redis_client=None) -> int:
    """Scan the frontier and enqueue expansion tasks for top-priority URLs.

    When ``FRONTIER_AUTO_ENABLED`` is ``False`` (the default), this function
    returns 0 immediately without touching Redis or the task queue.

    Args:
        redis_client: Optional Redis client. If None, a new client is created
                      from ``settings.REDIS_URL``. Pass an explicit client in
                      tests to avoid real Redis connections.

    Returns:
        Number of URLs enqueued (0 when auto-scanning is disabled or the
        frontier is empty).
    """
    if not settings.FRONTIER_AUTO_ENABLED:
        logger.debug("scan_frontier: FRONTIER_AUTO_ENABLED=False, skipping")
        return 0

    if redis_client is None:
        redis_client = redis_lib.from_url(settings.REDIS_URL)

    batch = pick_top(
        redis_client,
        batch_size=settings.MAX_EXPANSION_BREADTH,
        min_threshold=0.0,
        depth_policies=DEPTH_POLICIES,
    )

    if not batch:
        logger.debug("scan_frontier: frontier is empty")
        return 0

    # Late import to avoid circular dependency with worker package (Tier 0 vs Tier 1).
    # STUB: expand_from_frontier task provided by worker-agent (worker/tasks.py).
    from seed_storage.worker.tasks import expand_from_frontier  # noqa: PLC0415

    for item in batch:
        expand_from_frontier.delay(item["url_hash"])

    logger.info("scan_frontier: enqueued %d expansion tasks", len(batch))
    return len(batch)
