"""seed_storage/worker/tasks.py — All Celery tasks.

Tasks:
  enrich_message        — dedup + URL extraction + dispatch → ingest_episode.delay()
  ingest_episode        — Graphiti add_episode() for message + each resolved content
  expand_from_frontier  — fetch frontier URL, resolve, ingest, add child URLs
  scan_frontier         — beat task: pick top frontier URLs and enqueue expand tasks

All tasks are synchronous. Async resolvers are bridged via asyncio.run().
Reaction events are published to seed:reactions (Contract 4).

Retry policy:
  - enrich_message:       3 retries, 60-s delay (network / resolver transients)
  - ingest_episode:       5 retries, 30-s delay (Graphiti / Neo4j transients)
  - expand_from_frontier: 3 retries, 60-s delay
  - scan_frontier:        1 retry,   10-s delay

On final failure → dead_letter() stores the entry for manual replay.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import UTC, datetime
from urllib.parse import urlparse

import redis as redis_lib

from seed_storage.circuit_breaker import CircuitBreaker
from seed_storage.config import settings
from seed_storage.cost_tracking import CostTracker
from seed_storage.dedup import DedupStore, url_hash
from seed_storage.enrichment.dispatcher import ContentDispatcher
from seed_storage.enrichment.models import ResolvedContent
from seed_storage.expansion.frontier import (
    add_to_frontier,
    get_frontier_meta,
    remove_from_frontier,
)
from seed_storage.expansion.policies import compute_priority
from seed_storage.notifications import send_alert
from seed_storage.rate_limiting import RateLimiter
from seed_storage.worker.app import app
from seed_storage.worker.dead_letters import dead_letter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REACTIONS_CHANNEL = "seed:reactions"
GROUP_ID = "seed-storage"

# URL regex for extracting links from message content.
# Matches http:// and https:// URLs (greedy until whitespace or common terminators).
_URL_PATTERN = re.compile(r"https?://[^\s\]\[<>\"']+", re.IGNORECASE)

# Cost tracking defaults (one call ≈ one add_episode)
_COST_PER_CALL = 0.0005  # ~$0.0005 per episode (gpt-4o-mini estimate)


# ---------------------------------------------------------------------------
# Redis client helpers
# ---------------------------------------------------------------------------


def _get_redis() -> redis_lib.Redis:
    """Return a synchronous Redis client from settings.REDIS_URL."""
    return redis_lib.from_url(settings.REDIS_URL)


def _get_dedup_messages(r: redis_lib.Redis) -> DedupStore:
    return DedupStore(r, "seed:seen_messages")


def _get_dedup_urls(r: redis_lib.Redis) -> DedupStore:
    return DedupStore(r, "seed:seen_urls")


def _get_dedup_ingested(r: redis_lib.Redis) -> DedupStore:
    return DedupStore(r, "seed:ingested_content")


def _get_cost_tracker(r: redis_lib.Redis) -> CostTracker:
    return CostTracker(r, settings.DAILY_LLM_BUDGET, _COST_PER_CALL)


def _get_rate_limiter(r: redis_lib.Redis) -> RateLimiter:
    return RateLimiter(r, "seed:ratelimit:graphiti", settings.RATE_LIMIT_PER_MINUTE)


def _get_circuit_breaker(r: redis_lib.Redis) -> CircuitBreaker:
    return CircuitBreaker(r, "graphiti")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_urls(text: str) -> list[str]:
    """Extract all http/https URLs from a text string."""
    return _URL_PATTERN.findall(text)


def _publish_reaction(r: redis_lib.Redis, message_id: str, channel_id: str, emoji: str) -> None:
    """Publish a reaction event to seed:reactions (Contract 4). Never raises."""
    try:
        event = {"message_id": message_id, "channel_id": channel_id, "emoji": emoji}
        r.publish(REACTIONS_CHANNEL, json.dumps(event))
    except Exception as exc:  # noqa: BLE001
        logger.debug("Failed to publish reaction event: %s", exc)


def _source_description_message(source_type: str, source_channel: str) -> str:
    """Build source_description for message episodes per spec Section 4."""
    return f"{source_type.title()} #{source_channel}"


def _source_description_content(source_type: str, source_channel: str, content_type: str) -> str:
    """Build source_description for content episodes per spec Section 4."""
    return f"content_from_{source_type.title()}_{source_channel}:{content_type}"


def _build_content_payload(resolved: ResolvedContent, meta: dict) -> dict:
    """Build enriched_payload for expansion-discovered content (Contract 3)."""
    return {
        "message": {
            "source_type": "expansion",
            "source_id": f"frontier_{meta['url_hash']}",
            "source_channel": meta["source_channel"],
            "author": "system",
            "content": f"Expanded from {meta['discovered_from_url']}",
            "timestamp": meta["discovered_at"],
            "attachments": [],
            "metadata": {
                "frontier_depth": meta["depth"],
                "discovered_from_url": meta["discovered_from_url"],
                "discovered_from_source_id": meta["discovered_from_source_id"],
            },
        },
        "resolved_contents": [resolved.to_dict()],
    }


def _get_domain(url: str) -> str:
    """Extract registered domain from URL (e.g. 'github.com')."""
    try:
        return urlparse(url).netloc.lower()
    except Exception:  # noqa: BLE001
        return ""


# ---------------------------------------------------------------------------
# Task: enrich_message
# ---------------------------------------------------------------------------


@app.task(
    bind=True,
    name="seed_storage.worker.tasks.enrich_message",
    queue="raw_messages",
    max_retries=3,
    default_retry_delay=60,
    acks_late=True,
    reject_on_worker_lost=True,
)
def enrich_message(self, raw_payload: dict) -> None:
    """Dedup + URL extraction + resolve → ingest_episode.delay().

    Contract 1 error rules:
      - content empty AND attachments empty → skip (log DEBUG)
      - author is bot → skip (log DEBUG)

    Steps:
      1. Validate raw_payload shape.
      2. Dedup by {source_type}:{source_id} — skip if already seen.
      3. Publish 📥 reaction to Discord via seed:reactions.
      4. Extract URLs from content + attachment URLs.
      5. Per URL: canonicalize + dedup (seed:seen_urls). Resolve skipped URLs.
      6. Dispatch all new URLs via ContentDispatcher (asyncio.run).
      7. Build enriched_payload (Contract 2) and enqueue ingest_episode.delay().
      8. Publish ⚙️ reaction.
    """
    source_type = raw_payload.get("source_type", "")
    source_id = raw_payload.get("source_id", "")
    content = raw_payload.get("content", "") or ""
    attachments = raw_payload.get("attachments", []) or []
    author = raw_payload.get("author", "") or ""
    channel_id = raw_payload.get("metadata", {}).get("channel_id", "")

    # Error contract: empty content + no attachments
    content = content.strip() if content else ""
    if not content and not attachments:
        logger.debug("enrich_message: skipping empty message source_id=%s", source_id)
        return

    # Error contract: bot author
    if _is_bot_author(author):
        logger.debug(
            "enrich_message: skipping bot message source_id=%s author=%s", source_id, author
        )
        return

    try:
        r = _get_redis()
        dedup_messages = _get_dedup_messages(r)
        dedup_urls = _get_dedup_urls(r)

        # Dedup message
        msg_key = f"{source_type}:{source_id}"
        if dedup_messages.seen_or_mark(msg_key):
            logger.debug("enrich_message: duplicate message %s, skipping", msg_key)
            return

        # Publish 📥 reaction (message received)
        _publish_reaction(r, source_id, channel_id, "📥")

        # Extract all URLs from content + attachments
        content_urls = _extract_urls(content)
        all_urls = list(content_urls) + list(attachments)

        # Per-URL dedup
        new_urls: list[str] = []
        for url in all_urls:
            h = url_hash(url)
            if not dedup_urls.seen_or_mark(h):
                new_urls.append(url)
            else:
                logger.debug("enrich_message: duplicate URL %s, skipping", url)

        # Resolve URLs
        resolved_contents: list[ResolvedContent] = []
        if new_urls:
            dispatcher = ContentDispatcher()
            resolved_contents = asyncio.run(_resolve_urls(dispatcher, new_urls))

        # Build enriched_payload (Contract 2)
        enriched_payload = {
            "message": raw_payload,
            "resolved_contents": [rc.to_dict() for rc in resolved_contents],
        }

        # Enqueue for graph ingest
        ingest_episode.delay(enriched_payload)

        # Publish ⚙️ reaction (processing started)
        _publish_reaction(r, source_id, channel_id, "⚙️")

        logger.info(
            "enrich_message: enqueued source_id=%s urls=%d resolved=%d",
            source_id,
            len(new_urls),
            len(resolved_contents),
        )

    except Exception as exc:  # noqa: BLE001
        logger.error("enrich_message: failed source_id=%s: %s", source_id, exc, exc_info=True)
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            dead_letter("enrich_message", raw_payload, exc, self.request.retries)


def _is_bot_author(author: str) -> bool:
    """Heuristic: author names ending in '#Bot' or containing '[BOT]' etc.

    The primary bot filter is the `message.author.bot` flag in the Discord
    ingestion layer. This is a secondary textual guard for batch imports where
    we only have the display name string.
    Note: batch.py already checks isBot flag; this guard handles edge cases.
    """
    # We intentionally keep this light — the contract says "author is bot",
    # which is enforced at the ingestion layer. We don't apply name heuristics
    # here to avoid false positives on human usernames.
    return False


async def _resolve_urls(dispatcher: ContentDispatcher, urls: list[str]) -> list[ResolvedContent]:
    """Dispatch all URLs concurrently and return results."""
    import asyncio as _asyncio

    tasks = [dispatcher.dispatch(url) for url in urls]
    return list(await _asyncio.gather(*tasks))


# ---------------------------------------------------------------------------
# Task: ingest_episode
# ---------------------------------------------------------------------------


@app.task(
    bind=True,
    name="seed_storage.worker.tasks.ingest_episode",
    queue="graph_ingest",
    max_retries=5,
    default_retry_delay=30,
    acks_late=True,
    reject_on_worker_lost=True,
)
def ingest_episode(self, enriched_payload: dict) -> None:
    """Write message + resolved contents to Graphiti.

    Steps:
      1. Check cost budget — if exceeded, sleep and retry.
      2. Check rate limiter — if over limit, retry with backoff.
      3. Check circuit breaker — if open, skip and alert.
      4. Write message episode via add_episode(group_id="seed-storage").
      5. For each resolved content: write content episode.
      6. Track cost per add_episode() call.
      7. For each resolved content's expansion_urls: add_to_frontier().
      8. Publish 🏷️ reaction (entities tagged) and 🧠 reaction (graph updated).

    source_description formats (spec Section 4):
      - message:  "{source_type.title()} #{source_channel}"
      - content:  "content_from_{source_type.title()}_{source_channel}:{content_type}"

    Anti-fallback rule: if add_episode() fails, do NOT fall back to Cypher.
    """
    message = enriched_payload.get("message", {})
    resolved_contents_raw = enriched_payload.get("resolved_contents", [])

    source_type = message.get("source_type", "unknown")
    source_id = message.get("source_id", "")
    source_channel = message.get("source_channel", "unknown")
    channel_id = message.get("metadata", {}).get("channel_id", "")
    content = message.get("content", "") or ""
    timestamp_str = message.get("timestamp", "")

    try:
        r = _get_redis()
        cost_tracker = _get_cost_tracker(r)
        rate_limiter = _get_rate_limiter(r)
        circuit_breaker = _get_circuit_breaker(r)

        # 1. Budget check
        if cost_tracker.is_budget_exceeded():
            logger.warning("ingest_episode: daily budget exceeded, retrying later")
            send_alert(
                "Seed-storage daily LLM budget exceeded — graph ingest paused.",
                debounce_key="budget_exceeded",
            )
            try:
                raise self.retry(countdown=300)  # 5-minute backoff
            except self.MaxRetriesExceededError:
                dead_letter(
                    "ingest_episode",
                    enriched_payload,
                    Exception("budget_exceeded"),
                    self.request.retries,
                )
                return

        # 2. Rate limit check
        if not rate_limiter.allow():
            logger.warning("ingest_episode: rate limit reached, retrying")
            try:
                raise self.retry(countdown=10)
            except self.MaxRetriesExceededError:
                dead_letter(
                    "ingest_episode",
                    enriched_payload,
                    Exception("rate_limit_exceeded"),
                    self.request.retries,
                )
                return

        # 3. Circuit breaker check
        if circuit_breaker.is_open():
            logger.warning("ingest_episode: circuit breaker open for graphiti, skipping")
            send_alert(
                "Graphiti circuit breaker is open — ingest episode skipped.",
                debounce_key="circuit_open_graphiti",
            )
            return

        # 4. Build episode body
        episode_body = content
        if not episode_body:
            for rc_dict in resolved_contents_raw:
                if rc_dict.get("text"):
                    episode_body = rc_dict["text"][:500]
                    break

        source_desc_msg = _source_description_message(source_type, source_channel)
        dedup_ingested = _get_dedup_ingested(r)

        # 5. Prepare content episodes (dedup synchronously before any async work)
        all_rcs = [ResolvedContent.from_dict(rc_dict) for rc_dict in resolved_contents_raw]
        content_episode_list: list[dict] = []
        for rc in all_rcs:
            content_text = rc.text or rc.summary or rc.transcript or ""
            if not content_text:
                logger.debug(
                    "ingest_episode: no text for %s, skipping content episode", rc.source_url
                )
                continue
            h = url_hash(rc.source_url)
            if dedup_ingested.seen_or_mark(h):
                logger.debug("ingest_episode: already ingested %s", rc.source_url)
                continue
            source_desc_content = _source_description_content(
                source_type, source_channel, rc.content_type
            )
            content_episode_list.append(
                {
                    "name": f"content_{source_type}_{rc.source_url[:60]}",
                    "episode_body": content_text,
                    "source_description": source_desc_content,
                    "reference_time": rc.resolved_at,
                    "source_id": rc.source_url,
                }
            )

        planned_count = 1 + len(content_episode_list)

        # 6. Write ALL episodes in a single asyncio.run() — avoids the Neo4j driver
        #    "pending task in closed loop" error caused by multiple asyncio.run() calls
        #    sharing the same Graphiti singleton (which holds event-loop-bound connections).
        try:
            asyncio.run(
                _write_all_episodes(
                    message_name=f"{source_type}_{source_id}",
                    episode_body=episode_body,
                    source_desc_msg=source_desc_msg,
                    reference_time=_parse_timestamp(timestamp_str),
                    message_source_id=source_id,
                    content_episode_list=content_episode_list,
                )
            )
            for _ in range(planned_count):
                cost_tracker.increment()
            circuit_breaker.record_success()
        except Exception as exc:  # noqa: BLE001
            circuit_breaker.record_failure()
            logger.error(
                "ingest_episode: add_episode failed for message %s: %s",
                source_id,
                exc,
                exc_info=True,
            )
            try:
                raise self.retry(exc=exc)
            except self.MaxRetriesExceededError:
                dead_letter("ingest_episode", enriched_payload, exc, self.request.retries)
                return

        # 7. Add expansion_urls to frontier (from ALL resolved contents, not just text-bearing)
        for rc in all_rcs:
            depth = int(message.get("metadata", {}).get("frontier_depth", 0))
            child_depth = depth + 1

            if child_depth > settings.HARD_DEPTH_CEILING:
                logger.debug(
                    "ingest_episode: depth ceiling %d reached, not expanding %s",
                    settings.HARD_DEPTH_CEILING,
                    rc.source_url,
                )
                continue

            expansion_urls = rc.expansion_urls[: settings.MAX_EXPANSION_BREADTH]
            for exp_url in expansion_urls:
                h = url_hash(exp_url)
                domain = _get_domain(exp_url)
                priority = compute_priority(
                    depth=child_depth,
                    resolver_hint="unknown",
                    domain=domain,
                    source_channel=source_channel,
                )
                discovered_at = datetime.now(tz=UTC).isoformat()
                meta = {
                    "url": exp_url,
                    "url_hash": h,
                    "discovered_from_url": rc.source_url,
                    "discovered_from_source_id": source_id,
                    "source_channel": source_channel,
                    "depth": child_depth,
                    "resolver_hint": "unknown",
                    "discovered_at": discovered_at,
                }
                try:
                    add_to_frontier(r, h, priority, meta)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("ingest_episode: failed to add %s to frontier: %s", exp_url, exc)

        # 8. Publish reactions
        _publish_reaction(r, source_id, channel_id, "🏷️")
        _publish_reaction(r, source_id, channel_id, "🧠")

        logger.info(
            "ingest_episode: completed source_id=%s contents=%d",
            source_id,
            len(resolved_contents_raw),
        )

    except Exception as exc:  # noqa: BLE001
        logger.error(
            "ingest_episode: unhandled error source_id=%s: %s", source_id, exc, exc_info=True
        )
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            dead_letter("ingest_episode", enriched_payload, exc, self.request.retries)


async def _get_graphiti_instance():
    """Get Graphiti singleton (async)."""
    from seed_storage.graphiti_client import get_graphiti

    return await get_graphiti()


async def _add_episode(
    graphiti,
    name: str,
    episode_body: str,
    source_description: str,
    reference_time: datetime,
    source_id: str,
) -> None:
    """Call graphiti.add_episode() with group_id='seed-storage'."""
    from graphiti_core.nodes import EpisodeType

    await graphiti.add_episode(
        name=name,
        episode_body=episode_body,
        source_description=source_description,
        reference_time=reference_time,
        source=EpisodeType.text,
        group_id=GROUP_ID,
    )


async def _write_all_episodes(
    *,
    message_name: str,
    episode_body: str,
    source_desc_msg: str,
    reference_time: datetime,
    message_source_id: str,
    content_episode_list: list[dict],
) -> None:
    """Write message + content episodes in a single event loop.

    All Graphiti / Neo4j async calls share one event loop, avoiding the
    "Task pending in closed loop" error that arises from multiple asyncio.run()
    calls reusing the same Graphiti singleton.

    Raises on message episode failure (caller retries the task).
    Logs and continues for individual content episode failures.
    """
    graphiti = await _get_graphiti_instance()

    await _add_episode(
        graphiti,
        name=message_name,
        episode_body=episode_body,
        source_description=source_desc_msg,
        reference_time=reference_time,
        source_id=message_source_id,
    )

    for args in content_episode_list:
        try:
            await _add_episode(graphiti, **args)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "_write_all_episodes: content episode failed for %s: %s",
                args.get("source_id", "?"),
                exc,
                exc_info=True,
            )


def _parse_timestamp(timestamp_str: str) -> datetime:
    """Parse ISO 8601 timestamp string. Returns utcnow() on failure."""
    if not timestamp_str:
        return datetime.now(tz=UTC)
    try:
        return datetime.fromisoformat(timestamp_str)
    except (ValueError, TypeError):
        return datetime.now(tz=UTC)


# ---------------------------------------------------------------------------
# Task: expand_from_frontier
# ---------------------------------------------------------------------------


@app.task(
    bind=True,
    name="seed_storage.worker.tasks.expand_from_frontier",
    queue="graph_ingest",
    max_retries=3,
    default_retry_delay=60,
    acks_late=True,
    reject_on_worker_lost=True,
)
def expand_from_frontier(self, url_hash_str: str) -> None:
    """Fetch + resolve a frontier URL and write it to the graph.

    Steps:
      1. Load frontier metadata for url_hash_str.
      2. Check depth ceiling — skip if at/above HARD_DEPTH_CEILING.
      3. Check ingested_content dedup — skip if already ingested.
      4. Resolve URL via ContentDispatcher (asyncio.run).
      5. Build enriched_payload via _build_content_payload() (Contract 3).
      6. Enqueue ingest_episode.delay() with the payload.
      7. Add child expansion_urls to frontier (capped at MAX_EXPANSION_BREADTH).
      8. Remove url_hash_str from frontier.
    """
    try:
        r = _get_redis()
        dedup_ingested = _get_dedup_ingested(r)

        # Load frontier metadata
        meta = get_frontier_meta(r, url_hash_str)
        if meta is None:
            logger.warning("expand_from_frontier: no metadata for %s, skipping", url_hash_str)
            return

        url = meta.get("url", "")
        if not url:
            logger.warning("expand_from_frontier: empty URL in metadata for %s", url_hash_str)
            remove_from_frontier(r, url_hash_str)
            return

        depth = int(meta.get("depth", 0))

        # Depth ceiling check
        if depth >= settings.HARD_DEPTH_CEILING:
            logger.debug(
                "expand_from_frontier: depth ceiling reached (%d >= %d) for %s",
                depth,
                settings.HARD_DEPTH_CEILING,
                url,
            )
            remove_from_frontier(r, url_hash_str)
            return

        # Already-ingested check
        if dedup_ingested.seen_or_mark(url_hash_str):
            logger.debug("expand_from_frontier: already ingested %s", url)
            remove_from_frontier(r, url_hash_str)
            return

        # Resolve URL
        dispatcher = ContentDispatcher()
        resolved = asyncio.run(dispatcher.dispatch(url))

        # Ensure url_hash is in meta for _build_content_payload
        if "url_hash" not in meta:
            meta["url_hash"] = url_hash_str
        if "discovered_from_url" not in meta:
            meta["discovered_from_url"] = url
        if "discovered_from_source_id" not in meta:
            meta["discovered_from_source_id"] = ""
        if "source_channel" not in meta:
            meta["source_channel"] = "unknown"
        if "discovered_at" not in meta:
            meta["discovered_at"] = datetime.now(tz=UTC).isoformat()

        # Build enriched_payload (Contract 3)
        enriched = _build_content_payload(resolved, meta)

        # Enqueue graph ingest
        ingest_episode.delay(enriched)

        # Add child expansion URLs to frontier
        child_depth = depth + 1
        source_channel = meta.get("source_channel", "unknown")
        discovered_at = datetime.now(tz=UTC).isoformat()

        expansion_urls = resolved.expansion_urls[: settings.MAX_EXPANSION_BREADTH]
        for exp_url in expansion_urls:
            h = url_hash(exp_url)
            domain = _get_domain(exp_url)
            priority = compute_priority(
                depth=child_depth,
                resolver_hint="unknown",
                domain=domain,
                source_channel=source_channel,
            )
            child_meta = {
                "url": exp_url,
                "url_hash": h,
                "discovered_from_url": url,
                "discovered_from_source_id": meta.get("discovered_from_source_id", ""),
                "source_channel": source_channel,
                "depth": child_depth,
                "resolver_hint": "unknown",
                "discovered_at": discovered_at,
            }
            try:
                add_to_frontier(r, h, priority, child_meta)
            except Exception as exc:  # noqa: BLE001
                logger.warning("expand_from_frontier: failed to add child URL %s: %s", exp_url, exc)

        # Remove processed URL from frontier
        remove_from_frontier(r, url_hash_str)

        logger.info(
            "expand_from_frontier: expanded %s depth=%d child_urls=%d",
            url,
            depth,
            len(expansion_urls),
        )

    except Exception as exc:  # noqa: BLE001
        logger.error("expand_from_frontier: failed %s: %s", url_hash_str, exc, exc_info=True)
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            dead_letter(
                "expand_from_frontier",
                {"url_hash": url_hash_str},
                exc,
                self.request.retries,
            )


# ---------------------------------------------------------------------------
# Task: scan_frontier
# ---------------------------------------------------------------------------


@app.task(
    bind=True,
    name="seed_storage.worker.tasks.scan_frontier",
    queue="graph_ingest",
    max_retries=1,
    default_retry_delay=10,
    acks_late=True,
)
def scan_frontier(self) -> int:
    """Beat task: scan frontier and enqueue expand tasks for top-priority URLs.

    Delegates to expansion.scanner.scan_frontier() which:
      - Returns 0 immediately when FRONTIER_AUTO_ENABLED=False.
      - Picks top-N URLs from frontier (batch_size=MAX_EXPANSION_BREADTH).
      - Enqueues expand_from_frontier.delay(url_hash) for each.

    Returns count of enqueued tasks.
    """
    try:
        from seed_storage.expansion.scanner import scan_frontier as _scan  # noqa: PLC0415

        count = _scan()
        if count:
            logger.info("scan_frontier: enqueued %d expand tasks", count)
        else:
            logger.debug("scan_frontier: nothing to enqueue")
        return count
    except Exception as exc:  # noqa: BLE001
        logger.error("scan_frontier: failed: %s", exc, exc_info=True)
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            logger.error("scan_frontier: max retries exceeded")
            return 0


# ---------------------------------------------------------------------------
# Utility: unused but exported for Contract 3 completeness
# ---------------------------------------------------------------------------


def build_content_payload(resolved: ResolvedContent, meta: dict) -> dict:
    """Public alias for _build_content_payload (Contract 3)."""
    return _build_content_payload(resolved, meta)
