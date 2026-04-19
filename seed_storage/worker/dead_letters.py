"""seed_storage/worker/dead_letters.py — Dead-letter queue backed by Redis LIST.

Contract 11:
  dead_letter(task_name, payload, exc, retries) — RPUSH to seed:dead_letters
  list_dead_letters(redis_client) — LRANGE without consuming
  replay_one(redis_client) — LPOP oldest entry → (task_name, payload) or None
  replay_all(redis_client) — pop all entries → list of (task_name, payload)
"""

from __future__ import annotations

import json
import logging
import re
import traceback
from datetime import UTC, datetime

import redis as redis_lib

from seed_storage.config import _MASK, _SECRET_PATTERNS, settings

logger = logging.getLogger(__name__)

DEAD_LETTERS_KEY = "seed:dead_letters"

# Credential sanitization: reuse API-key patterns from config._SECRET_PATTERNS
# and extend with file-path patterns for credential files on disk.
_FILE_PATH_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"/opt/shared/[^\s,\"']+"),
    re.compile(r"/etc/[^\s,\"']*(?:token|secret|key|password|credential)[^\s,\"']*", re.IGNORECASE),
]

_SANITIZE_PATTERNS: list[re.Pattern[str]] = _SECRET_PATTERNS + _FILE_PATH_PATTERNS


def _sanitize(text: str) -> str:
    """Replace recognised credential patterns with ***MASKED***."""
    for pattern in _SANITIZE_PATTERNS:
        text = pattern.sub(_MASK, text)
    return text


def dead_letter(task_name: str, payload: dict, exc: Exception, retries: int) -> None:
    """RPUSH to seed:dead_letters. Sanitize traceback (strip credential paths, mask API keys)."""
    tb_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    tb_sanitized = _sanitize(tb_text)
    exc_msg = _sanitize(str(exc))

    # Best-effort source_id extraction from common payload shapes
    source_id = (
        payload.get("source_id") or payload.get("url") or payload.get("message_id") or "<unknown>"
    )

    entry = {
        "task_name": task_name,
        "payload": payload,
        "source_id": source_id,
        "exception_type": type(exc).__name__,
        "exception_message": exc_msg,
        "traceback": tb_sanitized,
        "retries": retries,
        "failed_at": datetime.now(UTC).isoformat(),
    }

    try:
        r = redis_lib.from_url(settings.REDIS_URL)
        r.rpush(DEAD_LETTERS_KEY, json.dumps(entry))
    except Exception as redis_exc:  # noqa: BLE001
        logger.warning("dead_letter: failed to push to Redis: %s", redis_exc)


def list_dead_letters(redis_client: redis_lib.Redis) -> tuple[int, list[dict]]:
    """LRANGE — count + preview without consuming."""
    raw_items = redis_client.lrange(DEAD_LETTERS_KEY, 0, -1)
    entries: list[dict] = []
    for item in raw_items:
        try:
            entries.append(json.loads(item))
        except (json.JSONDecodeError, TypeError):
            logger.warning("list_dead_letters: failed to decode entry: %r", item)
    return len(entries), entries


def replay_one(redis_client: redis_lib.Redis) -> tuple[str, dict] | None:
    """LPOP oldest entry. Returns (task_name, payload) or None if queue is empty."""
    raw = redis_client.lpop(DEAD_LETTERS_KEY)
    if raw is None:
        return None

    try:
        entry = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("replay_one: failed to decode dead-letter entry")
        return None

    task_name = entry.get("task_name") or ""
    if not task_name:
        logger.warning("replay_one: dead-letter entry has unknown task_name")

    return task_name, entry.get("payload", {})


def replay_all(redis_client: redis_lib.Redis) -> list[tuple[str, dict]]:
    """Pop all entries. Returns list of (task_name, payload)."""
    results: list[tuple[str, dict]] = []
    while True:
        item = replay_one(redis_client)
        if item is None:
            break
        results.append(item)
    return results
