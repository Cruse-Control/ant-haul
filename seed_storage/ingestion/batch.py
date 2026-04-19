"""seed_storage/ingestion/batch.py — DiscordChatExporter JSON batch import.

Parses a DiscordChatExporter JSON export file and enqueues each message via
enrich_message.delay(). Caps at BATCH_CAP (5000) messages per run. Skips
malformed entries with a log warning and continues processing.

DiscordChatExporter export format (top level):
    {
        "guild":    {"id": "...", "name": "..."},
        "channel":  {"id": "...", "name": "...", ...},
        "messages": [ ... ],
    }

Each message entry:
    {
        "id":        "...",
        "timestamp": "2024-01-01T12:00:00+00:00",
        "content":   "...",
        "author":    {"id": "...", "name": "...", "nickname": "...", "isBot": false},
        "attachments": [{"url": "..."}],
    }

Usage:
    python -m seed_storage.ingestion.batch <file.json> [--offset N]
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

BATCH_CAP = 5_000

# STUB: replace with worker-agent implementation
# This stub exists only for development. At merge, worker-agent's real
# seed_storage/worker/tasks.py replaces this.
try:
    from seed_storage.worker.tasks import enrich_message as _enrich_message  # type: ignore[import]
except ImportError:

    class _EnrichMessageStub:
        @staticmethod
        def delay(payload: dict) -> None:
            logger.debug("enrich_message.delay() stub called (worker not yet available)")

    _enrich_message = _EnrichMessageStub()  # type: ignore[assignment]


def _parse_message(
    msg: dict[str, Any],
    channel_name: str,
    channel_id: str = "",
    guild_id: str = "",
) -> dict[str, Any] | None:
    """Convert a DiscordChatExporter message dict to raw_payload (Contract 1).

    Returns None if the entry is malformed (missing required fields). Logs a
    WARNING before returning None so the caller can increment the failure count.
    """
    try:
        author_obj = msg.get("author", {})
        if not isinstance(author_obj, dict):
            logger.warning("Malformed message entry: 'author' is not a dict")
            return None

        author_name = author_obj.get("nickname") or author_obj.get("name", "")
        content = msg.get("content", "") or ""
        attachments_raw = msg.get("attachments", []) or []
        attachments = [
            a["url"]
            for a in attachments_raw
            if isinstance(a, dict) and "url" in a
        ]

        return {
            "source_type": "discord",
            "source_id": str(msg["id"]),
            "source_channel": channel_name,
            "author": author_name,
            "content": content,
            "timestamp": msg["timestamp"],
            "attachments": attachments,
            "metadata": {
                "channel_id": channel_id,
                "author_id": str(author_obj.get("id", "")),
                "guild_id": guild_id,
            },
        }
    except (KeyError, TypeError) as exc:
        logger.warning("Malformed message entry, skipping: %s", exc)
        return None


def import_file(path: str | Path, offset: int = 0) -> dict[str, int]:
    """Import a DiscordChatExporter JSON export file.

    Args:
        path:   Path to the JSON export file.
        offset: Number of messages to skip from the beginning (before the cap).

    Returns:
        Summary dict: {"total": int, "enqueued": int, "skipped": int, "failed": int}
        ``total`` is the total message count in the file (before offset/cap).
    """
    path = Path(path)

    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        logger.error("Cannot read file %s: %s", path, exc)
        return {"total": 0, "enqueued": 0, "skipped": 0, "failed": 0}

    if not raw.strip():
        logger.warning("Empty file: %s", path)
        return {"total": 0, "enqueued": 0, "skipped": 0, "failed": 0}

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON in %s: %s", path, exc)
        return {"total": 0, "enqueued": 0, "skipped": 0, "failed": 0}

    if not isinstance(data, dict):
        logger.error("Expected a JSON object at top level in %s", path)
        return {"total": 0, "enqueued": 0, "skipped": 0, "failed": 0}

    messages = data.get("messages", [])
    if not isinstance(messages, list):
        logger.error("'messages' key missing or not a list in %s", path)
        return {"total": 0, "enqueued": 0, "skipped": 0, "failed": 0}

    channel_obj = data.get("channel", {})
    channel_name = channel_obj.get("name", "unknown") if isinstance(channel_obj, dict) else "unknown"
    channel_id = str(channel_obj.get("id", "")) if isinstance(channel_obj, dict) else ""

    guild_obj = data.get("guild", {})
    guild_id = str(guild_obj.get("id", "")) if isinstance(guild_obj, dict) else ""

    total = len(messages)
    slice_ = messages[offset:]

    enqueued = 0
    skipped = 0
    failed = 0

    for i, msg in enumerate(slice_):
        if enqueued >= BATCH_CAP:
            logger.info("Batch cap of %d reached; stopping.", BATCH_CAP)
            break

        if not isinstance(msg, dict):
            logger.warning("Non-dict message entry at index %d, skipping", i + offset)
            skipped += 1
            continue

        # Skip bot messages (Contract 1 error contract).
        if msg.get("author", {}).get("isBot", False):
            logger.debug("Skipping bot message id=%s", msg.get("id"))
            skipped += 1
            continue

        payload = _parse_message(msg, channel_name, channel_id, guild_id)
        if payload is None:
            failed += 1
            continue

        # Skip messages with no content and no attachments (Contract 1 error contract).
        if not payload["content"] and not payload["attachments"]:
            logger.debug("Skipping empty message id=%s", payload["source_id"])
            skipped += 1
            continue

        _enrich_message.delay(payload)
        enqueued += 1

        if enqueued % 100 == 0:
            logger.info(
                "Progress: %d/%d messages enqueued",
                enqueued,
                min(len(slice_), BATCH_CAP),
            )

    summary = {"total": total, "enqueued": enqueued, "skipped": skipped, "failed": failed}
    logger.info(
        "Batch import complete: total=%d enqueued=%d skipped=%d failed=%d",
        summary["total"],
        summary["enqueued"],
        summary["skipped"],
        summary["failed"],
    )
    return summary


def main(argv: list[str] | None = None) -> None:
    """CLI entry point for batch import."""
    parser = argparse.ArgumentParser(
        description="Import a DiscordChatExporter JSON file into seed-storage"
    )
    parser.add_argument("file", help="Path to the DiscordChatExporter JSON export file")
    parser.add_argument(
        "--offset", type=int, default=0, metavar="N", help="Skip the first N messages"
    )
    args = parser.parse_args(argv)

    summary = import_file(args.file, offset=args.offset)
    print(
        f"Done. total={summary['total']} enqueued={summary['enqueued']} "
        f"skipped={summary['skipped']} failed={summary['failed']}"
    )


if __name__ == "__main__":
    main()
