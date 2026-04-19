"""Conversation threading — group message fragments into coherent threads.

Groups consecutive messages from the same channel within a time window
into single conversation threads. Dramatically improves Graphiti entity
extraction by giving it full conversation context instead of fragments.

Run as: python -m ingestion.threader
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

from seed_storage import staging

log = logging.getLogger("threader")

# Channels whose messages should be threaded.
THREADABLE_CHANNELS = {"imessages", "granola-flynn", "granola-wyler"}

# iMessage timestamps embedded in text: [Apr 01, 2026  15:22]
_IMSG_TS_RE = re.compile(r"\[(\w+ \d{2}, \d{4}\s+\d{2}:\d{2})\]")

# Speakers embedded in text: **Flynn A. Cruse**:
_SPEAKER_RE = re.compile(r"\*\*(.+?)\*\*:")


def _parse_embedded_time(text: str) -> datetime | None:
    """Extract the iMessage/Granola timestamp from message text."""
    m = _IMSG_TS_RE.search(text)
    if m:
        try:
            return datetime.strptime(m.group(1).strip(), "%b %d, %Y %H:%M").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def _get_time(item: dict) -> datetime:
    """Best-effort timestamp: embedded in text > created_at > staged_at."""
    embedded = _parse_embedded_time(item.get("raw_content", ""))
    if embedded:
        return embedded
    for field in ("created_at", "staged_at"):
        val = item.get(field)
        if val:
            if isinstance(val, datetime):
                return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
            try:
                return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass
    return datetime.now(timezone.utc)


def _extract_speakers(text: str) -> list[str]:
    """Extract unique speaker names from message text."""
    return list(dict.fromkeys(_SPEAKER_RE.findall(text)))


def group_into_threads(items: list[dict], gap_minutes: int = 5) -> list[list[dict]]:
    """Group consecutive messages by channel within a time gap."""
    sorted_items = sorted(items, key=lambda i: (i.get("channel", ""), _get_time(i)))
    threads: list[list[dict]] = []
    current: list[dict] = []

    for item in sorted_items:
        if current:
            prev_channel = current[-1].get("channel", "")
            curr_channel = item.get("channel", "")
            gap = (_get_time(item) - _get_time(current[-1])).total_seconds() / 60
            if curr_channel != prev_channel or gap > gap_minutes:
                threads.append(current)
                current = []
        current.append(item)

    if current:
        threads.append(current)
    return threads


def thread_conversations(gap_minutes: int = 5):
    """Thread all enriched plain_text messages from threadable channels."""
    items = staging.get_staged(status="enriched", limit=5000)
    threadable = [
        i for i in items
        if i.get("source_type") == "plain_text"
        and i.get("channel", "") in THREADABLE_CHANNELS
    ]

    if not threadable:
        log.info("No threadable messages found")
        return

    log.info("Threading %d messages from %s", len(threadable), THREADABLE_CHANNELS)
    threads = group_into_threads(threadable, gap_minutes)

    created = 0
    threaded_ids = []

    for thread in threads:
        if len(thread) < 2:
            # Single messages stay as-is, don't thread.
            continue

        channel = thread[0].get("channel", "")
        first_id = str(thread[0]["id"])
        last_id = str(thread[-1]["id"])

        # Concatenate all messages in the thread.
        combined = "\n".join(m.get("raw_content", "") for m in thread)

        # Extract all unique speakers from the thread.
        all_speakers = []
        for m in thread:
            all_speakers.extend(_extract_speakers(m.get("raw_content", "")))
        unique_speakers = list(dict.fromkeys(all_speakers))

        # Collect fragment IDs and metadata.
        fragment_ids = [str(m["id"]) for m in thread]
        first_time = _get_time(thread[0])

        thread_uri = f"thread://{channel}/{first_id[:8]}-{last_id[:8]}"

        sid = staging.stage(
            source_type="conversation_thread",
            source_uri=thread_uri,
            raw_content=combined,
            author=", ".join(unique_speakers) if unique_speakers else thread[0].get("author", ""),
            channel=channel,
            created_at=first_time.isoformat(),
            metadata={
                "thread_size": len(thread),
                "fragment_ids": fragment_ids,
                "speakers": [{"name": s, "role": "speaker", "platform": "discord"} for s in unique_speakers],
                "channel": channel,
            },
        )

        if sid:
            created += 1
            threaded_ids.extend(fragment_ids)

    # Mark original fragments so they don't get loaded individually.
    if threaded_ids:
        staging.update_status(threaded_ids, "threaded")

    non_threaded = len(threadable) - len(threaded_ids)
    log.info(
        "Created %d conversation threads from %d fragments (%d single messages left as-is)",
        created, len(threaded_ids), non_threaded,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    thread_conversations()
