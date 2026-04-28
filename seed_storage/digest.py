"""seed_storage/digest.py — Daily knowledge base digest posted to Discord.

Queries seed_staging for items loaded in the last 24 hours, groups by
source type, and posts a summary message to the #seed-storage channel.

Called by the ``post_daily_digest`` Celery beat task.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import UTC, datetime
from urllib.parse import urlparse

import httpx

from seed_storage import staging
from seed_storage.config import settings

logger = logging.getLogger(__name__)

DISCORD_API = "https://discord.com/api/v10"

DIGEST_CHANNELS = [
    "1487354063749382234",  # #seed-storage
]

TYPE_EMOJI = {
    "web": "\U0001f310",
    "instagram": "\U0001f4f9",
    "instagram_image": "\U0001f5bc",
    "youtube": "\U0001f3ac",
    "github": "\U0001f4e6",
    "x_twitter": "\U0001d54f",
    "plain_text": "\U0001f4ac",
    "conversation_thread": "\U0001f4ac",
    "discord_link": "\U0001f517",
    "media_link": "\U0001f3b5",
    "discord": "\U0001f4ac",
}


def _short_url(uri: str, max_len: int = 50) -> str:
    """Shorten a URL for display."""
    if uri.startswith("discord://") or uri.startswith("thread://"):
        return uri.split("/")[-1][:12] + "..."
    parsed = urlparse(uri)
    display = parsed.netloc + parsed.path
    return display[:max_len] if len(display) <= max_len else display[: max_len - 3] + "..."


def build_digest(items: list[dict]) -> str:
    """Build the digest message text from loaded staging items."""
    if not items:
        return ""

    today = datetime.now(UTC).strftime("%b %d, %Y")

    by_type: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        by_type[item.get("source_type", "unknown")].append(item)

    all_tags: dict[str, int] = defaultdict(int)
    for item in items:
        meta = item.get("metadata") or {}
        for tag in meta.get("tags", []):
            all_tags[tag] += 1
    top_tags = sorted(all_tags, key=all_tags.get, reverse=True)[:5]

    sections = []
    for stype, sitems in sorted(by_type.items(), key=lambda x: -len(x[1])):
        emoji = TYPE_EMOJI.get(stype, "\U0001f4c4")
        label = stype.replace("_", " ").title()
        lines = []
        for item in sitems[:3]:
            meta = item.get("metadata") or {}
            summary = meta.get("summary", "")
            short = _short_url(item.get("source_uri", ""))
            if summary:
                lines.append(f"  \u2022 {short} \u2014 {summary[:80]}")
            else:
                lines.append(f"  \u2022 {short}")
        if len(sitems) > 3:
            lines.append(f"  + {len(sitems) - 3} more")
        sections.append(f"{emoji} **{label}** ({len(sitems)})\n" + "\n".join(lines))

    body = "\n\n".join(sections)
    tags_line = ", ".join(top_tags) if top_tags else "none"

    msg = (
        f"\U0001f4ca **Knowledge Base Daily Digest** \u2014 {today}\n\n"
        f"Loaded **{len(items)}** items into the knowledge graph:\n\n"
        f"{body}\n\n"
        f"\U0001f3f7\ufe0f Top tags: {tags_line}"
    )
    if len(msg) > 1950:
        msg = msg[:1950] + "\n..."
    return msg


def post_digest(hours: int = 24) -> int:
    """Build and post the daily digest to Discord. Returns count of items.

    Uses sync httpx (Celery task convention). Reads the Discord bot token
    from ``settings.DISCORD_BOT_TOKEN`` (file-mode credential, resolved at
    startup by config.py).
    """
    items = staging.get_recently_loaded(hours=hours)

    if not items:
        logger.info("No items loaded in the last %d hours -- skipping digest", hours)
        return 0

    message = build_digest(items)
    logger.info("Digest: %d items, posting to %d channels", len(items), len(DIGEST_CHANNELS))

    token = settings.DISCORD_BOT_TOKEN
    if not token:
        logger.error("DISCORD_BOT_TOKEN not available -- can't post digest")
        return len(items)

    with httpx.Client(timeout=10.0) as client:
        for channel_id in DIGEST_CHANNELS:
            try:
                resp = client.post(
                    f"{DISCORD_API}/channels/{channel_id}/messages",
                    headers={"Authorization": f"Bot {token}"},
                    json={"content": message},
                )
                resp.raise_for_status()
                logger.info("Posted digest to channel %s", channel_id)
            except Exception:
                logger.exception("Failed to post digest to channel %s", channel_id)

    return len(items)
